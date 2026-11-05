package io.nexusdb.core.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void roundTripEveryRecordTypeAndVerifyCrc() {
        Path walFile = tempDir.resolve("roundtrip.wal");

        Map<Integer, Long> dpt = new LinkedHashMap<>();
        dpt.put(2, 10L);
        dpt.put(5, 14L);
        dpt.put(9, 21L);

        Map<Long, Long> att = new LinkedHashMap<>();
        att.put(101L, 19L);
        att.put(102L, 25L);

        byte[] beforeBytes = "old-tuple-payload-123".getBytes(StandardCharsets.UTF_8);
        byte[] afterBytes = "new-tuple-payload-456789".getBytes(StandardCharsets.UTF_8);

        List<LogRecord> expected = new ArrayList<>();
        try (LogManager lm = LogManager.open(walFile)) {
            expected.add(lm.appendRecord(LogRecord.begin(0L, 101L)));
            expected.add(lm.appendRecord(LogRecord.insert(0L, 101L, 1L, "accounts", 3, 0, afterBytes)));
            expected.add(lm.appendRecord(LogRecord.update(0L, 101L, 2L, "accounts", 3, 0, beforeBytes, afterBytes)));
            expected.add(lm.appendRecord(LogRecord.delete(0L, 101L, 3L, "accounts", 3, 0, afterBytes)));
            expected.add(lm.appendRecord(LogRecord.clr(0L, 101L, 4L, "accounts", 3, 0, beforeBytes, 2L)));
            expected.add(lm.appendRecord(LogRecord.pageWrite(0L, 101L, 5L, 7, beforeBytes, afterBytes)));
            expected.add(lm.appendRecord(LogRecord.commit(0L, 101L, 6L)));
            expected.add(lm.appendRecord(LogRecord.begin(0L, 102L)));
            expected.add(lm.appendRecord(LogRecord.abort(0L, 102L, 8L)));
            expected.add(lm.appendRecord(LogRecord.checkpointBegin(0L)));
            expected.add(lm.appendRecord(LogRecord.checkpointEnd(0L, dpt, att)));

            lm.flush();
            assertEquals(11L, lm.getFlushedLsn());
        }

        List<LogRecord> actual = LogReader.readAll(walFile);
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            LogRecord exp = expected.get(i);
            LogRecord act = actual.get(i);
            assertEquals(exp, act);
            assertEquals(i + 1L, act.lsn());
            assertArrayEquals(exp.beforeImage(), act.beforeImage());
            assertArrayEquals(exp.afterImage(), act.afterImage());
        }

        LogRecord ckptEnd = actual.get(actual.size() - 1);
        assertEquals(LogRecordType.CHECKPOINT_END, ckptEnd.type());
        assertEquals(dpt, ckptEnd.dirtyPageTable());
        assertEquals(att, ckptEnd.activeTransactionTable());

        // Flip a bit in a serialized record and verify LogRecord.fromBytes rejects the bad CRC32
        byte[] raw = expected.get(2).toBytes();
        raw[raw.length - 1] ^= 0x01;
        assertThrows(IllegalArgumentException.class, () -> LogRecord.fromBytes(raw));
    }

    @Test
    void tornWriteAndCorruptTailAreCleanlyTruncated() throws IOException {
        Path walFile = tempDir.resolve("torn.wal");

        try (LogManager lm = LogManager.open(walFile)) {
            lm.appendBegin(1L);
            lm.appendInsert(1L, 1L, "users", 2, 0, new byte[]{1, 2, 3, 4});
            lm.appendCommit(1L, 2L);
            lm.flush();
        }

        long validSize = Files.size(walFile);
        LogRecord uncommittedTail = LogRecord.insert(4L, 2L, 0L, "users", 2, 1, new byte[]{9, 9, 9, 9, 9, 9, 9, 9});
        byte[] fullRecordBytes = uncommittedTail.toBytes();

        // Case 1: Append only half of the record (torn write)
        try (FileChannel ch = FileChannel.open(walFile, StandardOpenOption.WRITE)) {
            ch.position(validSize);
            ch.write(ByteBuffer.wrap(fullRecordBytes, 0, fullRecordBytes.length / 2));
        }
        assertTrue(Files.size(walFile) > validSize);

        try (LogReader reader = new LogReader(walFile, true)) {
            List<LogRecord> recovered = new ArrayList<>();
            for (LogRecord r : reader) {
                recovered.add(r);
            }
            assertEquals(3, recovered.size());
            assertTrue(reader.isCorruptedTailDetected());
            assertEquals(validSize, reader.getValidEndOffset());
        }
        assertEquals(validSize, Files.size(walFile));

        // Case 2: Append full record with a corrupted payload byte (CRC mismatch) and reopen LogManager
        byte[] corruptedRecord = fullRecordBytes.clone();
        corruptedRecord[corruptedRecord.length - 2] ^= (byte) 0xFF;
        try (FileChannel ch = FileChannel.open(walFile, StandardOpenOption.WRITE)) {
            ch.position(validSize);
            ch.write(ByteBuffer.wrap(corruptedRecord));
        }

        try (LogManager lm = LogManager.open(walFile)) {
            assertEquals(3L, lm.getFlushedLsn());
            long nextLsn = lm.appendBegin(2L);
            assertEquals(4L, nextLsn);
            lm.flush();
        }

        List<LogRecord> afterRepair = LogReader.readAll(walFile);
        assertEquals(4, afterRepair.size());
        assertEquals(4L, afterRepair.get(3).lsn());
        assertEquals(LogRecordType.BEGIN, afterRepair.get(3).type());
    }

    @Test
    void concurrentAppendersReceiveUniqueMonotonicLsns() throws Exception {
        Path walFile = tempDir.resolve("concurrent.wal");
        int threads = 16;
        int recordsPerThread = 100;
        int totalRecords = threads * recordsPerThread;

        Set<Long> assignedLsns = ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);

        try (LogManager lm = LogManager.open(walFile, 4096)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final long txnId = t + 1L;
                futures.add(pool.submit(() -> {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    long prev = 0L;
                    for (int i = 0; i < recordsPerThread; i++) {
                        byte[] payload = ("row-" + txnId + "-" + i).getBytes(StandardCharsets.UTF_8);
                        long lsn = lm.appendInsert(txnId, prev, "items", (int) txnId, i, payload);
                        assertTrue(assignedLsns.add(lsn), "Duplicate LSN assigned: " + lsn);
                        prev = lsn;
                    }
                }));
            }

            startLatch.countDown();
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
            lm.flush();
            assertEquals(totalRecords, lm.getFlushedLsn());
        } finally {
            pool.shutdownNow();
        }

        assertEquals(totalRecords, assignedLsns.size());
        List<Long> sorted = new ArrayList<>(assignedLsns);
        Collections.sort(sorted);
        assertEquals(1L, sorted.get(0));
        assertEquals((long) totalRecords, sorted.get(sorted.size() - 1));

        List<LogRecord> diskRecords = LogReader.readAll(walFile);
        assertEquals(totalRecords, diskRecords.size());
        for (int i = 0; i < totalRecords; i++) {
            assertEquals(i + 1L, diskRecords.get(i).lsn());
        }
    }

    @Test
    void groupCommitFlushUpToSkipsRedundantFsyncs() {
        Path walFile = tempDir.resolve("group_commit.wal");
        try (LogManager lm = LogManager.open(walFile)) {
            assertEquals(0L, lm.getFlushedLsn());
            assertEquals(0, lm.getForceCount());

            long lsn1 = lm.appendBegin(1L);
            long lsn2 = lm.appendInsert(1L, lsn1, "t1", 2, 0, new byte[]{10, 20});
            long lsn3 = lm.appendCommit(1L, lsn2);

            assertFalse(lm.getFlushedLsn() >= lsn3);

            // Flushing up to lsn3 flushes the entire buffer (lsn1..lsn3) in a single force() call
            lm.flushUpTo(lsn3);
            assertEquals(lsn3, lm.getFlushedLsn());
            assertEquals(1, lm.getForceCount());

            // Subsequent flushUpTo calls for already-flushed LSNs must not invoke FileChannel.force
            lm.flushUpTo(lsn1);
            lm.flushUpTo(lsn2);
            lm.flushUpTo(lsn3);
            assertEquals(1, lm.getForceCount());

            // Append two more records and flushUpTo(lsn4); buffer drains up to lsn5 in one force()
            long lsn4 = lm.appendBegin(2L);
            long lsn5 = lm.appendCommit(2L, lsn4);
            lm.flushUpTo(lsn4);
            assertEquals(lsn5, lm.getFlushedLsn());
            assertEquals(2, lm.getForceCount());

            // Now flushUpTo(lsn5) is already satisfied by the batch drain!
            lm.flushUpTo(lsn5);
            assertEquals(2, lm.getForceCount());
        }
    }
}
