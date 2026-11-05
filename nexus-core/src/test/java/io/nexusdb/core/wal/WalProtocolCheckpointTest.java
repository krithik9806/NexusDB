package io.nexusdb.core.wal;

import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.engine.Database;
import io.nexusdb.core.index.BPlusTree;
import io.nexusdb.core.page.Page;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import io.nexusdb.core.storage.DiskManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WalProtocolCheckpointTest {

    @TempDir
    Path tempDir;

    @Test
    void dirtyDataPageCannotReachDiskBeforeWalIsFlushedToPageLsn() {
        Path dbFile = tempDir.resolve("wal_protocol.db");
        Path walFile = tempDir.resolve("wal_protocol.db.wal");

        try (DiskManager dm = DiskManager.open(dbFile);
             LogManager lm = LogManager.open(walFile);
             BufferPoolManager bpm = new BufferPoolManager(1, dm)) {
            bpm.setLogManager(lm);

            // Allocate and modify a page in a 1-frame buffer pool
            Page page1 = bpm.newPage();
            int pid1 = page1.getPageId();
            long lsn1 = lm.appendInsert(1L, 0L, "accounts", pid1, 0, new byte[]{1, 2, 3, 4});
            bpm.recordPageModification(page1, lsn1);
            bpm.unpinPage(pid1, true);

            // While buffered in memory, WAL has NOT been flushed yet
            assertEquals(0L, lm.getFlushedLsn());
            assertEquals(Map.of(pid1, lsn1), bpm.getDirtyPageTable());

            // Inject a faulty no-op flush hook that violates the Write-Ahead rule
            bpm.setWalFlushHook(ignored -> {
                // Intentionally do not flush LogManager
            });

            // Attempting to flush or evict the dirty page must fail BEFORE reaching DiskManager
            IllegalStateException violation = assertThrows(
                    IllegalStateException.class,
                    () -> bpm.flushPage(pid1));
            assertTrue(violation.getMessage().contains("Write-Ahead Logging protocol violation"));

            // Verify the dirty page did NOT reach DiskManager (disk still has pageLSN == 0)
            byte[] onDiskBeforeFlush = dm.readPage(pid1);
            long diskLsnBefore = ByteBuffer.wrap(onDiskBeforeFlush).order(ByteOrder.BIG_ENDIAN).getLong(Page.LSN_OFFSET);
            assertEquals(0L, diskLsnBefore);
            assertTrue(bpm.isDirty(pid1));

            // Restore the real WAL flush hook; now eviction by allocating page 2 flushes WAL first
            bpm.setLogManager(lm);
            Page page2 = bpm.newPage();
            int pid2 = page2.getPageId();
            bpm.unpinPage(pid2, true);

            assertTrue(lm.getFlushedLsn() >= lsn1);
            byte[] onDiskAfterEviction = dm.readPage(pid1);
            long diskLsnAfter = ByteBuffer.wrap(onDiskAfterEviction).order(ByteOrder.BIG_ENDIAN).getLong(Page.LSN_OFFSET);
            assertEquals(lsn1, diskLsnAfter);
            assertFalse(bpm.getDirtyPageTable().containsKey(pid1));
        }
    }

    @Test
    void dirtyPageTableTracksFirstDirtyingRecLsnAcrossMultipleMutations() {
        Path dbFile = tempDir.resolve("dpt_tracking.db");
        Path walFile = tempDir.resolve("dpt_tracking.db.wal");

        Schema schema = new Schema(List.of(
                Column.primaryKey("id", DataType.INT),
                new Column("name", DataType.VARCHAR, 64, false, false)
        ));

        try (DiskManager dm = DiskManager.open(dbFile);
             LogManager lm = LogManager.open(walFile);
             BufferPoolManager bpm = new BufferPoolManager(16, dm)) {
            bpm.setLogManager(lm);

            TableHeap heap = TableHeap.create(bpm, schema, "users");
            int heapPageId = heap.getFirstPageId();

            // Flush initial empty heap page so it starts clean
            bpm.flushPage(heapPageId);
            assertFalse(bpm.getDirtyPageTable().containsKey(heapPageId));

            // First insert dirties heapPageId at recLsn
            RecordId rid1 = heap.insert(new Tuple(Value.ofInt(1), Value.ofVarchar("alice")));
            Page p = bpm.fetchPage(heapPageId);
            long firstMutationLsn = p.getPageLsn();
            bpm.unpinPage(heapPageId, false);
            assertTrue(firstMutationLsn > 0L);
            assertEquals(firstMutationLsn, bpm.getDirtyPageTable().get(heapPageId));

            // Second insert and update advance pageLsn, but DPT recLsn must remain firstMutationLsn
            RecordId rid2 = heap.insert(new Tuple(Value.ofInt(2), Value.ofVarchar("bob")));
            heap.update(rid1, new Tuple(Value.ofInt(1), Value.ofVarchar("alice-updated")));
            heap.delete(rid2);

            Page pAfter = bpm.fetchPage(heapPageId);
            long latestPageLsn = pAfter.getPageLsn();
            bpm.unpinPage(heapPageId, false);

            assertTrue(latestPageLsn > firstMutationLsn);
            assertEquals(firstMutationLsn, bpm.getDirtyPageTable().get(heapPageId));

            // Also verify B+Tree page modifications stamp pageLsn and register in DPT
            BPlusTree tree = BPlusTree.create(bpm, DataType.INT, true);
            int rootPid = tree.getRootPageId();
            bpm.flushPage(rootPid);
            assertFalse(bpm.getDirtyPageTable().containsKey(rootPid));

            tree.insert(Value.ofInt(1), rid1);
            Long treeRecLsn = bpm.getDirtyPageTable().get(rootPid);
            assertNotNull(treeRecLsn);
            assertTrue(treeRecLsn > latestPageLsn);

            // Flushing heapPageId clears it from DPT; a subsequent mutation records a new recLsn
            bpm.flushPage(heapPageId);
            assertFalse(bpm.getDirtyPageTable().containsKey(heapPageId));

            heap.insert(new Tuple(Value.ofInt(3), Value.ofVarchar("charlie")));
            Long newRecLsn = bpm.getDirtyPageTable().get(heapPageId);
            assertNotNull(newRecLsn);
            assertTrue(newRecLsn > treeRecLsn);
        }
    }

    @Test
    void fuzzyCheckpointRecordsDptAndAttAndUpdatesDiskHeaderCheckpointLsn() {
        Path dbFile = tempDir.resolve("checkpoint_test.db");

        try (Database db = Database.open(dbFile, 32)) {
            db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT NOT NULL)");
            // Flush pages from DDL so only subsequent DML pages are in the DPT
            db.getBufferPoolManager().flushAll();
            assertTrue(db.getBufferPoolManager().getDirtyPageTable().isEmpty());

            db.execute("INSERT INTO accounts VALUES (1, 1000), (2, 2000)");
            Map<Integer, Long> expectedDpt = db.getBufferPoolManager().getDirtyPageTable();
            assertFalse(expectedDpt.isEmpty());

            Map<Long, Long> simulatedAtt = Map.of(10L, 4L, 11L, 7L);
            CheckpointManager.CheckpointResult ckpt = db.checkpoint(simulatedAtt);

            assertTrue(ckpt.beginLsn() > 0L);
            assertTrue(ckpt.endLsn() > ckpt.beginLsn());
            assertEquals(expectedDpt, ckpt.dirtyPageTable());
            assertEquals(simulatedAtt, ckpt.activeTransactionTable());

            // WAL is flushed up to endLsn, and DiskManager Page 0 stores beginLsn
            assertTrue(db.getLogManager().getFlushedLsn() >= ckpt.endLsn());
            assertEquals(ckpt.beginLsn(), db.getDiskManager().getCheckpointLsn());

            // Fuzzy checkpoint must NOT force dirty data pages to disk
            assertEquals(expectedDpt, db.getBufferPoolManager().getDirtyPageTable());

            List<LogRecord> walRecords = LogReader.readAll(db.getLogManager().getWalPath());
            LogRecord endRecord = walRecords.get(walRecords.size() - 1);
            LogRecord beginRecord = walRecords.get(walRecords.size() - 2);
            assertEquals(LogRecordType.CHECKPOINT_BEGIN, beginRecord.type());
            assertEquals(ckpt.beginLsn(), beginRecord.lsn());
            assertEquals(LogRecordType.CHECKPOINT_END, endRecord.type());
            assertEquals(ckpt.endLsn(), endRecord.lsn());
            assertEquals(expectedDpt, endRecord.dirtyPageTable());
            assertEquals(simulatedAtt, endRecord.activeTransactionTable());
        }
    }
}
