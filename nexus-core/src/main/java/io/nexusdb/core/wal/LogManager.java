package io.nexusdb.core.wal;

import io.nexusdb.core.storage.StorageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the sequential Write-Ahead Log (WAL) file, monotonic LSN assignment, in-memory log
 * buffering, and group-commit-friendly {@link #flushUpTo(long)} synchronization.
 *
 * <p>Concurrency & Crash-Safety Invariants:
 * <ul>
 *   <li><b>Monotonic LSN Assignment</b>: Every appended record receives a strictly increasing
 *       {@code lsn >= 1} while holding {@code bufferLock}, guaranteeing total log ordering across
 *       concurrent transactions.</li>
 *   <li><b>Group-Commit Optimization ({@link #flushUpTo(long)})</b>: Multiple threads calling
 *       {@code flushUpTo(targetLsn)} check {@code flushedLsn >= targetLsn} before and after acquiring
 *       {@code flushLock}. A single buffer drain + {@code FileChannel.force(false)} advances
 *       {@code flushedLsn} to {@code lastBufferedLsn}, satisfying all concurrent waiters whose
 *       {@code targetLsn <= flushedLsn} without redundant {@code fsync} calls.</li>
 *   <li><b>Startup Tail Repair</b>: On open, existing WAL records are scanned via {@link LogReader}
 *       with {@code truncateCorruptTail = true}, truncating any partial torn write at the end of the
 *       WAL file and initializing {@code nextLsn} to {@code maxRecoveredLsn + 1}.</li>
 * </ul>
 */
public final class LogManager implements AutoCloseable {
    public static final int DEFAULT_LOG_BUFFER_SIZE = 64 * 1024; // 64 KB

    private final Path walPath;
    private final FileChannel channel;
    private ByteBuffer logBuffer;
    private long fileWritePosition;
    private long nextLsn;
    private long lastBufferedLsn;
    private volatile long flushedLsn;
    private volatile boolean closed;

    private final Object bufferLock = new Object();
    private final Object flushLock = new Object();
    private final AtomicInteger forceCount = new AtomicInteger();

    public LogManager(Path walPath) {
        this(walPath, DEFAULT_LOG_BUFFER_SIZE);
    }

    public LogManager(Path walPath, int bufferCapacity) {
        this.walPath = Objects.requireNonNull(walPath, "walPath must not be null");
        if (bufferCapacity < 1024) {
            throw new IllegalArgumentException("bufferCapacity must be >= 1024, got: " + bufferCapacity);
        }
        try {
            if (walPath.getParent() != null) {
                Files.createDirectories(walPath.getParent());
            }
            long maxExistingLsn = 0L;
            if (Files.exists(walPath) && Files.size(walPath) > 0) {
                List<LogRecord> existing = LogReader.readAll(walPath, true);
                for (LogRecord rec : existing) {
                    if (rec.lsn() > maxExistingLsn) {
                        maxExistingLsn = rec.lsn();
                    }
                }
            }
            this.channel = FileChannel.open(
                    walPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            this.fileWritePosition = channel.size();
            this.logBuffer = ByteBuffer.allocate(bufferCapacity).order(ByteOrder.BIG_ENDIAN);
            this.flushedLsn = maxExistingLsn;
            this.lastBufferedLsn = maxExistingLsn;
            this.nextLsn = maxExistingLsn + 1L;
        } catch (IOException e) {
            throw new StorageException("Failed to open WAL file: " + walPath, e);
        }
    }

    public static LogManager open(Path walPath) {
        return new LogManager(walPath, DEFAULT_LOG_BUFFER_SIZE);
    }

    public static LogManager open(Path walPath, int bufferCapacity) {
        return new LogManager(walPath, bufferCapacity);
    }

    /**
     * Assigns the next strictly increasing LSN to {@code record}, serializes it into the in-memory
     * log buffer (draining the buffer to the OS channel if full), and returns the assigned {@code lsn}.
     */
    public long append(LogRecord record) {
        return appendRecord(record).lsn();
    }

    /**
     * Assigns the next strictly increasing LSN to {@code record}, appends it to the in-memory
     * log buffer, and returns the resulting {@link LogRecord} with its assigned LSN.
     */
    public LogRecord appendRecord(LogRecord record) {
        Objects.requireNonNull(record, "record must not be null");
        synchronized (bufferLock) {
            ensureOpenLocked();
            long assignedLsn = nextLsn++;
            LogRecord stamped = record.withLsn(assignedLsn);
            byte[] encoded = stamped.toBytes();

            if (encoded.length > logBuffer.remaining()) {
                drainBufferToChannelLocked();
                if (encoded.length > logBuffer.capacity()) {
                    writeBytesToChannelLocked(ByteBuffer.wrap(encoded));
                    lastBufferedLsn = assignedLsn;
                    return stamped;
                }
            }

            logBuffer.put(encoded);
            lastBufferedLsn = assignedLsn;
            return stamped;
        }
    }

    public long appendBegin(long txnId) {
        return append(LogRecord.begin(0L, txnId));
    }

    public long appendCommit(long txnId, long prevLsn) {
        return append(LogRecord.commit(0L, txnId, prevLsn));
    }

    public long appendAbort(long txnId, long prevLsn) {
        return append(LogRecord.abort(0L, txnId, prevLsn));
    }

    public long appendInsert(long txnId, long prevLsn, String tableName, int pageId, int slotId, byte[] afterImage) {
        return append(LogRecord.insert(0L, txnId, prevLsn, tableName, pageId, slotId, afterImage));
    }

    public long appendDelete(long txnId, long prevLsn, String tableName, int pageId, int slotId, byte[] beforeImage) {
        return append(LogRecord.delete(0L, txnId, prevLsn, tableName, pageId, slotId, beforeImage));
    }

    public long appendUpdate(
            long txnId,
            long prevLsn,
            String tableName,
            int pageId,
            int slotId,
            byte[] beforeImage,
            byte[] afterImage) {
        return append(LogRecord.update(0L, txnId, prevLsn, tableName, pageId, slotId, beforeImage, afterImage));
    }

    public long appendPageWrite(long txnId, long prevLsn, int pageId, byte[] beforeImage, byte[] afterImage) {
        return append(LogRecord.pageWrite(0L, txnId, prevLsn, pageId, beforeImage, afterImage));
    }

    public long appendClr(
            long txnId,
            long prevLsn,
            String tableName,
            int pageId,
            int slotId,
            byte[] afterImage,
            long undoNextLsn) {
        return append(LogRecord.clr(0L, txnId, prevLsn, tableName, pageId, slotId, afterImage, undoNextLsn));
    }

    public long appendCheckpointBegin() {
        return append(LogRecord.checkpointBegin(0L));
    }

    public long appendCheckpointEnd(Map<Integer, Long> dirtyPageTable, Map<Long, Long> activeTransactionTable) {
        return append(LogRecord.checkpointEnd(0L, dirtyPageTable, activeTransactionTable));
    }

    /**
     * Flushes all currently buffered WAL records to disk and forces an {@code fsync}.
     */
    public void flush() {
        long target;
        synchronized (bufferLock) {
            ensureOpenLocked();
            target = lastBufferedLsn;
        }
        flushUpTo(target);
    }

    /**
     * Ensures all WAL records with {@code LSN <= targetLsn} are written and forced to disk.
     * If {@code flushedLsn >= targetLsn} is already satisfied, returns immediately without
     * calling {@link FileChannel#force(boolean)}.
     */
    public void flushUpTo(long targetLsn) {
        if (targetLsn <= 0L || flushedLsn >= targetLsn) {
            return;
        }
        synchronized (flushLock) {
            if (flushedLsn >= targetLsn) {
                return;
            }
            long lsnToFlush;
            synchronized (bufferLock) {
                ensureOpenLocked();
                drainBufferToChannelLocked();
                lsnToFlush = lastBufferedLsn;
            }
            if (flushedLsn < lsnToFlush) {
                try {
                    channel.force(false);
                    forceCount.incrementAndGet();
                    flushedLsn = lsnToFlush;
                } catch (IOException e) {
                    throw new StorageException("Failed to fsync WAL file up to LSN " + lsnToFlush, e);
                }
            }
        }
    }

    private void drainBufferToChannelLocked() {
        if (logBuffer.position() == 0) {
            return;
        }
        logBuffer.flip();
        writeBytesToChannelLocked(logBuffer);
        logBuffer.clear();
    }

    private void writeBytesToChannelLocked(ByteBuffer src) {
        try {
            while (src.hasRemaining()) {
                int written = channel.write(src, fileWritePosition);
                fileWritePosition += written;
            }
        } catch (IOException e) {
            throw new StorageException("Failed to write WAL buffer to disk: " + walPath, e);
        }
    }

    /**
     * Reads all persisted and buffered records in the WAL (flushing any buffered records first).
     */
    public List<LogRecord> readAll() {
        flush();
        return LogReader.readAll(walPath, false);
    }

    public long getFlushedLsn() {
        return flushedLsn;
    }

    public long getLastBufferedLsn() {
        synchronized (bufferLock) {
            return lastBufferedLsn;
        }
    }

    public long getNextLsn() {
        synchronized (bufferLock) {
            return nextLsn;
        }
    }

    public Path getWalPath() {
        return walPath;
    }

    public int getForceCount() {
        return forceCount.get();
    }

    private void ensureOpenLocked() {
        if (closed) {
            throw new StorageException("LogManager is closed");
        }
    }

    @Override
    public void close() {
        synchronized (flushLock) {
            synchronized (bufferLock) {
                if (closed) {
                    return;
                }
                drainBufferToChannelLocked();
                if (flushedLsn < lastBufferedLsn) {
                    try {
                        channel.force(false);
                        forceCount.incrementAndGet();
                        flushedLsn = lastBufferedLsn;
                    } catch (IOException e) {
                        throw new StorageException("Failed to fsync WAL file on close", e);
                    }
                }
                closed = true;
                try {
                    channel.close();
                } catch (IOException e) {
                    throw new StorageException("Failed to close WAL file channel", e);
                }
            }
        }
    }
}
