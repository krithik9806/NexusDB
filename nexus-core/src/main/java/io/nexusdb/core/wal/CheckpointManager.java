package io.nexusdb.core.wal;

import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.storage.DiskManager;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Coordinates ARIES-lite fuzzy checkpoints without halting concurrent transactions or forcing
 * all dirty buffer pool pages to disk synchronously.
 *
 * <p>Fuzzy Checkpoint Protocol:
 * <ol>
 *   <li>Appends {@link LogRecordType#CHECKPOINT_BEGIN} to the WAL (`beginLsn`).</li>
 *   <li>Snapshots the current Dirty Page Table ({@code pageId -> recLsn}) from {@link BufferPoolManager}
 *       and the current Active Transaction Table ({@code txnId -> lastLsn}).</li>
 *   <li>Appends {@link LogRecordType#CHECKPOINT_END} carrying the DPT and ATT snapshots (`endLsn`).</li>
 *   <li>Forces the WAL up to {@code endLsn} via {@link LogManager#flushUpTo(long)}.</li>
 *   <li>Writes {@code beginLsn} to Page 0's {@code checkpointLsn} field in {@link DiskManager} and fsyncs Page 0.</li>
 * </ol>
 */
public final class CheckpointManager {
    private final LogManager logManager;
    private final BufferPoolManager bufferPoolManager;
    private final DiskManager diskManager;
    private volatile Supplier<Map<Long, Long>> activeTransactionTableSupplier;
    private final Object checkpointLock = new Object();

    public record CheckpointResult(
            long beginLsn,
            long endLsn,
            Map<Integer, Long> dirtyPageTable,
            Map<Long, Long> activeTransactionTable) {
        public CheckpointResult {
            dirtyPageTable = Collections.unmodifiableMap(new LinkedHashMap<>(dirtyPageTable));
            activeTransactionTable = Collections.unmodifiableMap(new LinkedHashMap<>(activeTransactionTable));
        }
    }

    public CheckpointManager(
            LogManager logManager,
            BufferPoolManager bufferPoolManager,
            DiskManager diskManager) {
        this(logManager, bufferPoolManager, diskManager, Map::of);
    }

    public CheckpointManager(
            LogManager logManager,
            BufferPoolManager bufferPoolManager,
            DiskManager diskManager,
            Supplier<Map<Long, Long>> activeTransactionTableSupplier) {
        this.logManager = Objects.requireNonNull(logManager, "logManager must not be null");
        this.bufferPoolManager = Objects.requireNonNull(bufferPoolManager, "bufferPoolManager must not be null");
        this.diskManager = Objects.requireNonNull(diskManager, "diskManager must not be null");
        this.activeTransactionTableSupplier = Objects.requireNonNull(
                activeTransactionTableSupplier, "activeTransactionTableSupplier must not be null");
    }

    public void setActiveTransactionTableSupplier(Supplier<Map<Long, Long>> supplier) {
        this.activeTransactionTableSupplier = Objects.requireNonNull(supplier, "supplier must not be null");
    }

    /**
     * Executes a fuzzy checkpoint using the registered active transaction table supplier.
     */
    public CheckpointResult checkpoint() {
        return checkpoint(activeTransactionTableSupplier.get());
    }

    /**
     * Executes a fuzzy checkpoint with the provided active transaction table snapshot.
     */
    public CheckpointResult checkpoint(Map<Long, Long> activeTransactions) {
        Map<Long, Long> attSnapshot = activeTransactions == null
                ? Map.of()
                : new LinkedHashMap<>(activeTransactions);
        synchronized (checkpointLock) {
            long beginLsn = logManager.appendCheckpointBegin();
            Map<Integer, Long> dptSnapshot = bufferPoolManager.getDirtyPageTable();
            long endLsn = logManager.appendCheckpointEnd(dptSnapshot, attSnapshot);
            logManager.flushUpTo(endLsn);
            diskManager.setCheckpointLsn(beginLsn);
            diskManager.fsync();
            return new CheckpointResult(beginLsn, endLsn, dptSnapshot, attSnapshot);
        }
    }
}
