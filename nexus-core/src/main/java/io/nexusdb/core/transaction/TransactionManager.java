package io.nexusdb.core.transaction;

import io.nexusdb.core.catalog.Catalog;
import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.index.BPlusTree;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.recovery.CrashFailpoints;
import io.nexusdb.core.wal.LogManager;
import io.nexusdb.core.wal.LogRecord;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinates transaction lifecycles ({@link #begin()}, {@link #commit(Transaction)},
 * {@link #rollback(Transaction)}), active transaction tracking for fuzzy checkpoints, and
 * ARIES-lite undo with Compensation Log Records (CLRs).
 *
 * <p>Concurrency & Crash-Safety Invariants:
 * <ul>
 *   <li><b>Commit Durability (No-Force)</b>: {@link #commit(Transaction)} appends a {@code COMMIT}
 *       record to the WAL and invokes {@link LogManager#flushUpTo(long)} before transitioning the
 *       transaction state to {@link TransactionState#COMMITTED}. Dirty data pages do not need to be
 *       forced to disk at commit time.</li>
 *   <li><b>Rollback & CLR Logging</b>: {@link #rollback(Transaction)} walks the transaction's logged
 *       modifications in reverse LSN order, writing a {@code CLR} ({@code undoNextLsn = rec.prevLsn()})
 *       for each undone {@code INSERT}, {@code DELETE}, or {@code UPDATE}, restoring the physical
 *       {@link TableHeap} slot and synchronizing all affected {@link BPlusTree} indexes before writing
 *       the terminal {@code ABORT} log record.</li>
 * </ul>
 */
public final class TransactionManager implements AutoCloseable {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final LogManager logManager;
    private final LockManager lockManager;
    private volatile Catalog catalog;
    private final AtomicLong nextTxnId = new AtomicLong(1L);
    private final Map<Long, Transaction> activeTransactions = new ConcurrentHashMap<>();
    private final Map<String, TableHeap> standaloneHeaps = new ConcurrentHashMap<>();
    private final Map<String, List<StandaloneIndexBinding>> standaloneIndexes = new ConcurrentHashMap<>();
    private final AtomicLong committedCount = new AtomicLong();
    private final AtomicLong abortedCount = new AtomicLong();

    public record StandaloneIndexBinding(int columnIndex, BPlusTree indexTree) {
        public StandaloneIndexBinding {
            Objects.requireNonNull(indexTree, "indexTree must not be null");
        }
    }

    public TransactionManager(LogManager logManager, Catalog catalog) {
        this(logManager, catalog, new LockManager());
    }

    public TransactionManager(LogManager logManager, Catalog catalog, LockManager lockManager) {
        this.logManager = Objects.requireNonNull(logManager, "logManager must not be null");
        this.catalog = catalog;
        this.lockManager = Objects.requireNonNull(lockManager, "lockManager must not be null");
    }

    public TransactionManager(LogManager logManager) {
        this(logManager, null, new LockManager());
    }

    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    /**
     * Advances the transaction ID generator so newly started transactions after crash recovery
     * never collide with historical transaction IDs in the WAL.
     */
    public void ensureNextTxnIdAtLeast(long minNextTxnId) {
        nextTxnId.updateAndGet(curr -> Math.max(curr, minNextTxnId));
    }

    /**
     * Registers a standalone {@link TableHeap} (and optional {@link BPlusTree} bindings) for tests
     * that exercise {@link TransactionManager} outside {@link Catalog}.
     */
    public void registerStandaloneTable(TableHeap heap, List<StandaloneIndexBinding> indexes) {
        Objects.requireNonNull(heap, "heap must not be null");
        String key = heap.getTableName().toLowerCase(Locale.ROOT);
        standaloneHeaps.put(key, heap);
        if (indexes != null && !indexes.isEmpty()) {
            standaloneIndexes.put(key, List.copyOf(indexes));
        }
    }

    public Transaction begin() {
        return begin(false);
    }

    public Transaction begin(boolean autoCommit) {
        long txnId = nextTxnId.getAndIncrement();
        long beginLsn = logManager.appendBegin(txnId);
        Transaction txn = new Transaction(txnId, beginLsn, autoCommit, lockManager);
        activeTransactions.put(txnId, txn);
        logManager.setThreadTransactionContext(txn);
        return txn;
    }

    /**
     * Starts a lightweight read-only transaction for autocommit {@code SELECT} statements so
     * readers acquire and release Strict 2PL {@link LockMode#SHARED} locks without writing
     * {@code BEGIN}/{@code COMMIT} records to the WAL.
     */
    public Transaction beginReadOnly() {
        long txnId = nextTxnId.getAndIncrement();
        Transaction txn = new Transaction(txnId, 0L, true, lockManager);
        logManager.setThreadTransactionContext(txn);
        return txn;
    }

    public void endReadOnly(Transaction txn) {
        if (txn == null) {
            return;
        }
        try {
            txn.setState(TransactionState.COMMITTED);
        } finally {
            lockManager.releaseAll(txn);
            if (logManager.getThreadTransactionContext() == txn) {
                logManager.clearThreadTransactionContext();
            }
        }
    }

    /**
     * Binds {@code txn} as the active transaction context on the calling thread.
     */
    public void bindToCurrentThread(Transaction txn) {
        logManager.setThreadTransactionContext(txn);
    }

    /**
     * Clears the active transaction context on the calling thread.
     */
    public void unbindFromCurrentThread() {
        logManager.clearThreadTransactionContext();
    }

    /**
     * Commits {@code txn} by appending a {@code COMMIT} WAL record and forcing the WAL up to
     * {@code commitLsn}.
     */
    public void commit(Transaction txn) {
        Objects.requireNonNull(txn, "txn must not be null");
        synchronized (txn) {
            if (txn.getState() != TransactionState.ACTIVE) {
                throw new IllegalStateException("Cannot commit non-active transaction: " + txn);
            }
            try {
                long commitLsn = logManager.appendCommit(txn.getTxnId(), txn.getLastLsn());
                txn.setLastLsn(commitLsn);
                CrashFailpoints.hit(CrashFailpoints.Point.BEFORE_COMMIT_WAL_FLUSH);
                logManager.flushUpTo(commitLsn);
                CrashFailpoints.hit(CrashFailpoints.Point.AFTER_COMMIT_WAL_FLUSH);
                txn.setState(TransactionState.COMMITTED);
                committedCount.incrementAndGet();
            } finally {
                activeTransactions.remove(txn.getTxnId());
                lockManager.releaseAll(txn);
                if (logManager.getThreadTransactionContext() == txn) {
                    logManager.clearThreadTransactionContext();
                }
            }
        }
    }

    /**
     * Rolls back all modifications made by {@code txn} in reverse LSN order, writing {@code CLR}
     * records, restoring heap slots and B+Tree indexes, and appending a final {@code ABORT} record.
     */
    public void rollback(Transaction txn) {
        Objects.requireNonNull(txn, "txn must not be null");
        synchronized (txn) {
            if (txn.getState() != TransactionState.ACTIVE) {
                throw new IllegalStateException("Cannot roll back non-active transaction: " + txn);
            }
            LogManager.TransactionLogContext previousCtx = logManager.getThreadTransactionContext();
            try {
                // Suspend automatic tuple logging while executing physical/logical undo
                logManager.clearThreadTransactionContext();
                List<LogRecord> mods = txn.drainLoggedModificationsFrom(0);
                for (int i = mods.size() - 1; i >= 0; i--) {
                    undoModificationRecord(txn, mods.get(i));
                }
                long abortLsn = logManager.appendAbort(txn.getTxnId(), txn.getLastLsn());
                txn.setLastLsn(abortLsn);
                logManager.flushUpTo(abortLsn);
                txn.setState(TransactionState.ABORTED);
                abortedCount.incrementAndGet();
            } finally {
                activeTransactions.remove(txn.getTxnId());
                lockManager.releaseAll(txn);
                if (previousCtx != null && previousCtx != txn) {
                    logManager.setThreadTransactionContext(previousCtx);
                } else {
                    logManager.clearThreadTransactionContext();
                }
            }
        }
    }

    /**
     * Undoes all modifications logged by {@code txn} since {@code savepointIndex} (used for
     * statement-level atomicity when a multi-row DML statement fails midway with a constraint error),
     * writing {@code CLR} records while keeping {@code txn} {@link TransactionState#ACTIVE}.
     */
    public void rollbackToSavepoint(Transaction txn, int savepointIndex) {
        Objects.requireNonNull(txn, "txn must not be null");
        synchronized (txn) {
            if (txn.getState() != TransactionState.ACTIVE) {
                throw new IllegalStateException("Transaction is not active: " + txn);
            }
            LogManager.TransactionLogContext previousCtx = logManager.getThreadTransactionContext();
            try {
                logManager.clearThreadTransactionContext();
                List<LogRecord> mods = txn.drainLoggedModificationsFrom(savepointIndex);
                for (int i = mods.size() - 1; i >= 0; i--) {
                    undoModificationRecord(txn, mods.get(i));
                }
            } finally {
                logManager.setThreadTransactionContext(previousCtx);
            }
        }
    }

    private void undoModificationRecord(Transaction txn, LogRecord rec) {
        String tableName = rec.tableName();
        TableMetadata tableMeta = (catalog != null && !tableName.isEmpty()) ? catalog.getTable(tableName) : null;
        TableHeap heap = tableMeta != null
                ? tableMeta.getTableHeap()
                : standaloneHeaps.get(tableName.toLowerCase(Locale.ROOT));
        if (heap == null) {
            return;
        }

        RecordId rid = new RecordId(rec.pageId(), rec.slotId());
        switch (rec.type()) {
            case INSERT -> {
                long clrLsn = logManager.appendClr(
                        txn.getTxnId(),
                        txn.getLastLsn(),
                        tableName,
                        rec.pageId(),
                        rec.slotId(),
                        EMPTY_BYTES,
                        rec.prevLsn());
                txn.setLastLsn(clrLsn);
                heap.deleteSlotForUndoOrRedo(rid, clrLsn);

                Tuple insertedTuple = Tuple.fromBytes(heap.getSchema(), rec.afterImage(), rid);
                if (tableMeta != null) {
                    for (IndexMetadata idx : tableMeta.getIndexes()) {
                        int colIdx = tableMeta.getSchema().getColumnIndex(idx.columnName());
                        tableMeta.getIndexTree(idx).delete(insertedTuple.getValue(colIdx), rid);
                    }
                } else {
                    List<StandaloneIndexBinding> bindings = standaloneIndexes.get(tableName.toLowerCase(Locale.ROOT));
                    if (bindings != null) {
                        for (StandaloneIndexBinding b : bindings) {
                            b.indexTree().delete(insertedTuple.getValue(b.columnIndex()), rid);
                        }
                    }
                }
            }
            case DELETE -> {
                byte[] beforeBytes = rec.beforeImage();
                long clrLsn = logManager.appendClr(
                        txn.getTxnId(),
                        txn.getLastLsn(),
                        tableName,
                        rec.pageId(),
                        rec.slotId(),
                        beforeBytes,
                        rec.prevLsn());
                txn.setLastLsn(clrLsn);
                heap.restoreSlotForUndoOrRedo(rid, beforeBytes, clrLsn);

                Tuple restoredTuple = Tuple.fromBytes(heap.getSchema(), beforeBytes, rid);
                if (tableMeta != null) {
                    for (IndexMetadata idx : tableMeta.getIndexes()) {
                        int colIdx = tableMeta.getSchema().getColumnIndex(idx.columnName());
                        BPlusTree tree = tableMeta.getIndexTree(idx);
                        tree.delete(restoredTuple.getValue(colIdx), rid);
                        tree.insert(restoredTuple.getValue(colIdx), rid);
                    }
                } else {
                    List<StandaloneIndexBinding> bindings = standaloneIndexes.get(tableName.toLowerCase(Locale.ROOT));
                    if (bindings != null) {
                        for (StandaloneIndexBinding b : bindings) {
                            b.indexTree().delete(restoredTuple.getValue(b.columnIndex()), rid);
                            b.indexTree().insert(restoredTuple.getValue(b.columnIndex()), rid);
                        }
                    }
                }
            }
            case UPDATE -> {
                if (rec.slotId() < 0) {
                    return;
                }
                byte[] beforeBytes = rec.beforeImage();
                byte[] afterBytes = rec.afterImage();
                long clrLsn = logManager.appendClr(
                        txn.getTxnId(),
                        txn.getLastLsn(),
                        tableName,
                        rec.pageId(),
                        rec.slotId(),
                        beforeBytes,
                        rec.prevLsn());
                txn.setLastLsn(clrLsn);
                heap.restoreSlotForUndoOrRedo(rid, beforeBytes, clrLsn);

                Tuple oldTuple = Tuple.fromBytes(heap.getSchema(), beforeBytes, rid);
                Tuple newTuple = Tuple.fromBytes(heap.getSchema(), afterBytes, rid);
                if (tableMeta != null) {
                    for (IndexMetadata idx : tableMeta.getIndexes()) {
                        int colIdx = tableMeta.getSchema().getColumnIndex(idx.columnName());
                        BPlusTree tree = tableMeta.getIndexTree(idx);
                        tree.delete(newTuple.getValue(colIdx), rid);
                        tree.insert(oldTuple.getValue(colIdx), rid);
                    }
                } else {
                    List<StandaloneIndexBinding> bindings = standaloneIndexes.get(tableName.toLowerCase(Locale.ROOT));
                    if (bindings != null) {
                        for (StandaloneIndexBinding b : bindings) {
                            b.indexTree().delete(newTuple.getValue(b.columnIndex()), rid);
                            b.indexTree().insert(oldTuple.getValue(b.columnIndex()), rid);
                        }
                    }
                }
            }
            default -> {
                // Other record types do not require data undo
            }
        }
    }

    /**
     * Returns an unmodifiable snapshot of the Active Transaction Table ({@code txnId -> lastLsn})
     * for fuzzy checkpoints.
     */
    public Map<Long, Long> getActiveTransactionTable() {
        Map<Long, Long> att = new LinkedHashMap<>();
        for (Map.Entry<Long, Transaction> entry : activeTransactions.entrySet()) {
            Transaction t = entry.getValue();
            if (t.getState() == TransactionState.ACTIVE) {
                att.put(entry.getKey(), t.getLastLsn());
            }
        }
        return Collections.unmodifiableMap(att);
    }

    public Transaction getTransaction(long txnId) {
        return activeTransactions.get(txnId);
    }

    public int getActiveTransactionCount() {
        return activeTransactions.size();
    }

    public long getCommittedCount() {
        return committedCount.get();
    }

    public long getAbortedCount() {
        return abortedCount.get();
    }

    public LogManager getLogManager() {
        return logManager;
    }

    @Override
    public void close() {
        lockManager.close();
    }
}
