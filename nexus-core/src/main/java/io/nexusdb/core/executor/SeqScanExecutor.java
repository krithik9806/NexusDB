package io.nexusdb.core.executor;

import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.TableIterator;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.transaction.LockId;
import io.nexusdb.core.transaction.LockMode;
import io.nexusdb.core.transaction.Transaction;
import io.nexusdb.core.wal.LogManager;

import java.util.Objects;

/**
 * Physical sequential scan operator iterating over all live tuples in a {@link TableHeap}.
 */
public final class SeqScanExecutor implements Executor {
    private final String tableName;
    private final TableHeap tableHeap;
    private TableIterator iterator;
    private boolean deferRowLockToFilter;
    private LockMode lockMode = LockMode.SHARED;

    private double estimatedCost;
    private long estimatedRows;
    private long actualRows;
    private long elapsedNanos;
    private boolean executed;

    public SeqScanExecutor(TableMetadata tableMetadata) {
        Objects.requireNonNull(tableMetadata, "tableMetadata must not be null");
        this.tableName = tableMetadata.getName();
        this.tableHeap = tableMetadata.getTableHeap();
        this.estimatedRows = tableHeap.getLiveRowCount();
        this.estimatedCost = tableHeap.getPageCount() * 1.0 + this.estimatedRows * 0.10;
    }

    public SeqScanExecutor(String tableName, TableHeap tableHeap) {
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        this.tableHeap = Objects.requireNonNull(tableHeap, "tableHeap must not be null");
        this.estimatedRows = tableHeap.getLiveRowCount();
        this.estimatedCost = tableHeap.getPageCount() * 1.0 + this.estimatedRows * 0.10;
    }

    public String getTableName() {
        return tableName;
    }

    public TableHeap getTableHeap() {
        return tableHeap;
    }

    public void setDeferRowLockToFilter(boolean deferRowLockToFilter) {
        this.deferRowLockToFilter = deferRowLockToFilter;
    }

    public LockMode getLockMode() {
        return lockMode;
    }

    public void setLockMode(LockMode lockMode) {
        this.lockMode = Objects.requireNonNull(lockMode, "lockMode must not be null");
    }

    @Override
    public void init() {
        long start = System.nanoTime();
        if (iterator != null) {
            iterator.close();
        }
        iterator = tableHeap.scan();
        this.actualRows = 0L;
        this.executed = true;
        this.elapsedNanos = System.nanoTime() - start;
    }

    @Override
    public Tuple next() {
        if (iterator == null) {
            throw new IllegalStateException("SeqScanExecutor.init() must be called before next()");
        }
        long start = System.nanoTime();
        try {
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                if (tuple == null) {
                    continue;
                }
                if (!deferRowLockToFilter && tuple.getRecordId() != null) {
                    LogManager lm = tableHeap.getBufferPoolManager().getLogManager();
                    if (lm != null
                            && lm.getThreadTransactionContext() instanceof Transaction txn
                            && txn.getLockManager() != null) {
                        txn.getLockManager().acquire(
                                txn,
                                LockId.row(tableName, tuple.getRecordId()),
                                lockMode);
                        Tuple fresh = tableHeap.get(tuple.getRecordId());
                        if (fresh == null) {
                            continue;
                        }
                        tuple = fresh;
                    }
                }
                actualRows++;
                return tuple;
            }
            return null;
        } finally {
            elapsedNanos += System.nanoTime() - start;
        }
    }

    @Override
    public void close() {
        if (iterator != null) {
            iterator.close();
            iterator = null;
        }
    }

    @Override
    public Schema getOutputSchema() {
        return tableHeap.getSchema();
    }

    @Override
    public double getEstimatedCost() {
        return estimatedCost;
    }

    @Override
    public long getEstimatedRows() {
        return estimatedRows;
    }

    @Override
    public void setEstimates(double estimatedCost, long estimatedRows) {
        this.estimatedCost = estimatedCost;
        this.estimatedRows = estimatedRows;
    }

    @Override
    public long getActualRows() {
        return actualRows;
    }

    @Override
    public long getElapsedNanos() {
        return elapsedNanos;
    }

    @Override
    public boolean isExecuted() {
        return executed;
    }

    @Override
    public String explain(int indent) {
        return "  ".repeat(Math.max(0, indent))
                + "SeqScan(table=" + tableName + ")"
                + formatMetrics();
    }
}
