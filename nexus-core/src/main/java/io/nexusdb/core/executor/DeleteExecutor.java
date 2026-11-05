package io.nexusdb.core.executor;

import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.index.BPlusTree;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import io.nexusdb.core.transaction.LockId;
import io.nexusdb.core.transaction.LockManager;
import io.nexusdb.core.transaction.LockMode;
import io.nexusdb.core.transaction.Transaction;
import io.nexusdb.core.wal.LogManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Physical {@code DELETE} operator that removes matching tuples from a {@link TableHeap},
 * deletes their corresponding entries from all table {@link BPlusTree} indexes, and emits
 * a single tuple containing the number of deleted rows.
 */
public final class DeleteExecutor implements Executor {
    private final TableMetadata table;
    private final Executor child;
    private boolean consumed;

    private double estimatedCost;
    private long estimatedRows;
    private long actualRows;
    private long elapsedNanos;
    private boolean executed;

    public DeleteExecutor(TableMetadata table, Executor child) {
        this.table = Objects.requireNonNull(table, "table must not be null");
        this.child = Objects.requireNonNull(child, "child must not be null");
        UpdateExecutor.configureExclusiveScanLockMode(this.child);
        this.estimatedRows = child.getEstimatedRows();
        this.estimatedCost = child.getEstimatedCost() + this.estimatedRows * (0.8 + table.getIndexes().size() * 0.5);
    }

    @Override
    public void init() {
        long start = System.nanoTime();
        this.consumed = false;
        this.actualRows = 0L;
        this.executed = true;
        child.init();
        this.elapsedNanos = System.nanoTime() - start;
    }

    @Override
    public Tuple next() {
        if (consumed) {
            return null;
        }
        consumed = true;
        long start = System.nanoTime();
        try {
            List<Tuple> targetTuples = new ArrayList<>();
            try {
                Tuple t;
                while ((t = child.next()) != null) {
                    if (t.getRecordId() == null) {
                        throw new IllegalStateException("Cannot delete tuple without RecordId");
                    }
                    targetTuples.add(t);
                }
            } finally {
                child.close();
            }

            Schema schema = table.getSchema();
            TableHeap heap = table.getTableHeap();
            List<IndexMetadata> indexes = table.getIndexes();
            LogManager lm = heap.getBufferPoolManager().getLogManager();
            Transaction txn = (lm != null && lm.getThreadTransactionContext() instanceof Transaction t) ? t : null;
            LockManager lockMgr = txn != null ? txn.getLockManager() : null;
            int deletedCount = 0;

            for (Tuple tuple : targetTuples) {
                RecordId rid = tuple.getRecordId();
                if (lockMgr != null) {
                    lockMgr.acquire(txn, LockId.row(table.getName(), rid), LockMode.EXCLUSIVE);
                    for (IndexMetadata idx : indexes) {
                        if (idx.unique()) {
                            int colIdx = schema.getColumnIndex(idx.columnName());
                            lockMgr.acquire(
                                    txn,
                                    LockId.key(table.getName(), idx.indexName(), tuple.getValue(colIdx)),
                                    LockMode.EXCLUSIVE);
                        }
                    }
                }
                if (heap.delete(rid)) {
                    for (IndexMetadata idx : indexes) {
                        int colIdx = schema.getColumnIndex(idx.columnName());
                        BPlusTree tree = table.getIndexTree(idx);
                        tree.delete(tuple.getValue(colIdx), rid);
                    }
                    deletedCount++;
                }
            }

            this.actualRows = deletedCount;
            return new Tuple(Value.ofInt(deletedCount));
        } finally {
            elapsedNanos += System.nanoTime() - start;
        }
    }

    @Override
    public void close() {
        child.close();
    }

    @Override
    public Schema getOutputSchema() {
        return InsertExecutor.DML_OUTPUT_SCHEMA;
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
        String prefix = "  ".repeat(Math.max(0, indent));
        return prefix + "Delete(table=" + table.getName() + ")" + formatMetrics()
                + "\n" + child.explain(indent + 1);
    }
}
