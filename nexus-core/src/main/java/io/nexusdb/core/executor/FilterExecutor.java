package io.nexusdb.core.executor;

import io.nexusdb.core.planner.BoundExpr;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.transaction.LockId;
import io.nexusdb.core.transaction.LockManager;
import io.nexusdb.core.transaction.LockMode;
import io.nexusdb.core.transaction.Transaction;
import io.nexusdb.core.wal.LogManager;

import java.util.Objects;

/**
 * Physical filter operator that emits only child tuples for which the predicate evaluates to {@code TRUE}.
 */
public final class FilterExecutor implements Executor {
    private final Executor child;
    private final BoundExpr predicate;

    private double estimatedCost;
    private long estimatedRows;
    private long actualRows;
    private long elapsedNanos;
    private boolean executed;

    public FilterExecutor(Executor child, BoundExpr predicate) {
        this.child = Objects.requireNonNull(child, "child must not be null");
        this.predicate = Objects.requireNonNull(predicate, "predicate must not be null");
        if (child instanceof SeqScanExecutor seqScan) {
            seqScan.setDeferRowLockToFilter(true);
        }
        long childRows = child.getEstimatedRows();
        this.estimatedRows = childRows == 0 ? 0 : Math.max(1L, Math.round(childRows * 0.25));
        this.estimatedCost = child.getEstimatedCost() + childRows * 0.05;
    }

    public void setLockMode(LockMode mode) {
        if (child instanceof SeqScanExecutor seqScan) {
            seqScan.setLockMode(mode);
        } else if (child instanceof IndexScanExecutor indexScan) {
            indexScan.setLockMode(mode);
        } else if (child instanceof FilterExecutor innerFilter) {
            innerFilter.setLockMode(mode);
        }
    }

    public Executor getChild() {
        return child;
    }

    public BoundExpr getPredicate() {
        return predicate;
    }

    @Override
    public void init() {
        long start = System.nanoTime();
        child.init();
        this.actualRows = 0L;
        this.executed = true;
        this.elapsedNanos = System.nanoTime() - start;
    }

    @Override
    public Tuple next() {
        long start = System.nanoTime();
        try {
            Tuple tuple;
            while ((tuple = child.next()) != null) {
                if (child instanceof SeqScanExecutor seqScan && tuple.getRecordId() != null) {
                    if (!ExpressionEvaluator.evaluatePredicate(predicate, tuple)) {
                        continue;
                    }
                    TableHeap heap = seqScan.getTableHeap();
                    LogManager lm = heap.getBufferPoolManager().getLogManager();
                    if (lm != null
                            && lm.getThreadTransactionContext() instanceof Transaction txn
                            && txn.getLockManager() != null) {
                        LockManager lockMgr = txn.getLockManager();
                        LockId rowLockId = LockId.row(seqScan.getTableName(), tuple.getRecordId());
                        lockMgr.acquire(txn, rowLockId, seqScan.getLockMode());
                        Tuple fresh = heap.get(tuple.getRecordId());
                        if (fresh != null && ExpressionEvaluator.evaluatePredicate(predicate, fresh)) {
                            actualRows++;
                            return fresh;
                        }
                        continue;
                    }
                    actualRows++;
                    return tuple;
                }
                if (ExpressionEvaluator.evaluatePredicate(predicate, tuple)) {
                    actualRows++;
                    return tuple;
                }
            }
            return null;
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
        return child.getOutputSchema();
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
        return prefix + "Filter(predicate=" + predicate + ")" + formatMetrics()
                + "\n" + child.explain(indent + 1);
    }
}
