package io.nexusdb.core.executor;

import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.Tuple;

import java.util.Objects;

/**
 * Physical limit operator that emits at most {@code limit} tuples from its child operator.
 */
public final class LimitExecutor implements Executor {
    private final Executor child;
    private final int limit;
    private int emitted;

    private double estimatedCost;
    private long estimatedRows;
    private long actualRows;
    private long elapsedNanos;
    private boolean executed;

    public LimitExecutor(Executor child, int limit) {
        this.child = Objects.requireNonNull(child, "child must not be null");
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0, got: " + limit);
        }
        this.limit = limit;
        this.estimatedRows = Math.min(limit, child.getEstimatedRows());
        this.estimatedCost = child.getEstimatedCost();
    }

    @Override
    public void init() {
        long start = System.nanoTime();
        this.emitted = 0;
        this.actualRows = 0L;
        this.executed = true;
        child.init();
        this.elapsedNanos = System.nanoTime() - start;
    }

    @Override
    public Tuple next() {
        long start = System.nanoTime();
        try {
            if (emitted >= limit) {
                return null;
            }
            Tuple tuple = child.next();
            if (tuple != null) {
                emitted++;
                actualRows++;
            }
            return tuple;
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
        return prefix + "Limit(count=" + limit + ")" + formatMetrics()
                + "\n" + child.explain(indent + 1);
    }
}
