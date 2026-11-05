package io.nexusdb.core.executor;

import io.nexusdb.core.planner.Binder;
import io.nexusdb.core.planner.BoundStatement;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Physical in-memory multi-key sort operator.
 *
 * <p>NULL Ordering Invariant (per {@code docs/sql-engine.md}):
 * {@code NULL} values sort <b>last</b> in {@code ASC} order ({@code NULLS LAST}) and
 * <b>first</b> in {@code DESC} order ({@code NULLS FIRST}).
 */
public final class SortExecutor implements Executor {
    private final Executor child;
    private final List<BoundStatement.BoundOrderBy> orderBy;
    private List<Tuple> sortedTuples;
    private int cursor;

    private double estimatedCost;
    private long estimatedRows;
    private long actualRows;
    private long elapsedNanos;
    private boolean executed;

    public SortExecutor(Executor child, List<BoundStatement.BoundOrderBy> orderBy) {
        this.child = Objects.requireNonNull(child, "child must not be null");
        this.orderBy = List.copyOf(Objects.requireNonNull(orderBy, "orderBy must not be null"));
        if (this.orderBy.isEmpty()) {
            throw new IllegalArgumentException("orderBy must not be empty");
        }
        this.estimatedRows = child.getEstimatedRows();
        double logN = Math.max(1.0, Math.log(Math.max(2L, this.estimatedRows)) / Math.log(2.0));
        this.estimatedCost = child.getEstimatedCost() + this.estimatedRows * logN * 0.02;
    }

    @Override
    public void init() {
        long start = System.nanoTime();
        this.actualRows = 0L;
        this.executed = true;
        child.init();
        List<Tuple> buffer = new ArrayList<>();
        try {
            Tuple t;
            while ((t = child.next()) != null) {
                buffer.add(t);
            }
        } finally {
            child.close();
        }
        buffer.sort(this::compareTuples);
        this.sortedTuples = buffer;
        this.cursor = 0;
        this.elapsedNanos = System.nanoTime() - start;
    }

    private int compareTuples(Tuple a, Tuple b) {
        for (BoundStatement.BoundOrderBy key : orderBy) {
            Value va = ExpressionEvaluator.evaluate(key.expr(), a);
            Value vb = ExpressionEvaluator.evaluate(key.expr(), b);
            int cmp = compareValuesWithNullOrdering(va, vb, key.ascending());
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    static int compareValuesWithNullOrdering(Value a, Value b, boolean ascending) {
        boolean aNull = a == null || a.isNull();
        boolean bNull = b == null || b.isNull();
        if (aNull && bNull) {
            return 0;
        }
        if (aNull) {
            return ascending ? 1 : -1;
        }
        if (bNull) {
            return ascending ? -1 : 1;
        }

        int rawCmp;
        if (Binder.isNumeric(a.dataType()) && Binder.isNumeric(b.dataType())) {
            DataType common = Binder.widestNumeric(a.dataType(), b.dataType());
            Value ca = ExpressionEvaluator.coerceValue(a, common);
            Value cb = ExpressionEvaluator.coerceValue(b, common);
            rawCmp = ca.compareTo(cb);
        } else {
            rawCmp = a.compareTo(b);
        }
        return ascending ? rawCmp : -rawCmp;
    }

    @Override
    public Tuple next() {
        if (sortedTuples == null) {
            throw new IllegalStateException("SortExecutor.init() must be called before next()");
        }
        long start = System.nanoTime();
        try {
            if (cursor >= sortedTuples.size()) {
                return null;
            }
            actualRows++;
            return sortedTuples.get(cursor++);
        } finally {
            elapsedNanos += System.nanoTime() - start;
        }
    }

    @Override
    public void close() {
        sortedTuples = null;
        cursor = 0;
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
        return prefix + "Sort(keys=" + orderBy.size() + ")" + formatMetrics()
                + "\n" + child.explain(indent + 1);
    }
}
