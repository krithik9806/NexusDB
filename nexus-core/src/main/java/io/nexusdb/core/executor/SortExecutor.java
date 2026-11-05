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

    public SortExecutor(Executor child, List<BoundStatement.BoundOrderBy> orderBy) {
        this.child = Objects.requireNonNull(child, "child must not be null");
        this.orderBy = List.copyOf(Objects.requireNonNull(orderBy, "orderBy must not be null"));
        if (this.orderBy.isEmpty()) {
            throw new IllegalArgumentException("orderBy must not be empty");
        }
    }

    @Override
    public void init() {
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
            // ASC -> NULLS LAST (+1); DESC -> NULLS FIRST (-1)
            return ascending ? 1 : -1;
        }
        if (bNull) {
            // ASC -> non-null before null (-1); DESC -> non-null after null (+1)
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
        if (cursor >= sortedTuples.size()) {
            return null;
        }
        return sortedTuples.get(cursor++);
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
    public String explain(int indent) {
        String prefix = "  ".repeat(Math.max(0, indent));
        return prefix + "Sort(keys=" + orderBy.size() + ")\n" + child.explain(indent + 1);
    }
}
