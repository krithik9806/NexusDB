package io.nexusdb.core.executor;

import io.nexusdb.core.ast.Expr;
import io.nexusdb.core.planner.Binder;
import io.nexusdb.core.planner.BoundExpr;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Physical hash-based aggregation operator implementing {@code GROUP BY} and SQL aggregate
 * functions ({@code COUNT(*)}, {@code COUNT(expr)}, {@code SUM(expr)}, {@code AVG(expr)},
 * {@code MIN(expr)}, {@code MAX(expr)}).
 *
 * <p>SQL Grouping & NULL Semantics Invariant (per {@code docs/sql-engine.md}):
 * <ul>
 *   <li>{@code COUNT(*)} counts every input row in the group, including rows with {@code NULL} columns.</li>
 *   <li>{@code COUNT(expr)}, {@code SUM(expr)}, {@code AVG(expr)}, {@code MIN(expr)}, and {@code MAX(expr)}
 *       ignore {@code NULL} inputs.</li>
 *   <li>On an empty input set when {@code GROUP BY} is absent (ungrouped aggregate), {@link #next()}
 *       emits exactly <b>1 row</b> where {@code COUNT} is {@code 0} and {@code SUM}/{@code AVG}/{@code MIN}/{@code MAX}
 *       are {@code NULL}.</li>
 *   <li>On an empty input set when {@code GROUP BY} is present, {@link #next()} emits <b>0 rows</b>.</li>
 *   <li>In {@code GROUP BY}, {@code NULL} group key values are treated as equal and form a valid group
 *       (leveraging {@link Value.NullValue#equals(Object)} and {@link Value.NullValue#hashCode()}).</li>
 * </ul>
 */
public final class HashAggregateExecutor implements Executor {
    private final Executor child;
    private final List<BoundExpr> groupBy;
    private final List<BoundExpr.BoundAggregate> aggregates;
    private final Schema outputSchema;

    private List<Tuple> outputTuples;
    private int cursor;

    private double estimatedCost;
    private long estimatedRows;
    private long actualRows;
    private long elapsedNanos;
    private boolean executed;

    public HashAggregateExecutor(
            Executor child,
            List<BoundExpr> groupBy,
            List<BoundExpr.BoundAggregate> aggregates) {
        this.child = Objects.requireNonNull(child, "child must not be null");
        this.groupBy = List.copyOf(Objects.requireNonNull(groupBy, "groupBy must not be null"));
        this.aggregates = List.copyOf(Objects.requireNonNull(aggregates, "aggregates must not be null"));
        this.outputSchema = buildAggregateSchema(child.getOutputSchema(), this.groupBy, this.aggregates);

        long childRows = child.getEstimatedRows();
        if (this.groupBy.isEmpty()) {
            this.estimatedRows = 1L;
        } else {
            this.estimatedRows = childRows == 0L ? 0L : Math.max(1L, Math.round(childRows * 0.25));
        }
        this.estimatedCost = child.getEstimatedCost() + childRows * 0.08;
    }

    private static Schema buildAggregateSchema(
            Schema childSchema,
            List<BoundExpr> groupBy,
            List<BoundExpr.BoundAggregate> aggregates) {
        List<Column> cols = new ArrayList<>(groupBy.size() + aggregates.size());
        for (int i = 0; i < groupBy.size(); i++) {
            BoundExpr g = groupBy.get(i);
            DataType dt = g.dataType() != null ? g.dataType() : DataType.INT;
            int maxLen = dt == DataType.VARCHAR ? resolveMaxLen(g, childSchema) : dt.getFixedSize();
            cols.add(new Column("group_" + i, dt, maxLen, true, false));
        }
        for (int i = 0; i < aggregates.size(); i++) {
            BoundExpr.BoundAggregate agg = aggregates.get(i);
            DataType dt = agg.dataType();
            int maxLen = dt == DataType.VARCHAR ? resolveMaxLen(agg.argument(), childSchema) : dt.getFixedSize();
            cols.add(new Column("agg_" + i, dt, maxLen, true, false));
        }
        if (cols.isEmpty()) {
            cols.add(new Column("agg_dummy", DataType.INT, DataType.INT.getFixedSize(), true, false));
        }
        return new Schema(cols);
    }

    private static int resolveMaxLen(BoundExpr expr, Schema childSchema) {
        if (expr instanceof BoundExpr.BoundColumnRef(int idx, String ignored, DataType ignoredType)
                && idx >= 0 && idx < childSchema.getColumnCount()) {
            return childSchema.getColumn(idx).maxLength();
        }
        return 255;
    }

    @Override
    public void init() {
        long start = System.nanoTime();
        this.actualRows = 0L;
        this.executed = true;
        child.init();

        Map<List<Value>, Accumulator[]> groups = new LinkedHashMap<>();
        try {
            Tuple row;
            while ((row = child.next()) != null) {
                List<Value> key = evaluateGroupKey(row);
                Accumulator[] accs = groups.computeIfAbsent(key, ignored -> createAccumulators());
                for (int i = 0; i < aggregates.size(); i++) {
                    BoundExpr.BoundAggregate agg = aggregates.get(i);
                    if (agg.star()) {
                        accs[i].accumulateStar();
                    } else {
                        Value val = ExpressionEvaluator.evaluate(agg.argument(), row);
                        accs[i].accumulate(val);
                    }
                }
            }
        } finally {
            child.close();
        }

        List<Tuple> results = new ArrayList<>();
        if (groups.isEmpty()) {
            if (groupBy.isEmpty()) {
                // SQL rule: ungrouped aggregate over empty input returns 1 row
                Accumulator[] emptyAccs = createAccumulators();
                List<Value> values = new ArrayList<>(aggregates.size());
                for (Accumulator acc : emptyAccs) {
                    values.add(acc.finish());
                }
                results.add(new Tuple(values));
            }
        } else {
            for (Map.Entry<List<Value>, Accumulator[]> entry : groups.entrySet()) {
                List<Value> groupKey = entry.getKey();
                Accumulator[] accs = entry.getValue();
                List<Value> rowValues = new ArrayList<>(groupKey.size() + accs.length);
                rowValues.addAll(groupKey);
                for (Accumulator acc : accs) {
                    rowValues.add(acc.finish());
                }
                results.add(new Tuple(rowValues));
            }
        }

        this.outputTuples = results;
        this.cursor = 0;
        this.elapsedNanos = System.nanoTime() - start;
    }

    private List<Value> evaluateGroupKey(Tuple row) {
        if (groupBy.isEmpty()) {
            return List.of();
        }
        List<Value> key = new ArrayList<>(groupBy.size());
        for (BoundExpr g : groupBy) {
            Value v = ExpressionEvaluator.evaluate(g, row);
            key.add(v == null ? Value.NULL : v);
        }
        return List.copyOf(key);
    }

    private Accumulator[] createAccumulators() {
        Accumulator[] accs = new Accumulator[aggregates.size()];
        for (int i = 0; i < aggregates.size(); i++) {
            accs[i] = new Accumulator(aggregates.get(i));
        }
        return accs;
    }

    @Override
    public Tuple next() {
        if (outputTuples == null) {
            throw new IllegalStateException("HashAggregateExecutor.init() must be called before next()");
        }
        long start = System.nanoTime();
        try {
            if (cursor >= outputTuples.size()) {
                return null;
            }
            actualRows++;
            return outputTuples.get(cursor++);
        } finally {
            elapsedNanos += System.nanoTime() - start;
        }
    }

    @Override
    public void close() {
        outputTuples = null;
        cursor = 0;
        child.close();
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
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
        String keys = groupBy.stream()
                .map(HashAggregateExecutor::formatExpr)
                .collect(Collectors.joining(", "));
        String aggs = aggregates.stream()
                .map(HashAggregateExecutor::formatAggregate)
                .collect(Collectors.joining(", "));
        return prefix + "HashAggregate(keys=[" + keys + "], aggregates=[" + aggs + "])"
                + formatMetrics()
                + "\n" + child.explain(indent + 1);
    }

    private static String formatAggregate(BoundExpr.BoundAggregate agg) {
        if (agg.star()) {
            return agg.function().name() + "(*)";
        }
        return agg.function().name() + "(" + formatExpr(agg.argument()) + ")";
    }

    private static String formatExpr(BoundExpr expr) {
        if (expr instanceof BoundExpr.BoundColumnRef(int ignored, String colName, DataType ignoredType)) {
            return colName;
        }
        return expr.toString();
    }

    private static final class Accumulator {
        private final Expr.AggregateFunc func;
        private final DataType returnType;
        private long count = 0L;
        private long longSum = 0L;
        private double doubleSum = 0.0;
        private Value minMax = Value.NULL;

        Accumulator(BoundExpr.BoundAggregate spec) {
            this.func = spec.function();
            this.returnType = spec.dataType();
        }

        void accumulateStar() {
            count++;
        }

        void accumulate(Value val) {
            if (val == null || val.isNull()) {
                return;
            }
            count++;
            switch (func) {
                case COUNT -> {
                    // count already incremented
                }
                case SUM -> {
                    if (returnType == DataType.DOUBLE) {
                        doubleSum += toDouble(val);
                    } else {
                        longSum += toLong(val);
                    }
                }
                case AVG -> doubleSum += toDouble(val);
                case MIN -> {
                    if (minMax.isNull() || compareValues(val, minMax) < 0) {
                        minMax = val;
                    }
                }
                case MAX -> {
                    if (minMax.isNull() || compareValues(val, minMax) > 0) {
                        minMax = val;
                    }
                }
            }
        }

        Value finish() {
            return switch (func) {
                case COUNT -> Value.ofBigInt(count);
                case SUM -> {
                    if (count == 0L) {
                        yield Value.NULL;
                    }
                    yield returnType == DataType.DOUBLE
                            ? Value.ofDouble(doubleSum)
                            : Value.ofBigInt(longSum);
                }
                case AVG -> count == 0L ? Value.NULL : Value.ofDouble(doubleSum / count);
                case MIN, MAX -> count == 0L ? Value.NULL : minMax;
            };
        }

        private static long toLong(Value v) {
            return switch (v) {
                case Value.IntValue(int i) -> i;
                case Value.BigIntValue(long l) -> l;
                default -> throw new IllegalArgumentException("Expected integer value for SUM, got: " + v);
            };
        }

        private static double toDouble(Value v) {
            return switch (v) {
                case Value.IntValue(int i) -> i;
                case Value.BigIntValue(long l) -> l;
                case Value.DoubleValue(double d) -> d;
                default -> throw new IllegalArgumentException("Expected numeric value for aggregate, got: " + v);
            };
        }

        private static int compareValues(Value a, Value b) {
            if (Binder.isNumeric(a.dataType()) && Binder.isNumeric(b.dataType())) {
                DataType common = Binder.widestNumeric(a.dataType(), b.dataType());
                return ExpressionEvaluator.coerceValue(a, common)
                        .compareTo(ExpressionEvaluator.coerceValue(b, common));
            }
            return a.compareTo(b);
        }
    }
}
