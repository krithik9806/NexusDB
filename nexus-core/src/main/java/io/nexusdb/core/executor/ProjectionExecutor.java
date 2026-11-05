package io.nexusdb.core.executor;

import io.nexusdb.core.planner.BoundStatement;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Physical projection operator evaluating a list of {@link BoundStatement.BoundProjection} expressions
 * for each tuple produced by its child operator.
 */
public final class ProjectionExecutor implements Executor {
    private final Executor child;
    private final List<BoundStatement.BoundProjection> projections;
    private final Schema outputSchema;

    private double estimatedCost;
    private long estimatedRows;
    private long actualRows;
    private long elapsedNanos;
    private boolean executed;

    public ProjectionExecutor(
            Executor child,
            List<BoundStatement.BoundProjection> projections,
            Schema outputSchema) {
        this.child = Objects.requireNonNull(child, "child must not be null");
        this.projections = List.copyOf(Objects.requireNonNull(projections, "projections must not be null"));
        this.outputSchema = Objects.requireNonNull(outputSchema, "outputSchema must not be null");
        this.estimatedRows = child.getEstimatedRows();
        this.estimatedCost = child.getEstimatedCost() + this.estimatedRows * 0.01;
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
            Tuple input = child.next();
            if (input == null) {
                return null;
            }
            List<Value> projected = new ArrayList<>(projections.size());
            for (BoundStatement.BoundProjection proj : projections) {
                projected.add(ExpressionEvaluator.evaluate(proj.expr(), input));
            }
            actualRows++;
            return new Tuple(projected, input.getRecordId());
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
        String cols = projections.stream()
                .map(BoundStatement.BoundProjection::alias)
                .collect(Collectors.joining(", "));
        return prefix + "Projection(columns=[" + cols + "])" + formatMetrics()
                + "\n" + child.explain(indent + 1);
    }
}
