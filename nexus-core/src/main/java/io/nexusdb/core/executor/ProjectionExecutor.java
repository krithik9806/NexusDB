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

    public ProjectionExecutor(
            Executor child,
            List<BoundStatement.BoundProjection> projections,
            Schema outputSchema) {
        this.child = Objects.requireNonNull(child, "child must not be null");
        this.projections = List.copyOf(Objects.requireNonNull(projections, "projections must not be null"));
        this.outputSchema = Objects.requireNonNull(outputSchema, "outputSchema must not be null");
    }

    @Override
    public void init() {
        child.init();
    }

    @Override
    public Tuple next() {
        Tuple input = child.next();
        if (input == null) {
            return null;
        }
        List<Value> projected = new ArrayList<>(projections.size());
        for (BoundStatement.BoundProjection proj : projections) {
            projected.add(ExpressionEvaluator.evaluate(proj.expr(), input));
        }
        return new Tuple(projected, input.getRecordId());
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
    public String explain(int indent) {
        String prefix = "  ".repeat(Math.max(0, indent));
        String cols = projections.stream()
                .map(BoundStatement.BoundProjection::alias)
                .collect(Collectors.joining(", "));
        return prefix + "Projection(columns=[" + cols + "])\n" + child.explain(indent + 1);
    }
}
