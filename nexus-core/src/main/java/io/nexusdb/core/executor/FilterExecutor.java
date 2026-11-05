package io.nexusdb.core.executor;

import io.nexusdb.core.planner.BoundExpr;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.Tuple;

import java.util.Objects;

/**
 * Physical filter operator that emits only child tuples for which the predicate evaluates to {@code TRUE}.
 */
public final class FilterExecutor implements Executor {
    private final Executor child;
    private final BoundExpr predicate;

    public FilterExecutor(Executor child, BoundExpr predicate) {
        this.child = Objects.requireNonNull(child, "child must not be null");
        this.predicate = Objects.requireNonNull(predicate, "predicate must not be null");
    }

    @Override
    public void init() {
        child.init();
    }

    @Override
    public Tuple next() {
        Tuple tuple;
        while ((tuple = child.next()) != null) {
            if (ExpressionEvaluator.evaluatePredicate(predicate, tuple)) {
                return tuple;
            }
        }
        return null;
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
    public String explain(int indent) {
        String prefix = "  ".repeat(Math.max(0, indent));
        return prefix + "Filter(predicate=" + predicate + ")\n" + child.explain(indent + 1);
    }
}
