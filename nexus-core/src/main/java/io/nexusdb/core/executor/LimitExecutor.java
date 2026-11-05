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

    public LimitExecutor(Executor child, int limit) {
        this.child = Objects.requireNonNull(child, "child must not be null");
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0, got: " + limit);
        }
        this.limit = limit;
    }

    @Override
    public void init() {
        this.emitted = 0;
        child.init();
    }

    @Override
    public Tuple next() {
        if (emitted >= limit) {
            return null;
        }
        Tuple tuple = child.next();
        if (tuple != null) {
            emitted++;
        }
        return tuple;
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
        return prefix + "Limit(count=" + limit + ")\n" + child.explain(indent + 1);
    }
}
