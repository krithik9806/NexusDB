package io.nexusdb.core.executor;

import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.Tuple;

/**
 * Volcano-style pull-based physical query operator interface.
 *
 * <p>Lifecycle Invariant:
 * Callers must invoke {@link #init()} before the first call to {@link #next()},
 * repeatedly call {@link #next()} until it returns {@code null}, and always invoke
 * {@link #close()} in a {@code finally} block or try-with-resources statement to
 * release underlying iterators and buffer pool resources.
 */
public interface Executor extends AutoCloseable {

    /**
     * Initializes operator state and any child executors.
     */
    void init();

    /**
     * Produces the next output {@link Tuple}, or {@code null} when the operator is exhausted.
     */
    Tuple next();

    /**
     * Releases any resources held by this operator and its children.
     */
    @Override
    void close();

    /**
     * Returns the output {@link Schema} of tuples emitted by {@link #next()}.
     */
    Schema getOutputSchema();

    /**
     * Formats this physical operator and its children as an indented plan tree for {@code EXPLAIN}.
     */
    default String explain(int indent) {
        return "  ".repeat(Math.max(0, indent)) + getClass().getSimpleName();
    }
}
