package io.nexusdb.core.executor;

import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.Tuple;

import java.util.Locale;

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

    default double getEstimatedCost() {
        return 0.0;
    }

    default long getEstimatedRows() {
        return 0L;
    }

    default void setEstimates(double estimatedCost, long estimatedRows) {
    }

    default long getActualRows() {
        return 0L;
    }

    default long getElapsedNanos() {
        return 0L;
    }

    default double getElapsedTimeMs() {
        return getElapsedNanos() / 1_000_000.0;
    }

    default boolean isExecuted() {
        return false;
    }

    default String formatMetrics() {
        if (isExecuted()) {
            return String.format(
                    Locale.US,
                    " [cost=%.2f, rows=%d, actualRows=%d, timeMs=%.3f]",
                    getEstimatedCost(),
                    getEstimatedRows(),
                    getActualRows(),
                    getElapsedTimeMs());
        }
        return String.format(
                Locale.US,
                " [cost=%.2f, rows=%d]",
                getEstimatedCost(),
                getEstimatedRows());
    }

    /**
     * Formats this physical operator and its children as an indented plan tree for {@code EXPLAIN}.
     */
    default String explain(int indent) {
        return "  ".repeat(Math.max(0, indent)) + getClass().getSimpleName() + formatMetrics();
    }
}
