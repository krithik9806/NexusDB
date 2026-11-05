package io.nexusdb.core.transaction;

/**
 * Immutable snapshot of transaction and concurrency-control metrics exposed by
 * {@link TransactionManager} and {@link io.nexusdb.core.engine.Database}.
 */
public record TransactionMetrics(
        int activeTransactions,
        long lockWaits,
        long deadlockCount,
        long committedCount,
        long abortedCount) {
}
