package io.nexusdb.core.transaction;

/**
 * Lifecycle states of a NexusDB transaction.
 */
public enum TransactionState {
    ACTIVE,
    COMMITTED,
    ABORTED
}
