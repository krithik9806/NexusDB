package io.nexusdb.core.transaction;

/**
 * Lock acquisition modes for Strict Two-Phase Locking (Strict 2PL).
 */
public enum LockMode {
    SHARED,
    EXCLUSIVE;

    /**
     * Returns {@code true} if a lock request of {@code requested} mode is compatible with an
     * existing lock held in {@code existing} mode by a different transaction.
     */
    public boolean isCompatibleWith(LockMode existing) {
        return this == SHARED && existing == SHARED;
    }
}
