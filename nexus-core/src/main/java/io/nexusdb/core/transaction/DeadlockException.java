package io.nexusdb.core.transaction;

/**
 * Thrown when the deadlock detector identifies a cycle in the transaction waits-for graph
 * and selects the youngest transaction in the cycle as the victim to abort.
 */
public final class DeadlockException extends RuntimeException {
    private final long victimTxnId;

    public DeadlockException(long victimTxnId, String message) {
        super(message);
        this.victimTxnId = victimTxnId;
    }

    public long getVictimTxnId() {
        return victimTxnId;
    }
}
