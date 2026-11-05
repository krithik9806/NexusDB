package io.nexusdb.core.transaction;

/**
 * Thrown when a transaction fails to acquire a requested lock within the configured timeout.
 */
public final class LockTimeoutException extends RuntimeException {
    private final long txnId;
    private final LockId lockId;
    private final LockMode requestedMode;

    public LockTimeoutException(long txnId, LockId lockId, LockMode requestedMode, long timeoutMillis) {
        super("Lock acquisition timed out after " + timeoutMillis + " ms for txnId="
                + txnId + " requesting " + requestedMode + " on " + lockId);
        this.txnId = txnId;
        this.lockId = lockId;
        this.requestedMode = requestedMode;
    }

    public long getTxnId() {
        return txnId;
    }

    public LockId getLockId() {
        return lockId;
    }

    public LockMode getRequestedMode() {
        return requestedMode;
    }
}
