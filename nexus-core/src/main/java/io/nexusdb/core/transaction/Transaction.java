package io.nexusdb.core.transaction;

import io.nexusdb.core.wal.LogManager;
import io.nexusdb.core.wal.LogRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents an in-flight or completed database transaction, tracking its {@code txnId},
 * {@link TransactionState}, WAL {@code prevLsn} chain, and logged modification records for undo.
 */
public final class Transaction implements LogManager.TransactionLogContext {
    private final long txnId;
    private final boolean autoCommit;
    private volatile TransactionState state;
    private final long startLsn;
    private volatile long lastLsn;
    private final long startTimestampNanos;
    private final LockManager lockManager;
    private final List<LogRecord> loggedModifications = new ArrayList<>();

    public Transaction(long txnId, long startLsn, boolean autoCommit) {
        this(txnId, startLsn, autoCommit, null);
    }

    public Transaction(long txnId, long startLsn, boolean autoCommit, LockManager lockManager) {
        if (txnId <= 0) {
            throw new IllegalArgumentException("txnId must be > 0, got: " + txnId);
        }
        this.txnId = txnId;
        this.autoCommit = autoCommit;
        this.state = TransactionState.ACTIVE;
        this.startLsn = startLsn;
        this.lastLsn = startLsn;
        this.startTimestampNanos = System.nanoTime();
        this.lockManager = lockManager;
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    @Override
    public long getTxnId() {
        return txnId;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public TransactionState getState() {
        return state;
    }

    public void setState(TransactionState state) {
        this.state = Objects.requireNonNull(state, "state must not be null");
    }

    public long getStartLsn() {
        return startLsn;
    }

    @Override
    public long getLastLsn() {
        return lastLsn;
    }

    public void setLastLsn(long lastLsn) {
        this.lastLsn = lastLsn;
    }

    public long getStartTimestampNanos() {
        return startTimestampNanos;
    }

    @Override
    public synchronized void onRecordLogged(LogRecord record) {
        Objects.requireNonNull(record, "record must not be null");
        this.lastLsn = record.lsn();
        this.loggedModifications.add(record);
    }

    public synchronized int getLoggedModificationCount() {
        return loggedModifications.size();
    }

    public synchronized List<LogRecord> getLoggedModificationsSnapshot() {
        return List.copyOf(loggedModifications);
    }

    public synchronized List<LogRecord> drainLoggedModificationsFrom(int savepointIndex) {
        if (savepointIndex < 0 || savepointIndex > loggedModifications.size()) {
            throw new IllegalArgumentException("Invalid savepointIndex: " + savepointIndex);
        }
        List<LogRecord> sub = new ArrayList<>(loggedModifications.subList(savepointIndex, loggedModifications.size()));
        loggedModifications.subList(savepointIndex, loggedModifications.size()).clear();
        return sub;
    }

    @Override
    public String toString() {
        return "Transaction{txnId=" + txnId + ", state=" + state + ", lastLsn=" + lastLsn + '}';
    }
}
