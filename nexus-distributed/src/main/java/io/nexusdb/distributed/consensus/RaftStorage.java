package io.nexusdb.distributed.consensus;

import java.util.List;
import java.util.Objects;

/**
 * Durable storage abstraction for Raft persistent state ({@code currentTerm}, {@code votedFor},
 * and {@code log[]}, per Raft Figure 2).
 *
 * <p>Crash-Safety Invariant:
 * Every mutation to {@code currentTerm}, {@code votedFor}, or {@code log[]} MUST be persisted
 * via this interface before a node sends any RPC or responds to any incoming RPC.
 */
public interface RaftStorage extends AutoCloseable {

    record PersistentState(long currentTerm, String votedFor, List<RaftLogEntry> log) {
        public PersistentState {
            if (currentTerm < 0) {
                throw new IllegalArgumentException("currentTerm must be >= 0");
            }
            Objects.requireNonNull(log, "log must not be null");
            log = List.copyOf(log);
        }
    }

    /**
     * Loads the durable state from storage on node startup or recovery.
     */
    PersistentState load();

    /**
     * Persists {@code currentTerm} and {@code votedFor} (which may be {@code null}) durably.
     */
    void saveTermAndVote(long currentTerm, String votedFor);

    /**
     * Appends one or more contiguous {@link RaftLogEntry} records to the end of the durable log.
     */
    void appendEntries(List<RaftLogEntry> entries);

    /**
     * Truncates all log entries at index {@code >= fromIndex} (1-based) and appends {@code newEntries},
     * atomically updating the durable log when resolving a follower log conflict.
     */
    void truncateAndAppend(long fromIndex, List<RaftLogEntry> newEntries);

    @Override
    default void close() {
    }
}
