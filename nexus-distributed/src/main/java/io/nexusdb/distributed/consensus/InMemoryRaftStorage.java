package io.nexusdb.distributed.consensus;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * In-memory implementation of {@link RaftStorage} that preserves state across simulated node crashes
 * and restarts without disk I/O overhead when testing in-memory clusters.
 */
public final class InMemoryRaftStorage implements RaftStorage {
    private long currentTerm = 0L;
    private String votedFor = null;
    private final List<RaftLogEntry> log = new ArrayList<>();

    @Override
    public synchronized PersistentState load() {
        return new PersistentState(currentTerm, votedFor, List.copyOf(log));
    }

    @Override
    public synchronized void saveTermAndVote(long currentTerm, String votedFor) {
        if (currentTerm < 0) {
            throw new IllegalArgumentException("currentTerm must be >= 0");
        }
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
    }

    @Override
    public synchronized void appendEntries(List<RaftLogEntry> entries) {
        Objects.requireNonNull(entries, "entries must not be null");
        for (RaftLogEntry entry : entries) {
            long expectedIdx = log.size() + 1L;
            if (entry.index() != expectedIdx) {
                throw new IllegalArgumentException(
                        "Non-contiguous log append: expected index " + expectedIdx + ", got " + entry.index());
            }
            log.add(entry);
        }
    }

    @Override
    public synchronized void truncateAndAppend(long fromIndex, List<RaftLogEntry> newEntries) {
        if (fromIndex < 1) {
            throw new IllegalArgumentException("fromIndex must be >= 1, got: " + fromIndex);
        }
        Objects.requireNonNull(newEntries, "newEntries must not be null");
        while (log.size() >= fromIndex) {
            log.remove(log.size() - 1);
        }
        appendEntries(newEntries);
    }
}
