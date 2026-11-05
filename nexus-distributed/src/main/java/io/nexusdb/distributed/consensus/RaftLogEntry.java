package io.nexusdb.distributed.consensus;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable entry in the replicated Raft log (1-based {@code index}, election {@code term}, and
 * opaque state-machine {@code command} payload).
 */
public final class RaftLogEntry {
    private final long index;
    private final long term;
    private final byte[] command;

    public RaftLogEntry(long index, long term, byte[] command) {
        if (index < 1) {
            throw new IllegalArgumentException("Raft log index must be >= 1, got: " + index);
        }
        if (term < 1) {
            throw new IllegalArgumentException("Raft log term must be >= 1, got: " + term);
        }
        Objects.requireNonNull(command, "command must not be null");
        this.index = index;
        this.term = term;
        this.command = command.clone();
    }

    public long index() {
        return index;
    }

    public long term() {
        return term;
    }

    public byte[] command() {
        return command.clone();
    }

    public int commandLength() {
        return command.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RaftLogEntry that)) {
            return false;
        }
        return index == that.index
                && term == that.term
                && Arrays.equals(command, that.command);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(index);
        result = 31 * result + Long.hashCode(term);
        result = 31 * result + Arrays.hashCode(command);
        return result;
    }

    @Override
    public String toString() {
        return "RaftLogEntry[index=" + index + ", term=" + term + ", bytes=" + command.length + "]";
    }
}
