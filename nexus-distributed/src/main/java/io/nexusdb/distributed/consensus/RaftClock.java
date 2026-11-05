package io.nexusdb.distributed.consensus;

/**
 * Injectable monotonic clock abstraction for deterministic Raft simulation and testing.
 */
@FunctionalInterface
public interface RaftClock {
    /**
     * Returns the current monotonic logical or wall-clock time in milliseconds.
     */
    long nowMillis();
}
