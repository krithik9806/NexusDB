package io.nexusdb.distributed.consensus;

/**
 * Deterministic replicated state machine interface driven by committed Raft log entries.
 *
 * <p>State Machine Safety Invariant (Raft §5.3):
 * {@link #apply(long, byte[])} is invoked strictly sequentially in increasing 1-based
 * {@code index} order ({@code lastApplied + 1 .. commitIndex}) once an entry is committed
 * by a majority quorum.
 */
@FunctionalInterface
public interface StateMachine {
    /**
     * Applies the committed {@code command} at 1-based Raft log {@code index} and returns
     * the deterministic execution result payload.
     */
    byte[] apply(long index, byte[] command);

    /**
     * No-op state machine that echoes the committed command bytes.
     */
    static StateMachine noop() {
        return (index, command) -> command;
    }
}
