package io.nexusdb.distributed.consensus;

/**
 * Injectable timer policy generating randomized election timeouts and fixed heartbeat intervals.
 *
 * <p>Split-Vote Avoidance Invariant (Raft §5.2):
 * Election timeouts are drawn from {@code [minElectionTimeoutMillis, maxElectionTimeoutMillis]}
 * (typically {@code [150ms, 300ms]}), which must be at least an order of magnitude larger than
 * {@link #heartbeatIntervalMillis()} so that followers do not time out while a healthy leader
 * is sending heartbeats, and concurrent candidates rapidly diverge after a split vote.
 */
public interface RaftTimer {

    /**
     * Returns the next randomized election timeout duration in milliseconds for {@code nodeId}.
     */
    long nextElectionTimeoutMillis(String nodeId);

    /**
     * Returns the leader heartbeat broadcast interval in milliseconds.
     */
    long heartbeatIntervalMillis();
}
