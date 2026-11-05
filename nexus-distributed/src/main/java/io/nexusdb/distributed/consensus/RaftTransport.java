package io.nexusdb.distributed.consensus;

/**
 * Injectable asynchronous message transport between Raft peers.
 */
@FunctionalInterface
public interface RaftTransport {
    /**
     * Sends {@code message} from {@code fromNodeId} to {@code toNodeId}.
     */
    void send(String fromNodeId, String toNodeId, RaftMessage message);
}
