package io.nexusdb.server.admin;

import io.nexusdb.distributed.consensus.RaftNode;
import io.nexusdb.distributed.consensus.RaftRole;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Aggregates cluster membership and replication lag metrics from {@link RaftNode}.
 */
public final class ClusterMembershipView {
    public record PeerStatus(String peerId, long matchIndex, long lag, boolean isAlive) {}

    private final RaftNode raftNode;

    public ClusterMembershipView(RaftNode raftNode) {
        this.raftNode = Objects.requireNonNull(raftNode, "raftNode must not be null");
    }

    public List<PeerStatus> getPeerStatuses() {
        List<PeerStatus> result = new ArrayList<>();
        long leaderLastIdx = raftNode.getLastLogIndex();

        for (String peerId : raftNode.getPeerIds()) {
            long match = raftNode.getMatchIndexForPeer(peerId);
            long lag = Math.max(0L, leaderLastIdx - match);
            boolean isAlive = (lag < 50); // Considered alive if lag is bounded
            result.add(new PeerStatus(peerId, match, lag, isAlive));
        }
        return result;
    }

    public String getNodeId() {
        return raftNode.getNodeId();
    }

    public RaftRole getRole() {
        return raftNode.getRole();
    }

    public long getCurrentTerm() {
        return raftNode.getCurrentTerm();
    }

    public long getCommitIndex() {
        return raftNode.getCommitIndex();
    }
}
