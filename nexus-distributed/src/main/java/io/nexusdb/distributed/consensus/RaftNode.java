package io.nexusdb.distributed.consensus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Deterministic single-threaded Raft consensus state machine driving leader election and heartbeats
 * over injectable {@link RaftClock}, {@link RaftTimer}, and {@link RaftTransport} abstractions.
 *
 * <p>Consensus Safety Invariants (Raft §5.1, §5.2, §5.4.1):
 * <ul>
 *   <li><b>Monotonic Term & Single Vote Per Term</b>: {@code currentTerm} never decreases. Whenever a node
 *       observes an RPC request or response with {@code msg.term() > currentTerm}, it immediately updates
 *       {@code currentTerm = msg.term()}, transitions to {@link RaftRole#FOLLOWER}, and clears {@code votedFor}.
 *       Within any term, a node grants its vote to at most one candidate ({@code votedFor == null || votedFor.equals(candidateId)}).</li>
 *   <li><b>Election Safety</b>: A candidate transitions to {@link RaftRole#LEADER} in {@code currentTerm}
 *       only after receiving {@code RequestVoteResponse(voteGranted=true)} from a strict majority
 *       ({@code (clusterSize / 2) + 1}) of peers for {@code requestTerm == currentTerm}. Because majorities
 *       intersect in at least one voter and each voter grants at most one vote per term, at most one leader
 *       can be elected per term.</li>
 *   <li><b>Election Restriction (§5.4.1 Log Up-to-Date Check)</b>: A voter grants a vote to a candidate
 *       only if the candidate's log is at least as up-to-date as the voter's own log, where logs are compared
 *       first by {@code lastLogTerm} and then by {@code lastLogIndex}.</li>
 * </ul>
 */
public final class RaftNode {
    private final String nodeId;
    private final List<String> peerIds;
    private final RaftClock clock;
    private final RaftTimer timer;
    private final RaftTransport transport;

    // Persistent Raft state (in-memory in Task 30; backed by RaftStorage in Task 31)
    private long currentTerm = 0L;
    private String votedFor = null;
    private final List<RaftLogEntry> log = new ArrayList<>();

    // Volatile Raft state
    private RaftRole role = RaftRole.FOLLOWER;
    private String leaderId = null;
    private long commitIndex = 0L;
    private long lastApplied = 0L;

    // Candidate vote tracking for currentTerm
    private final Set<String> votesReceived = new HashSet<>();

    // Deadlines on RaftClock
    private long electionDeadlineMillis;
    private long nextHeartbeatMillis;

    public RaftNode(
            String nodeId,
            List<String> allClusterNodeIds,
            RaftClock clock,
            RaftTimer timer,
            RaftTransport transport) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(allClusterNodeIds, "allClusterNodeIds must not be null");
        if (!allClusterNodeIds.contains(nodeId)) {
            throw new IllegalArgumentException("allClusterNodeIds must include this node's id: " + nodeId);
        }
        this.peerIds = allClusterNodeIds.stream()
                .filter(id -> !id.equals(nodeId))
                .toList();
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.timer = Objects.requireNonNull(timer, "timer must not be null");
        this.transport = Objects.requireNonNull(transport, "transport must not be null");
        resetElectionDeadline();
    }

    public String getNodeId() {
        return nodeId;
    }

    public List<String> getPeerIds() {
        return peerIds;
    }

    public int getClusterSize() {
        return peerIds.size() + 1;
    }

    public int getQuorumSize() {
        return (getClusterSize() / 2) + 1;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public RaftRole getRole() {
        return role;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public long getLastLogIndex() {
        return log.isEmpty() ? 0L : log.get(log.size() - 1).index();
    }

    public long getLastLogTerm() {
        return log.isEmpty() ? 0L : log.get(log.size() - 1).term();
    }

    public List<RaftLogEntry> getLogEntries() {
        return Collections.unmodifiableList(log);
    }

    public long getElectionDeadlineMillis() {
        return electionDeadlineMillis;
    }

    /**
     * Seeds pre-existing log entries on this node (used by election restriction tests and recovery).
     */
    public void seedLogEntries(List<RaftLogEntry> entries) {
        Objects.requireNonNull(entries, "entries must not be null");
        log.clear();
        for (int i = 0; i < entries.size(); i++) {
            RaftLogEntry e = entries.get(i);
            if (e.index() != i + 1L) {
                throw new IllegalArgumentException("Expected contiguous 1-based index " + (i + 1L) + ", got " + e.index());
            }
            log.add(e);
            if (e.term() > currentTerm) {
                currentTerm = e.term();
                votedFor = null;
            }
        }
    }

    /**
     * Advances the state machine according to the current {@link RaftClock#nowMillis()}.
     * Triggers an election if {@link #electionDeadlineMillis} has elapsed on a follower/candidate,
     * or sends heartbeats if {@link #nextHeartbeatMillis} has elapsed on a leader.
     */
    public void tick() {
        long now = clock.nowMillis();
        if (role == RaftRole.LEADER) {
            if (now >= nextHeartbeatMillis) {
                sendHeartbeats();
                nextHeartbeatMillis = now + timer.heartbeatIntervalMillis();
            }
        } else if (now >= electionDeadlineMillis) {
            startElection();
        }
    }

    /**
     * Transitions this node to {@link RaftRole#CANDIDATE}, increments {@code currentTerm},
     * votes for itself, resets the randomized election timer, and broadcasts {@code RequestVoteRequest}.
     */
    public void startElection() {
        this.role = RaftRole.CANDIDATE;
        this.currentTerm++;
        this.votedFor = nodeId;
        this.leaderId = null;
        this.votesReceived.clear();
        this.votesReceived.add(nodeId);
        resetElectionDeadline();

        if (votesReceived.size() >= getQuorumSize()) {
            becomeLeader();
            return;
        }

        long lastLogIdx = getLastLogIndex();
        long lastLogTerm = getLastLogTerm();
        RaftMessage.RequestVoteRequest req = new RaftMessage.RequestVoteRequest(
                currentTerm,
                nodeId,
                lastLogIdx,
                lastLogTerm);
        for (String peerId : peerIds) {
            transport.send(nodeId, peerId, req);
        }
    }

    /**
     * Processes an incoming {@link RaftMessage} from {@code fromNodeId}.
     */
    public void handleMessage(String fromNodeId, RaftMessage message) {
        Objects.requireNonNull(fromNodeId, "fromNodeId must not be null");
        Objects.requireNonNull(message, "message must not be null");

        if (message.term() > currentTerm) {
            stepDownToFollower(message.term(), null);
        }

        switch (message) {
            case RaftMessage.RequestVoteRequest req -> onRequestVoteRequest(fromNodeId, req);
            case RaftMessage.RequestVoteResponse resp -> onRequestVoteResponse(fromNodeId, resp);
            case RaftMessage.AppendEntriesRequest req -> onAppendEntriesRequest(fromNodeId, req);
            case RaftMessage.AppendEntriesResponse resp -> onAppendEntriesResponse(fromNodeId, resp);
        }
    }

    private void onRequestVoteRequest(String fromNodeId, RaftMessage.RequestVoteRequest req) {
        if (req.term() < currentTerm) {
            transport.send(
                    nodeId,
                    fromNodeId,
                    new RaftMessage.RequestVoteResponse(currentTerm, nodeId, req.term(), false));
            return;
        }

        boolean canVoteForCandidate = (votedFor == null || votedFor.equals(req.candidateId()));
        boolean logUpToDate = isCandidateLogUpToDate(req.lastLogTerm(), req.lastLogIndex());

        if (canVoteForCandidate && logUpToDate) {
            votedFor = req.candidateId();
            resetElectionDeadline();
            transport.send(
                    nodeId,
                    fromNodeId,
                    new RaftMessage.RequestVoteResponse(currentTerm, nodeId, req.term(), true));
        } else {
            transport.send(
                    nodeId,
                    fromNodeId,
                    new RaftMessage.RequestVoteResponse(currentTerm, nodeId, req.term(), false));
        }
    }

    /**
     * Raft §5.4.1 Election Restriction:
     * Determines whether a candidate's log is at least as up-to-date as this voter's log.
     */
    public boolean isCandidateLogUpToDate(long candidateLastTerm, long candidateLastIndex) {
        long myLastTerm = getLastLogTerm();
        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= getLastLogIndex();
    }

    private void onRequestVoteResponse(String fromNodeId, RaftMessage.RequestVoteResponse resp) {
        if (role != RaftRole.CANDIDATE) {
            return;
        }
        if (resp.requestTerm() != currentTerm || resp.term() != currentTerm) {
            return;
        }
        if (resp.voteGranted()) {
            votesReceived.add(resp.voterId());
            if (votesReceived.size() >= getQuorumSize()) {
                becomeLeader();
            }
        }
    }

    private void onAppendEntriesRequest(String fromNodeId, RaftMessage.AppendEntriesRequest req) {
        if (req.term() < currentTerm) {
            transport.send(
                    nodeId,
                    fromNodeId,
                    new RaftMessage.AppendEntriesResponse(currentTerm, nodeId, req.term(), false, 0L));
            return;
        }

        // Valid leader in currentTerm: recognize leader, step down if CANDIDATE, and reset election deadline
        if (role != RaftRole.FOLLOWER) {
            role = RaftRole.FOLLOWER;
            votesReceived.clear();
        }
        leaderId = req.leaderId();
        resetElectionDeadline();

        transport.send(
                nodeId,
                fromNodeId,
                new RaftMessage.AppendEntriesResponse(currentTerm, nodeId, req.term(), true, getLastLogIndex()));
    }

    private void onAppendEntriesResponse(String fromNodeId, RaftMessage.AppendEntriesResponse resp) {
        // Full log replication matchIndex/nextIndex handling is added in Task 31.
    }

    private void becomeLeader() {
        this.role = RaftRole.LEADER;
        this.leaderId = nodeId;
        this.votesReceived.clear();
        sendHeartbeats();
        this.nextHeartbeatMillis = clock.nowMillis() + timer.heartbeatIntervalMillis();
    }

    private void sendHeartbeats() {
        long prevIdx = getLastLogIndex();
        long prevTerm = getLastLogTerm();
        RaftMessage.AppendEntriesRequest heartbeat = RaftMessage.AppendEntriesRequest.heartbeat(
                currentTerm,
                nodeId,
                prevIdx,
                prevTerm,
                commitIndex);
        for (String peerId : peerIds) {
            transport.send(nodeId, peerId, heartbeat);
        }
    }

    private void stepDownToFollower(long newTerm, String knownLeaderId) {
        this.currentTerm = newTerm;
        this.role = RaftRole.FOLLOWER;
        this.votedFor = null;
        this.leaderId = knownLeaderId;
        this.votesReceived.clear();
        resetElectionDeadline();
    }

    private void resetElectionDeadline() {
        this.electionDeadlineMillis = clock.nowMillis() + timer.nextElectionTimeoutMillis(nodeId);
    }
}
