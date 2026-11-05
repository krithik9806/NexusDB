package io.nexusdb.distributed.consensus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Deterministic single-threaded Raft consensus state machine driving leader election, log replication,
 * commit advancement, durable storage, and state-machine application over injectable {@link RaftClock},
 * {@link RaftTimer}, {@link RaftTransport}, {@link RaftStorage}, and {@link StateMachine} abstractions.
 *
 * <p>Consensus & Crash-Safety Invariants (Raft §5.1–§5.4):
 * <ul>
 *   <li><b>Durable State Before RPCs (Figure 2)</b>: Every mutation to {@code currentTerm}, {@code votedFor},
 *       or {@code log[]} is persisted to {@link RaftStorage} before sending or replying to any RPC.</li>
 *   <li><b>Election Safety (§5.2)</b>: At most one leader can be elected in a given term because a candidate
 *       must collect votes from a strict majority of the cluster in {@code currentTerm}, and each voter
 *       persists at most one {@code votedFor} per term.</li>
 *   <li><b>Log Matching (§5.3)</b>: {@code AppendEntries} verifies that the follower contains an entry at
 *       {@code prevLogIndex} whose term equals {@code prevLogTerm}. On mismatch, the follower rejects the RPC
 *       and hints {@code conflictIndex}; on match, the follower deletes an existing entry only if a new entry
 *       at the same index has a different term, preserving idempotence under duplicate or out-of-order RPCs.</li>
 *   <li><b>Current-Term Commit Rule (§5.4.2)</b>: A leader NEVER commits log entries from previous terms by
 *       counting replicas alone. {@link #advanceLeaderCommitIndex()} advances {@code commitIndex} to index
 *       {@code N} if and only if a majority of peers have {@code matchIndex >= N} <b>and</b>
 *       {@code entryAt(N).term() == currentTerm}. Prior-term entries are committed indirectly once a
 *       current-term entry at a higher index is committed.</li>
 *   <li><b>State Machine Safety (§5.3)</b>: Entries are applied to {@link StateMachine#apply(long, byte[])}
 *       strictly sequentially via {@code while (lastApplied < commitIndex)}. Uncommitted entries are never
 *       applied.</li>
 * </ul>
 */
public final class RaftNode {

    private record PendingProposal(long term, CompletableFuture<byte[]> future) {
    }

    private final String nodeId;
    private final List<String> peerIds;
    private final RaftClock clock;
    private final RaftTimer timer;
    private final RaftTransport transport;
    private final RaftStorage storage;
    private final StateMachine stateMachine;

    // Persistent Raft state (mirrored in memory and backed by RaftStorage)
    private long currentTerm = 0L;
    private String votedFor = null;
    private final List<RaftLogEntry> log = new ArrayList<>();

    // Volatile state on all servers
    private RaftRole role = RaftRole.FOLLOWER;
    private String leaderId = null;
    private long commitIndex = 0L;
    private long lastApplied = 0L;

    // Volatile state on candidates
    private final Set<String> votesReceived = new HashSet<>();

    // Volatile state on leaders (reinitialized after election)
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Map<Long, PendingProposal> pendingProposals = new LinkedHashMap<>();

    // Deadlines on RaftClock
    private long electionDeadlineMillis;
    private long nextHeartbeatMillis;

    public RaftNode(
            String nodeId,
            List<String> allClusterNodeIds,
            RaftClock clock,
            RaftTimer timer,
            RaftTransport transport) {
        this(
                nodeId,
                allClusterNodeIds,
                clock,
                timer,
                transport,
                new InMemoryRaftStorage(),
                StateMachine.noop());
    }

    public RaftNode(
            String nodeId,
            List<String> allClusterNodeIds,
            RaftClock clock,
            RaftTimer timer,
            RaftTransport transport,
            RaftStorage storage,
            StateMachine stateMachine) {
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
        this.storage = Objects.requireNonNull(storage, "storage must not be null");
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine must not be null");

        RaftStorage.PersistentState loaded = this.storage.load();
        this.currentTerm = loaded.currentTerm();
        this.votedFor = loaded.votedFor();
        this.log.addAll(loaded.log());
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

    public RaftStorage getStorage() {
        return storage;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public long getLastLogIndex() {
        return log.isEmpty() ? 0L : log.get(log.size() - 1).index();
    }

    public long getLastLogTerm() {
        return log.isEmpty() ? 0L : log.get(log.size() - 1).term();
    }

    public long getTermAt(long index) {
        if (index == 0L) {
            return 0L;
        }
        if (index < 1L || index > log.size()) {
            return -1L;
        }
        return log.get((int) (index - 1)).term();
    }

    public RaftLogEntry getEntryAt(long index) {
        if (index < 1L || index > log.size()) {
            throw new IndexOutOfBoundsException("Log index out of range: " + index + " (size=" + log.size() + ")");
        }
        return log.get((int) (index - 1));
    }

    public List<RaftLogEntry> getLogEntries() {
        return Collections.unmodifiableList(log);
    }

    public long getNextIndexForPeer(String peerId) {
        return nextIndex.getOrDefault(peerId, 1L);
    }

    public long getMatchIndexForPeer(String peerId) {
        return matchIndex.getOrDefault(peerId, 0L);
    }

    public long getElectionDeadlineMillis() {
        return electionDeadlineMillis;
    }

    /**
     * Seeds pre-existing log entries on this node and persists them to {@link RaftStorage}.
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
        storage.saveTermAndVote(currentTerm, votedFor);
        storage.truncateAndAppend(1L, entries);
    }

    /**
     * Proposes a client command to be replicated in the Raft log and applied to the {@link StateMachine}.
     *
     * <p>The returned {@link CompletableFuture} completes with the state-machine result only after the
     * entry is committed by a majority quorum and applied to {@link StateMachine#apply(long, byte[])}.
     * If this node is not currently {@link RaftRole#LEADER} or steps down/loses its entry before commit,
     * the future completes exceptionally with {@link IllegalStateException}.
     */
    public CompletableFuture<byte[]> propose(byte[] command) {
        Objects.requireNonNull(command, "command must not be null");
        if (role != RaftRole.LEADER) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Node '" + nodeId + "' is not LEADER (role=" + role + ", leaderId=" + leaderId + ")"));
        }

        long entryIndex = getLastLogIndex() + 1L;
        RaftLogEntry entry = new RaftLogEntry(entryIndex, currentTerm, command);
        log.add(entry);
        storage.appendEntries(List.of(entry));

        CompletableFuture<byte[]> future = new CompletableFuture<>();
        pendingProposals.put(entryIndex, new PendingProposal(currentTerm, future));

        if (peerIds.isEmpty()) {
            advanceLeaderCommitIndex();
        } else {
            broadcastAppendEntries();
            nextHeartbeatMillis = clock.nowMillis() + timer.heartbeatIntervalMillis();
        }
        return future;
    }

    /**
     * Advances the state machine according to the current {@link RaftClock#nowMillis()}.
     */
    public void tick() {
        long now = clock.nowMillis();
        if (role == RaftRole.LEADER) {
            if (now >= nextHeartbeatMillis) {
                broadcastAppendEntries();
                nextHeartbeatMillis = now + timer.heartbeatIntervalMillis();
            }
        } else if (now >= electionDeadlineMillis) {
            startElection();
        }
    }

    /**
     * Transitions this node to {@link RaftRole#CANDIDATE}, increments {@code currentTerm},
     * persists the self-vote, resets the randomized election timer, and broadcasts {@code RequestVoteRequest}.
     */
    public void startElection() {
        this.role = RaftRole.CANDIDATE;
        this.currentTerm++;
        this.votedFor = nodeId;
        this.leaderId = null;
        this.votesReceived.clear();
        this.votesReceived.add(nodeId);
        this.storage.saveTermAndVote(this.currentTerm, this.votedFor);
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
            storage.saveTermAndVote(currentTerm, votedFor);
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
                    new RaftMessage.AppendEntriesResponse(currentTerm, nodeId, req.term(), false, 0L, 1L));
            return;
        }

        // Valid leader in currentTerm: recognize leader, step down if CANDIDATE, and reset election deadline
        if (role != RaftRole.FOLLOWER) {
            role = RaftRole.FOLLOWER;
            votesReceived.clear();
            failAllPendingProposals("Stepped down to FOLLOWER in term " + currentTerm);
        }
        leaderId = req.leaderId();
        resetElectionDeadline();

        // Raft §5.3 Log Consistency Check: verify entry at prevLogIndex matches prevLogTerm
        long prevIdx = req.prevLogIndex();
        if (prevIdx > 0L) {
            if (prevIdx > getLastLogIndex()) {
                long conflictIdx = getLastLogIndex() + 1L;
                transport.send(
                        nodeId,
                        fromNodeId,
                        new RaftMessage.AppendEntriesResponse(currentTerm, nodeId, req.term(), false, 0L, conflictIdx));
                return;
            }
            long localTermAtPrev = getTermAt(prevIdx);
            if (localTermAtPrev != req.prevLogTerm()) {
                // Fast conflict backtracking: find the first index in our log with localTermAtPrev
                long conflictIdx = prevIdx;
                while (conflictIdx > 1L && getTermAt(conflictIdx - 1L) == localTermAtPrev) {
                    conflictIdx--;
                }
                transport.send(
                        nodeId,
                        fromNodeId,
                        new RaftMessage.AppendEntriesResponse(
                                currentTerm, nodeId, req.term(), false, 0L, Math.max(1L, conflictIdx)));
                return;
            }
        }

        // Append any new entries not already in the log, truncating only if an existing entry conflicts (Raft Figure 2)
        List<RaftLogEntry> incoming = req.entries();
        if (!incoming.isEmpty()) {
            int firstNewOrConflictOffset = -1;
            boolean requiresTruncate = false;

            for (int i = 0; i < incoming.size(); i++) {
                RaftLogEntry entry = incoming.get(i);
                long targetIdx = entry.index();
                if (targetIdx <= getLastLogIndex()) {
                    if (getTermAt(targetIdx) != entry.term()) {
                        firstNewOrConflictOffset = i;
                        requiresTruncate = true;
                        break;
                    }
                } else {
                    firstNewOrConflictOffset = i;
                    break;
                }
            }

            if (firstNewOrConflictOffset >= 0) {
                List<RaftLogEntry> suffixToAppend = incoming.subList(firstNewOrConflictOffset, incoming.size());
                long fromIndex = suffixToAppend.get(0).index();
                if (requiresTruncate) {
                    while (log.size() >= fromIndex) {
                        RaftLogEntry removed = log.remove(log.size() - 1);
                        failProposalAt(removed.index(), "Conflicting uncommitted log entry overwritten at index " + removed.index());
                    }
                    log.addAll(suffixToAppend);
                    storage.truncateAndAppend(fromIndex, suffixToAppend);
                } else {
                    log.addAll(suffixToAppend);
                    storage.appendEntries(suffixToAppend);
                }
            }
        }

        long lastNewEntryIndex = prevIdx + incoming.size();
        if (req.leaderCommit() > commitIndex) {
            commitIndex = Math.min(req.leaderCommit(), Math.min(lastNewEntryIndex, getLastLogIndex()));
            applyCommittedEntries();
        }

        long matched = Math.min(lastNewEntryIndex, getLastLogIndex());
        transport.send(
                nodeId,
                fromNodeId,
                new RaftMessage.AppendEntriesResponse(currentTerm, nodeId, req.term(), true, matched, matched + 1L));
    }

    private void onAppendEntriesResponse(String fromNodeId, RaftMessage.AppendEntriesResponse resp) {
        if (role != RaftRole.LEADER) {
            return;
        }
        if (resp.requestTerm() != currentTerm || resp.term() != currentTerm) {
            return;
        }

        String peerId = resp.followerId();
        if (resp.success()) {
            long prevMatch = matchIndex.getOrDefault(peerId, 0L);
            if (resp.matchIndex() > prevMatch) {
                matchIndex.put(peerId, resp.matchIndex());
            }
            nextIndex.put(peerId, Math.max(nextIndex.getOrDefault(peerId, 1L), resp.matchIndex() + 1L));
            advanceLeaderCommitIndex();
        } else {
            long currentNext = nextIndex.getOrDefault(peerId, getLastLogIndex() + 1L);
            long hinted = Math.max(1L, resp.conflictIndex());
            long newNext = Math.max(1L, Math.min(currentNext - 1L, hinted));
            nextIndex.put(peerId, newNext);
            sendAppendEntriesToPeer(peerId);
        }
    }

    /**
     * Raft §5.4.2 Commit Rule:
     * Advances {@code commitIndex} to the highest index {@code N > commitIndex} replicated on a majority
     * of the cluster IF AND ONLY IF {@code log[N].term() == currentTerm}.
     */
    private void advanceLeaderCommitIndex() {
        long lastIdx = getLastLogIndex();
        long newCommitIndex = commitIndex;

        for (long n = lastIdx; n > commitIndex; n--) {
            if (getTermAt(n) != currentTerm) {
                continue;
            }
            int replicas = 1; // Leader itself has the entry
            for (String peerId : peerIds) {
                if (matchIndex.getOrDefault(peerId, 0L) >= n) {
                    replicas++;
                }
            }
            if (replicas >= getQuorumSize()) {
                newCommitIndex = n;
                break;
            }
        }

        if (newCommitIndex > commitIndex) {
            commitIndex = newCommitIndex;
            applyCommittedEntries();
            // Immediately broadcast updated leaderCommit to followers
            broadcastAppendEntries();
        }
    }

    /**
     * Deterministic apply loop (Raft Figure 2: {@code while (lastApplied < commitIndex)}).
     */
    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            RaftLogEntry entry = getEntryAt(lastApplied);
            byte[] result = stateMachine.apply(entry.index(), entry.command());

            PendingProposal pending = pendingProposals.remove(entry.index());
            if (pending != null) {
                if (pending.term() == entry.term()) {
                    pending.future().complete(result);
                } else {
                    pending.future().completeExceptionally(
                            new IllegalStateException(
                                    "Proposal at index " + entry.index() + " from term " + pending.term()
                                            + " was overwritten by term " + entry.term()));
                }
            }
        }
    }

    private void becomeLeader() {
        this.role = RaftRole.LEADER;
        this.leaderId = nodeId;
        this.votesReceived.clear();

        long initNext = getLastLogIndex() + 1L;
        this.nextIndex.clear();
        this.matchIndex.clear();
        for (String peerId : peerIds) {
            nextIndex.put(peerId, initNext);
            matchIndex.put(peerId, 0L);
        }

        broadcastAppendEntries();
        this.nextHeartbeatMillis = clock.nowMillis() + timer.heartbeatIntervalMillis();
    }

    public void broadcastAppendEntries() {
        for (String peerId : peerIds) {
            sendAppendEntriesToPeer(peerId);
        }
    }

    private void sendAppendEntriesToPeer(String peerId) {
        long peerNextIdx = Math.max(1L, nextIndex.getOrDefault(peerId, getLastLogIndex() + 1L));
        long prevIdx = peerNextIdx - 1L;
        long prevTerm = prevIdx == 0L ? 0L : getTermAt(prevIdx);

        List<RaftLogEntry> entriesToSend;
        long lastIdx = getLastLogIndex();
        if (peerNextIdx <= lastIdx) {
            entriesToSend = List.copyOf(log.subList((int) (peerNextIdx - 1L), (int) lastIdx));
        } else {
            entriesToSend = List.of();
        }

        RaftMessage.AppendEntriesRequest req = new RaftMessage.AppendEntriesRequest(
                currentTerm,
                nodeId,
                prevIdx,
                prevTerm,
                entriesToSend,
                commitIndex);
        transport.send(nodeId, peerId, req);
    }

    private void stepDownToFollower(long newTerm, String knownLeaderId) {
        boolean termChanged = newTerm != this.currentTerm;
        this.currentTerm = newTerm;
        this.role = RaftRole.FOLLOWER;
        if (termChanged) {
            this.votedFor = null;
            this.storage.saveTermAndVote(this.currentTerm, null);
        }
        this.leaderId = knownLeaderId;
        this.votesReceived.clear();
        failAllPendingProposals("Node '" + nodeId + "' stepped down to FOLLOWER in term " + newTerm);
        resetElectionDeadline();
    }

    private void failProposalAt(long index, String reason) {
        PendingProposal p = pendingProposals.remove(index);
        if (p != null && !p.future().isDone()) {
            p.future().completeExceptionally(new IllegalStateException(reason));
        }
    }

    private void failAllPendingProposals(String reason) {
        if (pendingProposals.isEmpty()) {
            return;
        }
        Iterator<Map.Entry<Long, PendingProposal>> it = pendingProposals.entrySet().iterator();
        while (it.hasNext()) {
            PendingProposal p = it.next().getValue();
            it.remove();
            if (!p.future().isDone()) {
                p.future().completeExceptionally(new IllegalStateException(reason));
            }
        }
    }

    private void resetElectionDeadline() {
        this.electionDeadlineMillis = clock.nowMillis() + timer.nextElectionTimeoutMillis(nodeId);
    }
}
