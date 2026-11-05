package io.nexusdb.distributed.consensus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

/**
 * Deterministic in-memory cluster simulator driving a group of {@link RaftNode}s with
 * a shared {@link SimClock}, seeded {@link RaftTimer}, and fault-injectable {@link SimTransport}.
 */
public final class SimCluster {
    public static final long DEFAULT_MIN_ELECTION_TIMEOUT_MS = 150L;
    public static final long DEFAULT_MAX_ELECTION_TIMEOUT_MS = 300L;
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 40L;

    private final List<String> nodeIds;
    private final SimClock clock;
    private final RaftTimer timer;
    private final SimTransport transport;
    private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
    private final Map<Long, String> electedLeaderByTerm = new LinkedHashMap<>();

    public SimCluster(List<String> nodeIds, long seed) {
        this(
                nodeIds,
                seed,
                DEFAULT_MIN_ELECTION_TIMEOUT_MS,
                DEFAULT_MAX_ELECTION_TIMEOUT_MS,
                DEFAULT_HEARTBEAT_INTERVAL_MS);
    }

    public SimCluster(
            List<String> nodeIds,
            long seed,
            long minElectionTimeoutMs,
            long maxElectionTimeoutMs,
            long heartbeatIntervalMs) {
        Objects.requireNonNull(nodeIds, "nodeIds must not be null");
        if (nodeIds.isEmpty()) {
            throw new IllegalArgumentException("nodeIds must not be empty");
        }
        if (minElectionTimeoutMs <= 0 || maxElectionTimeoutMs < minElectionTimeoutMs || heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("Invalid timeout configuration");
        }
        this.nodeIds = List.copyOf(nodeIds);
        this.clock = new SimClock(0L);
        Random timerRandom = new Random(seed ^ 0x9E3779B97F4A7C15L);
        this.timer = new RaftTimer() {
            @Override
            public long nextElectionTimeoutMillis(String nodeId) {
                long span = maxElectionTimeoutMs - minElectionTimeoutMs + 1L;
                return minElectionTimeoutMs + timerRandom.nextLong(span);
            }

            @Override
            public long heartbeatIntervalMillis() {
                return heartbeatIntervalMs;
            }
        };
        this.transport = new SimTransport(clock, seed);

        for (String id : this.nodeIds) {
            nodes.put(id, new RaftNode(id, this.nodeIds, clock, timer, transport));
        }
    }

    public SimClock getClock() {
        return clock;
    }

    public RaftTimer getTimer() {
        return timer;
    }

    public SimTransport getTransport() {
        return transport;
    }

    public List<String> getNodeIds() {
        return nodeIds;
    }

    public RaftNode getNode(String nodeId) {
        RaftNode n = nodes.get(nodeId);
        if (n == null) {
            throw new IllegalArgumentException("Unknown nodeId: " + nodeId);
        }
        return n;
    }

    public List<RaftNode> getNodes() {
        return List.copyOf(nodes.values());
    }

    public List<RaftNode> getLiveNodes() {
        List<RaftNode> live = new ArrayList<>();
        for (RaftNode n : nodes.values()) {
            if (!transport.isNodeCrashed(n.getNodeId())) {
                live.add(n);
            }
        }
        return live;
    }

    public void crashNode(String nodeId) {
        getNode(nodeId);
        transport.markNodeCrashed(nodeId);
    }

    public void restartNode(String nodeId) {
        getNode(nodeId);
        transport.markNodeRecovered(nodeId);
    }

    public void partition(Set<String> sideA, Set<String> sideB) {
        transport.partition(sideA, sideB);
    }

    public void isolateNode(String nodeId) {
        getNode(nodeId);
        transport.isolateNode(nodeId, nodeIds);
    }

    public void healPartitions() {
        transport.healAllPartitions();
    }

    /**
     * Advances the simulation by 1 millisecond, delivers all due messages, ticks all live nodes,
     * and verifies Election Safety (at most one leader per term).
     */
    public void step() {
        stepMillis(1L);
    }

    /**
     * Advances the simulation by {@code totalMillis} in 1-millisecond increments.
     */
    public void stepMillis(long totalMillis) {
        if (totalMillis < 0) {
            throw new IllegalArgumentException("totalMillis must be >= 0");
        }
        for (long i = 0; i < totalMillis; i++) {
            clock.advanceMillis(1L);
            transport.deliverDueMessages((env, msg) -> {
                RaftNode target = nodes.get(env.toNodeId());
                if (target != null && !transport.isNodeCrashed(target.getNodeId())) {
                    target.handleMessage(env.fromNodeId(), msg);
                    verifyElectionSafety();
                }
            });
            for (RaftNode node : nodes.values()) {
                if (!transport.isNodeCrashed(node.getNodeId())) {
                    node.tick();
                    verifyElectionSafety();
                }
            }
            transport.deliverDueMessages((env, msg) -> {
                RaftNode target = nodes.get(env.toNodeId());
                if (target != null && !transport.isNodeCrashed(target.getNodeId())) {
                    target.handleMessage(env.fromNodeId(), msg);
                    verifyElectionSafety();
                }
            });
        }
    }

    /**
     * Verifies Raft Election Safety invariant: at most one leader can be elected in any given term.
     */
    public void verifyElectionSafety() {
        for (RaftNode node : nodes.values()) {
            if (node.getRole() == RaftRole.LEADER) {
                long term = node.getCurrentTerm();
                String existing = electedLeaderByTerm.putIfAbsent(term, node.getNodeId());
                if (existing != null && !existing.equals(node.getNodeId())) {
                    throw new AssertionError(
                            "Election Safety violation in term " + term
                                    + ": both '" + existing + "' and '" + node.getNodeId() + "' became LEADER");
                }
            }
        }
    }

    /**
     * Returns all live nodes that currently consider themselves {@link RaftRole#LEADER} at the highest
     * active term among live nodes.
     */
    public List<RaftNode> getCurrentLeaders() {
        long maxTerm = 0L;
        for (RaftNode node : getLiveNodes()) {
            if (node.getCurrentTerm() > maxTerm) {
                maxTerm = node.getCurrentTerm();
            }
        }
        List<RaftNode> leaders = new ArrayList<>();
        for (RaftNode node : getLiveNodes()) {
            if (node.getRole() == RaftRole.LEADER && node.getCurrentTerm() == maxTerm) {
                leaders.add(node);
            }
        }
        return leaders;
    }

    /**
     * Steps the cluster up to {@code maxMillis} until a single leader is elected in the highest live term,
     * and returns that leader (or {@code null} if no leader is elected within {@code maxMillis}).
     */
    public RaftNode awaitLeader(long maxMillis) {
        for (long elapsed = 0; elapsed < maxMillis; elapsed++) {
            List<RaftNode> leaders = getCurrentLeaders();
            if (leaders.size() == 1) {
                return leaders.get(0);
            }
            stepMillis(1L);
        }
        List<RaftNode> leaders = getCurrentLeaders();
        return leaders.size() == 1 ? leaders.get(0) : null;
    }

    public Map<Long, String> getElectedLeaderByTerm() {
        return Collections.unmodifiableMap(electedLeaderByTerm);
    }
}
