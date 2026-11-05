package io.nexusdb.distributed.consensus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class RaftChaosTest {

    @TempDir
    Path tempDir;

    private static class RecordingStateMachine implements StateMachine {
        private final List<byte[]> appliedCommands = new ArrayList<>();
        private final Map<Long, byte[]> appliedByIndex = new HashMap<>();

        @Override
        public synchronized byte[] apply(long index, byte[] command) {
            byte[] existing = appliedByIndex.put(index, command);
            if (existing != null && !Arrays.equals(existing, command)) {
                throw new AssertionError("State Machine Safety violated at index " + index);
            }
            appliedCommands.add(command);
            return ("ACK:" + index).getBytes(StandardCharsets.UTF_8);
        }

        public synchronized List<byte[]> getAppliedCommands() {
            return new ArrayList<>(appliedCommands);
        }

        public synchronized Map<Long, byte[]> getAppliedByIndex() {
            return new HashMap<>(appliedByIndex);
        }
    }

    @Test
    void randomizedChaosAcrossMultipleSeeds() {
        long[] seeds = {42L, 1337L, 2026L, 9999L};
        for (long seed : seeds) {
            try {
                runChaosSimulation(seed, 2500);
            } catch (Throwable t) {
                System.err.println("Chaos test failed on SEED: " + seed);
                throw new AssertionError("Chaos test failed on seed: " + seed, t);
            }
        }
    }

    private void runChaosSimulation(long seed, int steps) {
        List<String> nodeIds = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
        Map<String, RecordingStateMachine> stateMachines = new ConcurrentHashMap<>();
        Map<String, Path> storageDirs = new HashMap<>();
        for (String id : nodeIds) {
            stateMachines.put(id, new RecordingStateMachine());
            storageDirs.put(id, tempDir.resolve("chaos-" + seed + "-" + id));
        }

        try (SimCluster cluster = new SimCluster(
                nodeIds,
                seed,
                id -> new FileRaftStorage(storageDirs.get(id)),
                stateMachines::get)) {

            Random rng = new Random(seed);
            Set<String> crashedNodes = new HashSet<>();
            boolean partitioned = false;
            int proposalCounter = 0;

            for (int step = 0; step < steps; step++) {
                int action = rng.nextInt(100);

                // 1. Propose command if there is an active leader
                if (action < 20) {
                    List<RaftNode> leaders = cluster.getCurrentLeaders();
                    if (!leaders.isEmpty()) {
                        RaftNode leader = leaders.get(0);
                        if (!crashedNodes.contains(leader.getNodeId())) {
                            proposalCounter++;
                            byte[] cmd = ("cmd-" + seed + "-" + proposalCounter).getBytes(StandardCharsets.UTF_8);
                            try {
                                leader.propose(cmd);
                            } catch (Exception ignored) {
                            }
                        }
                    }
                }
                // 2. Crash a random node (keep at most 2 nodes crashed to retain quorum)
                else if (action < 28 && crashedNodes.size() < 2) {
                    List<String> alive = nodeIds.stream().filter(id -> !crashedNodes.contains(id)).toList();
                    if (alive.size() > 3) {
                        String victim = alive.get(rng.nextInt(alive.size()));
                        cluster.crashNode(victim);
                        crashedNodes.add(victim);
                    }
                }
                // 3. Restart a crashed node
                else if (action < 38 && !crashedNodes.isEmpty()) {
                    List<String> crashedList = new ArrayList<>(crashedNodes);
                    String revive = crashedList.get(rng.nextInt(crashedList.size()));
                    cluster.restartNode(revive);
                    crashedNodes.remove(revive);
                }
                // 4. Create or heal network partition
                else if (action < 46) {
                    if (!partitioned) {
                        // 3 nodes on side A, 2 nodes on side B
                        Set<String> sideA = Set.of(nodeIds.get(0), nodeIds.get(1), nodeIds.get(2));
                        Set<String> sideB = Set.of(nodeIds.get(3), nodeIds.get(4));
                        cluster.partition(sideA, sideB);
                        partitioned = true;
                    } else {
                        cluster.healPartitions();
                        partitioned = false;
                    }
                }
                // 5. Normal tick step
                else {
                    cluster.step();
                }

                // Check Raft safety invariants on every step
                verifyInvariants(cluster, stateMachines, seed, step);
            }

            // Heal and revive all nodes at end of test to allow cluster to converge
            if (partitioned) {
                cluster.healPartitions();
            }
            for (String crashed : new ArrayList<>(crashedNodes)) {
                cluster.restartNode(crashed);
            }
            cluster.awaitLeader(3000);
            verifyInvariants(cluster, stateMachines, seed, steps);
        }
    }

    private void verifyInvariants(
            SimCluster cluster,
            Map<String, RecordingStateMachine> stateMachines,
            long seed,
            int step) {
        List<RaftNode> nodes = cluster.getNodes();

        // 1. Election Safety: at most one leader per term
        Map<Long, String> leadersByTerm = new HashMap<>();
        for (RaftNode node : nodes) {
            if (node.getRole() == RaftRole.LEADER) {
                long term = node.getCurrentTerm();
                String existing = leadersByTerm.put(term, node.getNodeId());
                if (existing != null && !existing.equals(node.getNodeId())) {
                    fail(String.format("Election Safety violated on seed %d, step %d: term %d has leaders %s and %s",
                            seed, step, term, existing, node.getNodeId()));
                }
            }
        }

        // 2. Log Matching: if two entries have same index and term, all preceding entries must match
        for (int i = 0; i < nodes.size(); i++) {
            RaftNode n1 = nodes.get(i);
            List<RaftLogEntry> l1 = n1.getLogEntries();
            for (int j = i + 1; j < nodes.size(); j++) {
                RaftNode n2 = nodes.get(j);
                List<RaftLogEntry> l2 = n2.getLogEntries();
                int minSize = Math.min(l1.size(), l2.size());
                for (int idx = 0; idx < minSize; idx++) {
                    RaftLogEntry e1 = l1.get(idx);
                    RaftLogEntry e2 = l2.get(idx);
                    if (e1.index() == e2.index() && e1.term() == e2.term()) {
                        assertEquals(e1.index(), e2.index(), "Index mismatch");
                        assertArrayEquals(e1.command(), e2.command(),
                                String.format("Log Matching violated on seed %d, step %d between %s and %s at index %d",
                                        seed, step, n1.getNodeId(), n2.getNodeId(), e1.index()));
                    }
                }
            }
        }

        // 3. State Machine Safety: applied commands at any index must be identical across nodes
        Map<Long, byte[]> canonicalApplied = new HashMap<>();
        for (Map.Entry<String, RecordingStateMachine> entry : stateMachines.entrySet()) {
            Map<Long, byte[]> applied = entry.getValue().getAppliedByIndex();
            for (Map.Entry<Long, byte[]> appEntry : applied.entrySet()) {
                long idx = appEntry.getKey();
                byte[] cmd = appEntry.getValue();
                byte[] canonical = canonicalApplied.putIfAbsent(idx, cmd);
                if (canonical != null && !Arrays.equals(canonical, cmd)) {
                    fail(String.format("State Machine Safety violated on seed %d, step %d: index %d has conflicting applications across nodes",
                            seed, step, idx));
                }
            }
        }
    }
}
