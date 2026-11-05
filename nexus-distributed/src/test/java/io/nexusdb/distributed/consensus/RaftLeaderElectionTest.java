package io.nexusdb.distributed.consensus;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class RaftLeaderElectionTest {

    @Test
    void testInitialLeaderElectionAndElectionSafetyOnFiveNodeCluster() {
        List<String> nodes = List.of("n1", "n2", "n3", "n4", "n5");
        SimCluster cluster = new SimCluster(nodes, 42L);

        RaftNode leader = cluster.awaitLeader(1000L);
        assertNotNull(leader, "A single leader must be elected within 1000ms");
        assertEquals(RaftRole.LEADER, leader.getRole());
        long term = leader.getCurrentTerm();
        assertTrue(term >= 1L);

        // Allow heartbeats to reach all followers and stabilize
        cluster.stepMillis(200L);

        int leaderCount = 0;
        for (RaftNode node : cluster.getNodes()) {
            assertEquals(term, node.getCurrentTerm(), "All nodes must converge on the leader's term");
            assertEquals(leader.getNodeId(), node.getLeaderId(), "All followers must recognize the elected leader");
            if (node.getRole() == RaftRole.LEADER) {
                leaderCount++;
            } else {
                assertEquals(RaftRole.FOLLOWER, node.getRole());
            }
        }
        assertEquals(1, leaderCount, "Exactly one leader must exist in term " + term);
    }

    @Test
    void testReElectionAfterLeaderCrashAndPartitionAndOldLeaderStepsDownOnHeal() {
        List<String> nodes = List.of("n1", "n2", "n3", "n4", "n5");
        SimCluster cluster = new SimCluster(nodes, 101L);

        RaftNode firstLeader = cluster.awaitLeader(1000L);
        assertNotNull(firstLeader);
        long firstTerm = firstLeader.getCurrentTerm();
        String firstLeaderId = firstLeader.getNodeId();

        // Isolate the first leader from the 4 remaining nodes
        cluster.isolateNode(firstLeaderId);
        cluster.stepMillis(600L);

        // Majority partition (4 of 5 nodes) must elect a new leader in a higher term
        List<RaftNode> majorityLiveLeaders = cluster.getNodes().stream()
                .filter(n -> !n.getNodeId().equals(firstLeaderId))
                .filter(n -> n.getRole() == RaftRole.LEADER)
                .toList();
        assertEquals(1, majorityLiveLeaders.size(), "Majority side must elect a new leader");
        RaftNode secondLeader = majorityLiveLeaders.get(0);
        assertNotEquals(firstLeaderId, secondLeader.getNodeId());
        assertTrue(secondLeader.getCurrentTerm() > firstTerm,
                "New leader must be in a strictly higher term than isolated leader");

        // Now heal the partition; the old leader must observe the higher term and step down to FOLLOWER
        cluster.healPartitions();
        cluster.stepMillis(200L);

        assertEquals(RaftRole.FOLLOWER, firstLeader.getRole(),
                "Previously partitioned leader must step down to FOLLOWER upon healing");
        assertEquals(secondLeader.getCurrentTerm(), firstLeader.getCurrentTerm());
        assertEquals(secondLeader.getNodeId(), firstLeader.getLeaderId());

        // Crash the second leader and verify a third leader is elected
        String secondLeaderId = secondLeader.getNodeId();
        long secondTerm = secondLeader.getCurrentTerm();
        cluster.crashNode(secondLeaderId);
        cluster.stepMillis(600L);

        RaftNode thirdLeader = cluster.awaitLeader(500L);
        assertNotNull(thirdLeader);
        assertNotEquals(secondLeaderId, thirdLeader.getNodeId());
        assertTrue(thirdLeader.getCurrentTerm() > secondTerm);
    }

    @Test
    void testSplitVoteResolvesViaRandomizedElectionTimeouts() {
        List<String> nodeIds = List.of("n1", "n2", "n3", "n4");
        SimClock clock = new SimClock(0L);
        AtomicBoolean forceTieOnFirstRound = new AtomicBoolean(true);
        java.util.Random fallbackRandom = new java.util.Random(777L);

        // On the very first round, force all 4 nodes to have the exact same election timeout (200ms)
        // so that all 4 become CANDIDATE simultaneously in term 1 and split the vote (1 vote each).
        // On subsequent rounds, use randomized timeouts [150..300ms] to resolve the split vote.
        RaftTimer timer = new RaftTimer() {
            private int initialAssignments = 0;

            @Override
            public long nextElectionTimeoutMillis(String nodeId) {
                if (forceTieOnFirstRound.get() && initialAssignments < 4) {
                    initialAssignments++;
                    if (initialAssignments == 4) {
                        forceTieOnFirstRound.set(false);
                    }
                    return 200L;
                }
                return 150L + fallbackRandom.nextLong(151L);
            }

            @Override
            public long heartbeatIntervalMillis() {
                return 40L;
            }
        };

        SimTransport transport = new SimTransport(clock, 777L);
        // Set 5ms network delay so all 4 nodes time out at t=200ms before any RequestVote arrives
        transport.setNetworkBehavior(5L, 0L, 0.0, 0.0, 0.0);

        List<RaftNode> nodes = nodeIds.stream()
                .map(id -> new RaftNode(id, nodeIds, clock, timer, transport))
                .toList();

        // Advance to t=200ms: all 4 nodes start an election in term 1 and vote for themselves
        for (int t = 1; t <= 200; t++) {
            clock.advanceMillis(1L);
            transport.deliverDueMessages((env, msg) ->
                    nodes.stream().filter(n -> n.getNodeId().equals(env.toNodeId())).findFirst().orElseThrow()
                            .handleMessage(env.fromNodeId(), msg));
            for (RaftNode n : nodes) {
                n.tick();
            }
        }

        // Deliver the term-1 RequestVote messages at t=205ms -> nobody gets a 3-of-4 majority in term 1!
        for (int t = 201; t <= 210; t++) {
            clock.advanceMillis(1L);
            transport.deliverDueMessages((env, msg) ->
                    nodes.stream().filter(n -> n.getNodeId().equals(env.toNodeId())).findFirst().orElseThrow()
                            .handleMessage(env.fromNodeId(), msg));
            for (RaftNode n : nodes) {
                n.tick();
            }
        }
        assertTrue(nodes.stream().noneMatch(n -> n.getRole() == RaftRole.LEADER),
                "Split vote in term 1 must not elect any leader");
        assertTrue(nodes.stream().allMatch(n -> n.getCurrentTerm() == 1L && n.getRole() == RaftRole.CANDIDATE));

        // Continue stepping with randomized timeouts -> split vote resolves in a higher term
        for (int t = 211; t <= 1000; t++) {
            clock.advanceMillis(1L);
            transport.deliverDueMessages((env, msg) ->
                    nodes.stream().filter(n -> n.getNodeId().equals(env.toNodeId())).findFirst().orElseThrow()
                            .handleMessage(env.fromNodeId(), msg));
            for (RaftNode n : nodes) {
                n.tick();
            }
        }

        List<RaftNode> leaders = nodes.stream().filter(n -> n.getRole() == RaftRole.LEADER).toList();
        assertEquals(1, leaders.size(), "Randomized election timeouts must resolve the split vote to 1 leader");
        assertTrue(leaders.get(0).getCurrentTerm() >= 2L, "Leader must be elected in term >= 2 after term-1 split vote");
    }

    @Test
    void testMinorityPartitionCannotElectLeader() {
        List<String> nodes = List.of("n1", "n2", "n3", "n4", "n5");
        SimCluster cluster = new SimCluster(nodes, 999L);

        // Partition cluster into minority {n1, n2} (2 of 5) and majority {n3, n4, n5} (3 of 5)
        // before any leader is elected
        Set<String> minority = Set.of("n1", "n2");
        Set<String> majority = Set.of("n3", "n4", "n5");
        cluster.partition(minority, majority);

        cluster.stepMillis(1500L);

        // Minority nodes {n1, n2} must NEVER become LEADER despite timing out repeatedly
        for (String minId : minority) {
            RaftNode minNode = cluster.getNode(minId);
            assertNotEquals(RaftRole.LEADER, minNode.getRole(),
                    "Minority partition node " + minId + " must never become LEADER");
        }

        // Majority nodes {n3, n4, n5} must have elected exactly one leader
        List<RaftNode> majorityLeaders = majority.stream()
                .map(cluster::getNode)
                .filter(n -> n.getRole() == RaftRole.LEADER)
                .toList();
        assertEquals(1, majorityLeaders.size(), "Majority partition (3 of 5) must elect a leader");
    }

    @Test
    void testCandidateWithStaleLogIsDeniedVotesPerSection541() {
        List<String> nodes = List.of("n1", "n2", "n3");
        SimCluster cluster = new SimCluster(nodes, 2026L);

        RaftNode n1 = cluster.getNode("n1");
        RaftNode n2 = cluster.getNode("n2");
        RaftNode n3 = cluster.getNode("n3");

        byte[] cmd = "SET x=1".getBytes(StandardCharsets.UTF_8);

        // n1 has only term 1 entry at index 1 (stale)
        n1.seedLogEntries(List.of(new RaftLogEntry(1L, 1L, cmd)));

        // n2 has term 1 entries at index 1 and 2 (longer log in same term)
        n2.seedLogEntries(List.of(
                new RaftLogEntry(1L, 1L, cmd),
                new RaftLogEntry(2L, 1L, cmd)));

        // n3 has term 2 entry at index 1 (higher lastLogTerm beats longer log of term 1!)
        n3.seedLogEntries(List.of(new RaftLogEntry(1L, 2L, cmd)));

        // Direct unit verification of §5.4.1 log comparison rules:
        // n2 (lastTerm=1, lastIdx=2) rejects n1 (lastTerm=1, lastIdx=1)
        assertFalse(n2.isCandidateLogUpToDate(1L, 1L));
        // n3 (lastTerm=2, lastIdx=1) rejects both n1 (1, 1) and n2 (1, 2)
        assertFalse(n3.isCandidateLogUpToDate(1L, 1L));
        assertFalse(n3.isCandidateLogUpToDate(1L, 2L));
        // Both n1 and n2 accept n3 (lastTerm=2, lastIdx=1)
        assertTrue(n1.isCandidateLogUpToDate(2L, 1L));
        assertTrue(n2.isCandidateLogUpToDate(2L, 1L));

        // Force n1 (stale log) to trigger an election first; n2 and n3 must deny their votes to n1
        n1.startElection();
        cluster.stepMillis(20L);
        assertNotEquals(RaftRole.LEADER, n1.getRole(),
                "Candidate n1 with stale log (term=1, idx=1) must be denied votes by n2 and n3");

        // Let the cluster run until a leader is elected: only n3 has a log up-to-date enough to win a majority!
        RaftNode elected = cluster.awaitLeader(1000L);
        assertNotNull(elected);
        assertEquals("n3", elected.getNodeId(),
                "Only n3 (with highest lastLogTerm=2) can obtain votes from a majority");
    }
}
