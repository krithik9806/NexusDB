package io.nexusdb.distributed.consensus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class RaftLogReplicationTest {

    @TempDir
    Path tempDir;

    /**
     * Deterministic recording state machine that records every applied {@code (index, command)} in order.
     */
    static final class RecordingStateMachine implements StateMachine {
        private final List<String> appliedCommands = new ArrayList<>();
        private final Map<Long, String> appliedByIndex = new LinkedHashMap<>();

        @Override
        public byte[] apply(long index, byte[] command) {
            long expectedIndex = appliedCommands.size() + 1L;
            assertEquals(expectedIndex, index, "StateMachine entries must be applied in contiguous 1-based order");
            String cmdStr = new String(command, StandardCharsets.UTF_8);
            appliedCommands.add(cmdStr);
            appliedByIndex.put(index, cmdStr);
            return ("ACK:" + index + ":" + cmdStr).getBytes(StandardCharsets.UTF_8);
        }

        List<String> appliedCommands() {
            return List.copyOf(appliedCommands);
        }

        Map<Long, String> appliedByIndex() {
            return Map.copyOf(appliedByIndex);
        }
    }

    @Test
    void testNormalLogReplicationCommitAdvancementAndLaggingFollowerCatchUp() throws Exception {
        List<String> nodeIds = List.of("n1", "n2", "n3", "n4", "n5");
        Map<String, RecordingStateMachine> stateMachines = new LinkedHashMap<>();
        try (SimCluster cluster = new SimCluster(
                nodeIds,
                42L,
                id -> new FileRaftStorage(tempDir.resolve("catchup_" + id + ".raft")),
                id -> {
                    RecordingStateMachine sm = new RecordingStateMachine();
                    stateMachines.put(id, sm);
                    return sm;
                })) {

            RaftNode leader = cluster.awaitLeader(1000L);
            assertNotNull(leader);

            // Isolate lagging follower n5 while leader replicates 5 commands to {n1, n2, n3, n4}
            String laggingId = leader.getNodeId().equals("n5") ? "n4" : "n5";
            cluster.isolateNode(laggingId);

            List<CompletableFuture<byte[]>> futures = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                futures.add(leader.propose(("CMD_" + i).getBytes(StandardCharsets.UTF_8)));
            }

            cluster.stepMillis(200L);

            for (int i = 0; i < 5; i++) {
                assertTrue(futures.get(i).isDone(), "Proposal " + (i + 1) + " must complete once committed by quorum");
                String result = new String(futures.get(i).get(), StandardCharsets.UTF_8);
                assertEquals("ACK:" + (i + 1) + ":CMD_" + (i + 1), result);
            }

            assertEquals(5L, leader.getCommitIndex());
            assertEquals(5L, leader.getLastApplied());

            // Lagging node has 0 entries while partitioned
            RaftNode laggingNode = cluster.getNode(laggingId);
            assertEquals(0L, laggingNode.getLastLogIndex());
            assertEquals(0L, laggingNode.getCommitIndex());
            assertTrue(stateMachines.get(laggingId).appliedCommands().isEmpty());

            // Heal partition -> lagging follower catches up all 5 entries and applies them in order
            cluster.healPartitions();
            RaftNode activeLeader = cluster.awaitLeader(1000L);
            assertNotNull(activeLeader);
            cluster.stepMillis(250L);

            for (String id : nodeIds) {
                RaftNode n = cluster.getNode(id);
                assertEquals(5L, n.getLastLogIndex(), "Node " + id + " must have all 5 log entries");
                assertEquals(5L, n.getCommitIndex(), "Node " + id + " must advance commitIndex to 5");
                assertEquals(5L, n.getLastApplied(), "Node " + id + " must apply all 5 entries");
                assertEquals(
                        List.of("CMD_1", "CMD_2", "CMD_3", "CMD_4", "CMD_5"),
                        stateMachines.get(id).appliedCommands());
            }
        }
    }

    @Test
    void testDivergentUncommittedFollowerLogsAreRepairedAndUncommittedNeverApplied() throws Exception {
        List<String> nodeIds = List.of("n1", "n2", "n3", "n4", "n5");
        Map<String, RecordingStateMachine> stateMachines = new LinkedHashMap<>();
        try (SimCluster cluster = new SimCluster(
                nodeIds,
                2026L,
                id -> new InMemoryRaftStorage(),
                id -> {
                    RecordingStateMachine sm = new RecordingStateMachine();
                    stateMachines.put(id, sm);
                    return sm;
                })) {

            RaftNode firstLeader = cluster.awaitLeader(1000L);
            assertNotNull(firstLeader);
            String oldLeaderId = firstLeader.getNodeId();

            // Commit 2 entries across the whole cluster first
            CompletableFuture<byte[]> f1 = firstLeader.propose("COMMITTED_1".getBytes(StandardCharsets.UTF_8));
            CompletableFuture<byte[]> f2 = firstLeader.propose("COMMITTED_2".getBytes(StandardCharsets.UTF_8));
            cluster.stepMillis(150L);
            assertTrue(f1.isDone() && f2.isDone());
            assertEquals(2L, firstLeader.getCommitIndex());

            // Now isolate oldLeader with ONE follower (minority of 2 out of 5)
            String minorityFollowerId = nodeIds.stream()
                    .filter(id -> !id.equals(oldLeaderId))
                    .findFirst()
                    .orElseThrow();
            Set<String> minoritySide = Set.of(oldLeaderId, minorityFollowerId);
            Set<String> majoritySide = nodeIds.stream()
                    .filter(id -> !minoritySide.contains(id))
                    .collect(java.util.stream.Collectors.toSet());

            cluster.partition(minoritySide, majoritySide);

            // Propose 3 uncommitted commands on the partitioned oldLeader (replicated only to minoritySide)
            CompletableFuture<byte[]> uncommitted1 = firstLeader.propose("STALE_UNCOMMITTED_3".getBytes(StandardCharsets.UTF_8));
            CompletableFuture<byte[]> uncommitted2 = firstLeader.propose("STALE_UNCOMMITTED_4".getBytes(StandardCharsets.UTF_8));
            CompletableFuture<byte[]> uncommitted3 = firstLeader.propose("STALE_UNCOMMITTED_5".getBytes(StandardCharsets.UTF_8));

            cluster.stepMillis(100L);
            // Minority has 5 log entries (indices 1..5), but commitIndex MUST stay at 2!
            assertEquals(5L, firstLeader.getLastLogIndex());
            assertEquals(5L, cluster.getNode(minorityFollowerId).getLastLogIndex());
            assertEquals(2L, firstLeader.getCommitIndex(), "Minority partition must NEVER commit new entries");
            assertEquals(2L, firstLeader.getLastApplied(), "Uncommitted entries must NEVER be applied to StateMachine");
            assertFalse(uncommitted1.isDone());
            assertFalse(uncommitted2.isDone());
            assertFalse(uncommitted3.isDone());

            // Advance time so majoritySide elects a new leader in a higher term
            cluster.stepMillis(600L);
            RaftNode majorityLeader = majoritySide.stream()
                    .map(cluster::getNode)
                    .filter(n -> n.getRole() == RaftRole.LEADER)
                    .findFirst()
                    .orElseThrow();

            // Propose 2 new commands on majorityLeader (indices 3 and 4 in new term)
            CompletableFuture<byte[]> new3 = majorityLeader.propose("VALID_3".getBytes(StandardCharsets.UTF_8));
            CompletableFuture<byte[]> new4 = majorityLeader.propose("VALID_4".getBytes(StandardCharsets.UTF_8));
            cluster.stepMillis(200L);
            assertTrue(new3.isDone() && new4.isDone());

            // Heal the partition! oldLeader steps down, itspending uncommitted futures fail,
            // and both oldLeader and minorityFollower truncate their conflicting uncommitted entries (3..5)
            // and replace them with VALID_3 and VALID_4!
            cluster.healPartitions();
            cluster.stepMillis(350L);

            assertTrue(uncommitted1.isCompletedExceptionally(), "Overwritten proposal on stepped-down leader must fail");
            assertTrue(uncommitted2.isCompletedExceptionally());
            assertTrue(uncommitted3.isCompletedExceptionally());
            assertThrows(ExecutionException.class, uncommitted1::get);

            for (String id : nodeIds) {
                RaftNode node = cluster.getNode(id);
                assertEquals(4L, node.getLastLogIndex(), "Divergent log on " + id + " must be repaired to length 4");
                assertEquals(4L, node.getCommitIndex());
                assertEquals(4L, node.getLastApplied());
                assertEquals(majorityLeader.getLogEntries(), node.getLogEntries(),
                        "Follower " + id + " log must match leader log identically");
                assertEquals(
                        List.of("COMMITTED_1", "COMMITTED_2", "VALID_3", "VALID_4"),
                        stateMachines.get(id).appliedCommands(),
                        "StateMachine on " + id + " must never have applied STALE_UNCOMMITTED entries");
            }
        }
    }

    @Test
    void testSection542LeaderOnlyCommitsEntriesByCountingReplicasFromCurrentTerm() {
        List<String> nodeIds = List.of("n1", "n2", "n3", "n4", "n5");
        try (SimCluster cluster2 = new SimCluster(nodeIds, 1234L)) {
            RaftNode c1 = cluster2.getNode("n1");

            // Seed c1 with (index=1, term=1), and advance c1 to term 2 where c1 has (index=1, term=1)
            // and becomes LEADER in term 2:
            byte[] oldTermCmd = "OLD_TERM_CMD".getBytes(StandardCharsets.UTF_8);
            c1.seedLogEntries(List.of(new RaftLogEntry(1L, 1L, oldTermCmd)));

            // c1 starts election -> becomes LEADER in term 2!
            c1.startElection();
            cluster2.stepMillis(20L);
            assertEquals(RaftRole.LEADER, c1.getRole());
            assertEquals(2L, c1.getCurrentTerm());

            // c1 replicates (index=1, term=1) to all 5 nodes during term 2 heartbeats!
            cluster2.stepMillis(60L);
            for (RaftNode node : cluster2.getNodes()) {
                assertEquals(1L, node.getLastLogIndex(), "All 5 nodes now store (index=1, term=1)");
                assertEquals(1L, node.getTermAt(1L));
            }

            // CRITICAL §5.4.2 ASSERTION:
            // Even though all 5 out of 5 nodes have replicated (index=1, term=1), c1 is in term 2 (currentTerm=2 != 1),
            // so c1 MUST NOT advance commitIndex to 1 by counting replicas from an older term!
            assertEquals(0L, c1.getCommitIndex(),
                    "Raft §5.4.2: Leader in term 2 must NOT commit entry from term 1 even when replicated on all 5 nodes");
            assertEquals(0L, c1.getLastApplied());

            // Now propose a new entry in currentTerm (term 2) at index 2 on leader c1:
            CompletableFuture<byte[]> term2Proposal = c1.propose("CURRENT_TERM_2_CMD".getBytes(StandardCharsets.UTF_8));
            cluster2.stepMillis(60L);

            // Once (index=2, term=2) is replicated on a majority, BOTH index 1 (term 1) and index 2 (term 2) commit!
            assertTrue(term2Proposal.isDone());
            assertEquals(2L, c1.getCommitIndex(),
                    "Raft §5.4.2: Committing an entry from currentTerm (term 2) indirectly commits prior entry at index 1");
            assertEquals(2L, c1.getLastApplied());
        }
    }

    @Test
    void testFileRaftStorageDurabilityAcrossNodeRestartAndTornTrailingWriteRecovery() throws IOException {
        Path storageFile = tempDir.resolve("node_durable.raft");
        byte[] c1 = "INSERT INTO kv VALUES (1, 'a')".getBytes(StandardCharsets.UTF_8);
        byte[] c2 = "INSERT INTO kv VALUES (2, 'b')".getBytes(StandardCharsets.UTF_8);
        byte[] c3 = "INSERT INTO kv VALUES (3, 'c')".getBytes(StandardCharsets.UTF_8);

        try (FileRaftStorage storage = new FileRaftStorage(storageFile)) {
            storage.saveTermAndVote(3L, "n2");
            storage.appendEntries(List.of(
                    new RaftLogEntry(1L, 1L, c1),
                    new RaftLogEntry(2L, 2L, c2),
                    new RaftLogEntry(3L, 2L, c3)));
            // Overwrite index 3 with term 3
            storage.truncateAndAppend(3L, List.of(new RaftLogEntry(3L, 3L, c3)));
        }

        // Reopen FileRaftStorage and verify currentTerm, votedFor, and log are preserved
        try (FileRaftStorage reopened = new FileRaftStorage(storageFile)) {
            RaftStorage.PersistentState state = reopened.load();
            assertEquals(3L, state.currentTerm());
            assertEquals("n2", state.votedFor());
            assertEquals(3, state.log().size());
            assertEquals(new RaftLogEntry(1L, 1L, c1), state.log().get(0));
            assertEquals(new RaftLogEntry(2L, 2L, c2), state.log().get(1));
            assertEquals(new RaftLogEntry(3L, 3L, c3), state.log().get(2));
        }

        // Simulate a torn / corrupted write at the end of the file (e.g., power loss mid-record)
        long validSize;
        try (FileChannel ch = FileChannel.open(storageFile, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            validSize = ch.size();
            ch.position(validSize);
            ByteBuffer tornGarbage = ByteBuffer.wrap(new byte[]{0, 0, 0, 25, 0x12, 0x34, 0x56});
            ch.write(tornGarbage);
            ch.force(true);
        }

        // Reopen and verify FileRaftStorage detects the torn frame, truncates it, and recovers all valid state
        try (FileRaftStorage recovered = new FileRaftStorage(storageFile)) {
            RaftStorage.PersistentState state = recovered.load();
            assertEquals(3L, state.currentTerm());
            assertEquals("n2", state.votedFor());
            assertEquals(3, state.log().size());
        }
    }

    @Test
    void testClusterNodeRestartFromFileRaftStoragePreservesStateAndReappliesCommittedLog() {
        List<String> nodeIds = List.of("n1", "n2", "n3");
        Map<String, RecordingStateMachine> stateMachines = new LinkedHashMap<>();

        try (SimCluster cluster = new SimCluster(
                nodeIds,
                889L,
                id -> new FileRaftStorage(tempDir.resolve("restart2_" + id + ".raft")),
                id -> {
                    RecordingStateMachine sm = new RecordingStateMachine();
                    stateMachines.put(id, sm);
                    return sm;
                })) {

            RaftNode leader = cluster.awaitLeader(1000L);
            assertNotNull(leader);

            CompletableFuture<byte[]> p1 = leader.propose("TX_1".getBytes(StandardCharsets.UTF_8));
            CompletableFuture<byte[]> p2 = leader.propose("TX_2".getBytes(StandardCharsets.UTF_8));
            cluster.stepMillis(150L);
            assertTrue(p1.isDone() && p2.isDone());

            String followerId = nodeIds.stream()
                    .filter(id -> !id.equals(leader.getNodeId()))
                    .findFirst()
                    .orElseThrow();
            RaftNode beforeCrash = cluster.getNode(followerId);
            long savedTerm = beforeCrash.getCurrentTerm();
            String savedVote = beforeCrash.getVotedFor();
            List<RaftLogEntry> savedLog = beforeCrash.getLogEntries();
            assertEquals(2, savedLog.size());

            // Crash and restart the follower from its FileRaftStorage
            cluster.crashNode(followerId);
            RaftNode restarted = cluster.restartNode(followerId);

            assertEquals(savedTerm, restarted.getCurrentTerm(), "Restarted node must preserve currentTerm from FileRaftStorage");
            assertEquals(savedVote, restarted.getVotedFor(), "Restarted node must preserve votedFor from FileRaftStorage");
            assertEquals(savedLog, restarted.getLogEntries(), "Restarted node must preserve log from FileRaftStorage");
            assertEquals(0L, restarted.getCommitIndex(), "Volatile commitIndex starts at 0 on fresh restart");

            // Step cluster so leader sends heartbeat with leaderCommit=2 -> restarted node advances commitIndex and applies TX_1, TX_2
            cluster.stepMillis(150L);
            assertEquals(2L, restarted.getCommitIndex());
            assertEquals(2L, restarted.getLastApplied());
            assertEquals(List.of("TX_1", "TX_2"), stateMachines.get(followerId).appliedCommands());
        }
    }
}
