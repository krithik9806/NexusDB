package io.nexusdb.server;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.nexusdb.distributed.consensus.RaftRole;
import io.nexusdb.proto.AdminServiceGrpc;
import io.nexusdb.proto.ClusterMembershipRequest;
import io.nexusdb.proto.ClusterMembershipResponse;
import io.nexusdb.proto.FaultInjectionRequest;
import io.nexusdb.proto.FaultType;
import io.nexusdb.proto.SqlRequest;
import io.nexusdb.proto.SqlResponse;
import io.nexusdb.proto.SqlServiceGrpc;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class FaultInjectionClusterTest {

    @TempDir
    Path tempDir;

    private static int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    private static SqlResponse executeSql(ManagedChannel channel, String sql) {
        SqlServiceGrpc.SqlServiceBlockingStub stub = SqlServiceGrpc.newBlockingStub(channel);
        return stub.execute(SqlRequest.newBuilder().setSql(sql).build());
    }

    @Test
    void stopFollowerRestartAndVerifyLogCatchup() throws Exception {
        int p1 = findFreePort();
        int p2 = findFreePort();
        int p3 = findFreePort();

        List<String> peers1 = List.of("node-2=localhost:" + p2, "node-3=localhost:" + p3);
        List<String> peers2 = List.of("node-1=localhost:" + p1, "node-3=localhost:" + p3);
        List<String> peers3 = List.of("node-1=localhost:" + p1, "node-2=localhost:" + p2);

        NexusNode n1 = new NexusNode("node-1", p1, tempDir.resolve("n1"), peers1);
        NexusNode n2 = new NexusNode("node-2", p2, tempDir.resolve("n2"), peers2);
        NexusNode n3 = new NexusNode("node-3", p3, tempDir.resolve("n3"), peers3);

        List<NexusNode> cluster = new ArrayList<>(List.of(n1, n2, n3));
        for (NexusNode n : cluster) {
            n.start();
        }

        try {
            NexusNode leader = awaitLeader(cluster, 6000);
            assertNotNull(leader, "A leader should be elected");

            // Find a follower to stop
            NexusNode follower = cluster.stream()
                    .filter(n -> !n.getNodeId().equals(leader.getNodeId()))
                    .findFirst()
                    .orElseThrow();

            // 1. Initial write
            ManagedChannel leaderCh = ManagedChannelBuilder.forAddress("localhost", leader.getPort())
                    .usePlaintext().build();
            try {
                assertTrue(executeSql(leaderCh, "CREATE TABLE items (id INT PRIMARY KEY, val VARCHAR(20));").getSuccess());
                assertTrue(executeSql(leaderCh, "INSERT INTO items VALUES (1, 'One');").getSuccess());
            } finally {
                leaderCh.shutdown();
                leaderCh.awaitTermination(2, TimeUnit.SECONDS);
            }

            // 2. Stop follower
            String followerId = follower.getNodeId();
            int followerPort = follower.getPort();
            Path followerDir = tempDir.resolve(followerId.replace("-", ""));
            List<String> followerPeers = (followerId.equals("node-2")) ? peers2 : peers3;

            follower.stop();
            cluster.remove(follower);

            // 3. Leader writes more entries while follower is down
            leaderCh = ManagedChannelBuilder.forAddress("localhost", leader.getPort())
                    .usePlaintext().build();
            try {
                assertTrue(executeSql(leaderCh, "INSERT INTO items VALUES (2, 'Two');").getSuccess());
                assertTrue(executeSql(leaderCh, "INSERT INTO items VALUES (3, 'Three');").getSuccess());
            } finally {
                leaderCh.shutdown();
                leaderCh.awaitTermination(2, TimeUnit.SECONDS);
            }

            // 4. Restart follower and await catch-up
            NexusNode restartedFollower = new NexusNode(followerId, followerPort, followerDir, followerPeers);
            restartedFollower.start();
            cluster.add(restartedFollower);

            long leaderCommit = leader.getRaftNode().getCommitIndex();
            awaitCommitCatchup(cluster, leaderCommit, 7000);

            // Verify follower has all 3 rows
            ManagedChannel restartedCh = ManagedChannelBuilder.forAddress("localhost", restartedFollower.getPort())
                    .usePlaintext().build();
            try {
                SqlResponse readResp = executeSql(restartedCh, "SELECT id, val FROM items ORDER BY id;");
                assertTrue(readResp.getSuccess());
                assertEquals(3, readResp.getRowsCount(), "Restarted follower should catch up all 3 rows");
            } finally {
                restartedCh.shutdown();
                restartedCh.awaitTermination(2, TimeUnit.SECONDS);
            }

            // Verify AdminService cluster view from current leader
            NexusNode currentLeader = awaitLeader(cluster, 5000);
            assertNotNull(currentLeader);
            ManagedChannel adminCh = ManagedChannelBuilder.forAddress("localhost", currentLeader.getPort())
                    .usePlaintext().build();
            try {
                AdminServiceGrpc.AdminServiceBlockingStub adminStub = AdminServiceGrpc.newBlockingStub(adminCh);
                ClusterMembershipResponse memResp = adminStub.getClusterMembership(ClusterMembershipRequest.getDefaultInstance());
                assertEquals(currentLeader.getNodeId(), memResp.getNodeId());
                assertEquals("LEADER", memResp.getRole());
                assertTrue(memResp.getPeersCount() >= 2);
            } finally {
                adminCh.shutdown();
                adminCh.awaitTermination(2, TimeUnit.SECONDS);
            }
        } finally {
            for (NexusNode n : cluster) {
                n.stop();
            }
        }
    }

    @Test
    void partitionLeaderFromMajorityHealAndVerifySingleHistory() throws Exception {
        int p1 = findFreePort();
        int p2 = findFreePort();
        int p3 = findFreePort();

        List<String> peers1 = List.of("node-2=localhost:" + p2, "node-3=localhost:" + p3);
        List<String> peers2 = List.of("node-1=localhost:" + p1, "node-3=localhost:" + p3);
        List<String> peers3 = List.of("node-1=localhost:" + p1, "node-2=localhost:" + p2);

        NexusNode n1 = new NexusNode("node-1", p1, tempDir.resolve("p1"), peers1);
        NexusNode n2 = new NexusNode("node-2", p2, tempDir.resolve("p2"), peers2);
        NexusNode n3 = new NexusNode("node-3", p3, tempDir.resolve("p3"), peers3);

        List<NexusNode> cluster = new ArrayList<>(List.of(n1, n2, n3));
        for (NexusNode n : cluster) {
            n.start();
        }

        try {
            NexusNode leader = awaitLeader(cluster, 6000);
            assertNotNull(leader, "Initial leader should be elected");

            // Write initial table
            ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", leader.getPort())
                    .usePlaintext().build();
            try {
                assertTrue(executeSql(ch, "CREATE TABLE ledger (id INT PRIMARY KEY, amount DOUBLE);").getSuccess());
                assertTrue(executeSql(ch, "INSERT INTO ledger VALUES (1, 100.0);").getSuccess());
            } finally {
                ch.shutdown();
                ch.awaitTermination(2, TimeUnit.SECONDS);
            }

            // Partition leader from the other 2 nodes using FaultInjector
            List<NexusNode> majority = cluster.stream()
                    .filter(n -> !n.getNodeId().equals(leader.getNodeId()))
                    .toList();

            for (NexusNode m : majority) {
                leader.getFaultInjector().setDropTraffic(m.getNodeId(), true);
                m.getFaultInjector().setDropTraffic(leader.getNodeId(), true);
            }

            // Majority elects a new leader
            NexusNode newLeader = awaitLeader(majority, 7000);
            assertNotNull(newLeader, "Majority should elect a new leader");
            assertNotEquals(leader.getNodeId(), newLeader.getNodeId());

            // Write to majority
            ManagedChannel majorityCh = ManagedChannelBuilder.forAddress("localhost", newLeader.getPort())
                    .usePlaintext().build();
            try {
                assertTrue(executeSql(majorityCh, "INSERT INTO ledger VALUES (2, 250.0);").getSuccess());
            } finally {
                majorityCh.shutdown();
                majorityCh.awaitTermination(2, TimeUnit.SECONDS);
            }

            // Heal the partition
            leader.getFaultInjector().clearAll();
            for (NexusNode m : majority) {
                m.getFaultInjector().clearAll();
            }

            // Await catchup across all 3 nodes
            long targetCommit = newLeader.getRaftNode().getCommitIndex();
            awaitCommitCatchup(cluster, targetCommit, 7000);

            // Verify all nodes converged to the exact same history (2 rows: 1 and 2)
            for (NexusNode n : cluster) {
                ManagedChannel nodeCh = ManagedChannelBuilder.forAddress("localhost", n.getPort())
                        .usePlaintext().build();
                try {
                    SqlResponse res = executeSql(nodeCh, "SELECT id, amount FROM ledger ORDER BY id;");
                    assertTrue(res.getSuccess());
                    assertEquals(2, res.getRowsCount(), "Node " + n.getNodeId() + " should have 2 rows");
                } finally {
                    nodeCh.shutdown();
                    nodeCh.awaitTermination(2, TimeUnit.SECONDS);
                }
            }
        } finally {
            for (NexusNode n : cluster) {
                n.stop();
            }
        }
    }

    private NexusNode awaitLeader(List<NexusNode> nodes, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (NexusNode n : nodes) {
                if (n.isRunning() && n.getRaftNode() != null && n.getRaftNode().getRole() == RaftRole.LEADER) {
                    return n;
                }
            }
            Thread.sleep(50);
        }
        return null;
    }

    private void awaitCommitCatchup(List<NexusNode> nodes, long targetCommit, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean allCaughtUp = true;
            for (NexusNode n : nodes) {
                if (n.isRunning() && n.getRaftNode() != null) {
                    if (n.getRaftNode().getLastApplied() < targetCommit) {
                        allCaughtUp = false;
                        break;
                    }
                }
            }
            if (allCaughtUp) {
                return;
            }
            Thread.sleep(50);
        }
    }
}
