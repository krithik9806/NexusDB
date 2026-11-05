package io.nexusdb.server;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.nexusdb.distributed.consensus.RaftRole;
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

class RaftReplicatedClusterTest {

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
    void threeNodeClusterReplicatesWritesAndSurvivesLeaderFailure() throws Exception {
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
            // 1. Await leader election
            NexusNode leader = awaitLeader(cluster, 6000);
            assertNotNull(leader, "A leader should be elected");

            // 2. Write on leader over gRPC
            ManagedChannel leaderChannel = ManagedChannelBuilder.forAddress("localhost", leader.getPort())
                    .usePlaintext()
                    .build();
            try {
                SqlResponse r1 = executeSql(leaderChannel, "CREATE TABLE cluster_items (id INT PRIMARY KEY, name VARCHAR(20));");
                assertTrue(r1.getSuccess(), () -> "Failed to create table: " + r1.getErrorMessage());

                SqlResponse r2 = executeSql(leaderChannel, "INSERT INTO cluster_items VALUES (1, 'Widget'), (2, 'Gadget');");
                assertTrue(r2.getSuccess(), () -> "Failed to insert items: " + r2.getErrorMessage());
            } finally {
                leaderChannel.shutdown();
                leaderChannel.awaitTermination(2, TimeUnit.SECONDS);
            }

            // 3. Await replication to all live nodes
            long commitIdx = leader.getRaftNode().getCommitIndex();
            awaitCommitCatchup(cluster, commitIdx, 6000);

            // Verify read everywhere
            for (NexusNode n : cluster) {
                ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", n.getPort())
                        .usePlaintext()
                        .build();
                try {
                    SqlResponse res = executeSql(ch, "SELECT id, name FROM cluster_items ORDER BY id;");
                    assertTrue(res.getSuccess());
                    assertEquals(2, res.getRowsCount(), "Node " + n.getNodeId() + " should have 2 rows");
                } finally {
                    ch.shutdown();
                    ch.awaitTermination(2, TimeUnit.SECONDS);
                }
            }

            // 4. Kill the leader!
            String killedId = leader.getNodeId();
            leader.stop();
            cluster.remove(leader);

            // 5. Await new leader election among remaining 2 nodes
            NexusNode newLeader = awaitLeader(cluster, 7000);
            assertNotNull(newLeader, "A new leader should be elected among survivors");
            assertNotEquals(killedId, newLeader.getNodeId());

            // 6. Verify data intact on new leader and perform a new write
            ManagedChannel newLeaderChannel = ManagedChannelBuilder.forAddress("localhost", newLeader.getPort())
                    .usePlaintext()
                    .build();
            try {
                SqlResponse readResp = executeSql(newLeaderChannel, "SELECT id, name FROM cluster_items ORDER BY id;");
                assertTrue(readResp.getSuccess());
                assertEquals(2, readResp.getRowsCount());

                SqlResponse writeResp = executeSql(newLeaderChannel, "INSERT INTO cluster_items VALUES (3, 'Doohickey');");
                assertTrue(writeResp.getSuccess());

                SqlResponse finalRead = executeSql(newLeaderChannel, "SELECT id, name FROM cluster_items ORDER BY id;");
                assertEquals(3, finalRead.getRowsCount());
            } finally {
                newLeaderChannel.shutdown();
                newLeaderChannel.awaitTermination(2, TimeUnit.SECONDS);
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
