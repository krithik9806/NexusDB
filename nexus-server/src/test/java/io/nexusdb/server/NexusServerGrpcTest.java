package io.nexusdb.server;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.nexusdb.proto.HealthRequest;
import io.nexusdb.proto.HealthServiceGrpc;
import io.nexusdb.proto.NodeHealthResponse;
import io.nexusdb.proto.SqlRequest;
import io.nexusdb.proto.SqlResponse;
import io.nexusdb.proto.SqlServiceGrpc;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NexusServerGrpcTest {

    @TempDir
    Path tempDir;

    @Test
    void inProcessServerExecutesSqlAndReportsHealthOverGrpc() throws Exception {
        Path dataDir = tempDir.resolve("node-data");
        // Use port 0 for dynamic ephemeral port allocation
        try (NexusNode node = new NexusNode("node-test-1", 0, dataDir, Collections.emptyList())) {
            node.start();
            assertTrue(node.isRunning());
            int boundPort = node.getPort();
            assertTrue(boundPort > 0);

            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", boundPort)
                    .usePlaintext()
                    .build();

            try {
                SqlServiceGrpc.SqlServiceBlockingStub sqlStub = SqlServiceGrpc.newBlockingStub(channel);
                HealthServiceGrpc.HealthServiceBlockingStub healthStub = HealthServiceGrpc.newBlockingStub(channel);

                // 1. DDL: Create table
                SqlResponse createResp = sqlStub.execute(SqlRequest.newBuilder()
                        .setSql("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(30), score DOUBLE);")
                        .build());
                assertTrue(createResp.getSuccess(), () -> "Failed to create table: " + createResp.getErrorMessage());

                // 2. DML: Insert rows
                SqlResponse insertResp = sqlStub.execute(SqlRequest.newBuilder()
                        .setSql("INSERT INTO users VALUES (1, 'Alice', 95.5), (2, 'Bob', 82.0), (3, 'Charlie', 88.5);")
                        .build());
                assertTrue(insertResp.getSuccess(), () -> "Failed to insert rows: " + insertResp.getErrorMessage());
                assertEquals(3, insertResp.getRowsAffected());

                // 3. Query: Select with Filter & Order By
                SqlResponse selectResp = sqlStub.execute(SqlRequest.newBuilder()
                        .setSql("SELECT id, name, score FROM users WHERE score >= 85.0 ORDER BY score DESC;")
                        .build());
                assertTrue(selectResp.getSuccess(), () -> "Failed to select: " + selectResp.getErrorMessage());
                assertEquals(3, selectResp.getColumnsCount());
                assertEquals("id", selectResp.getColumns(0));
                assertEquals("name", selectResp.getColumns(1));
                assertEquals("score", selectResp.getColumns(2));

                assertEquals(2, selectResp.getRowsCount());
                assertEquals("1", selectResp.getRows(0).getValues(0));
                assertEquals("Alice", selectResp.getRows(0).getValues(1));
                assertEquals("95.5", selectResp.getRows(0).getValues(2));

                assertEquals("3", selectResp.getRows(1).getValues(0));
                assertEquals("Charlie", selectResp.getRows(1).getValues(1));
                assertEquals("88.5", selectResp.getRows(1).getValues(2));

                // 4. Health Check
                NodeHealthResponse health = healthStub.check(HealthRequest.newBuilder().setClientId("test-client").build());
                assertEquals("node-test-1", health.getNodeId());
                assertEquals("STANDALONE", health.getRole());
                assertTrue(health.getUptimeMs() >= 0);
                assertTrue(health.getCacheHitRatio() >= 0.0 && health.getCacheHitRatio() <= 1.0);
            } finally {
                channel.shutdown();
                channel.awaitTermination(3, TimeUnit.SECONDS);
            }
        }
    }
}
