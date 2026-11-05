package io.nexusdb.client;

import io.nexusdb.proto.NodeHealthResponse;
import io.nexusdb.proto.SqlResponse;
import io.nexusdb.server.NexusNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class NexusClientServerTest {

    @TempDir
    Path tempDir;

    @Test
    void clientAndCliConnectToNodeOverGrpc() throws Exception {
        Path dataDir = tempDir.resolve("node-data");
        try (NexusNode node = new NexusNode("node-cli-test", 0, dataDir, Collections.emptyList())) {
            node.start();
            int port = node.getPort();
            assertTrue(port > 0);

            // 1. Direct NexusClient calls
            try (NexusClient client = new NexusClient("localhost", port, false)) {
                SqlResponse r1 = client.execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(20), price DOUBLE);");
                assertTrue(r1.getSuccess());

                SqlResponse r2 = client.execute("INSERT INTO products VALUES (1, 'Book', 19.99), (2, 'Pen', 2.50);");
                assertTrue(r2.getSuccess());
                assertEquals(2, r2.getRowsAffected());

                SqlResponse r3 = client.execute("SELECT id, name, price FROM products ORDER BY price;");
                assertTrue(r3.getSuccess());
                assertEquals(2, r3.getRowsCount());
                assertEquals("Pen", r3.getRows(0).getValues(1));

                NodeHealthResponse health = client.checkHealth();
                assertEquals("node-cli-test", health.getNodeId());
            }

            // 2. NexusCli remote interaction
            String script = "SELECT id, name FROM products WHERE id = 1;\n.quit\n";
            ByteArrayInputStream in = new ByteArrayInputStream(script.getBytes(StandardCharsets.UTF_8));
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayOutputStream err = new ByteArrayOutputStream();

            NexusCli cli = new NexusCli();
            int code = cli.run(new String[]{"--host", "localhost", "--port", String.valueOf(port)},
                    in, new PrintStream(out), new PrintStream(err));

            assertEquals(0, code);
            String output = out.toString(StandardCharsets.UTF_8);
            assertTrue(output.contains("Book"));
        }
    }
}
