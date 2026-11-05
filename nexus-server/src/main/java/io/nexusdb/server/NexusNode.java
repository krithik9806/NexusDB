package io.nexusdb.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.nexusdb.core.engine.Database;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Main daemon process representing a single NexusDB node hosting storage and gRPC endpoints.
 */
public final class NexusNode implements AutoCloseable {
    private final String nodeId;
    private final int port;
    private final Path dataDir;
    private final List<String> peers;
    private final Database database;
    private final Server grpcServer;
    private volatile boolean running;

    public NexusNode(String nodeId, int port, Path dataDir, List<String> peers) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.port = port;
        this.dataDir = Objects.requireNonNull(dataDir, "dataDir must not be null");
        this.peers = peers != null ? List.copyOf(peers) : Collections.emptyList();

        try {
            Files.createDirectories(dataDir);
            Path dbPath = dataDir.resolve("nexus.db");
            this.database = Database.open(dbPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize database directory: " + dataDir, e);
        }

        SqlServiceImpl sqlService = new SqlServiceImpl(database);
        HealthServiceImpl healthService = new HealthServiceImpl(
                nodeId,
                () -> "STANDALONE",
                () -> 0L,
                () -> 0L,
                database);

        this.grpcServer = ServerBuilder.forPort(port)
                .addService(sqlService)
                .addService(healthService)
                .build();
    }

    public void start() throws IOException {
        grpcServer.start();
        running = true;
    }

    public void stop() {
        if (running) {
            running = false;
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
            database.close();
        }
    }

    public int getPort() {
        return grpcServer.getPort();
    }

    public String getNodeId() {
        return nodeId;
    }

    public Database getDatabase() {
        return database;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void close() {
        stop();
    }

    public static void main(String[] args) throws Exception {
        String nodeId = "node-1";
        int port = 9000;
        Path dataDir = Paths.get("./data");
        List<String> peers = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--node-id":
                    nodeId = args[++i];
                    break;
                case "--port":
                    port = Integer.parseInt(args[++i]);
                    break;
                case "--data-dir":
                    dataDir = Paths.get(args[++i]);
                    break;
                case "--peers":
                    String[] parts = args[++i].split(",");
                    for (String p : parts) {
                        if (!p.isBlank()) {
                            peers.add(p.trim());
                        }
                    }
                    break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
            }
        }

        NexusNode node = new NexusNode(nodeId, port, dataDir, peers);
        node.start();
        System.out.println("NexusDB Node [" + nodeId + "] started on port " + node.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
        node.grpcServer.awaitTermination();
    }
}
