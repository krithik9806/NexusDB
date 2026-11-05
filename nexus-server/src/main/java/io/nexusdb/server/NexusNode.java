package io.nexusdb.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;
import io.nexusdb.distributed.consensus.FileRaftStorage;
import io.nexusdb.distributed.consensus.RaftClock;
import io.nexusdb.distributed.consensus.RaftNode;
import io.nexusdb.distributed.consensus.RaftRole;
import io.nexusdb.distributed.consensus.RaftStorage;
import io.nexusdb.distributed.consensus.RaftTimer;
import io.nexusdb.server.raft.DatabaseStateMachine;
import io.nexusdb.server.raft.GrpcRaftTransport;
import io.nexusdb.server.raft.RaftServiceImpl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Main daemon process representing a single NexusDB node hosting storage, Raft consensus, and gRPC endpoints.
 */
public final class NexusNode implements AutoCloseable {
    private final String nodeId;
    private final int port;
    private final Path dataDir;
    private final List<String> peerSpecs;
    private final Database database;
    private final Server grpcServer;

    private final RaftNode raftNode;
    private final GrpcRaftTransport raftTransport;
    private final io.nexusdb.server.admin.FaultInjector faultInjector;
    private final io.nexusdb.server.admin.ClusterMembershipView membershipView;
    private final ScheduledExecutorService tickerExecutor;
    private volatile boolean running;

    public NexusNode(String nodeId, int port, Path dataDir, List<String> peerSpecs) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.port = port;
        this.dataDir = Objects.requireNonNull(dataDir, "dataDir must not be null");
        this.peerSpecs = peerSpecs != null ? List.copyOf(peerSpecs) : Collections.emptyList();

        try {
            Files.createDirectories(dataDir);
            Path dbPath = dataDir.resolve("nexus.db");
            this.database = Database.open(dbPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize database directory: " + dataDir, e);
        }

        if (!this.peerSpecs.isEmpty()) {
            List<String> allNodeIds = new ArrayList<>();
            allNodeIds.add(nodeId);

            List<PeerInfo> peers = new ArrayList<>();
            for (String spec : this.peerSpecs) {
                // Format: "nodeId=host:port" or "host:port"
                if (spec.contains("=")) {
                    String[] parts = spec.split("=", 2);
                    String pid = parts[0].trim();
                    String[] hostPort = parts[1].trim().split(":", 2);
                    peers.add(new PeerInfo(pid, hostPort[0], Integer.parseInt(hostPort[1])));
                    allNodeIds.add(pid);
                } else if (spec.contains(":")) {
                    String[] hostPort = spec.trim().split(":", 2);
                    String pid = "node-" + hostPort[1];
                    peers.add(new PeerInfo(pid, hostPort[0], Integer.parseInt(hostPort[1])));
                    allNodeIds.add(pid);
                }
            }

            Collections.sort(allNodeIds);
            RaftClock clock = System::currentTimeMillis;
            RaftTimer timer = new RaftTimer() {
                @Override
                public long nextElectionTimeoutMillis(String id) {
                    return ThreadLocalRandom.current().nextLong(150, 300);
                }

                @Override
                public long heartbeatIntervalMillis() {
                    return 40L;
                }
            };

            this.raftTransport = new GrpcRaftTransport(nodeId);
            for (PeerInfo p : peers) {
                raftTransport.registerPeer(p.id(), p.host(), p.port());
            }

            Path raftDir = dataDir.resolve("raft");
            RaftStorage raftStorage = new FileRaftStorage(raftDir);
            DatabaseStateMachine stateMachine = new DatabaseStateMachine(database);

            this.raftNode = new RaftNode(
                    nodeId,
                    allNodeIds,
                    clock,
                    timer,
                    raftTransport,
                    raftStorage,
                    stateMachine);

            this.raftTransport.setMessageHandler(delivery -> {
                this.raftNode.handleMessage(delivery.fromNodeId(), delivery.message());
            });

            this.faultInjector = this.raftTransport.getFaultInjector();
            this.membershipView = new io.nexusdb.server.admin.ClusterMembershipView(this.raftNode);

            this.tickerExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "raft-ticker-" + nodeId);
                t.setDaemon(true);
                return t;
            });
        } else {
            this.raftNode = null;
            this.raftTransport = null;
            this.faultInjector = null;
            this.membershipView = null;
            this.tickerExecutor = null;
        }

        SqlServiceImpl sqlService = new SqlServiceImpl(database, raftNode);
        HealthServiceImpl healthService = new HealthServiceImpl(
                nodeId,
                () -> raftNode != null ? raftNode.getRole().name() : "STANDALONE",
                () -> raftNode != null ? raftNode.getCurrentTerm() : 0L,
                () -> raftNode != null ? raftNode.getCommitIndex() : 0L,
                database);

        ServerBuilder<?> builder = ServerBuilder.forPort(port)
                .addService(sqlService)
                .addService(healthService);

        if (raftNode != null) {
            builder.addService(new RaftServiceImpl(raftNode));
            builder.addService(new io.nexusdb.server.admin.AdminServiceImpl(membershipView, faultInjector));
        }

        this.grpcServer = builder.build();
    }

    private record PeerInfo(String id, String host, int port) {}

    public void start() throws IOException {
        grpcServer.start();
        running = true;

        if (raftNode != null && tickerExecutor != null) {
            tickerExecutor.scheduleWithFixedDelay(() -> {
                try {
                    raftNode.tick();
                } catch (Throwable t) {
                    // Suppress ticker errors to keep background thread alive
                }
            }, 20, 20, TimeUnit.MILLISECONDS);
        }
    }

    public void stop() {
        if (running) {
            running = false;
            if (tickerExecutor != null) {
                tickerExecutor.shutdownNow();
            }
            if (raftTransport != null) {
                raftTransport.close();
            }
            if (raftNode != null) {
                try {
                    raftNode.getStorage().close();
                } catch (Exception ignored) {
                }
            }
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(3, TimeUnit.SECONDS)) {
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

    public RaftNode getRaftNode() {
        return raftNode;
    }

    public io.nexusdb.server.admin.FaultInjector getFaultInjector() {
        return faultInjector;
    }

    public io.nexusdb.server.admin.ClusterMembershipView getMembershipView() {
        return membershipView;
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
