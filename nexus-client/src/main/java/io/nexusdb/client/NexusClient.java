package io.nexusdb.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.nexusdb.proto.HealthRequest;
import io.nexusdb.proto.HealthServiceGrpc;
import io.nexusdb.proto.NodeHealthResponse;
import io.nexusdb.proto.SqlRequest;
import io.nexusdb.proto.SqlResponse;
import io.nexusdb.proto.SqlServiceGrpc;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * gRPC client for interacting with a remote {@link io.nexusdb.server.NexusNode}.
 */
public final class NexusClient implements AutoCloseable {
    private final String host;
    private final int port;
    private final boolean useTls;
    private final ManagedChannel channel;
    private final SqlServiceGrpc.SqlServiceBlockingStub sqlStub;
    private final HealthServiceGrpc.HealthServiceBlockingStub healthStub;

    public NexusClient(String host, int port) {
        this(host, port, false);
    }

    public NexusClient(String host, int port, boolean useTls) {
        this.host = Objects.requireNonNull(host, "host must not be null");
        this.port = port;
        this.useTls = useTls;

        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(host, port);
        if (!useTls) {
            builder.usePlaintext();
        }
        this.channel = builder.build();
        this.sqlStub = SqlServiceGrpc.newBlockingStub(channel);
        this.healthStub = HealthServiceGrpc.newBlockingStub(channel);
    }

    public SqlResponse execute(String sql) {
        SqlRequest req = SqlRequest.newBuilder()
                .setSql(sql)
                .build();
        return sqlStub.execute(req);
    }

    public NodeHealthResponse checkHealth() {
        HealthRequest req = HealthRequest.newBuilder()
                .setClientId("nexus-client")
                .build();
        return healthStub.check(req);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isUseTls() {
        return useTls;
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
