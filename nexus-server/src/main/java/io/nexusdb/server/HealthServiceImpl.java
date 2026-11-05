package io.nexusdb.server;

import io.grpc.stub.StreamObserver;
import io.nexusdb.core.engine.Database;
import io.nexusdb.proto.HealthRequest;
import io.nexusdb.proto.HealthServiceGrpc;
import io.nexusdb.proto.NodeHealthResponse;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * gRPC service reporting node health, role, uptime, buffer pool hit ratio, and active transaction count.
 */
public final class HealthServiceImpl extends HealthServiceGrpc.HealthServiceImplBase {
    private final String nodeId;
    private final long startTimeMillis;
    private final Supplier<String> roleSupplier;
    private final Supplier<Long> termSupplier;
    private final Supplier<Long> commitIndexSupplier;
    private final Database database;

    public HealthServiceImpl(
            String nodeId,
            Supplier<String> roleSupplier,
            Supplier<Long> termSupplier,
            Supplier<Long> commitIndexSupplier,
            Database database) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.roleSupplier = Objects.requireNonNull(roleSupplier, "roleSupplier must not be null");
        this.termSupplier = Objects.requireNonNull(termSupplier, "termSupplier must not be null");
        this.commitIndexSupplier = Objects.requireNonNull(commitIndexSupplier, "commitIndexSupplier must not be null");
        this.database = Objects.requireNonNull(database, "database must not be null");
        this.startTimeMillis = System.currentTimeMillis();
    }

    @Override
    public void check(HealthRequest request, StreamObserver<NodeHealthResponse> responseObserver) {
        long uptime = Math.max(0L, System.currentTimeMillis() - startTimeMillis);
        double hitRatio = database.getBufferPoolManager() != null
                ? database.getBufferPoolManager().getHitRatio()
                : 1.0;
        int activeTxns = database.getTransactionManager() != null
                ? database.getTransactionManager().getMetrics().activeTransactions()
                : 0;

        NodeHealthResponse response = NodeHealthResponse.newBuilder()
                .setNodeId(nodeId)
                .setRole(roleSupplier.get())
                .setUptimeMs(uptime)
                .setCacheHitRatio(hitRatio)
                .setActiveTransactions(activeTxns)
                .setCurrentTerm(termSupplier.get())
                .setCommitIndex(commitIndexSupplier.get())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
