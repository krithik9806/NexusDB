package io.nexusdb.server.admin;

import io.grpc.stub.StreamObserver;
import io.nexusdb.proto.AdminServiceGrpc;
import io.nexusdb.proto.ClusterMembershipRequest;
import io.nexusdb.proto.ClusterMembershipResponse;
import io.nexusdb.proto.FaultInjectionRequest;
import io.nexusdb.proto.FaultInjectionResponse;
import io.nexusdb.proto.PeerReplicationStatus;

import java.util.Objects;

/**
 * gRPC service handling administrative cluster operations, membership queries, and fault injection.
 */
public final class AdminServiceImpl extends AdminServiceGrpc.AdminServiceImplBase {
    private final ClusterMembershipView membershipView;
    private final FaultInjector faultInjector;

    public AdminServiceImpl(ClusterMembershipView membershipView, FaultInjector faultInjector) {
        this.membershipView = Objects.requireNonNull(membershipView, "membershipView must not be null");
        this.faultInjector = Objects.requireNonNull(faultInjector, "faultInjector must not be null");
    }

    @Override
    public void getClusterMembership(ClusterMembershipRequest request, StreamObserver<ClusterMembershipResponse> responseObserver) {
        ClusterMembershipResponse.Builder builder = ClusterMembershipResponse.newBuilder()
                .setNodeId(membershipView.getNodeId())
                .setRole(membershipView.getRole().name())
                .setCurrentTerm(membershipView.getCurrentTerm())
                .setLeaderCommitIndex(membershipView.getCommitIndex());

        for (ClusterMembershipView.PeerStatus p : membershipView.getPeerStatuses()) {
            builder.addPeers(PeerReplicationStatus.newBuilder()
                    .setPeerId(p.peerId())
                    .setMatchIndex(p.matchIndex())
                    .setLag(p.lag())
                    .setIsAlive(p.isAlive())
                    .build());
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void injectFault(FaultInjectionRequest request, StreamObserver<FaultInjectionResponse> responseObserver) {
        String target = request.getTargetNodeId();
        switch (request.getFaultType()) {
            case DROP_TRAFFIC -> {
                faultInjector.setDropTraffic(target, true);
                responseObserver.onNext(FaultInjectionResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Traffic drop enabled for peer " + target)
                        .build());
            }
            case ADD_LATENCY -> {
                long latency = request.getLatencyMs();
                faultInjector.setLatency(target, latency);
                responseObserver.onNext(FaultInjectionResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Artificial latency of " + latency + "ms set for peer " + target)
                        .build());
            }
            case CLEAR_FAULTS -> {
                faultInjector.clearAll();
                responseObserver.onNext(FaultInjectionResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("All faults cleared")
                        .build());
            }
            default -> {
                responseObserver.onNext(FaultInjectionResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Unsupported fault type: " + request.getFaultType())
                        .build());
            }
        }
        responseObserver.onCompleted();
    }
}
