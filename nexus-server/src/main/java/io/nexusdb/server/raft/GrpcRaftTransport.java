package io.nexusdb.server.raft;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.nexusdb.distributed.consensus.RaftLogEntry;
import io.nexusdb.distributed.consensus.RaftMessage;
import io.nexusdb.distributed.consensus.RaftNode;
import io.nexusdb.distributed.consensus.RaftTransport;
import io.nexusdb.proto.AppendEntriesArgs;
import io.nexusdb.proto.AppendEntriesReply;
import io.nexusdb.proto.LogEntryProto;
import io.nexusdb.proto.RaftServiceGrpc;
import io.nexusdb.proto.RequestVoteArgs;
import io.nexusdb.proto.RequestVoteReply;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Real network implementation of {@link RaftTransport} using asynchronous gRPC client stubs.
 */
public final class GrpcRaftTransport implements RaftTransport, AutoCloseable {
    private final String localNodeId;
    private final io.nexusdb.server.admin.FaultInjector faultInjector;
    private final Map<String, String> peerEndpoints = new ConcurrentHashMap<>();
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> stubs = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private volatile Consumer<RaftMessageDelivery> messageHandler;

    public record RaftMessageDelivery(String fromNodeId, RaftMessage message) {}

    public GrpcRaftTransport(String localNodeId) {
        this(localNodeId, new io.nexusdb.server.admin.FaultInjector());
    }

    public GrpcRaftTransport(String localNodeId, io.nexusdb.server.admin.FaultInjector faultInjector) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId must not be null");
        this.faultInjector = Objects.requireNonNull(faultInjector, "faultInjector must not be null");
    }

    public io.nexusdb.server.admin.FaultInjector getFaultInjector() {
        return faultInjector;
    }

    public void registerPeer(String peerId, String host, int port) {
        peerEndpoints.put(peerId, host + ":" + port);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        channels.put(peerId, channel);
        stubs.put(peerId, RaftServiceGrpc.newBlockingStub(channel));
    }

    public void setMessageHandler(Consumer<RaftMessageDelivery> handler) {
        this.messageHandler = handler;
    }

    public void deliverLocal(String fromNodeId, RaftMessage message) {
        Consumer<RaftMessageDelivery> handler = this.messageHandler;
        if (handler != null) {
            handler.accept(new RaftMessageDelivery(fromNodeId, message));
        }
    }

    @Override
    public void send(String fromNodeId, String toNodeId, RaftMessage message) {
        if (faultInjector.isDropping(toNodeId)) {
            return;
        }

        RaftServiceGrpc.RaftServiceBlockingStub stub = stubs.get(toNodeId);
        if (stub == null) {
            return;
        }

        executor.submit(() -> {
            try {
                long latency = faultInjector.getLatency(toNodeId);
                if (latency > 0) {
                    Thread.sleep(latency);
                }

                if (message instanceof RaftMessage.RequestVoteRequest req) {
                    RequestVoteArgs args = RequestVoteArgs.newBuilder()
                            .setCandidateId(req.candidateId())
                            .setTerm(req.term())
                            .setLastLogIndex(req.lastLogIndex())
                            .setLastLogTerm(req.lastLogTerm())
                            .build();

                    RequestVoteReply reply = stub.withDeadlineAfter(500, TimeUnit.MILLISECONDS).requestVote(args);
                    deliverLocal(toNodeId, new RaftMessage.RequestVoteResponse(
                            reply.getTerm(),
                            reply.getVoterId(),
                            req.term(),
                            reply.getVoteGranted()));
                } else if (message instanceof RaftMessage.AppendEntriesRequest req) {
                    List<LogEntryProto> protoEntries = new ArrayList<>();
                    for (RaftLogEntry e : req.entries()) {
                        protoEntries.add(LogEntryProto.newBuilder()
                                .setIndex(e.index())
                                .setTerm(e.term())
                                .setCommand(ByteString.copyFrom(e.command()))
                                .build());
                    }

                    AppendEntriesArgs args = AppendEntriesArgs.newBuilder()
                            .setLeaderId(req.leaderId())
                            .setTerm(req.term())
                            .setPrevLogIndex(req.prevLogIndex())
                            .setPrevLogTerm(req.prevLogTerm())
                            .setLeaderCommitIndex(req.leaderCommit())
                            .addAllEntries(protoEntries)
                            .build();

                    AppendEntriesReply reply = stub.withDeadlineAfter(500, TimeUnit.MILLISECONDS).appendEntries(args);
                    deliverLocal(toNodeId, new RaftMessage.AppendEntriesResponse(
                            reply.getTerm(),
                            reply.getNodeId(),
                            req.term(),
                            reply.getSuccess(),
                            reply.getMatchIndex(),
                            reply.getConflictIndex()));
                }
            } catch (Exception ignored) {
                // Network failure or timeout in Raft is expected; leader/candidate re-sends on next tick
            }
        });
    }

    @Override
    public void close() {
        executor.shutdownNow();
        for (ManagedChannel ch : channels.values()) {
            ch.shutdownNow();
        }
    }
}
