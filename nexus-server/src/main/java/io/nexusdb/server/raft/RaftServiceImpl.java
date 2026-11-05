package io.nexusdb.server.raft;

import io.grpc.stub.StreamObserver;
import io.nexusdb.distributed.consensus.RaftLogEntry;
import io.nexusdb.distributed.consensus.RaftMessage;
import io.nexusdb.distributed.consensus.RaftNode;
import io.nexusdb.proto.AppendEntriesArgs;
import io.nexusdb.proto.AppendEntriesReply;
import io.nexusdb.proto.LogEntryProto;
import io.nexusdb.proto.RaftServiceGrpc;
import io.nexusdb.proto.RequestVoteArgs;
import io.nexusdb.proto.RequestVoteReply;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * gRPC service receiving Raft RPCs from peer nodes and delivering them to the local {@link RaftNode}.
 */
public final class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
    private final RaftNode raftNode;

    public RaftServiceImpl(RaftNode raftNode) {
        this.raftNode = Objects.requireNonNull(raftNode, "raftNode must not be null");
    }

    @Override
    public void requestVote(RequestVoteArgs args, StreamObserver<RequestVoteReply> responseObserver) {
        RaftMessage.RequestVoteRequest req = new RaftMessage.RequestVoteRequest(
                args.getTerm(),
                args.getCandidateId(),
                args.getLastLogIndex(),
                args.getLastLogTerm());

        RaftMessage.RequestVoteResponse resp = raftNode.handleRequestVote(args.getCandidateId(), req);

        RequestVoteReply reply = RequestVoteReply.newBuilder()
                .setTerm(resp.term())
                .setVoterId(resp.voterId())
                .setVoteGranted(resp.voteGranted())
                .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void appendEntries(AppendEntriesArgs args, StreamObserver<AppendEntriesReply> responseObserver) {
        List<RaftLogEntry> entries = new ArrayList<>();
        for (LogEntryProto p : args.getEntriesList()) {
            entries.add(new RaftLogEntry(p.getIndex(), p.getTerm(), p.getCommand().toByteArray()));
        }

        RaftMessage.AppendEntriesRequest req = new RaftMessage.AppendEntriesRequest(
                args.getTerm(),
                args.getLeaderId(),
                args.getPrevLogIndex(),
                args.getPrevLogTerm(),
                entries,
                args.getLeaderCommitIndex());

        RaftMessage.AppendEntriesResponse resp = raftNode.handleAppendEntries(args.getLeaderId(), req);

        AppendEntriesReply reply = AppendEntriesReply.newBuilder()
                .setTerm(resp.term())
                .setNodeId(resp.followerId())
                .setSuccess(resp.success())
                .setMatchIndex(resp.matchIndex())
                .setConflictIndex(resp.conflictIndex())
                .build();

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
