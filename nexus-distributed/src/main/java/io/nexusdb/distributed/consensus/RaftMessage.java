package io.nexusdb.distributed.consensus;

import java.util.List;
import java.util.Objects;

/**
 * Sealed hierarchy of inter-node Raft RPC requests and responses (Raft §5.2, §5.3).
 */
public sealed interface RaftMessage
        permits RaftMessage.RequestVoteRequest,
                RaftMessage.RequestVoteResponse,
                RaftMessage.AppendEntriesRequest,
                RaftMessage.AppendEntriesResponse {

    /**
     * Sender's current term at the time this RPC message was created.
     */
    long term();

    /**
     * Invoked by candidates to gather votes during leader election (Raft §5.2, §5.4.1).
     */
    record RequestVoteRequest(
            long term,
            String candidateId,
            long lastLogIndex,
            long lastLogTerm) implements RaftMessage {
        public RequestVoteRequest {
            if (term < 0) {
                throw new IllegalArgumentException("term must be >= 0");
            }
            Objects.requireNonNull(candidateId, "candidateId must not be null");
            if (lastLogIndex < 0 || lastLogTerm < 0) {
                throw new IllegalArgumentException("lastLogIndex and lastLogTerm must be >= 0");
            }
        }
    }

    /**
     * Response to a {@link RequestVoteRequest}.
     */
    record RequestVoteResponse(
            long term,
            String voterId,
            long requestTerm,
            boolean voteGranted) implements RaftMessage {
        public RequestVoteResponse {
            if (term < 0 || requestTerm < 0) {
                throw new IllegalArgumentException("term and requestTerm must be >= 0");
            }
            Objects.requireNonNull(voterId, "voterId must not be null");
        }
    }

    /**
     * Invoked by the leader to replicate log entries and to serve as a heartbeat when {@code entries} is empty.
     */
    record AppendEntriesRequest(
            long term,
            String leaderId,
            long prevLogIndex,
            long prevLogTerm,
            List<RaftLogEntry> entries,
            long leaderCommit) implements RaftMessage {
        public AppendEntriesRequest {
            if (term < 0 || prevLogIndex < 0 || prevLogTerm < 0 || leaderCommit < 0) {
                throw new IllegalArgumentException("Term and log indices must be >= 0");
            }
            Objects.requireNonNull(leaderId, "leaderId must not be null");
            Objects.requireNonNull(entries, "entries must not be null");
            entries = List.copyOf(entries);
        }

        public static AppendEntriesRequest heartbeat(
                long term,
                String leaderId,
                long prevLogIndex,
                long prevLogTerm,
                long leaderCommit) {
            return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, List.of(), leaderCommit);
        }
    }

    /**
     * Response to an {@link AppendEntriesRequest}.
     *
     * <p>{@code matchIndex} reports the highest log index known to be replicated on the follower
     * when {@code success == true}, and {@code conflictIndex} hints where the follower's log
     * diverges when {@code success == false} for fast log backtracking.
     */
    record AppendEntriesResponse(
            long term,
            String followerId,
            long requestTerm,
            boolean success,
            long matchIndex,
            long conflictIndex) implements RaftMessage {
        public AppendEntriesResponse {
            if (term < 0 || requestTerm < 0) {
                throw new IllegalArgumentException("term and requestTerm must be >= 0");
            }
            Objects.requireNonNull(followerId, "followerId must not be null");
        }

        public AppendEntriesResponse(
                long term,
                String followerId,
                long requestTerm,
                boolean success,
                long matchIndex) {
            this(term, followerId, requestTerm, success, matchIndex, 1L);
        }
    }
}
