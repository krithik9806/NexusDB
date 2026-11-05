package io.nexusdb.distributed.consensus;

/**
 * Role of a Raft consensus peer at any given logical instant (Raft §5.1).
 */
public enum RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
