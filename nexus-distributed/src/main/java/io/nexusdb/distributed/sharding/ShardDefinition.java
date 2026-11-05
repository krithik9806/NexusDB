package io.nexusdb.distributed.sharding;

import java.util.List;
import java.util.Objects;

/**
 * Definition of an individual shard replica group and its partition boundary.
 */
public record ShardDefinition(
        int shardId,
        String shardName,
        List<String> replicaNodeIds,
        Long rangeStartInclusive,
        Long rangeEndExclusive) {

    public ShardDefinition {
        if (shardId < 0) {
            throw new IllegalArgumentException("shardId must be >= 0");
        }
        Objects.requireNonNull(shardName, "shardName must not be null");
        Objects.requireNonNull(replicaNodeIds, "replicaNodeIds must not be null");
        if (replicaNodeIds.size() < 3) {
            throw new IllegalArgumentException("Each shard must have at least 3 replica nodes");
        }
        replicaNodeIds = List.copyOf(replicaNodeIds);
    }

    public ShardDefinition(int shardId, String shardName, List<String> replicaNodeIds) {
        this(shardId, shardName, replicaNodeIds, null, null);
    }
}
