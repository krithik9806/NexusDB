package io.nexusdb.distributed.sharding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps partitioned tables to shard definitions based on configurable HASH or RANGE partitioning strategies.
 */
public final class ShardMap {
    public static final class TablePartitioning {
        private final String tableName;
        private final String shardKeyColumn;
        private final ShardStrategy strategy;
        private final List<ShardDefinition> shards;

        public TablePartitioning(
                String tableName,
                String shardKeyColumn,
                ShardStrategy strategy,
                List<ShardDefinition> shards) {
            this.tableName = Objects.requireNonNull(tableName, "tableName must not be null").toUpperCase(Locale.ROOT);
            this.shardKeyColumn = Objects.requireNonNull(shardKeyColumn, "shardKeyColumn must not be null").toUpperCase(Locale.ROOT);
            this.strategy = Objects.requireNonNull(strategy, "strategy must not be null");
            if (shards == null || shards.isEmpty()) {
                throw new IllegalArgumentException("Table must have at least one shard");
            }
            this.shards = List.copyOf(shards);
        }

        public String getTableName() {
            return tableName;
        }

        public String getShardKeyColumn() {
            return shardKeyColumn;
        }

        public ShardStrategy getStrategy() {
            return strategy;
        }

        public List<ShardDefinition> getShards() {
            return shards;
        }

        public ShardDefinition locateShard(Object key) {
            if (key == null) {
                return shards.get(0);
            }

            if (strategy == ShardStrategy.HASH) {
                int hash = Math.abs(key.hashCode());
                return shards.get(hash % shards.size());
            }

            if (strategy == ShardStrategy.RANGE) {
                long num;
                if (key instanceof Number n) {
                    num = n.longValue();
                } else {
                    try {
                        num = Long.parseLong(key.toString());
                    } catch (NumberFormatException e) {
                        return shards.get(0);
                    }
                }

                for (ShardDefinition s : shards) {
                    Long start = s.rangeStartInclusive();
                    Long end = s.rangeEndExclusive();
                    boolean afterStart = (start == null || num >= start);
                    boolean beforeEnd = (end == null || num < end);
                    if (afterStart && beforeEnd) {
                        return s;
                    }
                }
                return shards.get(shards.size() - 1);
            }

            return shards.get(0);
        }
    }

    private final Map<String, TablePartitioning> tablePartitionings = new ConcurrentHashMap<>();

    public void registerTable(TablePartitioning partitioning) {
        tablePartitionings.put(partitioning.getTableName(), partitioning);
    }

    public TablePartitioning getPartitioning(String tableName) {
        if (tableName == null) {
            return null;
        }
        return tablePartitionings.get(tableName.toUpperCase(Locale.ROOT));
    }

    public boolean isSharded(String tableName) {
        return getPartitioning(tableName) != null;
    }
}
