package io.nexusdb.distributed.sharding;

import io.nexusdb.core.engine.ResultSet;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Distributed coordinator and query router across sharded Multi-Raft groups.
 *
 * <p>Enforces ADR-004:
 * <ul>
 *   <li>Point writes with a single shard key are routed to the owning shard.</li>
 *   <li>Cross-shard distributed transactions and multi-shard writes are rejected.</li>
 *   <li>Scatter-gather is performed for full scans with in-memory merge for ORDER BY, LIMIT, and simple aggregates.</li>
 * </ul>
 */
public final class ShardRouter {
    private static final Pattern INSERT_PATTERN = Pattern.compile("INSERT\\s+INTO\\s+(\\w+)\\s*(?:\\(([^)]+)\\))?\\s*VALUES\\s*\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern WHERE_KEY_PATTERN = Pattern.compile("WHERE\\s+(\\w+)\\s*=\\s*(\\d+|'[^']*')", Pattern.CASE_INSENSITIVE);
    private static final Pattern FROM_TABLE_PATTERN = Pattern.compile("FROM\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern LIMIT_PATTERN = Pattern.compile("LIMIT\\s+(\\d+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern ORDER_BY_PATTERN = Pattern.compile("ORDER\\s+BY\\s+(\\w+)(?:\\s+(ASC|DESC))?", Pattern.CASE_INSENSITIVE);

    private final ShardMap shardMap;
    private final BiFunction<Integer, String, ResultSet> shardExecutor;

    public ShardRouter(ShardMap shardMap, BiFunction<Integer, String, ResultSet> shardExecutor) {
        this.shardMap = Objects.requireNonNull(shardMap, "shardMap must not be null");
        this.shardExecutor = Objects.requireNonNull(shardExecutor, "shardExecutor must not be null");
    }

    public ResultSet execute(String sql) {
        String trimmed = sql.trim();
        String upper = trimmed.toUpperCase(Locale.ROOT);

        if (upper.startsWith("BEGIN") || upper.startsWith("COMMIT") || upper.startsWith("ROLLBACK")) {
            throw new UnsupportedCrossShardOperationException("Cross-shard distributed transactions are unsupported (ADR-004)");
        }

        if (upper.startsWith("INSERT")) {
            return routeInsert(trimmed);
        }

        if (upper.startsWith("UPDATE") || upper.startsWith("DELETE")) {
            return routeUpdateOrDelete(trimmed);
        }

        if (upper.startsWith("SELECT")) {
            return routeSelect(trimmed);
        }

        // DDL (e.g. CREATE TABLE) executed across all shards
        Matcher fromMat = Pattern.compile("(?:TABLE)\\s+(\\w+)", Pattern.CASE_INSENSITIVE).matcher(trimmed);
        if (fromMat.find()) {
            String table = fromMat.group(1);
            ShardMap.TablePartitioning part = shardMap.getPartitioning(table);
            if (part != null) {
                ResultSet last = null;
                for (ShardDefinition shard : part.getShards()) {
                    last = shardExecutor.apply(shard.shardId(), trimmed);
                }
                return last != null ? last : new ResultSet(List.of(), List.of(), 0, 0L);
            }
        }

        return shardExecutor.apply(0, trimmed);
    }

    private ResultSet routeInsert(String sql) {
        Matcher m = INSERT_PATTERN.matcher(sql);
        if (!m.find()) {
            throw new UnsupportedCrossShardOperationException("Complex or multi-row cross-shard INSERT is unsupported");
        }

        String table = m.group(1);
        ShardMap.TablePartitioning part = shardMap.getPartitioning(table);
        if (part == null) {
            return shardExecutor.apply(0, sql);
        }

        String colList = m.group(2);
        String valList = m.group(3);

        Object keyValue = extractKeyValue(colList, valList, part.getShardKeyColumn());
        if (keyValue == null) {
            throw new UnsupportedCrossShardOperationException("INSERT into sharded table '" + table + "' must provide shard key '" + part.getShardKeyColumn() + "'");
        }

        ShardDefinition targetShard = part.locateShard(keyValue);
        return shardExecutor.apply(targetShard.shardId(), sql);
    }

    private ResultSet routeUpdateOrDelete(String sql) {
        Matcher fromMat = FROM_TABLE_PATTERN.matcher(sql);
        String table = null;
        if (fromMat.find()) {
            table = fromMat.group(1);
        } else {
            Matcher updateMat = Pattern.compile("UPDATE\\s+(\\w+)", Pattern.CASE_INSENSITIVE).matcher(sql);
            if (updateMat.find()) {
                table = updateMat.group(1);
            }
        }

        if (table == null) {
            return shardExecutor.apply(0, sql);
        }

        ShardMap.TablePartitioning part = shardMap.getPartitioning(table);
        if (part == null) {
            return shardExecutor.apply(0, sql);
        }

        Matcher keyMat = WHERE_KEY_PATTERN.matcher(sql);
        if (keyMat.find() && keyMat.group(1).equalsIgnoreCase(part.getShardKeyColumn())) {
            String rawVal = keyMat.group(2).replace("'", "");
            ShardDefinition targetShard = part.locateShard(rawVal);
            return shardExecutor.apply(targetShard.shardId(), sql);
        }

        throw new UnsupportedCrossShardOperationException("Non-keyed multi-shard UPDATE/DELETE is unsupported per ADR-004");
    }

    private ResultSet routeSelect(String sql) {
        Matcher fromMat = FROM_TABLE_PATTERN.matcher(sql);
        if (!fromMat.find()) {
            return shardExecutor.apply(0, sql);
        }

        String table = fromMat.group(1);
        ShardMap.TablePartitioning part = shardMap.getPartitioning(table);
        if (part == null) {
            return shardExecutor.apply(0, sql);
        }

        // Single key routing
        Matcher keyMat = WHERE_KEY_PATTERN.matcher(sql);
        if (keyMat.find() && keyMat.group(1).equalsIgnoreCase(part.getShardKeyColumn())) {
            String rawVal = keyMat.group(2).replace("'", "");
            ShardDefinition targetShard = part.locateShard(rawVal);
            return shardExecutor.apply(targetShard.shardId(), sql);
        }

        // Scatter-gather across all shards of this table
        List<ResultSet> results = new ArrayList<>();
        List<ShardDefinition> shards = part.getShards();
        for (ShardDefinition shard : shards) {
            results.add(shardExecutor.apply(shard.shardId(), sql));
        }

        return mergeResults(results, sql);
    }

    private ResultSet mergeResults(List<ResultSet> results, String sql) {
        if (results.isEmpty()) {
            return new ResultSet(List.of(), List.of(), 0, 0L);
        }

        List<Column> columns = results.get(0).columns();
        List<Tuple> mergedRows = new ArrayList<>();
        int totalRowsAffected = 0;
        long maxTime = 0L;

        for (ResultSet r : results) {
            mergedRows.addAll(r.rows());
            totalRowsAffected += r.rowsAffected();
            maxTime = Math.max(maxTime, r.executionTimeNanos());
        }

        // Merge simple aggregates if query is COUNT(*)
        if (columns.size() == 1 && columns.get(0).name().toUpperCase(Locale.ROOT).contains("COUNT")) {
            long totalCount = 0;
            for (Tuple t : mergedRows) {
                Value v = t.getValue(0);
                if (v instanceof Value.BigIntValue bv) {
                    totalCount += bv.value();
                } else if (v instanceof Value.IntValue iv) {
                    totalCount += iv.value();
                }
            }
            Tuple countTuple = new Tuple(List.of(Value.ofBigInt(totalCount)));
            return new ResultSet(columns, List.of(countTuple), 0, maxTime);
        }

        // Sort if ORDER BY
        Matcher orderMat = ORDER_BY_PATTERN.matcher(sql);
        if (orderMat.find()) {
            String colName = orderMat.group(1);
            boolean desc = "DESC".equalsIgnoreCase(orderMat.group(2));
            int colIdx = -1;
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).name().equalsIgnoreCase(colName)) {
                    colIdx = i;
                    break;
                }
            }

            if (colIdx >= 0) {
                final int finalIdx = colIdx;
                mergedRows.sort((t1, t2) -> {
                    Value v1 = t1.getValue(finalIdx);
                    Value v2 = t2.getValue(finalIdx);
                    int cmp = v1.compareTo(v2);
                    return desc ? -cmp : cmp;
                });
            }
        }

        // Limit if LIMIT
        Matcher limitMat = LIMIT_PATTERN.matcher(sql);
        if (limitMat.find()) {
            int limit = Integer.parseInt(limitMat.group(1));
            if (mergedRows.size() > limit) {
                mergedRows = new ArrayList<>(mergedRows.subList(0, limit));
            }
        }

        return new ResultSet(columns, mergedRows, totalRowsAffected, maxTime);
    }

    private static Object extractKeyValue(String colList, String valList, String shardKeyCol) {
        String[] vals = valList.split(",");
        if (colList != null) {
            String[] cols = colList.split(",");
            for (int i = 0; i < cols.length; i++) {
                if (cols[i].trim().equalsIgnoreCase(shardKeyCol)) {
                    if (i < vals.length) {
                        return vals[i].trim().replace("'", "");
                    }
                }
            }
        } else {
            // Default assumes first column is shard key
            if (vals.length > 0) {
                return vals[0].trim().replace("'", "");
            }
        }
        return null;
    }
}
