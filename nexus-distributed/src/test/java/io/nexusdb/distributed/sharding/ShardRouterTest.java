package io.nexusdb.distributed.sharding;

import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ShardRouterTest {

    @TempDir
    Path tempDir;

    @Test
    void twoShardsThreeNodesScatterGatherAndRouting() {
        // Setup 2 shards
        ShardDefinition shard0 = new ShardDefinition(0, "shard-0", List.of("node-1", "node-2", "node-3"), 0L, 500L);
        ShardDefinition shard1 = new ShardDefinition(1, "shard-1", List.of("node-1", "node-2", "node-3"), 500L, 1000L);

        ShardMap shardMap = new ShardMap();
        shardMap.registerTable(new ShardMap.TablePartitioning(
                "users",
                "id",
                ShardStrategy.RANGE,
                List.of(shard0, shard1)));

        // Create 2 databases simulating the 2 independent shard Raft state machines
        Database dbShard0 = Database.open(tempDir.resolve("db-shard-0.db"));
        Database dbShard1 = Database.open(tempDir.resolve("db-shard-1.db"));

        Map<Integer, Database> shardDatabases = Map.of(0, dbShard0, 1, dbShard1);

        ShardRouter router = new ShardRouter(shardMap, (shardId, sql) -> {
            Database db = shardDatabases.get(shardId);
            return db.execute(sql);
        });

        try {
            // 1. DDL: Create table on all shards
            router.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(20), age INT);");

            // 2. Point Writes: route to correct shard
            router.execute("INSERT INTO users VALUES (100, 'Alice', 30);"); // shard 0
            router.execute("INSERT INTO users VALUES (700, 'Bob', 25);");   // shard 1
            router.execute("INSERT INTO users VALUES (200, 'Charlie', 35);"); // shard 0
            router.execute("INSERT INTO users VALUES (800, 'David', 40);");   // shard 1

            // Verify shard isolation
            assertEquals(2, dbShard0.execute("SELECT * FROM users;").rows().size());
            assertEquals(2, dbShard1.execute("SELECT * FROM users;").rows().size());

            // 3. Point Queries: route to single shard
            ResultSet r100 = router.execute("SELECT id, name FROM users WHERE id = 100;");
            assertEquals(1, r100.rows().size());
            assertEquals("Alice", r100.rows().get(0).getValue(1).toString().replace("VarcharValue[value=", "").replace("]", ""));

            ResultSet r700 = router.execute("SELECT id, name FROM users WHERE id = 700;");
            assertEquals(1, r700.rows().size());
            assertEquals("Bob", r700.rows().get(0).getValue(1).toString().replace("VarcharValue[value=", "").replace("]", ""));

            // 4. Scatter-Gather with ORDER BY and LIMIT
            ResultSet allSorted = router.execute("SELECT id, name FROM users ORDER BY id ASC;");
            assertEquals(4, allSorted.rows().size());
            assertEquals("100", allSorted.rows().get(0).getValue(0).toString().replace("IntValue[value=", "").replace("]", ""));
            assertEquals("200", allSorted.rows().get(1).getValue(0).toString().replace("IntValue[value=", "").replace("]", ""));
            assertEquals("700", allSorted.rows().get(2).getValue(0).toString().replace("IntValue[value=", "").replace("]", ""));
            assertEquals("800", allSorted.rows().get(3).getValue(0).toString().replace("IntValue[value=", "").replace("]", ""));

            ResultSet limited = router.execute("SELECT id, name FROM users ORDER BY id ASC LIMIT 2;");
            assertEquals(2, limited.rows().size());

            // 5. Aggregate Scatter-Gather (COUNT(*))
            ResultSet countRes = router.execute("SELECT COUNT(*) FROM users;");
            assertEquals(1, countRes.rows().size());
            assertEquals(4L, ((io.nexusdb.core.record.Value.BigIntValue) countRes.rows().get(0).getValue(0)).value());

            // 6. Rejected Cross-Shard Operations (ADR-004)
            assertThrows(UnsupportedCrossShardOperationException.class, () -> router.execute("BEGIN;"));
            assertThrows(UnsupportedCrossShardOperationException.class, () -> router.execute("UPDATE users SET age = 99;"));
        } finally {
            dbShard0.close();
            dbShard1.close();
        }
    }
}
