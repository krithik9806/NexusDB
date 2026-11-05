package io.nexusdb.core.engine;

import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseEndToEndTest {

    @TempDir
    Path tempDir;

    @Test
    void testEndToEndMultiStatementScriptAndPersistenceAcrossRestart() {
        Path dbPath = tempDir.resolve("shop.db");

        try (Database db = Database.open(dbPath)) {
            ResultSet createDbRes = db.execute("CREATE DATABASE shop;");
            assertEquals("CREATE DATABASE shop", createDbRes.statusMessage());

            String script = """
                    CREATE TABLE products (
                        id INT PRIMARY KEY,
                        name VARCHAR(40) NOT NULL,
                        category VARCHAR(20),
                        price DOUBLE NOT NULL,
                        in_stock BOOLEAN NOT NULL
                    );
                    INSERT INTO products VALUES
                        (1, 'Mechanical Keyboard', 'Peripherals', 129.50, TRUE),
                        (2, 'USB-C Hub', 'Accessories', 39.50, TRUE),
                        (3, '27-inch Monitor', 'Displays', 299.00, FALSE),
                        (4, 'Desk Mat', NULL, 24.50, TRUE),
                        (5, 'Webcam', 'Peripherals', 79.00, TRUE);
                    UPDATE products SET price = price - 10.0 WHERE category = 'Peripherals';
                    DELETE FROM products WHERE in_stock = FALSE;
                    SELECT id, name, price FROM products WHERE price >= 30.0 ORDER BY price DESC LIMIT 2;
                    """;

            ResultSet selectRes = db.execute(script);
            assertTrue(selectRes.hasResultSet());
            assertEquals(3, selectRes.columns().size());
            assertEquals(2, selectRes.rows().size());
            assertTrue(selectRes.executionTimeNanos() > 0);

            assertEquals(
                    new Tuple(Value.ofInt(1), Value.ofVarchar("Mechanical Keyboard"), Value.ofDouble(119.50)),
                    selectRes.rows().get(0));
            assertEquals(
                    new Tuple(Value.ofInt(5), Value.ofVarchar("Webcam"), Value.ofDouble(69.00)),
                    selectRes.rows().get(1));
        }

        // Reopen database and verify catalog & data persisted on disk
        try (Database reopened = Database.open(dbPath)) {
            ResultSet res = reopened.execute("SELECT COUNT_ID.id, COUNT_ID.name FROM products AS COUNT_ID ORDER BY id ASC;");
            assertEquals(4, res.rows().size());
            assertEquals(Value.ofInt(1), res.rows().get(0).getValue(0));
            assertEquals(Value.ofInt(2), res.rows().get(1).getValue(0));
            assertEquals(Value.ofInt(4), res.rows().get(2).getValue(0));
            assertEquals(Value.ofInt(5), res.rows().get(3).getValue(0));
        }
    }

    @Test
    void testSortWithNullOrderingAscNullsLastAndDescNullsFirst() {
        try (Database db = Database.open(tempDir.resolve("sort_nulls.db"))) {
            db.execute("""
                    CREATE TABLE scores (
                        id INT PRIMARY KEY,
                        tier INT,
                        points INT
                    );
                    INSERT INTO scores VALUES
                        (1, 2, 50),
                        (2, NULL, 100),
                        (3, 1, 80),
                        (4, 2, NULL),
                        (5, 1, 90);
                    """);

            // ASC: NULLS LAST -> tier 1, tier 1, tier 2, tier 2, tier NULL
            // Secondary key points DESC: NULLS FIRST -> for tier 2, (id=4, points=NULL) comes before (id=1, points=50)
            ResultSet ascTierDescPoints = db.execute(
                    "SELECT id, tier, points FROM scores ORDER BY tier ASC, points DESC");
            List<Tuple> rows = ascTierDescPoints.rows();
            assertEquals(5, rows.size());
            assertEquals(Value.ofInt(5), rows.get(0).getValue(0)); // tier=1, points=90
            assertEquals(Value.ofInt(3), rows.get(1).getValue(0)); // tier=1, points=80
            assertEquals(Value.ofInt(4), rows.get(2).getValue(0)); // tier=2, points=NULL (NULLS FIRST in DESC)
            assertEquals(Value.ofInt(1), rows.get(3).getValue(0)); // tier=2, points=50
            assertEquals(Value.ofInt(2), rows.get(4).getValue(0)); // tier=NULL (NULLS LAST in ASC)

            // DESC on tier: NULLS FIRST -> (id=2, tier=NULL) is first
            ResultSet descTier = db.execute("SELECT id, tier FROM scores ORDER BY tier DESC, id ASC");
            assertEquals(Value.ofInt(2), descTier.rows().get(0).getValue(0));
            assertTrue(descTier.rows().get(0).getValue(1).isNull());
        }
    }

    @Test
    void testAsciiTableFormattingAndExplain() {
        try (Database db = Database.open(tempDir.resolve("format.db"))) {
            db.execute("""
                    CREATE TABLE items (id INT PRIMARY KEY, label VARCHAR(20));
                    INSERT INTO items VALUES (1, 'Alpha'), (2, NULL);
                    """);

            ResultSet rs = db.execute("SELECT id, label FROM items ORDER BY id ASC");
            String table = rs.formatAsciiTable();
            String expected = """
                    +----+-------+
                    | id | label |
                    +----+-------+
                    | 1  | Alpha |
                    | 2  | NULL  |
                    +----+-------+
                    (2 rows)""";
            assertEquals(expected, table);

            ResultSet explainRs = db.execute("EXPLAIN SELECT id FROM items WHERE id = 1 ORDER BY id DESC LIMIT 1");
            String explainFormatted = explainRs.formatAsciiTable();
            assertTrue(explainFormatted.contains("Limit(count=1)"));
            assertTrue(explainFormatted.contains("Projection(columns=[id])"));
            assertTrue(explainFormatted.contains("Sort(keys=1)"));
            assertTrue(explainFormatted.contains("Filter("));
            assertTrue(explainFormatted.contains("SeqScan(table=items)"));
        }
    }
}
