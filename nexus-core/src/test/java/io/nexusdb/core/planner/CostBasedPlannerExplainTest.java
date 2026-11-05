package io.nexusdb.core.planner;

import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class CostBasedPlannerExplainTest {

    @TempDir
    Path tempDir;

    @Test
    void testTableStatisticsTrackingAndPersistence() {
        Path dbFile = tempDir.resolve("stats_db.db");
        try (Database db = Database.open(dbFile, 128)) {
            db.execute("CREATE TABLE metrics (id INT PRIMARY KEY, payload VARCHAR(100))");
            TableMetadata meta = db.getCatalog().getTable("metrics");
            assertNotNull(meta);
            TableStatistics emptyStats = TableStatistics.from(meta);
            assertEquals(0L, emptyStats.rowCount());
            assertEquals(1, emptyStats.pageCount());

            String pad = "x".repeat(80);
            for (int i = 1; i <= 200; i++) {
                db.execute("INSERT INTO metrics VALUES (" + i + ", '" + pad + "')");
            }

            TableStatistics afterInsert = TableStatistics.from(meta);
            assertEquals(200L, afterInsert.rowCount());
            assertTrue(afterInsert.pageCount() > 1, "200 90-byte tuples must span multiple 4KB pages");

            db.execute("DELETE FROM metrics WHERE id <= 50");
            TableStatistics afterDelete = TableStatistics.from(meta);
            assertEquals(150L, afterDelete.rowCount());
        }

        // Reopen database and verify statistics are accurately reconstructed from the heap
        try (Database db = Database.open(dbFile, 128)) {
            TableMetadata meta = db.getCatalog().getTable("metrics");
            assertNotNull(meta);
            TableStatistics reopenedStats = TableStatistics.from(meta);
            assertEquals(150L, reopenedStats.rowCount());
            assertTrue(reopenedStats.pageCount() > 1);
        }
    }

    @Test
    void testCostBasedPlannerIndexVsSeqScanAndExplain() {
        Path dbFile = tempDir.resolve("planner_db.db");
        try (Database db = Database.open(dbFile, 256)) {
            db.execute("CREATE TABLE orders (" +
                    "id INT PRIMARY KEY, " +
                    "customer_id INT NOT NULL, " +
                    "amount INT NOT NULL, " +
                    "status VARCHAR(20) NOT NULL)");
            db.execute("CREATE INDEX idx_orders_customer ON orders (customer_id)");

            for (int i = 1; i <= 500; i++) {
                String status = (i % 2 == 0) ? "PAID" : "PENDING";
                db.execute("INSERT INTO orders VALUES (" + i + ", " + (i % 50) + ", " + (i * 10) + ", '" + status + "')");
            }

            // 1. Primary key equality lookup -> IndexScan on pk_orders_id
            String pkExplain = explainText(db, "EXPLAIN SELECT * FROM orders WHERE id = 250");
            assertTrue(pkExplain.contains("IndexScan(table=orders, index=pk_orders_id, predicate=id = 250)"), pkExplain);
            assertTrue(pkExplain.contains("cost="), pkExplain);
            assertTrue(pkExplain.contains("rows=1"), pkExplain);
            assertFalse(pkExplain.contains("actualRows="), "Standard EXPLAIN must not include actualRows");

            // 2. Secondary non-unique index equality with residual filter -> Filter over IndexScan
            String secExplain = explainText(db,
                    "EXPLAIN SELECT id, amount FROM orders WHERE customer_id = 10 AND status = 'PAID'");
            assertTrue(secExplain.contains("IndexScan(table=orders, index=idx_orders_customer, predicate=customer_id = 10)"),
                    secExplain);
            assertTrue(secExplain.contains("Filter(predicate="), secExplain);
            assertTrue(secExplain.contains("columnName=status"), secExplain);
            assertTrue(secExplain.contains("value=PAID"), secExplain);

            // 3. Unindexed column predicate -> Filter over SeqScan
            String unindexedExplain = explainText(db,
                    "EXPLAIN SELECT id FROM orders WHERE status = 'PAID'");
            assertTrue(unindexedExplain.contains("SeqScan(table=orders)"), unindexedExplain);
            assertTrue(unindexedExplain.contains("Filter(predicate="), unindexedExplain);

            // 4. Selective range query on primary key -> IndexScan
            String rangeExplain = explainText(db,
                    "EXPLAIN SELECT id FROM orders WHERE id >= 100 AND id <= 110");
            assertTrue(rangeExplain.contains("IndexScan(table=orders, index=pk_orders_id, predicate=id >= 100 AND id <= 110)"),
                    rangeExplain);
        }
    }

    @Test
    void testExplainAnalyzeIncludesActualRowsAndTiming() {
        Path dbFile = tempDir.resolve("explain_analyze_db.db");
        try (Database db = Database.open(dbFile, 256)) {
            db.execute("CREATE TABLE products (id INT PRIMARY KEY, category INT NOT NULL, price INT NOT NULL)");
            db.execute("CREATE INDEX idx_products_category ON products (category)");

            for (int i = 1; i <= 300; i++) {
                db.execute("INSERT INTO products VALUES (" + i + ", " + (i % 10) + ", " + (i * 5) + ")");
            }

            String analyzeOutput = explainText(db,
                    "EXPLAIN ANALYZE SELECT id, price FROM products WHERE category = 3 AND price > 500 ORDER BY price DESC LIMIT 5");

            assertTrue(analyzeOutput.contains("Limit(count=5) [cost="), analyzeOutput);
            assertTrue(analyzeOutput.contains("actualRows=5, timeMs="), analyzeOutput);
            assertTrue(analyzeOutput.contains("Sort(keys=1) [cost="), analyzeOutput);
            assertTrue(analyzeOutput.contains("Projection(columns=[id, price]) [cost="), analyzeOutput);
            assertTrue(analyzeOutput.contains("Filter(predicate="), analyzeOutput);
            assertTrue(analyzeOutput.contains("IndexScan(table=products, index=idx_products_category, predicate=category = 3)"),
                    analyzeOutput);
            assertTrue(analyzeOutput.contains("actualRows=30, timeMs="),
                    "IndexScan on category = 3 (1/10 of 300 rows) must report actualRows=30: " + analyzeOutput);
        }
    }

    private String explainText(Database db, String sql) {
        ResultSet rs = db.execute(sql);
        List<Tuple> rows = rs.rows();
        return rows.stream()
                .map(r -> ((Value.VarcharValue) r.getValue(0)).value())
                .collect(Collectors.joining("\n"));
    }
}
