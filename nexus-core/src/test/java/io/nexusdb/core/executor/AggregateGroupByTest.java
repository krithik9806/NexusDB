package io.nexusdb.core.executor;

import io.nexusdb.core.ast.AstPrinter;
import io.nexusdb.core.ast.Statement;
import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;
import io.nexusdb.core.parser.ParseException;
import io.nexusdb.core.parser.Parser;
import io.nexusdb.core.planner.BindException;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class AggregateGroupByTest {

    @TempDir
    Path tempDir;

    @Test
    void testAstPrinterRoundTripForAggregatesGroupByHaving() {
        String sql = "SELECT dept, COUNT(*) AS cnt, SUM(salary) AS total_sal, AVG(bonus) AS avg_bonus, MIN(age), MAX(age) " +
                "FROM employees WHERE active = TRUE GROUP BY dept HAVING COUNT(*) >= 2 AND SUM(salary) > 100000 " +
                "ORDER BY total_sal DESC, dept ASC LIMIT 5";
        Statement parsed = Parser.parse(sql);
        String printed = AstPrinter.print(parsed);
        Statement reparsed = Parser.parse(printed);
        assertEquals(parsed, reparsed);
        assertEquals(printed, AstPrinter.print(reparsed));

        assertThrows(ParseException.class, () -> Parser.parse("SELECT SUM(*) FROM employees"));
        assertThrows(ParseException.class, () -> Parser.parse("SELECT AVG(*) FROM employees"));
    }

    @Test
    void testUngroupedAggregatesAndNullValueHandling() {
        try (Database db = Database.open(tempDir.resolve("agg_ungrouped.db"), 64)) {
            db.execute("CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR(20), amount INT, discount DOUBLE)");
            db.execute("""
                    INSERT INTO sales VALUES
                        (1, 'North', 100, 10.0),
                        (2, 'North', 200, NULL),
                        (3, 'South', NULL, 5.0),
                        (4, NULL, 300, 15.0)
                    """);

            ResultSet rs = db.execute(
                    "SELECT COUNT(*), COUNT(amount), COUNT(discount), SUM(amount), AVG(amount), MIN(amount), MAX(amount), SUM(discount), AVG(discount) FROM sales");
            assertEquals(1, rs.rows().size());
            Tuple row = rs.rows().get(0);
            assertEquals(Value.ofBigInt(4L), row.getValue(0));      // COUNT(*) counts all 4 rows
            assertEquals(Value.ofBigInt(3L), row.getValue(1));      // COUNT(amount) ignores NULL
            assertEquals(Value.ofBigInt(3L), row.getValue(2));      // COUNT(discount) ignores NULL
            assertEquals(Value.ofBigInt(600L), row.getValue(3));    // SUM(amount) = 100 + 200 + 300
            assertEquals(Value.ofDouble(200.0), row.getValue(4));   // AVG(amount) = 600 / 3
            assertEquals(Value.ofInt(100), row.getValue(5));        // MIN(amount) = 100
            assertEquals(Value.ofInt(300), row.getValue(6));        // MAX(amount) = 300
            assertEquals(Value.ofDouble(30.0), row.getValue(7));    // SUM(discount) = 10 + 5 + 15
            assertEquals(Value.ofDouble(10.0), row.getValue(8));    // AVG(discount) = 30 / 3
        }
    }

    @Test
    void testEmptyInputBehaviorForUngroupedAndGroupedAggregates() {
        try (Database db = Database.open(tempDir.resolve("agg_empty.db"), 64)) {
            db.execute("CREATE TABLE items (id INT PRIMARY KEY, category VARCHAR(20), price INT)");

            // 1. Empty table without GROUP BY -> 1 row: COUNT = 0, SUM/AVG/MIN/MAX = NULL
            ResultSet ungroupedEmpty = db.execute(
                    "SELECT COUNT(*), COUNT(price), SUM(price), AVG(price), MIN(price), MAX(price) FROM items");
            assertEquals(1, ungroupedEmpty.rows().size());
            Tuple emptyRow = ungroupedEmpty.rows().get(0);
            assertEquals(Value.ofBigInt(0L), emptyRow.getValue(0));
            assertEquals(Value.ofBigInt(0L), emptyRow.getValue(1));
            assertEquals(Value.NULL, emptyRow.getValue(2));
            assertEquals(Value.NULL, emptyRow.getValue(3));
            assertEquals(Value.NULL, emptyRow.getValue(4));
            assertEquals(Value.NULL, emptyRow.getValue(5));

            // 2. Empty table with GROUP BY -> 0 rows
            ResultSet groupedEmpty = db.execute(
                    "SELECT category, COUNT(*), SUM(price) FROM items GROUP BY category");
            assertTrue(groupedEmpty.rows().isEmpty());

            // 3. Non-empty table where all rows are filtered out by WHERE:
            db.execute("INSERT INTO items VALUES (1, 'Books', NULL), (2, 'Books', NULL)");
            ResultSet allNullCol = db.execute(
                    "SELECT COUNT(*), COUNT(price), SUM(price), AVG(price), MIN(price), MAX(price) FROM items");
            assertEquals(1, allNullCol.rows().size());
            Tuple allNullRow = allNullCol.rows().get(0);
            assertEquals(Value.ofBigInt(2L), allNullRow.getValue(0));
            assertEquals(Value.ofBigInt(0L), allNullRow.getValue(1));
            assertEquals(Value.NULL, allNullRow.getValue(2));
            assertEquals(Value.NULL, allNullRow.getValue(3));
            assertEquals(Value.NULL, allNullRow.getValue(4));
            assertEquals(Value.NULL, allNullRow.getValue(5));

            // 4. Ungrouped aggregate with HAVING that filters out the single empty group -> 0 rows
            ResultSet filteredHaving = db.execute(
                    "SELECT COUNT(*) FROM items WHERE id > 100 HAVING COUNT(*) > 0");
            assertTrue(filteredHaving.rows().isEmpty());
        }
    }

    @Test
    void testGroupByWithNullGroupKeysHavingOrderByAndLimit() {
        try (Database db = Database.open(tempDir.resolve("agg_grouped.db"), 64)) {
            db.execute("CREATE TABLE employees (id INT PRIMARY KEY, dept VARCHAR(20), role VARCHAR(20), salary BIGINT)");
            db.execute("""
                    INSERT INTO employees VALUES
                        (1, 'Eng', 'Senior', 150000),
                        (2, 'Eng', 'Junior', 100000),
                        (3, 'Eng', 'Senior', 170000),
                        (4, 'Sales', 'Rep', 80000),
                        (5, 'Sales', 'Lead', 120000),
                        (6, 'HR', 'Spec', 70000),
                        (7, NULL, 'Intern', 40000),
                        (8, NULL, 'Intern', 50000)
                    """);

            // Group by single column including NULL group key
            ResultSet byDept = db.execute("""
                    SELECT dept, COUNT(*) AS headcount, SUM(salary) AS total_salary, AVG(salary) AS avg_salary, MIN(salary), MAX(salary)
                    FROM employees
                    GROUP BY dept
                    ORDER BY total_salary DESC
                    """);
            assertEquals(4, byDept.rows().size());
            assertEquals(
                    new Tuple(Value.ofVarchar("Eng"), Value.ofBigInt(3L), Value.ofBigInt(420000L), Value.ofDouble(140000.0), Value.ofBigInt(100000L), Value.ofBigInt(170000L)),
                    byDept.rows().get(0));
            assertEquals(
                    new Tuple(Value.ofVarchar("Sales"), Value.ofBigInt(2L), Value.ofBigInt(200000L), Value.ofDouble(100000.0), Value.ofBigInt(80000L), Value.ofBigInt(120000L)),
                    byDept.rows().get(1));
            assertEquals(
                    new Tuple(Value.NULL, Value.ofBigInt(2L), Value.ofBigInt(90000L), Value.ofDouble(45000.0), Value.ofBigInt(40000L), Value.ofBigInt(50000L)),
                    byDept.rows().get(2));
            assertEquals(
                    new Tuple(Value.ofVarchar("HR"), Value.ofBigInt(1L), Value.ofBigInt(70000L), Value.ofDouble(70000.0), Value.ofBigInt(70000L), Value.ofBigInt(70000L)),
                    byDept.rows().get(3));

            // Group by multiple columns + HAVING + ORDER BY aggregate expression + LIMIT
            ResultSet havingRs = db.execute("""
                    SELECT dept, COUNT(*) AS cnt, SUM(salary) AS sum_sal
                    FROM employees
                    GROUP BY dept
                    HAVING COUNT(*) >= 2 AND AVG(salary) >= 45000
                    ORDER BY COUNT(*) DESC, sum_sal ASC
                    LIMIT 2
                    """);
            assertEquals(2, havingRs.rows().size());
            assertEquals(new Tuple(Value.ofVarchar("Eng"), Value.ofBigInt(3L), Value.ofBigInt(420000L)), havingRs.rows().get(0));
            assertEquals(new Tuple(Value.NULL, Value.ofBigInt(2L), Value.ofBigInt(90000L)), havingRs.rows().get(1));
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @CsvSource(delimiter = '|', value = {
            "Non-grouped col in SELECT with GROUP BY  | SELECT dept, role, COUNT(*) FROM employees GROUP BY dept                  | must appear in the GROUP BY clause",
            "Non-grouped col in SELECT with aggregate | SELECT dept, COUNT(*) FROM employees                                      | must appear in the GROUP BY clause",
            "Non-grouped col in HAVING                | SELECT dept, COUNT(*) FROM employees GROUP BY dept HAVING salary > 50000  | must appear in the GROUP BY clause",
            "Non-grouped col in ORDER BY              | SELECT dept, COUNT(*) FROM employees GROUP BY dept ORDER BY salary DESC   | must appear in the GROUP BY clause",
            "Nested aggregate SUM(COUNT(*))           | SELECT SUM(COUNT(*)) FROM employees                                       | Nested aggregate functions are not allowed",
            "Nested aggregate MAX(AVG(salary))        | SELECT MAX(AVG(salary)) FROM employees                                    | Nested aggregate functions are not allowed",
            "Aggregate in WHERE clause                | SELECT dept FROM employees WHERE COUNT(*) > 1                             | not allowed in this clause",
            "Aggregate in GROUP BY clause             | SELECT dept, COUNT(*) FROM employees GROUP BY COUNT(*)                    | not allowed in this clause",
            "SUM on VARCHAR column                    | SELECT SUM(dept) FROM employees                                           | Aggregate SUM requires a numeric argument",
            "AVG on VARCHAR column                    | SELECT AVG(dept) FROM employees                                           | Aggregate AVG requires a numeric argument"
    })
    void testGroupingAndAggregateSemanticErrorsThrowBindException(
            String description,
            String sql,
            String expectedFragment) {
        try (Database db = Database.open(tempDir.resolve("agg_errors.db"), 32)) {
            db.execute("CREATE TABLE employees (id INT PRIMARY KEY, dept VARCHAR(20), role VARCHAR(20), salary BIGINT)");
            BindException ex = assertThrows(BindException.class, () -> db.execute(sql), description);
            assertTrue(
                    ex.getMessage().contains(expectedFragment),
                    () -> "Expected '" + expectedFragment + "' in message, got: " + ex.getMessage());
        }
    }

    @Test
    void testExplainAndExplainAnalyzeShowHashAggregateOperator() {
        try (Database db = Database.open(tempDir.resolve("agg_explain.db"), 64)) {
            db.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, total INT)");
            db.execute("INSERT INTO orders VALUES (1, 10, 100), (2, 10, 250), (3, 20, 50)");

            String explain = explainText(db,
                    "EXPLAIN SELECT customer_id, COUNT(*), SUM(total) FROM orders GROUP BY customer_id HAVING SUM(total) > 100");
            assertTrue(explain.contains("HashAggregate(keys=[customer_id], aggregates=[COUNT(*), SUM(total)])"), explain);
            assertTrue(explain.contains("Filter(predicate="), explain);

            String explainAnalyze = explainText(db,
                    "EXPLAIN ANALYZE SELECT customer_id, COUNT(*), SUM(total) FROM orders GROUP BY customer_id HAVING SUM(total) > 100");
            assertTrue(explainAnalyze.contains("HashAggregate(keys=[customer_id], aggregates=[COUNT(*), SUM(total)])"), explainAnalyze);
            assertTrue(explainAnalyze.contains("actualRows=2"), explainAnalyze);
            assertTrue(explainAnalyze.contains("actualRows=1"), explainAnalyze);
        }
    }

    private static String explainText(Database db, String sql) {
        ResultSet rs = db.execute(sql);
        List<Tuple> rows = rs.rows();
        return rows.stream()
                .map(r -> ((Value.VarcharValue) r.getValue(0)).value())
                .collect(Collectors.joining("\n"));
    }
}
