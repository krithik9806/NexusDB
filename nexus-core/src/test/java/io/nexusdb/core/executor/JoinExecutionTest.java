package io.nexusdb.core.executor;

import io.nexusdb.core.ast.AstPrinter;
import io.nexusdb.core.ast.Statement;
import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;
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

class JoinExecutionTest {

    @TempDir
    Path tempDir;

    @Test
    void testAstPrinterRoundTripForInnerAndLeftJoins() {
        String sql = "SELECT u.id, u.name, o.total, p.method " +
                "FROM users AS u " +
                "INNER JOIN orders AS o ON u.id = o.user_id " +
                "LEFT OUTER JOIN payments p ON o.id = p.order_id " +
                "WHERE o.total >= 50 " +
                "ORDER BY u.id ASC";
        Statement parsed = Parser.parse(sql);
        String printed = AstPrinter.print(parsed);
        Statement reparsed = Parser.parse(printed);
        assertEquals(parsed, reparsed);
        assertEquals(printed, AstPrinter.print(reparsed));
    }

    @Test
    void testInnerJoinLeftJoinAndNullKeysNeverMatch() {
        try (Database db = Database.open(tempDir.resolve("join_basic.db"), 64)) {
            db.execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(30), ref_code INT)");
            db.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, ref_code INT, amount INT)");

            db.execute("""
                    INSERT INTO customers VALUES
                        (1, 'Alice', 100),
                        (2, 'Bob', 200),
                        (3, 'Charlie', NULL),
                        (4, 'Diana', 400)
                    """);
            db.execute("""
                    INSERT INTO orders VALUES
                        (10, 1, 100, 50),
                        (11, 1, 100, 75),
                        (12, 2, 999, 120),
                        (13, NULL, NULL, 300)
                    """);

            // 1. INNER JOIN on customers.id = orders.customer_id
            ResultSet innerRs = db.execute("""
                    SELECT c.name, o.id AS order_id, o.amount
                    FROM customers AS c
                    INNER JOIN orders AS o ON c.id = o.customer_id
                    ORDER BY o.id ASC
                    """);
            assertEquals(3, innerRs.rows().size());
            assertEquals(new Tuple(Value.ofVarchar("Alice"), Value.ofInt(10), Value.ofInt(50)), innerRs.rows().get(0));
            assertEquals(new Tuple(Value.ofVarchar("Alice"), Value.ofInt(11), Value.ofInt(75)), innerRs.rows().get(1));
            assertEquals(new Tuple(Value.ofVarchar("Bob"), Value.ofInt(12), Value.ofInt(120)), innerRs.rows().get(2));

            // 2. SQL rule: NULL = NULL never matches in join ON conditions!
            // Both Charlie (id=3) and order 13 have ref_code = NULL; they must NOT match each other.
            ResultSet nullKeyInner = db.execute("""
                    SELECT c.name, o.id
                    FROM customers c
                    JOIN orders o ON c.ref_code = o.ref_code
                    ORDER BY o.id ASC
                    """);
            assertEquals(2, nullKeyInner.rows().size());
            assertEquals(new Tuple(Value.ofVarchar("Alice"), Value.ofInt(10)), nullKeyInner.rows().get(0));
            assertEquals(new Tuple(Value.ofVarchar("Alice"), Value.ofInt(11)), nullKeyInner.rows().get(1));

            // 3. LEFT JOIN pads unmatched left rows (including NULL join keys on left) with NULLs on right
            ResultSet leftRs = db.execute("""
                    SELECT c.id, c.name, o.id AS order_id, o.amount
                    FROM customers c
                    LEFT OUTER JOIN orders o ON c.id = o.customer_id
                    ORDER BY c.id ASC, o.id ASC
                    """);
            assertEquals(5, leftRs.rows().size());
            assertEquals(new Tuple(Value.ofInt(1), Value.ofVarchar("Alice"), Value.ofInt(10), Value.ofInt(50)), leftRs.rows().get(0));
            assertEquals(new Tuple(Value.ofInt(1), Value.ofVarchar("Alice"), Value.ofInt(11), Value.ofInt(75)), leftRs.rows().get(1));
            assertEquals(new Tuple(Value.ofInt(2), Value.ofVarchar("Bob"), Value.ofInt(12), Value.ofInt(120)), leftRs.rows().get(2));
            assertEquals(new Tuple(Value.ofInt(3), Value.ofVarchar("Charlie"), Value.NULL, Value.NULL), leftRs.rows().get(3));
            assertEquals(new Tuple(Value.ofInt(4), Value.ofVarchar("Diana"), Value.NULL, Value.NULL), leftRs.rows().get(4));
        }
    }

    @Test
    void testThreeTableJoinWithGroupByHavingAndOrderBy() {
        try (Database db = Database.open(tempDir.resolve("join_3table.db"), 64)) {
            db.execute("CREATE TABLE departments (id INT PRIMARY KEY, dept_name VARCHAR(20))");
            db.execute("CREATE TABLE employees (id INT PRIMARY KEY, emp_name VARCHAR(20), dept_id INT)");
            db.execute("CREATE TABLE projects (id INT PRIMARY KEY, emp_id INT, budget BIGINT)");

            db.execute("""
                    INSERT INTO departments VALUES
                        (1, 'Engineering'),
                        (2, 'Sales'),
                        (3, 'Legal')
                    """);
            db.execute("""
                    INSERT INTO employees VALUES
                        (10, 'Alice', 1),
                        (20, 'Bob', 1),
                        (30, 'Charlie', 2)
                    """);
            db.execute("""
                    INSERT INTO projects VALUES
                        (100, 10, 50000),
                        (101, 10, 70000),
                        (102, 20, 30000),
                        (103, 30, 40000)
                    """);

            // 3-table LEFT JOIN + GROUP BY + COUNT(col) + SUM(col) + ORDER BY
            ResultSet rs = db.execute("""
                    SELECT d.dept_name, COUNT(p.id) AS project_count, SUM(p.budget) AS total_budget
                    FROM departments d
                    LEFT JOIN employees e ON d.id = e.dept_id
                    LEFT JOIN projects p ON e.id = p.emp_id
                    GROUP BY d.dept_name
                    ORDER BY project_count DESC, d.dept_name ASC
                    """);
            assertEquals(3, rs.rows().size());
            assertEquals(
                    new Tuple(Value.ofVarchar("Engineering"), Value.ofBigInt(3L), Value.ofBigInt(150000L)),
                    rs.rows().get(0));
            assertEquals(
                    new Tuple(Value.ofVarchar("Sales"), Value.ofBigInt(1L), Value.ofBigInt(40000L)),
                    rs.rows().get(1));
            // Legal has 0 employees and 0 projects -> COUNT(p.id) = 0, SUM(p.budget) = NULL
            assertEquals(
                    new Tuple(Value.ofVarchar("Legal"), Value.ofBigInt(0L), Value.NULL),
                    rs.rows().get(2));
        }
    }

    @Test
    void testNonEquiJoinUsesNestedLoopJoinExecutorAndExplainOutput() {
        try (Database db = Database.open(tempDir.resolve("join_nonequi_explain.db"), 64)) {
            db.execute("CREATE TABLE tax_brackets (id INT PRIMARY KEY, min_income INT, max_income INT, rate DOUBLE)");
            db.execute("CREATE TABLE earners (id INT PRIMARY KEY, name VARCHAR(20), income INT)");

            db.execute("""
                    INSERT INTO tax_brackets VALUES
                        (1, 0, 50000, 0.10),
                        (2, 50001, 100000, 0.20),
                        (3, 100001, 500000, 0.30)
                    """);
            db.execute("""
                    INSERT INTO earners VALUES
                        (1, 'Alice', 45000),
                        (2, 'Bob', 80000),
                        (3, 'Charlie', 150000)
                    """);

            String explainNonEqui = explainText(db, """
                    EXPLAIN SELECT e.name, b.rate
                    FROM earners e
                    INNER JOIN tax_brackets b ON e.income >= b.min_income AND e.income <= b.max_income
                    ORDER BY e.id ASC
                    """);
            assertTrue(explainNonEqui.contains("NestedLoopJoin(type=INNER"), explainNonEqui);

            ResultSet rs = db.execute("""
                    SELECT e.name, b.rate
                    FROM earners e
                    INNER JOIN tax_brackets b ON e.income >= b.min_income AND e.income <= b.max_income
                    ORDER BY e.id ASC
                    """);
            assertEquals(3, rs.rows().size());
            assertEquals(new Tuple(Value.ofVarchar("Alice"), Value.ofDouble(0.10)), rs.rows().get(0));
            assertEquals(new Tuple(Value.ofVarchar("Bob"), Value.ofDouble(0.20)), rs.rows().get(1));
            assertEquals(new Tuple(Value.ofVarchar("Charlie"), Value.ofDouble(0.30)), rs.rows().get(2));

            // Verify HashJoin chooses the smaller table as build side in EXPLAIN
            db.execute("CREATE TABLE small_lookup (code INT PRIMARY KEY, label VARCHAR(20))");
            db.execute("CREATE TABLE large_events (id INT PRIMARY KEY, code INT)");
            db.execute("INSERT INTO small_lookup VALUES (1, 'LOGIN'), (2, 'LOGOUT')");
            for (int i = 1; i <= 30; i++) {
                db.execute("INSERT INTO large_events VALUES (" + i + ", " + ((i % 2) + 1) + ")");
            }

            // When small_lookup is on the left (2 rows vs 30 rows), HashJoin builds on left
            String explainBuildLeft = explainText(db, """
                    EXPLAIN ANALYZE SELECT s.label, l.id
                    FROM small_lookup s
                    INNER JOIN large_events l ON s.code = l.code
                    """);
            assertTrue(explainBuildLeft.contains("HashJoin(type=INNER, condition=s.code = l.code, build=left)"), explainBuildLeft);
            assertTrue(explainBuildLeft.contains("actualRows=30"), explainBuildLeft);
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @CsvSource(delimiter = '|', value = {
            "Ambiguous unqualified column in SELECT  | SELECT id FROM customers c JOIN orders o ON c.id = o.customer_id           | Ambiguous column reference 'id'",
            "Ambiguous unqualified column in WHERE   | SELECT c.id FROM customers c JOIN orders o ON c.id = o.customer_id WHERE id = 1 | Ambiguous column reference 'id'",
            "Unknown table qualifier in SELECT       | SELECT x.id FROM customers c JOIN orders o ON c.id = o.customer_id         | Unknown table qualifier 'x'",
            "Duplicate table alias in JOIN           | SELECT c.id FROM customers c JOIN orders c ON c.id = c.customer_id         | Duplicate table name or alias 'c'",
            "Non-boolean JOIN ON condition           | SELECT c.id FROM customers c JOIN orders o ON c.id + o.customer_id         | JOIN ON clause must evaluate to BOOLEAN"
    })
    void testJoinSemanticErrorsThrowBindException(
            String description,
            String sql,
            String expectedFragment) {
        try (Database db = Database.open(tempDir.resolve("join_errors.db"), 32)) {
            db.execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(20))");
            db.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT)");
            BindException ex = assertThrows(BindException.class, () -> db.execute(sql), description);
            assertTrue(
                    ex.getMessage().contains(expectedFragment),
                    () -> "Expected '" + expectedFragment + "' in message, got: " + ex.getMessage());
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
