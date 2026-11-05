package io.nexusdb.core.executor;

import io.nexusdb.core.ast.Expr;
import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.catalog.Catalog;
import io.nexusdb.core.parser.Parser;
import io.nexusdb.core.planner.Binder;
import io.nexusdb.core.planner.BoundExpr;
import io.nexusdb.core.planner.BoundStatement;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import io.nexusdb.core.storage.DiskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExecutorTest {

    @TempDir
    Path tempDir;

    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private Catalog catalog;
    private Binder binder;
    private ExecutionPlanner planner;

    @BeforeEach
    void setUp() {
        diskManager = DiskManager.open(tempDir.resolve("executor_test.db"));
        bufferPoolManager = new BufferPoolManager(32, diskManager);
        catalog = new Catalog(bufferPoolManager);
        binder = new Binder(catalog);
        planner = new ExecutionPlanner();

        Schema empSchema = new Schema(List.of(
                Column.primaryKey("id", DataType.INT),
                new Column("name", DataType.VARCHAR, 32, false),
                new Column("dept", DataType.VARCHAR, 16, true),
                new Column("salary", DataType.BIGINT, true),
                new Column("bonus", DataType.DOUBLE, true),
                new Column("active", DataType.BOOLEAN, true)
        ));
        catalog.createTable("employees", empSchema);
    }

    @AfterEach
    void tearDown() {
        if (bufferPoolManager != null) {
            bufferPoolManager.close();
        }
        if (diskManager != null) {
            diskManager.close();
        }
    }

    private List<Tuple> executeSql(String sql) {
        BoundStatement bound = binder.bind(Parser.parse(sql));
        try (Executor exec = planner.plan(bound)) {
            exec.init();
            List<Tuple> results = new ArrayList<>();
            Tuple row;
            while ((row = exec.next()) != null) {
                results.add(row);
            }
            return results;
        }
    }

    @Test
    void testThreeValuedNullLogicTruthTables() {
        BoundExpr trueLit = new BoundExpr.BoundLiteral(Value.ofBoolean(true), DataType.BOOLEAN);
        BoundExpr falseLit = new BoundExpr.BoundLiteral(Value.ofBoolean(false), DataType.BOOLEAN);
        BoundExpr nullLit = new BoundExpr.BoundLiteral(Value.NULL, DataType.BOOLEAN);

        // AND truth table
        assertEquals(Value.ofBoolean(true), evalBinary(trueLit, Expr.BinaryOp.AND, trueLit));
        assertEquals(Value.ofBoolean(false), evalBinary(trueLit, Expr.BinaryOp.AND, falseLit));
        assertEquals(Value.NULL, evalBinary(trueLit, Expr.BinaryOp.AND, nullLit));

        assertEquals(Value.ofBoolean(false), evalBinary(falseLit, Expr.BinaryOp.AND, trueLit));
        assertEquals(Value.ofBoolean(false), evalBinary(falseLit, Expr.BinaryOp.AND, falseLit));
        assertEquals(Value.ofBoolean(false), evalBinary(falseLit, Expr.BinaryOp.AND, nullLit));

        assertEquals(Value.NULL, evalBinary(nullLit, Expr.BinaryOp.AND, trueLit));
        assertEquals(Value.ofBoolean(false), evalBinary(nullLit, Expr.BinaryOp.AND, falseLit));
        assertEquals(Value.NULL, evalBinary(nullLit, Expr.BinaryOp.AND, nullLit));

        // OR truth table
        assertEquals(Value.ofBoolean(true), evalBinary(trueLit, Expr.BinaryOp.OR, trueLit));
        assertEquals(Value.ofBoolean(true), evalBinary(trueLit, Expr.BinaryOp.OR, falseLit));
        assertEquals(Value.ofBoolean(true), evalBinary(trueLit, Expr.BinaryOp.OR, nullLit));

        assertEquals(Value.ofBoolean(true), evalBinary(falseLit, Expr.BinaryOp.OR, trueLit));
        assertEquals(Value.ofBoolean(false), evalBinary(falseLit, Expr.BinaryOp.OR, falseLit));
        assertEquals(Value.NULL, evalBinary(falseLit, Expr.BinaryOp.OR, nullLit));

        assertEquals(Value.ofBoolean(true), evalBinary(nullLit, Expr.BinaryOp.OR, trueLit));
        assertEquals(Value.NULL, evalBinary(nullLit, Expr.BinaryOp.OR, falseLit));
        assertEquals(Value.NULL, evalBinary(nullLit, Expr.BinaryOp.OR, nullLit));

        // NOT truth table
        assertEquals(
                Value.ofBoolean(false),
                ExpressionEvaluator.evaluate(new BoundExpr.BoundUnaryExpr(Expr.UnaryOp.NOT, trueLit, DataType.BOOLEAN), null));
        assertEquals(
                Value.ofBoolean(true),
                ExpressionEvaluator.evaluate(new BoundExpr.BoundUnaryExpr(Expr.UnaryOp.NOT, falseLit, DataType.BOOLEAN), null));
        assertEquals(
                Value.NULL,
                ExpressionEvaluator.evaluate(new BoundExpr.BoundUnaryExpr(Expr.UnaryOp.NOT, nullLit, DataType.BOOLEAN), null));

        // Arithmetic and comparisons with NULL yield NULL
        BoundExpr intFive = new BoundExpr.BoundLiteral(Value.ofInt(5), DataType.INT);
        BoundExpr nullInt = new BoundExpr.BoundLiteral(Value.NULL, DataType.INT);
        assertEquals(Value.NULL, evalBinary(intFive, Expr.BinaryOp.ADD, nullInt));
        assertEquals(Value.NULL, evalBinary(intFive, Expr.BinaryOp.EQ, nullInt));
        assertEquals(Value.NULL, evalBinary(nullInt, Expr.BinaryOp.EQ, nullInt));

        // IS NULL / IS NOT NULL always evaluate to TRUE or FALSE
        assertEquals(
                Value.ofBoolean(true),
                ExpressionEvaluator.evaluate(new BoundExpr.BoundIsNullExpr(nullInt, false), null));
        assertEquals(
                Value.ofBoolean(false),
                ExpressionEvaluator.evaluate(new BoundExpr.BoundIsNullExpr(nullInt, true), null));
    }

    private static Value evalBinary(BoundExpr left, Expr.BinaryOp op, BoundExpr right) {
        return ExpressionEvaluator.evaluate(
                new BoundExpr.BoundBinaryExpr(left, op, right, DataType.BOOLEAN),
                null);
    }

    @Test
    void testInsertSelectWhereWithAndOrNotAndNulls() {
        List<Tuple> ins = executeSql("""
                INSERT INTO employees VALUES
                    (1, 'Alice', 'Eng', 120000, 15000.5, TRUE),
                    (2, 'Bob', 'Sales', 90000, NULL, TRUE),
                    (3, 'Charlie', NULL, 110000, 5000.0, FALSE),
                    (4, 'Diana', 'Eng', NULL, 10000.0, NULL)
                """);
        assertEquals(1, ins.size());
        assertEquals(Value.ofInt(4), ins.get(0).getValue(0));

        // WHERE with AND, OR, NOT and NULL values (rows where predicate is NULL must be filtered out)
        List<Tuple> rows = executeSql("""
                SELECT id, name FROM employees
                WHERE (dept = 'Eng' AND active = TRUE) OR (NOT active AND bonus IS NOT NULL)
                """);
        assertEquals(2, rows.size());
        assertEquals(new Tuple(Value.ofInt(1), Value.ofVarchar("Alice")), rows.get(0));
        assertEquals(new Tuple(Value.ofInt(3), Value.ofVarchar("Charlie")), rows.get(1));

        // Verify x = NULL filters out all rows whereas x IS NULL matches NULL rows
        List<Tuple> eqNullRows = executeSql("SELECT id FROM employees WHERE dept = NULL");
        assertTrue(eqNullRows.isEmpty());

        List<Tuple> isNullRows = executeSql("SELECT id FROM employees WHERE dept IS NULL");
        assertEquals(1, isNullRows.size());
        assertEquals(Value.ofInt(3), isNullRows.get(0).getValue(0));
    }

    @Test
    void testUpdateAndDeleteAffectExactExpectedRows() {
        executeSql("""
                INSERT INTO employees VALUES
                    (1, 'Alice', 'Eng', 100000, 1000.0, TRUE),
                    (2, 'Bob', 'Eng', 80000, 500.0, TRUE),
                    (3, 'Charlie', 'HR', 70000, 200.0, FALSE)
                """);

        // Update only Eng employees with salary >= 90000
        List<Tuple> updated = executeSql("""
                UPDATE employees
                SET salary = salary + 10000, dept = 'CoreEng'
                WHERE dept = 'Eng' AND salary >= 90000
                """);
        assertEquals(Value.ofInt(1), updated.get(0).getValue(0));

        List<Tuple> alice = executeSql("SELECT salary, dept FROM employees WHERE id = 1");
        assertEquals(new Tuple(Value.ofBigInt(110000L), Value.ofVarchar("CoreEng")), alice.get(0));

        List<Tuple> bob = executeSql("SELECT salary, dept FROM employees WHERE id = 2");
        assertEquals(new Tuple(Value.ofBigInt(80000L), Value.ofVarchar("Eng")), bob.get(0));

        // Delete inactive employees
        List<Tuple> deleted = executeSql("DELETE FROM employees WHERE NOT active");
        assertEquals(Value.ofInt(1), deleted.get(0).getValue(0));

        List<Tuple> remaining = executeSql("SELECT id FROM employees");
        assertEquals(2, remaining.size());
        assertEquals(Value.ofInt(1), remaining.get(0).getValue(0));
        assertEquals(Value.ofInt(2), remaining.get(1).getValue(0));
    }

    @Test
    void testNumericTypeCoercionInProjectionsComparisonsAndUpdates() {
        executeSql("INSERT INTO employees VALUES (10, 'Eve', 'R&D', 50000, 2500.25, TRUE)");

        // INT (id=10) + BIGINT (salary=50000) -> BIGINT (50010L)
        // BIGINT (salary=50000) + DOUBLE (bonus=2500.25) -> DOUBLE (52500.25)
        List<Tuple> projected = executeSql(
                "SELECT id + salary AS id_plus_sal, salary + bonus AS total_comp FROM employees WHERE id = 10.0");
        assertEquals(1, projected.size());
        assertEquals(Value.ofBigInt(50010L), projected.get(0).getValue(0));
        assertEquals(Value.ofDouble(52500.25), projected.get(0).getValue(1));
    }
}
