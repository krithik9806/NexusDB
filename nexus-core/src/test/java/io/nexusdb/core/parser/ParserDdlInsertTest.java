package io.nexusdb.core.parser;

import io.nexusdb.core.ast.ColumnDef;
import io.nexusdb.core.ast.Expr;
import io.nexusdb.core.ast.Statement;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParserDdlInsertTest {

    @Test
    void testCreateDatabase() {
        Statement stmt = Parser.parse("CREATE DATABASE analytics_db;");
        assertInstanceOf(Statement.CreateDatabaseStatement.class, stmt);
        Statement.CreateDatabaseStatement createDb = (Statement.CreateDatabaseStatement) stmt;
        assertEquals("analytics_db", createDb.databaseName());
    }

    @Test
    void testCreateTableWithAllDataTypesAndConstraints() {
        String sql = """
                CREATE TABLE users (
                    id INT PRIMARY KEY,
                    org_id INTEGER NOT NULL,
                    balance BIGINT,
                    score DOUBLE NOT NULL,
                    ratio FLOAT,
                    active BOOLEAN NOT NULL,
                    verified BOOL,
                    username VARCHAR(64) NOT NULL
                );
                """;
        Statement stmt = Parser.parse(sql);
        assertInstanceOf(Statement.CreateTableStatement.class, stmt);
        Statement.CreateTableStatement createTable = (Statement.CreateTableStatement) stmt;
        assertEquals("users", createTable.tableName());

        List<ColumnDef> cols = createTable.columns();
        assertEquals(8, cols.size());

        assertEquals(new ColumnDef("id", DataType.INT, 4, false, true), cols.get(0));
        assertEquals(new ColumnDef("org_id", DataType.INT, 4, false, false), cols.get(1));
        assertEquals(new ColumnDef("balance", DataType.BIGINT, 8, true, false), cols.get(2));
        assertEquals(new ColumnDef("score", DataType.DOUBLE, 8, false, false), cols.get(3));
        assertEquals(new ColumnDef("ratio", DataType.DOUBLE, 8, true, false), cols.get(4));
        assertEquals(new ColumnDef("active", DataType.BOOLEAN, 1, false, false), cols.get(5));
        assertEquals(new ColumnDef("verified", DataType.BOOLEAN, 1, true, false), cols.get(6));
        assertEquals(new ColumnDef("username", DataType.VARCHAR, 64, false, false), cols.get(7));
    }

    @Test
    void testDropTable() {
        Statement stmt = Parser.parse("DROP TABLE obsolete_logs");
        assertInstanceOf(Statement.DropTableStatement.class, stmt);
        assertEquals("obsolete_logs", ((Statement.DropTableStatement) stmt).tableName());
    }

    @Test
    void testInsertWithoutColumnListSingleRow() {
        Statement stmt = Parser.parse("INSERT INTO users VALUES (1, 'Alice', 99.5, TRUE, NULL);");
        assertInstanceOf(Statement.InsertStatement.class, stmt);
        Statement.InsertStatement insert = (Statement.InsertStatement) stmt;
        assertEquals("users", insert.tableName());
        assertTrue(insert.columns().isEmpty());
        assertEquals(1, insert.rows().size());

        List<Expr> row = insert.rows().get(0);
        assertEquals(5, row.size());
        assertEquals(new Expr.LiteralExpr(Value.ofInt(1)), row.get(0));
        assertEquals(new Expr.LiteralExpr(Value.ofVarchar("Alice")), row.get(1));
        assertEquals(new Expr.LiteralExpr(Value.ofDouble(99.5)), row.get(2));
        assertEquals(new Expr.LiteralExpr(Value.ofBoolean(true)), row.get(3));
        assertEquals(new Expr.LiteralExpr(Value.NULL), row.get(4));
    }

    @Test
    void testInsertWithColumnListAndMultiRowValues() {
        String sql = """
                INSERT INTO items (id, name, delta)
                VALUES (10, 'Bolt', -5),
                       (20, 'Nut', 5000000000);
                """;
        Statement stmt = Parser.parse(sql);
        assertInstanceOf(Statement.InsertStatement.class, stmt);
        Statement.InsertStatement insert = (Statement.InsertStatement) stmt;
        assertEquals("items", insert.tableName());
        assertEquals(List.of("id", "name", "delta"), insert.columns());
        assertEquals(2, insert.rows().size());

        List<Expr> row0 = insert.rows().get(0);
        assertEquals(new Expr.LiteralExpr(Value.ofInt(10)), row0.get(0));
        assertEquals(new Expr.LiteralExpr(Value.ofVarchar("Bolt")), row0.get(1));
        assertEquals(
                new Expr.UnaryExpr(Expr.UnaryOp.NEGATE, new Expr.LiteralExpr(Value.ofInt(5))),
                row0.get(2));

        List<Expr> row1 = insert.rows().get(1);
        assertEquals(new Expr.LiteralExpr(Value.ofBigInt(5000000000L)), row1.get(2));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @CsvSource(delimiter = '|', value = {
            "Empty statement                  | ''                                          | 1 | 1",
            "Unknown statement keyword        | FOOBAR TABLE t (id INT)                     | 1 | 1",
            "CREATE without target            | CREATE                                      | 1 | 7",
            "CREATE with invalid object       | CREATE VIEW v AS SELECT 1                   | 1 | 8",
            "CREATE DATABASE missing name     | CREATE DATABASE ;                           | 1 | 17",
            "CREATE TABLE missing name        | CREATE TABLE (id INT)                       | 1 | 14",
            "CREATE TABLE missing open paren  | CREATE TABLE t id INT)                      | 1 | 16",
            "CREATE TABLE empty columns       | CREATE TABLE t ()                           | 1 | 17",
            "CREATE TABLE unknown data type   | CREATE TABLE t (id DECIMAL)                 | 1 | 20",
            "CREATE TABLE VARCHAR no parens   | CREATE TABLE t (name VARCHAR)               | 1 | 29",
            "CREATE TABLE VARCHAR zero length | CREATE TABLE t (name VARCHAR(0))            | 1 | 30",
            "CREATE TABLE PRIMARY without KEY | CREATE TABLE t (id INT PRIMARY NOT NULL)    | 1 | 32",
            "CREATE TABLE NOT without NULL    | CREATE TABLE t (id INT NOT PRIMARY)         | 1 | 28",
            "DROP without TABLE               | DROP DATABASE db1                           | 1 | 6",
            "DROP TABLE missing name          | DROP TABLE                                  | 1 | 11",
            "INSERT missing INTO              | INSERT users VALUES (1)                     | 1 | 8",
            "INSERT empty column list         | INSERT INTO users () VALUES (1)             | 1 | 20",
            "INSERT missing VALUES            | INSERT INTO users (id) (1)                  | 1 | 24",
            "INSERT empty row tuple           | INSERT INTO users VALUES ()                 | 1 | 27",
            "INSERT trailing tokens           | DROP TABLE t EXTRA                          | 1 | 14"
    })
    void testInvalidStatementsThrowParseExceptionWithPosition(
            String description,
            String sql,
            int expectedLine,
            int expectedColumn) {
        ParseException ex = assertThrows(ParseException.class, () -> Parser.parse(sql), description);
        assertEquals(expectedLine, ex.getLine(), "Line mismatch for: " + description);
        assertEquals(expectedColumn, ex.getColumn(), "Column mismatch for: " + description);
        assertTrue(ex.getMessage().contains("line " + expectedLine + ", column " + expectedColumn));
    }
}
