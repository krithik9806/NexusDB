package io.nexusdb.core.planner;

import io.nexusdb.core.ast.Statement;
import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.catalog.Catalog;
import io.nexusdb.core.parser.Parser;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.Value;
import io.nexusdb.core.storage.DiskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BinderTest {

    @TempDir
    Path tempDir;

    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private Catalog catalog;
    private Binder binder;

    @BeforeEach
    void setUp() {
        diskManager = DiskManager.open(tempDir.resolve("binder_test.db"));
        bufferPoolManager = new BufferPoolManager(32, diskManager);
        catalog = new Catalog(bufferPoolManager);
        binder = new Binder(catalog);

        Schema usersSchema = new Schema(List.of(
                Column.primaryKey("id", DataType.INT),
                new Column("username", DataType.VARCHAR, 10, false),
                new Column("age", DataType.INT, true),
                new Column("points", DataType.BIGINT, false),
                new Column("balance", DataType.DOUBLE, true),
                new Column("active", DataType.BOOLEAN, false)
        ));
        catalog.createTable("users", usersSchema);
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

    private BoundStatement bindSql(String sql) {
        Statement ast = Parser.parse(sql);
        return binder.bind(ast);
    }

    @Test
    void testValidCreateTableAndDropTableBinding() {
        BoundStatement createStmt = bindSql(
                "CREATE TABLE products (sku VARCHAR(16) PRIMARY KEY, price DOUBLE NOT NULL, stock INT)");
        assertInstanceOf(BoundStatement.BoundCreateTable.class, createStmt);
        BoundStatement.BoundCreateTable boundCreate = (BoundStatement.BoundCreateTable) createStmt;
        assertEquals("products", boundCreate.tableName());
        assertEquals(3, boundCreate.schema().getColumnCount());
        assertEquals(0, boundCreate.schema().getPrimaryKeyIndex());

        BoundStatement dropStmt = bindSql("DROP TABLE USERS");
        assertInstanceOf(BoundStatement.BoundDropTable.class, dropStmt);
        assertEquals("users", ((BoundStatement.BoundDropTable) dropStmt).tableName());
    }

    @Test
    void testValidInsertWithColumnReorderingDefaultNullsAndNumericWidening() {
        // Specify subset of columns out of order; omit nullable columns (age, balance);
        // widen INT literal 100 -> BIGINT (points)
        BoundStatement stmt = bindSql(
                "INSERT INTO users (username, id, points, active) VALUES ('alice', 1, 100, TRUE)");
        assertInstanceOf(BoundStatement.BoundInsert.class, stmt);
        BoundStatement.BoundInsert insert = (BoundStatement.BoundInsert) stmt;
        assertEquals(1, insert.rows().size());

        List<BoundExpr> row = insert.rows().get(0);
        assertEquals(6, row.size());
        assertEquals(new BoundExpr.BoundLiteral(Value.ofInt(1), DataType.INT), row.get(0));
        assertEquals(new BoundExpr.BoundLiteral(Value.ofVarchar("alice"), DataType.VARCHAR), row.get(1));
        assertEquals(new BoundExpr.BoundLiteral(Value.NULL, DataType.INT), row.get(2));
        assertEquals(new BoundExpr.BoundLiteral(Value.ofBigInt(100L), DataType.BIGINT), row.get(3));
        assertEquals(new BoundExpr.BoundLiteral(Value.NULL, DataType.DOUBLE), row.get(4));
        assertEquals(new BoundExpr.BoundLiteral(Value.ofBoolean(true), DataType.BOOLEAN), row.get(5));
    }

    @Test
    void testValidSelectUpdateDeleteWithColumnResolutionAndNumericWidening() {
        // INT + BIGINT -> BIGINT, BIGINT + DOUBLE -> DOUBLE
        String selectSql = """
                SELECT u.id, u.age + u.points AS total_pts, u.points + u.balance AS score
                FROM users AS u
                WHERE u.id = 1 AND u.balance >= 50 AND u.username IS NOT NULL
                ORDER BY score DESC, u.id ASC
                LIMIT 5
                """;
        BoundStatement selectStmt = bindSql(selectSql);
        assertInstanceOf(BoundStatement.BoundSelect.class, selectStmt);
        BoundStatement.BoundSelect select = (BoundStatement.BoundSelect) selectStmt;

        assertEquals(3, select.projections().size());
        assertEquals(DataType.INT, select.projections().get(0).expr().dataType());
        assertEquals(DataType.BIGINT, select.projections().get(1).expr().dataType());
        assertEquals(DataType.DOUBLE, select.projections().get(2).expr().dataType());
        assertEquals(DataType.BOOLEAN, select.where().dataType());

        // UPDATE with INT -> DOUBLE widening
        BoundStatement updateStmt = bindSql("UPDATE users SET balance = age + 10, active = FALSE WHERE points > 5");
        assertInstanceOf(BoundStatement.BoundUpdate.class, updateStmt);
        BoundStatement.BoundUpdate update = (BoundStatement.BoundUpdate) updateStmt;
        assertEquals(4, update.assignments().get(0).columnIndex());
        assertEquals(DataType.DOUBLE, update.assignments().get(0).value().dataType());

        // DELETE
        BoundStatement deleteStmt = bindSql("DELETE FROM users WHERE NOT active");
        assertInstanceOf(BoundStatement.BoundDelete.class, deleteStmt);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @CsvSource(delimiter = '|', value = {
            "CREATE existing table             | CREATE TABLE users (id INT)                                               | Table already exists",
            "CREATE duplicate column           | CREATE TABLE t (id INT, ID BIGINT)                                        | Duplicate column name",
            "CREATE multiple PRIMARY KEYs      | CREATE TABLE t (a INT PRIMARY KEY, b INT PRIMARY KEY)                     | Multiple PRIMARY KEY",
            "DROP unknown table                | DROP TABLE nonexistent                                                    | Unknown table",
            "SELECT from unknown table         | SELECT * FROM ghost                                                       | Unknown table",
            "SELECT unknown column             | SELECT missing_col FROM users                                             | Unknown column",
            "SELECT invalid table qualifier    | SELECT x.id FROM users AS u                                               | Unknown table qualifier",
            "SELECT non-boolean WHERE          | SELECT * FROM users WHERE age + 1                                         | WHERE clause must evaluate to BOOLEAN",
            "SELECT arithmetic on VARCHAR      | SELECT username + 1 FROM users                                            | requires numeric operands",
            "SELECT arithmetic on BOOLEAN      | SELECT active * 2 FROM users                                              | requires numeric operands",
            "SELECT compare VARCHAR and INT    | SELECT * FROM users WHERE username = 42                                   | Cannot compare incompatible types",
            "SELECT compare BOOLEAN and INT    | SELECT * FROM users WHERE active = 1                                      | Cannot compare incompatible types",
            "SELECT AND with non-boolean       | SELECT * FROM users WHERE active AND age                                  | requires BOOLEAN operands",
            "SELECT NOT with non-boolean       | SELECT * FROM users WHERE NOT username                                    | requires a BOOLEAN operand",
            "SELECT unary minus on VARCHAR     | SELECT -username FROM users                                               | Unary '-' requires a numeric operand",
            "SELECT unknown col in ORDER BY    | SELECT id FROM users ORDER BY unknown_col                                 | Unknown column",
            "INSERT wrong column count         | INSERT INTO users VALUES (1, 'bob')                                       | expected 6",
            "INSERT unknown column in list     | INSERT INTO users (id, nope) VALUES (1, 2)                                | Unknown column",
            "INSERT duplicate column in list   | INSERT INTO users (id, username, id, points, active) VALUES (1,'a',1,2,TRUE) | Duplicate column",
            "INSERT missing NOT NULL column    | INSERT INTO users (id, username) VALUES (1, 'bob')                        | Missing value for NOT NULL column",
            "INSERT NULL into NOT NULL column  | INSERT INTO users VALUES (1, NULL, 20, 100, 1.0, TRUE)                    | is NOT NULL",
            "INSERT VARCHAR exceeds maxLength  | INSERT INTO users VALUES (1, 'this_name_is_way_too_long', 20, 100, 1.0, TRUE) | exceeds maximum length 10",
            "INSERT narrowing DOUBLE to INT    | INSERT INTO users VALUES (1.5, 'bob', 20, 100, 1.0, TRUE)                 | Type mismatch for column 'id'",
            "INSERT narrowing BIGINT to INT    | INSERT INTO users VALUES (5000000000, 'bob', 20, 100, 1.0, TRUE)          | Type mismatch for column 'id'",
            "INSERT column ref in VALUES       | INSERT INTO users VALUES (age, 'bob', 20, 100, 1.0, TRUE)                 | not allowed in INSERT VALUES",
            "UPDATE unknown table              | UPDATE ghost SET id = 1                                                   | Unknown table",
            "UPDATE unknown column             | UPDATE users SET missing = 1                                              | Unknown column",
            "UPDATE duplicate column           | UPDATE users SET age = 1, AGE = 2                                         | Duplicate assignment",
            "UPDATE NULL into NOT NULL column  | UPDATE users SET username = NULL                                          | is NOT NULL",
            "UPDATE VARCHAR exceeds maxLength  | UPDATE users SET username = '0123456789_extra'                            | exceeds maximum length 10",
            "UPDATE incompatible type          | UPDATE users SET age = 'twenty'                                           | Type mismatch for column 'age'"
    })
    void testSemanticErrorsThrowBindException(String description, String sql, String expectedMessageFragment) {
        BindException ex = assertThrows(BindException.class, () -> bindSql(sql), description);
        assertTrue(
                ex.getMessage().contains(expectedMessageFragment),
                () -> "Expected '" + expectedMessageFragment + "' in message, but got: " + ex.getMessage());
    }
}
