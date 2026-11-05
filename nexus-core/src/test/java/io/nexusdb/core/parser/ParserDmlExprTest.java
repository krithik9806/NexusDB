package io.nexusdb.core.parser;

import io.nexusdb.core.ast.Assignment;
import io.nexusdb.core.ast.AstPrinter;
import io.nexusdb.core.ast.Expr;
import io.nexusdb.core.ast.OrderByItem;
import io.nexusdb.core.ast.SelectItem;
import io.nexusdb.core.ast.Statement;
import io.nexusdb.core.ast.TableRef;
import io.nexusdb.core.record.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParserDmlExprTest {

    @Test
    void testSelectAllAndTableStarAndAliases() {
        String sql = """
                SELECT *, u.*, u.id AS user_id, u.salary * 12 annual_pay
                FROM users AS u
                WHERE u.active = TRUE AND u.age >= 21
                ORDER BY annual_pay DESC, u.id ASC
                LIMIT 25;
                """;
        Statement stmt = Parser.parse(sql);
        assertInstanceOf(Statement.SelectStatement.class, stmt);
        Statement.SelectStatement select = (Statement.SelectStatement) stmt;

        assertEquals(4, select.selectItems().size());
        assertEquals(new SelectItem.AllColumns(null), select.selectItems().get(0));
        assertEquals(new SelectItem.AllColumns("u"), select.selectItems().get(1));
        assertEquals(
                new SelectItem.ExprItem(new Expr.ColumnRefExpr("u", "id"), "user_id"),
                select.selectItems().get(2));
        assertEquals(
                new SelectItem.ExprItem(
                        new Expr.BinaryExpr(
                                new Expr.ColumnRefExpr("u", "salary"),
                                Expr.BinaryOp.MUL,
                                new Expr.LiteralExpr(Value.ofInt(12))),
                        "annual_pay"),
                select.selectItems().get(3));

        assertEquals(new TableRef("users", "u"), select.from());
        assertNotNull(select.where());
        assertEquals(2, select.orderBy().size());
        assertEquals(new OrderByItem(new Expr.ColumnRefExpr(null, "annual_pay"), false), select.orderBy().get(0));
        assertEquals(new OrderByItem(new Expr.ColumnRefExpr("u", "id"), true), select.orderBy().get(1));
        assertEquals(25, select.limit());
    }

    @Test
    void testUpdateAndDeleteStatements() {
        Statement updateStmt = Parser.parse("UPDATE accounts SET balance = balance + 100, active = TRUE WHERE id = 7;");
        assertInstanceOf(Statement.UpdateStatement.class, updateStmt);
        Statement.UpdateStatement update = (Statement.UpdateStatement) updateStmt;
        assertEquals("accounts", update.tableName());
        assertEquals(2, update.assignments().size());
        assertEquals(
                new Assignment(
                        "balance",
                        new Expr.BinaryExpr(
                                new Expr.ColumnRefExpr(null, "balance"),
                                Expr.BinaryOp.ADD,
                                new Expr.LiteralExpr(Value.ofInt(100)))),
                update.assignments().get(0));
        assertEquals(
                new Assignment("active", new Expr.LiteralExpr(Value.ofBoolean(true))),
                update.assignments().get(1));
        assertEquals(
                new Expr.BinaryExpr(
                        new Expr.ColumnRefExpr(null, "id"),
                        Expr.BinaryOp.EQ,
                        new Expr.LiteralExpr(Value.ofInt(7))),
                update.where());

        Statement deleteStmt = Parser.parse("DELETE FROM sessions WHERE expired_at IS NOT NULL;");
        assertInstanceOf(Statement.DeleteStatement.class, deleteStmt);
        Statement.DeleteStatement delete = (Statement.DeleteStatement) deleteStmt;
        assertEquals("sessions", delete.tableName());
        assertEquals(new Expr.IsNullExpr(new Expr.ColumnRefExpr(null, "expired_at"), true), delete.where());
    }

    @Test
    void testExplainAndExplainAnalyze() {
        Statement explain = Parser.parse("EXPLAIN SELECT id FROM users WHERE id = 1");
        assertInstanceOf(Statement.ExplainStatement.class, explain);
        Statement.ExplainStatement exp1 = (Statement.ExplainStatement) explain;
        assertFalse(exp1.analyze());
        assertInstanceOf(Statement.SelectStatement.class, exp1.statement());

        Statement explainAnalyze = Parser.parse("EXPLAIN ANALYZE DELETE FROM users WHERE active = FALSE;");
        assertInstanceOf(Statement.ExplainStatement.class, explainAnalyze);
        Statement.ExplainStatement exp2 = (Statement.ExplainStatement) explainAnalyze;
        assertTrue(exp2.analyze());
        assertInstanceOf(Statement.DeleteStatement.class, exp2.statement());
    }

    @Test
    void testExactOperatorPrecedenceHierarchy() {
        // Precedence: OR < AND < NOT < comparison/IS NULL < + - < * / < unary - < primary
        Expr expr = Parser.parseExpr("a OR NOT b AND c + d * -e >= 10 OR f IS NULL");

        // Top-level is left-associative OR: ((a OR (NOT b AND ((c + (d * (-e))) >= 10))) OR (f IS NULL))
        assertInstanceOf(Expr.BinaryExpr.class, expr);
        Expr.BinaryExpr outerOr = (Expr.BinaryExpr) expr;
        assertEquals(Expr.BinaryOp.OR, outerOr.op());
        assertEquals(new Expr.IsNullExpr(new Expr.ColumnRefExpr("f"), false), outerOr.right());

        Expr.BinaryExpr innerOr = (Expr.BinaryExpr) outerOr.left();
        assertEquals(Expr.BinaryOp.OR, innerOr.op());
        assertEquals(new Expr.ColumnRefExpr("a"), innerOr.left());

        Expr.BinaryExpr andExpr = (Expr.BinaryExpr) innerOr.right();
        assertEquals(Expr.BinaryOp.AND, andExpr.op());
        assertEquals(new Expr.UnaryExpr(Expr.UnaryOp.NOT, new Expr.ColumnRefExpr("b")), andExpr.left());

        Expr.BinaryExpr gteExpr = (Expr.BinaryExpr) andExpr.right();
        assertEquals(Expr.BinaryOp.GTE, gteExpr.op());
        assertEquals(new Expr.LiteralExpr(Value.ofInt(10)), gteExpr.right());

        Expr.BinaryExpr addExpr = (Expr.BinaryExpr) gteExpr.left();
        assertEquals(Expr.BinaryOp.ADD, addExpr.op());
        assertEquals(new Expr.ColumnRefExpr("c"), addExpr.left());

        Expr.BinaryExpr mulExpr = (Expr.BinaryExpr) addExpr.right();
        assertEquals(Expr.BinaryOp.MUL, mulExpr.op());
        assertEquals(new Expr.ColumnRefExpr("d"), mulExpr.left());
        assertEquals(new Expr.UnaryExpr(Expr.UnaryOp.NEGATE, new Expr.ColumnRefExpr("e")), mulExpr.right());
    }

    @Test
    void testNestedParenthesesOverridePrecedence() {
        Expr expr = Parser.parseExpr("((a + b) * (c - (d - e))) / -(f + 1)");
        assertEquals("(a + b) * (c - (d - e)) / -(f + 1)", AstPrinter.print(expr));
        assertEquals(expr, Parser.parseExpr(AstPrinter.print(expr)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE DATABASE prod_db",
            "CREATE TABLE orders (id INT PRIMARY KEY, user_id BIGINT NOT NULL, total DOUBLE, notes VARCHAR(120), paid BOOLEAN NOT NULL)",
            "DROP TABLE temp_orders",
            "INSERT INTO orders (id, user_id, total, notes, paid) VALUES (1, 100, 49.95, ' Rush''s order ', TRUE), (2, 200, -10.5, NULL, FALSE)",
            "SELECT *, o.*, o.id AS order_id, (o.total + 5) * 2 AS gross FROM orders AS o WHERE NOT (o.paid = FALSE OR o.total IS NULL) AND o.id <> 99 ORDER BY gross DESC, o.id ASC LIMIT 10",
            "UPDATE orders SET total = total - (total / 10), notes = 'discounted' WHERE id >= 100 AND paid = FALSE",
            "DELETE FROM orders WHERE notes IS NOT NULL OR total <= -(-5)",
            "EXPLAIN SELECT id FROM orders WHERE id = 1",
            "EXPLAIN ANALYZE UPDATE orders SET paid = TRUE WHERE id = 1"
    })
    void testAstPrinterRoundTrip(String sql) {
        Statement firstAst = Parser.parse(sql);
        String printed = AstPrinter.print(firstAst);
        Statement secondAst = Parser.parse(printed);
        assertEquals(firstAst, secondAst, () -> "Round-trip AST mismatch for printed SQL: " + printed);
        assertEquals(printed, AstPrinter.print(secondAst));
    }

    @Test
    void testInvalidSelectUpdateDeleteAndExpressionsThrowParseException() {
        assertThrows(ParseException.class, () -> Parser.parse("SELECT FROM users"));
        assertThrows(ParseException.class, () -> Parser.parse("SELECT * users"));
        assertThrows(ParseException.class, () -> Parser.parse("SELECT a AS FROM users"));
        assertThrows(ParseException.class, () -> Parser.parse("SELECT * FROM users ORDER a"));
        assertThrows(ParseException.class, () -> Parser.parse("SELECT * FROM users LIMIT 'ten'"));
        assertThrows(ParseException.class, () -> Parser.parse("UPDATE users x = 1"));
        assertThrows(ParseException.class, () -> Parser.parse("UPDATE users SET x 1"));
        assertThrows(ParseException.class, () -> Parser.parse("DELETE users WHERE id = 1"));
        assertThrows(ParseException.class, () -> Parser.parse("EXPLAIN EXPLAIN SELECT * FROM users"));
        assertThrows(ParseException.class, () -> Parser.parse("SELECT (1 + 2 FROM users"));
        assertThrows(ParseException.class, () -> Parser.parse("SELECT * FROM users WHERE a IS 5"));
    }
}
