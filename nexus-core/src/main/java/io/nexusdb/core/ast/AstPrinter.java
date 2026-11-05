package io.nexusdb.core.ast;

import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Value;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Pretty-printer converting {@link Statement} and {@link Expr} AST nodes back into canonical SQL.
 *
 * <p>Round-Trip Invariant:
 * Child expressions are parenthesized whenever their operator precedence is lower than (or, for
 * right-hand operands of left-associative/non-associative binary operators, equal to) their parent's
 * precedence, guaranteeing {@code Parser.parse(AstPrinter.print(stmt)).equals(stmt)}.
 */
public final class AstPrinter {

    private AstPrinter() {
    }

    public static String print(Statement statement) {
        Objects.requireNonNull(statement, "statement must not be null");
        return switch (statement) {
            case Statement.CreateDatabaseStatement(String dbName) ->
                    "CREATE DATABASE " + dbName;
            case Statement.CreateTableStatement(String tableName, List<ColumnDef> cols) ->
                    "CREATE TABLE " + tableName + " ("
                            + cols.stream().map(AstPrinter::printColumnDef).collect(Collectors.joining(", "))
                            + ")";
            case Statement.DropTableStatement(String tableName) ->
                    "DROP TABLE " + tableName;
            case Statement.CreateIndexStatement(
                    String indexName,
                    String tableName,
                    String columnName,
                    boolean unique) ->
                    (unique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ")
                            + indexName + " ON " + tableName + " (" + columnName + ")";
            case Statement.DropIndexStatement(String indexName, String tableName) ->
                    tableName == null
                            ? "DROP INDEX " + indexName
                            : "DROP INDEX " + indexName + " ON " + tableName;
            case Statement.InsertStatement(String tableName, List<String> cols, List<List<Expr>> rows) -> {
                StringBuilder sb = new StringBuilder("INSERT INTO ").append(tableName);
                if (!cols.isEmpty()) {
                    sb.append(" (").append(String.join(", ", cols)).append(")");
                }
                sb.append(" VALUES ");
                sb.append(rows.stream()
                        .map(row -> "(" + row.stream().map(AstPrinter::print).collect(Collectors.joining(", ")) + ")")
                        .collect(Collectors.joining(", ")));
                yield sb.toString();
            }
            case Statement.SelectStatement(
                    List<SelectItem> items,
                    TableRef from,
                    List<JoinClause> joins,
                    Expr where,
                    List<Expr> groupBy,
                    Expr having,
                    List<OrderByItem> orderBy,
                    Integer limit) -> {
                StringBuilder sb = new StringBuilder("SELECT ");
                sb.append(items.stream().map(AstPrinter::printSelectItem).collect(Collectors.joining(", ")));
                sb.append(" FROM ").append(printTableRef(from));
                for (JoinClause join : joins) {
                    sb.append(" ")
                            .append(join.joinType().sql())
                            .append(" ")
                            .append(printTableRef(join.rightTable()))
                            .append(" ON ")
                            .append(print(join.condition()));
                }
                if (where != null) {
                    sb.append(" WHERE ").append(print(where));
                }
                if (!groupBy.isEmpty()) {
                    sb.append(" GROUP BY ");
                    sb.append(groupBy.stream().map(AstPrinter::print).collect(Collectors.joining(", ")));
                }
                if (having != null) {
                    sb.append(" HAVING ").append(print(having));
                }
                if (!orderBy.isEmpty()) {
                    sb.append(" ORDER BY ");
                    sb.append(orderBy.stream().map(AstPrinter::printOrderByItem).collect(Collectors.joining(", ")));
                }
                if (limit != null) {
                    sb.append(" LIMIT ").append(limit);
                }
                yield sb.toString();
            }
            case Statement.UpdateStatement(String tableName, List<Assignment> assignments, Expr where) -> {
                StringBuilder sb = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
                sb.append(assignments.stream()
                        .map(a -> a.columnName() + " = " + print(a.value()))
                        .collect(Collectors.joining(", ")));
                if (where != null) {
                    sb.append(" WHERE ").append(print(where));
                }
                yield sb.toString();
            }
            case Statement.DeleteStatement(String tableName, Expr where) -> {
                StringBuilder sb = new StringBuilder("DELETE FROM ").append(tableName);
                if (where != null) {
                    sb.append(" WHERE ").append(print(where));
                }
                yield sb.toString();
            }
            case Statement.BeginStatement ignored -> "BEGIN";
            case Statement.CommitStatement ignored -> "COMMIT";
            case Statement.RollbackStatement ignored -> "ROLLBACK";
            case Statement.ExplainStatement(boolean analyze, Statement inner) ->
                    (analyze ? "EXPLAIN ANALYZE " : "EXPLAIN ") + print(inner);
        };
    }

    public static String print(Expr expr) {
        Objects.requireNonNull(expr, "expr must not be null");
        return switch (expr) {
            case Expr.LiteralExpr(Value val) -> printValue(val);
            case Expr.ColumnRefExpr(String qualifier, String col) ->
                    qualifier == null ? col : qualifier + "." + col;
            case Expr.UnaryExpr(Expr.UnaryOp op, Expr operand) -> {
                int myPrec = precedence(expr);
                int childPrec = precedence(operand);
                boolean needParens = childPrec < myPrec
                        || (op == Expr.UnaryOp.NEGATE && operand instanceof Expr.UnaryExpr(Expr.UnaryOp innerOp, Expr ignoredInner)
                        && innerOp == Expr.UnaryOp.NEGATE);
                String formattedOperand = needParens ? "(" + print(operand) + ")" : print(operand);
                yield op == Expr.UnaryOp.NEGATE ? "-" + formattedOperand : "NOT " + formattedOperand;
            }
            case Expr.BinaryExpr(Expr left, Expr.BinaryOp op, Expr right) -> {
                int myPrec = precedence(expr);
                int leftPrec = precedence(left);
                int rightPrec = precedence(right);
                boolean isComparison = isComparisonOp(op);
                boolean wrapLeft = leftPrec < myPrec || (isComparison && leftPrec == myPrec);
                boolean wrapRight = rightPrec <= myPrec;
                String leftStr = wrapLeft ? "(" + print(left) + ")" : print(left);
                String rightStr = wrapRight ? "(" + print(right) + ")" : print(right);
                yield leftStr + " " + op.sql() + " " + rightStr;
            }
            case Expr.IsNullExpr(Expr operand, boolean negated) -> {
                int myPrec = precedence(expr);
                int childPrec = precedence(operand);
                String operandStr = childPrec <= myPrec ? "(" + print(operand) + ")" : print(operand);
                yield operandStr + (negated ? " IS NOT NULL" : " IS NULL");
            }
            case Expr.AggregateCall(Expr.AggregateFunc func, Expr arg, boolean star) ->
                    func.name() + "(" + (star ? "*" : print(arg)) + ")";
        };
    }

    private static String printColumnDef(ColumnDef col) {
        StringBuilder sb = new StringBuilder(col.name()).append(" ");
        if (col.dataType() == DataType.VARCHAR) {
            sb.append("VARCHAR(").append(col.maxLength()).append(")");
        } else {
            sb.append(col.dataType().name());
        }
        if (col.primaryKey()) {
            sb.append(" PRIMARY KEY");
        } else if (!col.nullable()) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    private static String printSelectItem(SelectItem item) {
        return switch (item) {
            case SelectItem.AllColumns(String qualifier) ->
                    qualifier == null ? "*" : qualifier + ".*";
            case SelectItem.ExprItem(Expr expr, String alias) ->
                    alias == null ? print(expr) : print(expr) + " AS " + alias;
        };
    }

    private static String printTableRef(TableRef ref) {
        return ref.alias() == null ? ref.tableName() : ref.tableName() + " AS " + ref.alias();
    }

    private static String printOrderByItem(OrderByItem item) {
        return print(item.expr()) + (item.ascending() ? " ASC" : " DESC");
    }

    private static String printValue(Value val) {
        return switch (val) {
            case Value.IntValue(int v) -> Integer.toString(v);
            case Value.BigIntValue(long v) -> Long.toString(v);
            case Value.DoubleValue(double v) -> Double.toString(v);
            case Value.BooleanValue(boolean v) -> v ? "TRUE" : "FALSE";
            case Value.VarcharValue(String s) -> "'" + s.replace("'", "''") + "'";
            case Value.NullValue ignored -> "NULL";
        };
    }

    private static boolean isComparisonOp(Expr.BinaryOp op) {
        return switch (op) {
            case EQ, NEQ, LT, LTE, GT, GTE -> true;
            default -> false;
        };
    }

    private static int precedence(Expr expr) {
        return switch (expr) {
            case Expr.LiteralExpr ignored -> 70;
            case Expr.ColumnRefExpr ignored -> 70;
            case Expr.AggregateCall ignored -> 70;
            case Expr.UnaryExpr(Expr.UnaryOp op, Expr ignoredOperand) -> op == Expr.UnaryOp.NEGATE ? 60 : 25;
            case Expr.BinaryExpr(Expr ignoredLeft, Expr.BinaryOp op, Expr ignoredRight) -> switch (op) {
                case MUL, DIV -> 50;
                case ADD, SUB -> 40;
                case EQ, NEQ, LT, LTE, GT, GTE -> 30;
                case AND -> 20;
                case OR -> 10;
            };
            case Expr.IsNullExpr ignored -> 30;
        };
    }
}
