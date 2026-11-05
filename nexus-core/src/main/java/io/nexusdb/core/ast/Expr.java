package io.nexusdb.core.ast;

import io.nexusdb.core.record.Value;

import java.util.Objects;

/**
 * Sealed AST hierarchy for SQL expressions in NexusDB.
 */
public sealed interface Expr
        permits Expr.LiteralExpr,
                Expr.ColumnRefExpr,
                Expr.UnaryExpr,
                Expr.BinaryExpr,
                Expr.IsNullExpr,
                Expr.AggregateCall {

    /**
     * Literal constant expression (integer, bigint, double, boolean, varchar, or NULL).
     */
    record LiteralExpr(Value value) implements Expr {
        public LiteralExpr {
            Objects.requireNonNull(value, "value must not be null (use Value.NULL for SQL NULL)");
        }
    }

    /**
     * Column reference expression, optionally qualified by a table name or alias (e.g. {@code u.id}).
     */
    record ColumnRefExpr(String tableQualifier, String columnName) implements Expr {
        public ColumnRefExpr {
            Objects.requireNonNull(columnName, "columnName must not be null");
        }

        public ColumnRefExpr(String columnName) {
            this(null, columnName);
        }
    }

    /**
     * Unary operators supported in NexusDB expressions.
     */
    enum UnaryOp {
        NEGATE("-"),
        NOT("NOT");

        private final String sql;

        UnaryOp(String sql) {
            this.sql = sql;
        }

        public String sql() {
            return sql;
        }
    }

    /**
     * Unary expression ({@code -expr} or {@code NOT expr}).
     */
    record UnaryExpr(UnaryOp op, Expr operand) implements Expr {
        public UnaryExpr {
            Objects.requireNonNull(op, "op must not be null");
            Objects.requireNonNull(operand, "operand must not be null");
        }
    }

    /**
     * Binary operators in NexusDB expressions.
     */
    enum BinaryOp {
        OR("OR"),
        AND("AND"),
        EQ("="),
        NEQ("!="),
        LT("<"),
        LTE("<="),
        GT(">"),
        GTE(">="),
        ADD("+"),
        SUB("-"),
        MUL("*"),
        DIV("/");

        private final String sql;

        BinaryOp(String sql) {
            this.sql = sql;
        }

        public String sql() {
            return sql;
        }
    }

    /**
     * Binary expression ({@code left op right}).
     */
    record BinaryExpr(Expr left, BinaryOp op, Expr right) implements Expr {
        public BinaryExpr {
            Objects.requireNonNull(left, "left must not be null");
            Objects.requireNonNull(op, "op must not be null");
            Objects.requireNonNull(right, "right must not be null");
        }
    }

    /**
     * {@code expr IS NULL} or {@code expr IS NOT NULL} predicate expression.
     */
    record IsNullExpr(Expr operand, boolean negated) implements Expr {
        public IsNullExpr {
            Objects.requireNonNull(operand, "operand must not be null");
        }
    }

    /**
     * SQL aggregate functions supported in NexusDB.
     */
    enum AggregateFunc {
        COUNT,
        SUM,
        AVG,
        MIN,
        MAX
    }

    /**
     * Aggregate function call ({@code COUNT(*)} when {@code star == true} and {@code argument == null},
     * or {@code FUNC(expr)} when {@code star == false}).
     */
    record AggregateCall(AggregateFunc function, Expr argument, boolean star) implements Expr {
        public AggregateCall {
            Objects.requireNonNull(function, "function must not be null");
            if (star) {
                if (function != AggregateFunc.COUNT) {
                    throw new IllegalArgumentException(function + "(*) is not supported; only COUNT(*) is valid");
                }
                if (argument != null) {
                    throw new IllegalArgumentException("COUNT(*) must have a null argument");
                }
            } else {
                Objects.requireNonNull(argument, "Aggregate argument must not be null when star is false");
            }
        }

        public static AggregateCall countStar() {
            return new AggregateCall(AggregateFunc.COUNT, null, true);
        }

        public static AggregateCall of(AggregateFunc function, Expr argument) {
            return new AggregateCall(function, argument, false);
        }
    }
}
