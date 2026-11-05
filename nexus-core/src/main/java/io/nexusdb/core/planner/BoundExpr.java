package io.nexusdb.core.planner;

import io.nexusdb.core.ast.Expr;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Value;

import java.util.Objects;

/**
 * Sealed hierarchy of semantically validated and type-annotated expressions with
 * 0-based resolved tuple column indexes.
 */
public sealed interface BoundExpr
        permits BoundExpr.BoundLiteral,
                BoundExpr.BoundColumnRef,
                BoundExpr.BoundUnaryExpr,
                BoundExpr.BoundBinaryExpr,
                BoundExpr.BoundIsNullExpr,
                BoundExpr.BoundCastExpr,
                BoundExpr.BoundAggregate {

    /**
     * Resolved static {@link DataType} of this expression, or {@code null} only for an untyped {@code NULL} literal.
     */
    DataType dataType();

    /**
     * Constant literal value with an optional resolved {@link DataType}.
     */
    record BoundLiteral(Value value, DataType dataType) implements BoundExpr {
        public BoundLiteral {
            Objects.requireNonNull(value, "value must not be null");
        }

        public BoundLiteral(Value value) {
            this(value, value.dataType());
        }
    }

    /**
     * Resolved 0-based column reference into an input tuple schema.
     */
    record BoundColumnRef(int columnIndex, String columnName, DataType dataType) implements BoundExpr {
        public BoundColumnRef {
            if (columnIndex < 0) {
                throw new IllegalArgumentException("columnIndex must be >= 0, got: " + columnIndex);
            }
            Objects.requireNonNull(columnName, "columnName must not be null");
            Objects.requireNonNull(dataType, "dataType must not be null");
        }
    }

    /**
     * Type-checked unary expression ({@code -expr} or {@code NOT expr}).
     */
    record BoundUnaryExpr(Expr.UnaryOp op, BoundExpr operand, DataType dataType) implements BoundExpr {
        public BoundUnaryExpr {
            Objects.requireNonNull(op, "op must not be null");
            Objects.requireNonNull(operand, "operand must not be null");
            Objects.requireNonNull(dataType, "dataType must not be null");
        }
    }

    /**
     * Type-checked binary expression ({@code left op right}).
     */
    record BoundBinaryExpr(
            BoundExpr left,
            Expr.BinaryOp op,
            BoundExpr right,
            DataType dataType) implements BoundExpr {
        public BoundBinaryExpr {
            Objects.requireNonNull(left, "left must not be null");
            Objects.requireNonNull(op, "op must not be null");
            Objects.requireNonNull(right, "right must not be null");
            Objects.requireNonNull(dataType, "dataType must not be null");
        }
    }

    /**
     * Type-checked {@code expr IS [NOT] NULL} predicate.
     */
    record BoundIsNullExpr(BoundExpr operand, boolean negated) implements BoundExpr {
        public BoundIsNullExpr {
            Objects.requireNonNull(operand, "operand must not be null");
        }

        @Override
        public DataType dataType() {
            return DataType.BOOLEAN;
        }
    }

    /**
     * Implicit numeric widening cast ({@code INT -> BIGINT -> DOUBLE}) or typed {@code NULL} coercion.
     */
    record BoundCastExpr(BoundExpr operand, DataType dataType) implements BoundExpr {
        public BoundCastExpr {
            Objects.requireNonNull(operand, "operand must not be null");
            Objects.requireNonNull(dataType, "dataType must not be null");
        }
    }

    /**
     * Type-checked aggregate function call ({@code COUNT(*)}, {@code COUNT(expr)}, {@code SUM(expr)},
     * {@code AVG(expr)}, {@code MIN(expr)}, {@code MAX(expr)}).
     */
    record BoundAggregate(
            Expr.AggregateFunc function,
            BoundExpr argument,
            boolean star,
            DataType dataType) implements BoundExpr {
        public BoundAggregate {
            Objects.requireNonNull(function, "function must not be null");
            Objects.requireNonNull(dataType, "dataType must not be null");
            if (star) {
                if (function != Expr.AggregateFunc.COUNT) {
                    throw new IllegalArgumentException(function + "(*) is not supported; only COUNT(*) is valid");
                }
                if (argument != null) {
                    throw new IllegalArgumentException("COUNT(*) must have a null argument");
                }
            } else {
                Objects.requireNonNull(argument, "argument must not be null when star is false");
            }
        }

        public String displayText() {
            return function.name() + "(" + (star ? "*" : argument) + ")";
        }
    }
}
