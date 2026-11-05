package io.nexusdb.core.executor;

import io.nexusdb.core.ast.Expr;
import io.nexusdb.core.planner.Binder;
import io.nexusdb.core.planner.BoundExpr;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;

import java.util.Objects;

/**
 * Evaluates {@link BoundExpr} trees against runtime {@link Tuple}s using SQL three-valued
 * {@code NULL} semantics and numeric type widening ({@code INT -> BIGINT -> DOUBLE}).
 *
 * <p>Three-Valued Logic Invariant (per {@code docs/sql-engine.md}):
 * <ul>
 *   <li>{@code FALSE AND NULL -> FALSE}; {@code TRUE AND NULL -> NULL}; {@code NULL AND NULL -> NULL}</li>
 *   <li>{@code TRUE OR NULL -> TRUE}; {@code FALSE OR NULL -> NULL}; {@code NULL OR NULL -> NULL}</li>
 *   <li>{@code NOT NULL -> NULL}</li>
 *   <li>{@code WHERE} predicates pass a tuple if and only if {@link #evaluatePredicate} returns {@code true}
 *       (both {@code FALSE} and {@code NULL} filter out the row).</li>
 * </ul>
 */
public final class ExpressionEvaluator {

    private ExpressionEvaluator() {
    }

    /**
     * Evaluates {@code expr} against {@code tuple} (which may be {@code null} for constant expressions).
     */
    public static Value evaluate(BoundExpr expr, Tuple tuple) {
        Objects.requireNonNull(expr, "expr must not be null");
        return switch (expr) {
            case BoundExpr.BoundLiteral(Value val, DataType ignored) -> val;
            case BoundExpr.BoundColumnRef(int colIdx, String ignoredName, DataType ignoredType) -> {
                if (tuple == null) {
                    throw new IllegalStateException("Cannot evaluate column reference without an input tuple");
                }
                yield tuple.getValue(colIdx);
            }
            case BoundExpr.BoundUnaryExpr(Expr.UnaryOp op, BoundExpr operand, DataType ignoredType) -> {
                Value val = evaluate(operand, tuple);
                yield evaluateUnary(op, val);
            }
            case BoundExpr.BoundBinaryExpr(BoundExpr left, Expr.BinaryOp op, BoundExpr right, DataType resultType) ->
                    evaluateBinary(left, op, right, resultType, tuple);
            case BoundExpr.BoundIsNullExpr(BoundExpr operand, boolean negated) -> {
                Value val = evaluate(operand, tuple);
                boolean isNull = val.isNull();
                yield Value.ofBoolean(negated ? !isNull : isNull);
            }
            case BoundExpr.BoundCastExpr(BoundExpr operand, DataType targetType) -> {
                Value val = evaluate(operand, tuple);
                yield coerceValue(val, targetType);
            }
            case BoundExpr.BoundAggregate agg ->
                    throw new IllegalStateException(
                            "BoundAggregate must be evaluated by HashAggregateExecutor and rewritten to a post-aggregate column reference: "
                                    + agg.displayText());
        };
    }

    /**
     * Evaluates a boolean filter predicate under SQL three-valued logic: returns {@code true}
     * if and only if the expression evaluates to {@code TRUE}. Both {@code FALSE} and {@code NULL}
     * return {@code false}.
     */
    public static boolean evaluatePredicate(BoundExpr predicate, Tuple tuple) {
        if (predicate == null) {
            return true;
        }
        Value result = evaluate(predicate, tuple);
        return result instanceof Value.BooleanValue(boolean b) && b;
    }

    private static Value evaluateUnary(Expr.UnaryOp op, Value val) {
        if (val.isNull()) {
            return Value.NULL;
        }
        return switch (op) {
            case NOT -> {
                if (val instanceof Value.BooleanValue(boolean b)) {
                    yield Value.ofBoolean(!b);
                }
                throw new IllegalArgumentException("Expected BOOLEAN for NOT, got: " + val);
            }
            case NEGATE -> switch (val) {
                case Value.IntValue(int v) -> Value.ofInt(-v);
                case Value.BigIntValue(long v) -> Value.ofBigInt(-v);
                case Value.DoubleValue(double v) -> Value.ofDouble(-v);
                default -> throw new IllegalArgumentException("Expected numeric value for unary '-', got: " + val);
            };
        };
    }

    private static Value evaluateBinary(
            BoundExpr leftExpr,
            Expr.BinaryOp op,
            BoundExpr rightExpr,
            DataType resultType,
            Tuple tuple) {
        if (op == Expr.BinaryOp.AND) {
            Value leftVal = evaluate(leftExpr, tuple);
            if (leftVal instanceof Value.BooleanValue(boolean lb) && !lb) {
                return Value.ofBoolean(false);
            }
            Value rightVal = evaluate(rightExpr, tuple);
            if (rightVal instanceof Value.BooleanValue(boolean rb) && !rb) {
                return Value.ofBoolean(false);
            }
            if (leftVal.isNull() || rightVal.isNull()) {
                return Value.NULL;
            }
            return Value.ofBoolean(true);
        }

        if (op == Expr.BinaryOp.OR) {
            Value leftVal = evaluate(leftExpr, tuple);
            if (leftVal instanceof Value.BooleanValue(boolean lb) && lb) {
                return Value.ofBoolean(true);
            }
            Value rightVal = evaluate(rightExpr, tuple);
            if (rightVal instanceof Value.BooleanValue(boolean rb) && rb) {
                return Value.ofBoolean(true);
            }
            if (leftVal.isNull() || rightVal.isNull()) {
                return Value.NULL;
            }
            return Value.ofBoolean(false);
        }

        Value leftVal = evaluate(leftExpr, tuple);
        Value rightVal = evaluate(rightExpr, tuple);
        if (leftVal.isNull() || rightVal.isNull()) {
            return Value.NULL;
        }

        return switch (op) {
            case ADD, SUB, MUL, DIV -> evaluateArithmetic(leftVal, op, rightVal, resultType);
            case EQ, NEQ, LT, LTE, GT, GTE -> evaluateComparison(leftVal, op, rightVal);
            default -> throw new IllegalStateException("Unhandled operator: " + op);
        };
    }

    private static Value evaluateArithmetic(Value left, Expr.BinaryOp op, Value right, DataType targetType) {
        DataType commonType = targetType != null && Binder.isNumeric(targetType)
                ? targetType
                : Binder.widestNumeric(left.dataType(), right.dataType());
        Value l = coerceValue(left, commonType);
        Value r = coerceValue(right, commonType);

        return switch (commonType) {
            case INT -> {
                int a = ((Value.IntValue) l).value();
                int b = ((Value.IntValue) r).value();
                yield switch (op) {
                    case ADD -> Value.ofInt(a + b);
                    case SUB -> Value.ofInt(a - b);
                    case MUL -> Value.ofInt(a * b);
                    case DIV -> {
                        if (b == 0) {
                            throw new ArithmeticException("Division by zero");
                        }
                        yield Value.ofInt(a / b);
                    }
                    default -> throw new IllegalStateException("Unexpected arithmetic op: " + op);
                };
            }
            case BIGINT -> {
                long a = ((Value.BigIntValue) l).value();
                long b = ((Value.BigIntValue) r).value();
                yield switch (op) {
                    case ADD -> Value.ofBigInt(a + b);
                    case SUB -> Value.ofBigInt(a - b);
                    case MUL -> Value.ofBigInt(a * b);
                    case DIV -> {
                        if (b == 0L) {
                            throw new ArithmeticException("Division by zero");
                        }
                        yield Value.ofBigInt(a / b);
                    }
                    default -> throw new IllegalStateException("Unexpected arithmetic op: " + op);
                };
            }
            case DOUBLE -> {
                double a = ((Value.DoubleValue) l).value();
                double b = ((Value.DoubleValue) r).value();
                yield switch (op) {
                    case ADD -> Value.ofDouble(a + b);
                    case SUB -> Value.ofDouble(a - b);
                    case MUL -> Value.ofDouble(a * b);
                    case DIV -> {
                        if (b == 0.0) {
                            throw new ArithmeticException("Division by zero");
                        }
                        yield Value.ofDouble(a / b);
                    }
                    default -> throw new IllegalStateException("Unexpected arithmetic op: " + op);
                };
            }
            default -> throw new IllegalArgumentException("Non-numeric arithmetic type: " + commonType);
        };
    }

    private static Value evaluateComparison(Value left, Expr.BinaryOp op, Value right) {
        int cmp;
        if (Binder.isNumeric(left.dataType()) && Binder.isNumeric(right.dataType())) {
            DataType commonType = Binder.widestNumeric(left.dataType(), right.dataType());
            Value l = coerceValue(left, commonType);
            Value r = coerceValue(right, commonType);
            cmp = l.compareTo(r);
        } else {
            cmp = left.compareTo(right);
        }

        boolean res = switch (op) {
            case EQ -> cmp == 0;
            case NEQ -> cmp != 0;
            case LT -> cmp < 0;
            case LTE -> cmp <= 0;
            case GT -> cmp > 0;
            case GTE -> cmp >= 0;
            default -> throw new IllegalStateException("Unexpected comparison op: " + op);
        };
        return Value.ofBoolean(res);
    }

    /**
     * Coerces {@code value} to {@code targetType} following {@code INT -> BIGINT -> DOUBLE}.
     */
    public static Value coerceValue(Value value, DataType targetType) {
        Objects.requireNonNull(value, "value must not be null");
        if (value.isNull() || targetType == null || value.dataType() == targetType) {
            return value;
        }
        return switch (value) {
            case Value.IntValue(int v) -> switch (targetType) {
                case BIGINT -> Value.ofBigInt(v);
                case DOUBLE -> Value.ofDouble(v);
                default -> throw new IllegalArgumentException("Cannot coerce INT to " + targetType);
            };
            case Value.BigIntValue(long v) -> switch (targetType) {
                case DOUBLE -> Value.ofDouble(v);
                default -> throw new IllegalArgumentException("Cannot coerce BIGINT to " + targetType);
            };
            default -> throw new IllegalArgumentException("Cannot coerce " + value.dataType() + " to " + targetType);
        };
    }
}
