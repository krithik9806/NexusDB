package io.nexusdb.core.executor;

import io.nexusdb.core.ast.Expr;
import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.planner.BoundExpr;
import io.nexusdb.core.planner.BoundStatement;
import io.nexusdb.core.planner.CostBasedPlanner;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Logical-to-physical execution planner that translates a validated {@link BoundStatement}
 * into a Volcano {@link Executor} pipeline, utilizing {@link IndexScanExecutor} when an
 * equality or range predicate matches a registered B+Tree index.
 */
public final class ExecutionPlanner {
    private final CostBasedPlanner costBasedPlanner = new CostBasedPlanner();

    public ExecutionPlanner() {
    }

    public Executor plan(BoundStatement statement) {
        return costBasedPlanner.plan(statement);
    }

    public Executor planTableScan(TableMetadata table, BoundExpr where) {
        return costBasedPlanner.planTableAccess(table, where);
    }

    public record IndexCandidate(
            IndexMetadata index,
            Value lowKey,
            boolean lowInclusive,
            Value highKey,
            boolean highInclusive,
            BoundExpr residualPredicate) {

        public boolean isEquality() {
            return lowKey != null
                    && highKey != null
                    && lowInclusive
                    && highInclusive
                    && !lowKey.isNull()
                    && !highKey.isNull()
                    && lowKey.compareTo(highKey) == 0;
        }
    }

    public static IndexCandidate selectBestIndexCandidate(TableMetadata table, BoundExpr where) {
        List<IndexCandidate> candidates = extractIndexCandidates(table, where);
        if (candidates.isEmpty()) {
            return null;
        }
        IndexCandidate best = null;
        int bestRank = Integer.MAX_VALUE;
        for (IndexCandidate c : candidates) {
            int rank;
            if (c.isEquality() && c.index().unique()) {
                rank = 1;
            } else if (c.isEquality()) {
                rank = 2;
            } else if (c.lowKey() != null && c.highKey() != null) {
                rank = 3;
            } else {
                rank = 4;
            }
            if (rank < bestRank) {
                bestRank = rank;
                best = c;
            }
        }
        return best;
    }

    public static List<IndexCandidate> extractIndexCandidates(TableMetadata table, BoundExpr where) {
        if (where == null || table.getIndexes().isEmpty()) {
            return List.of();
        }
        List<BoundExpr> conjuncts = new ArrayList<>();
        collectAndConjuncts(where, conjuncts);

        List<IndexCandidate> results = new ArrayList<>();
        for (IndexMetadata idx : table.getIndexes()) {
            int colIdx = table.getSchema().getColumnIndex(idx.columnName());
            Column col = table.getSchema().getColumn(colIdx);

            Value lowKey = null;
            boolean lowInc = false;
            Value highKey = null;
            boolean highInc = false;
            Set<Integer> consumedConjuncts = new HashSet<>();

            for (int i = 0; i < conjuncts.size(); i++) {
                SingleColumnBound bound = matchSingleColumnComparison(conjuncts.get(i), colIdx, col.type());
                if (bound == null) {
                    continue;
                }
                consumedConjuncts.add(i);
                if (bound.hasLow) {
                    if (lowKey == null || bound.lowKey.isNull()) {
                        lowKey = bound.lowKey;
                        lowInc = bound.lowInclusive;
                    } else if (!lowKey.isNull()) {
                        int cmp = bound.lowKey.compareTo(lowKey);
                        if (cmp > 0) {
                            lowKey = bound.lowKey;
                            lowInc = bound.lowInclusive;
                        } else if (cmp == 0 && !bound.lowInclusive) {
                            lowInc = false;
                        }
                    }
                }
                if (bound.hasHigh) {
                    if (highKey == null || bound.highKey.isNull()) {
                        highKey = bound.highKey;
                        highInc = bound.highInclusive;
                    } else if (!highKey.isNull()) {
                        int cmp = bound.highKey.compareTo(highKey);
                        if (cmp < 0) {
                            highKey = bound.highKey;
                            highInc = bound.highInclusive;
                        } else if (cmp == 0 && !bound.highInclusive) {
                            highInc = false;
                        }
                    }
                }
            }

            if (!consumedConjuncts.isEmpty()) {
                BoundExpr residual = null;
                for (int i = 0; i < conjuncts.size(); i++) {
                    if (!consumedConjuncts.contains(i)) {
                        residual = (residual == null)
                                ? conjuncts.get(i)
                                : new BoundExpr.BoundBinaryExpr(residual, Expr.BinaryOp.AND, conjuncts.get(i), DataType.BOOLEAN);
                    }
                }
                results.add(new IndexCandidate(idx, lowKey, lowInc, highKey, highInc, residual));
            }
        }
        return results;
    }

    private static void collectAndConjuncts(BoundExpr expr, List<BoundExpr> out) {
        if (expr instanceof BoundExpr.BoundBinaryExpr(
                BoundExpr left, Expr.BinaryOp op, BoundExpr right, DataType ignored)
                && op == Expr.BinaryOp.AND) {
            collectAndConjuncts(left, out);
            collectAndConjuncts(right, out);
        } else {
            out.add(expr);
        }
    }

    private record SingleColumnBound(
            boolean hasLow,
            Value lowKey,
            boolean lowInclusive,
            boolean hasHigh,
            Value highKey,
            boolean highInclusive) {}

    private static SingleColumnBound matchSingleColumnComparison(
            BoundExpr expr,
            int targetColIdx,
            DataType colType) {
        if (!(expr instanceof BoundExpr.BoundBinaryExpr(
                BoundExpr left, Expr.BinaryOp op, BoundExpr right, DataType ignored))) {
            return null;
        }
        if (op != Expr.BinaryOp.EQ
                && op != Expr.BinaryOp.LT
                && op != Expr.BinaryOp.LTE
                && op != Expr.BinaryOp.GT
                && op != Expr.BinaryOp.GTE) {
            return null;
        }

        if (isTargetColumnRef(left, targetColIdx) && isConstantExpr(right)) {
            Value lit = evaluateConstantForColumn(right, colType);
            if (lit == null) {
                return null;
            }
            return buildBoundForOp(op, lit);
        }

        if (isTargetColumnRef(right, targetColIdx) && isConstantExpr(left)) {
            Value lit = evaluateConstantForColumn(left, colType);
            if (lit == null) {
                return null;
            }
            return buildBoundForOp(flipComparisonOp(op), lit);
        }
        return null;
    }

    private static boolean isTargetColumnRef(BoundExpr expr, int targetColIdx) {
        if (expr instanceof BoundExpr.BoundColumnRef(int idx, String ignoredName, DataType ignoredType)) {
            return idx == targetColIdx;
        }
        return false;
    }

    private static boolean isConstantExpr(BoundExpr expr) {
        return switch (expr) {
            case BoundExpr.BoundLiteral ignored -> true;
            case BoundExpr.BoundColumnRef ignored -> false;
            case BoundExpr.BoundUnaryExpr(var ignoredOp, BoundExpr operand, var ignoredType) ->
                    isConstantExpr(operand);
            case BoundExpr.BoundBinaryExpr(BoundExpr left, var ignoredOp, BoundExpr right, var ignoredType) ->
                    isConstantExpr(left) && isConstantExpr(right);
            case BoundExpr.BoundIsNullExpr(BoundExpr operand, boolean ignoredNeg) ->
                    isConstantExpr(operand);
            case BoundExpr.BoundCastExpr(BoundExpr operand, var ignoredType) ->
                    isConstantExpr(operand);
        };
    }

    private static Value evaluateConstantForColumn(BoundExpr constExpr, DataType colType) {
        Value raw = ExpressionEvaluator.evaluate(constExpr, null);
        if (raw == null || raw.isNull()) {
            return Value.NULL;
        }
        if (raw.dataType() == colType) {
            return raw;
        }
        try {
            return ExpressionEvaluator.coerceValue(raw, colType);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static Expr.BinaryOp flipComparisonOp(Expr.BinaryOp op) {
        return switch (op) {
            case EQ -> Expr.BinaryOp.EQ;
            case LT -> Expr.BinaryOp.GT;
            case LTE -> Expr.BinaryOp.GTE;
            case GT -> Expr.BinaryOp.LT;
            case GTE -> Expr.BinaryOp.LTE;
            default -> op;
        };
    }

    private static SingleColumnBound buildBoundForOp(Expr.BinaryOp op, Value val) {
        return switch (op) {
            case EQ -> new SingleColumnBound(true, val, true, true, val, true);
            case GT -> new SingleColumnBound(true, val, false, false, null, false);
            case GTE -> new SingleColumnBound(true, val, true, false, null, false);
            case LT -> new SingleColumnBound(false, null, false, true, val, false);
            case LTE -> new SingleColumnBound(false, null, false, true, val, true);
            default -> null;
        };
    }
}
