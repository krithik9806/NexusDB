package io.nexusdb.core.planner;

import io.nexusdb.core.ast.Expr;
import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.executor.DeleteExecutor;
import io.nexusdb.core.executor.ExecutionPlanner;
import io.nexusdb.core.executor.Executor;
import io.nexusdb.core.executor.FilterExecutor;
import io.nexusdb.core.executor.HashAggregateExecutor;
import io.nexusdb.core.executor.HashJoinExecutor;
import io.nexusdb.core.executor.IndexScanExecutor;
import io.nexusdb.core.executor.InsertExecutor;
import io.nexusdb.core.executor.LimitExecutor;
import io.nexusdb.core.executor.NestedLoopJoinExecutor;
import io.nexusdb.core.executor.ProjectionExecutor;
import io.nexusdb.core.executor.SeqScanExecutor;
import io.nexusdb.core.executor.SortExecutor;
import io.nexusdb.core.executor.UpdateExecutor;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Cost-based physical query planner that uses {@link TableStatistics} and predicate selectivity
 * estimation to choose between {@link SeqScanExecutor} and {@link IndexScanExecutor} and annotates
 * every operator in the resulting Volcano pipeline with estimated {@code cost} and {@code rows}.
 */
public final class CostBasedPlanner {
    public static final double SEQ_PAGE_COST = 1.0;
    public static final double CPU_TUPLE_COST = 0.10;
    public static final double CPU_FILTER_COST = 0.05;
    public static final double INDEX_ENTRY_FETCH_COST = 0.35;

    public static final double DEFAULT_EQUALITY_SELECTIVITY = 0.02;
    public static final double DEFAULT_BOUNDED_RANGE_SELECTIVITY = 0.10;
    public static final double DEFAULT_OPEN_RANGE_SELECTIVITY = 0.25;
    public static final double DEFAULT_FILTER_SELECTIVITY = 0.25;

    public CostBasedPlanner() {
    }

    public Executor plan(BoundStatement statement) {
        Objects.requireNonNull(statement, "statement must not be null");
        return switch (statement) {
            case BoundStatement.BoundInsert insert ->
                    new InsertExecutor(insert.table(), insert.rows());
            case BoundStatement.BoundSelect select -> planSelect(select);
            case BoundStatement.BoundUpdate update -> {
                Executor scan = planTableAccess(update.table(), update.where());
                yield new UpdateExecutor(update.table(), scan, update.assignments());
            }
            case BoundStatement.BoundDelete delete -> {
                Executor scan = planTableAccess(delete.table(), delete.where());
                yield new DeleteExecutor(delete.table(), scan);
            }
            default -> throw new IllegalArgumentException(
                    "Statement type does not produce a physical tuple Executor directly: "
                            + statement.getClass().getSimpleName());
        };
    }

    private Executor planSelect(BoundStatement.BoundSelect select) {
        Executor plan;
        if (select.joins().isEmpty()) {
            plan = planTableAccess(select.table(), select.where());
        } else {
            plan = planTableAccess(select.table(), null);
            for (BoundStatement.BoundJoin join : select.joins()) {
                Executor rightPlan = planTableAccess(join.rightTable(), null);
                plan = planJoin(plan, rightPlan, join);
            }
            if (select.where() != null) {
                plan = new FilterExecutor(plan, select.where());
            }
        }
        if (select.isAggregated()) {
            plan = new HashAggregateExecutor(plan, select.groupBy(), select.aggregates());
            if (select.having() != null) {
                plan = new FilterExecutor(plan, select.having());
            }
        }
        if (!select.orderBy().isEmpty()) {
            plan = new SortExecutor(plan, select.orderBy());
        }
        plan = new ProjectionExecutor(plan, select.projections(), select.outputSchema());
        if (select.limit() != null) {
            plan = new LimitExecutor(plan, select.limit());
        }
        return plan;
    }

    private Executor planJoin(Executor leftPlan, Executor rightPlan, BoundStatement.BoundJoin join) {
        int splitIndex = join.rightStartColumnIndex();
        int maxIndex = splitIndex + join.rightTable().getSchema().getColumnCount();
        List<BoundExpr> conjuncts = new ArrayList<>();
        collectJoinConjuncts(join.condition(), conjuncts);

        List<BoundExpr> leftKeys = new ArrayList<>();
        List<BoundExpr> rightKeys = new ArrayList<>();
        List<BoundExpr> residuals = new ArrayList<>();

        for (BoundExpr c : conjuncts) {
            if (c instanceof BoundExpr.BoundBinaryExpr(
                    BoundExpr left, Expr.BinaryOp op, BoundExpr right, DataType ignoredType)
                    && op == Expr.BinaryOp.EQ) {
                if (referencesOnlyRange(left, 0, splitIndex)
                        && referencesOnlyRange(right, splitIndex, maxIndex)) {
                    leftKeys.add(left);
                    rightKeys.add(right);
                    continue;
                }
                if (referencesOnlyRange(right, 0, splitIndex)
                        && referencesOnlyRange(left, splitIndex, maxIndex)) {
                    leftKeys.add(right);
                    rightKeys.add(left);
                    continue;
                }
            }
            residuals.add(c);
        }

        if (!leftKeys.isEmpty()) {
            BoundExpr residual = null;
            for (BoundExpr r : residuals) {
                residual = (residual == null)
                        ? r
                        : new BoundExpr.BoundBinaryExpr(residual, Expr.BinaryOp.AND, r, DataType.BOOLEAN);
            }
            return new HashJoinExecutor(leftPlan, rightPlan, join.joinType(), leftKeys, rightKeys, residual);
        }
        return new NestedLoopJoinExecutor(leftPlan, rightPlan, join.joinType(), join.condition());
    }

    private static void collectJoinConjuncts(BoundExpr expr, List<BoundExpr> out) {
        if (expr instanceof BoundExpr.BoundBinaryExpr(
                BoundExpr left, Expr.BinaryOp op, BoundExpr right, DataType ignored)
                && op == Expr.BinaryOp.AND) {
            collectJoinConjuncts(left, out);
            collectJoinConjuncts(right, out);
        } else {
            out.add(expr);
        }
    }

    private static boolean referencesOnlyRange(BoundExpr expr, int minInclusive, int maxExclusive) {
        boolean[] hasCol = {false};
        boolean[] allInRange = {true};
        checkColumnRange(expr, minInclusive, maxExclusive, hasCol, allInRange);
        return hasCol[0] && allInRange[0];
    }

    private static void checkColumnRange(
            BoundExpr expr,
            int minInclusive,
            int maxExclusive,
            boolean[] hasCol,
            boolean[] allInRange) {
        if (!allInRange[0] || expr == null) {
            return;
        }
        switch (expr) {
            case BoundExpr.BoundColumnRef(int colIdx, String ignoredName, DataType ignoredType) -> {
                hasCol[0] = true;
                if (colIdx < minInclusive || colIdx >= maxExclusive) {
                    allInRange[0] = false;
                }
            }
            case BoundExpr.BoundLiteral ignored -> {
            }
            case BoundExpr.BoundUnaryExpr(var ignoredOp, BoundExpr operand, var ignoredType) ->
                    checkColumnRange(operand, minInclusive, maxExclusive, hasCol, allInRange);
            case BoundExpr.BoundBinaryExpr(BoundExpr left, var ignoredOp, BoundExpr right, var ignoredType) -> {
                checkColumnRange(left, minInclusive, maxExclusive, hasCol, allInRange);
                checkColumnRange(right, minInclusive, maxExclusive, hasCol, allInRange);
            }
            case BoundExpr.BoundIsNullExpr(BoundExpr operand, boolean ignoredNeg) ->
                    checkColumnRange(operand, minInclusive, maxExclusive, hasCol, allInRange);
            case BoundExpr.BoundCastExpr(BoundExpr operand, var ignoredType) ->
                    checkColumnRange(operand, minInclusive, maxExclusive, hasCol, allInRange);
            case BoundExpr.BoundAggregate ignored -> allInRange[0] = false;
        }
    }

    /**
     * Evaluates sequential scan vs all applicable B+Tree index scan candidates for {@code table}
     * and {@code where}, returning the lowest-cost physical access plan.
     */
    public Executor planTableAccess(TableMetadata table, BoundExpr where) {
        Objects.requireNonNull(table, "table must not be null");
        TableStatistics stats = TableStatistics.from(table);
        long rowCount = stats.rowCount();
        int pageCount = stats.pageCount();

        SeqScanExecutor seqScan = new SeqScanExecutor(table);
        double seqScanCost = pageCount * SEQ_PAGE_COST + rowCount * CPU_TUPLE_COST;
        seqScan.setEstimates(seqScanCost, rowCount);

        if (where == null) {
            return seqScan;
        }

        double filterSel = estimatePredicateSelectivity(table, where, stats);
        long filteredRows = rowCount == 0 ? 0L : Math.max(1L, Math.round(rowCount * filterSel));
        double seqFilterCost = seqScanCost + rowCount * CPU_FILTER_COST;

        FilterExecutor seqFilterPlan = new FilterExecutor(seqScan, where);
        seqFilterPlan.setEstimates(seqFilterCost, filteredRows);

        Executor bestPlan = seqFilterPlan;
        double bestCost = seqFilterCost;

        List<ExecutionPlanner.IndexCandidate> candidates = ExecutionPlanner.extractIndexCandidates(table, where);
        for (ExecutionPlanner.IndexCandidate candidate : candidates) {
            IndexMetadata idx = candidate.index();
            long estIndexRows = estimateIndexCandidateRows(candidate, stats);
            int treeHeight = table.getIndexTree(idx).getHeight();

            double treeProbeCost = idx.indexName().startsWith("pk_")
                    ? treeHeight * 1.0
                    : treeHeight * 0.6;
            double indexScanCost = treeProbeCost + estIndexRows * INDEX_ENTRY_FETCH_COST;

            IndexScanExecutor indexScan = new IndexScanExecutor(
                    table,
                    idx,
                    candidate.lowKey(),
                    candidate.lowInclusive(),
                    candidate.highKey(),
                    candidate.highInclusive());
            indexScan.setEstimates(indexScanCost, estIndexRows);

            Executor candidatePlan = indexScan;
            double candidateTotalCost = indexScanCost;

            if (candidate.residualPredicate() != null) {
                double residualSel = estimatePredicateSelectivity(table, candidate.residualPredicate(), stats);
                long postResidualRows = estIndexRows == 0
                        ? 0L
                        : Math.max(1L, Math.round(estIndexRows * residualSel));
                candidateTotalCost = indexScanCost + estIndexRows * CPU_FILTER_COST;
                FilterExecutor residualFilter = new FilterExecutor(indexScan, candidate.residualPredicate());
                residualFilter.setEstimates(candidateTotalCost, postResidualRows);
                candidatePlan = residualFilter;
            }

            if (candidateTotalCost < bestCost) {
                bestCost = candidateTotalCost;
                bestPlan = candidatePlan;
            }
        }

        return bestPlan;
    }

    public static long estimateIndexCandidateRows(
            ExecutionPlanner.IndexCandidate candidate,
            TableStatistics stats) {
        long n = stats.rowCount();
        if (n == 0) {
            return 0L;
        }
        Value low = candidate.lowKey();
        Value high = candidate.highKey();
        if ((low != null && low.isNull()) || (high != null && high.isNull())) {
            return 0L;
        }
        if (candidate.isEquality()) {
            if (candidate.index().unique()) {
                return 1L;
            }
            return Math.max(1L, Math.round(n * DEFAULT_EQUALITY_SELECTIVITY));
        }
        if (low != null && high != null) {
            int cmp = low.compareTo(high);
            if (cmp > 0 || (cmp == 0 && (!candidate.lowInclusive() || !candidate.highInclusive()))) {
                return 0L;
            }
            Double lowNum = toNumericDouble(low);
            Double highNum = toNumericDouble(high);
            if (lowNum != null && highNum != null) {
                double span = Math.max(
                        0.0,
                        (highNum - lowNum) + (candidate.lowInclusive() && candidate.highInclusive() ? 1.0 : 0.0));
                double sel = Math.min(1.0, span / Math.max(10.0, (double) n));
                return Math.min(n, Math.max(1L, Math.round(n * sel)));
            }
            return Math.max(1L, Math.round(n * DEFAULT_BOUNDED_RANGE_SELECTIVITY));
        }
        return Math.max(1L, Math.round(n * DEFAULT_OPEN_RANGE_SELECTIVITY));
    }

    public static double estimatePredicateSelectivity(
            TableMetadata table,
            BoundExpr expr,
            TableStatistics stats) {
        if (expr == null) {
            return 1.0;
        }
        long n = Math.max(1L, stats.rowCount());
        return switch (expr) {
            case BoundExpr.BoundBinaryExpr(BoundExpr left, Expr.BinaryOp op, BoundExpr right, var ignoredType) -> {
                if (op == Expr.BinaryOp.AND) {
                    yield estimatePredicateSelectivity(table, left, stats)
                            * estimatePredicateSelectivity(table, right, stats);
                }
                if (op == Expr.BinaryOp.OR) {
                    double s1 = estimatePredicateSelectivity(table, left, stats);
                    double s2 = estimatePredicateSelectivity(table, right, stats);
                    yield Math.min(1.0, s1 + s2 - (s1 * s2));
                }
                if (op == Expr.BinaryOp.EQ) {
                    int colIdx = extractColumnIndex(left, right);
                    if (colIdx >= 0 && table.getSchema().getColumn(colIdx).primaryKey()) {
                        yield 1.0 / n;
                    }
                    yield DEFAULT_EQUALITY_SELECTIVITY;
                }
                if (op == Expr.BinaryOp.LT || op == Expr.BinaryOp.LTE
                        || op == Expr.BinaryOp.GT || op == Expr.BinaryOp.GTE) {
                    yield DEFAULT_OPEN_RANGE_SELECTIVITY;
                }
                yield DEFAULT_FILTER_SELECTIVITY;
            }
            case BoundExpr.BoundUnaryExpr(Expr.UnaryOp op, BoundExpr operand, var ignoredType) ->
                    op == Expr.UnaryOp.NOT
                            ? Math.max(0.01, 1.0 - estimatePredicateSelectivity(table, operand, stats))
                            : DEFAULT_FILTER_SELECTIVITY;
            default -> DEFAULT_FILTER_SELECTIVITY;
        };
    }

    private static int extractColumnIndex(BoundExpr left, BoundExpr right) {
        if (left instanceof BoundExpr.BoundColumnRef(int idx, var ignoredName, var ignoredType)) {
            return idx;
        }
        if (right instanceof BoundExpr.BoundColumnRef(int idx, var ignoredName, var ignoredType)) {
            return idx;
        }
        return -1;
    }

    private static Double toNumericDouble(Value value) {
        return switch (value) {
            case Value.IntValue(int v) -> (double) v;
            case Value.BigIntValue(long v) -> (double) v;
            case Value.DoubleValue(double v) -> v;
            default -> null;
        };
    }
}
