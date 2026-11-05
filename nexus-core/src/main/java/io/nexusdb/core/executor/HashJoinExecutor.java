package io.nexusdb.core.executor;

import io.nexusdb.core.ast.JoinType;
import io.nexusdb.core.planner.BoundExpr;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Physical in-memory hash join operator supporting {@link JoinType#INNER} and {@link JoinType#LEFT}
 * for equi-join predicates, with an optional residual predicate for mixed conditions.
 *
 * <p>Build-Side Selection & SQL NULL Invariants:
 * <ul>
 *   <li>For {@link JoinType#INNER}, the planner/executor compares {@code leftChild.getEstimatedRows()}
 *       and {@code rightChild.getEstimatedRows()} and builds the in-memory hash table on the smaller
 *       input side ({@code buildLeft = leftRows < rightRows}), streaming the larger side as the probe
 *       input while preserving canonical {@code (leftTuple, rightTuple)} output column order.</li>
 *   <li>For {@link JoinType#LEFT}, the right child is hashed as the build side so that every left
 *       probe tuple with zero matching right rows (or with a {@code NULL} join key) is emitted once
 *       padded with {@code NULL}s on the right.</li>
 *   <li>Per SQL three-valued logic, {@code NULL = NULL} evaluates to {@code UNKNOWN} (never {@code TRUE}).
 *       Rows where any equi-join key evaluates to {@link Value#NULL} are never inserted into the build
 *       hash table and never match during probing.</li>
 * </ul>
 */
public final class HashJoinExecutor implements Executor {
    private final Executor leftChild;
    private final Executor rightChild;
    private final JoinType joinType;
    private final List<BoundExpr> leftKeys;
    private final List<BoundExpr> rightKeys;
    private final BoundExpr residualPredicate;
    private final boolean buildLeft;
    private final Schema outputSchema;
    private final int leftColCount;
    private final int rightColCount;

    private Map<List<Value>, List<Tuple>> hashTable;
    private Tuple currentProbeTuple;
    private List<Tuple> currentBucket;
    private int bucketCursor;
    private boolean currentLeftMatched;

    private double estimatedCost;
    private long estimatedRows;
    private long actualRows;
    private long elapsedNanos;
    private boolean executed;

    public HashJoinExecutor(
            Executor leftChild,
            Executor rightChild,
            JoinType joinType,
            List<BoundExpr> leftKeys,
            List<BoundExpr> rightKeys,
            BoundExpr residualPredicate) {
        this.leftChild = Objects.requireNonNull(leftChild, "leftChild must not be null");
        this.rightChild = Objects.requireNonNull(rightChild, "rightChild must not be null");
        this.joinType = Objects.requireNonNull(joinType, "joinType must not be null");
        this.leftKeys = List.copyOf(Objects.requireNonNull(leftKeys, "leftKeys must not be null"));
        this.rightKeys = List.copyOf(Objects.requireNonNull(rightKeys, "rightKeys must not be null"));
        if (this.leftKeys.isEmpty() || this.leftKeys.size() != this.rightKeys.size()) {
            throw new IllegalArgumentException("leftKeys and rightKeys must be non-empty and of equal size");
        }
        this.residualPredicate = residualPredicate;
        this.leftColCount = leftChild.getOutputSchema().getColumnCount();
        this.rightColCount = rightChild.getOutputSchema().getColumnCount();
        this.outputSchema = NestedLoopJoinExecutor.buildJoinedSchema(
                leftChild.getOutputSchema(),
                rightChild.getOutputSchema());

        long leftRows = leftChild.getEstimatedRows();
        long rightRows = rightChild.getEstimatedRows();
        this.buildLeft = (joinType == JoinType.INNER) && (leftRows < rightRows);

        if (joinType == JoinType.LEFT) {
            this.estimatedRows = Math.max(leftRows, Math.max(leftRows, rightRows));
        } else {
            this.estimatedRows = (leftRows == 0L || rightRows == 0L)
                    ? 0L
                    : Math.max(1L, Math.max(leftRows, rightRows));
        }
        long buildRows = this.buildLeft ? leftRows : rightRows;
        long probeRows = this.buildLeft ? rightRows : leftRows;
        this.estimatedCost = leftChild.getEstimatedCost()
                + rightChild.getEstimatedCost()
                + buildRows * 0.08
                + probeRows * 0.05;
    }

    public boolean isBuildLeft() {
        return buildLeft;
    }

    @Override
    public void init() {
        long start = System.nanoTime();
        this.actualRows = 0L;
        this.executed = true;
        this.hashTable = new HashMap<>();

        Executor buildExec = buildLeft ? leftChild : rightChild;
        Executor probeExec = buildLeft ? rightChild : leftChild;

        buildExec.init();
        try {
            Tuple buildRow;
            while ((buildRow = buildExec.next()) != null) {
                List<Value> key = evaluateJoinKeys(buildRow, buildLeft);
                if (key != null) {
                    hashTable.computeIfAbsent(key, ignored -> new ArrayList<>()).add(buildRow);
                }
            }
        } finally {
            buildExec.close();
        }

        probeExec.init();
        this.currentProbeTuple = probeExec.next();
        this.currentBucket = prepareBucketForCurrentProbe();
        this.bucketCursor = 0;
        this.currentLeftMatched = false;
        this.elapsedNanos = System.nanoTime() - start;
    }

    private List<Tuple> prepareBucketForCurrentProbe() {
        if (currentProbeTuple == null) {
            return List.of();
        }
        List<Value> probeKey = evaluateJoinKeys(currentProbeTuple, !buildLeft);
        if (probeKey == null) {
            return List.of();
        }
        return hashTable.getOrDefault(probeKey, List.of());
    }

    /**
     * Evaluates the join key expressions for {@code tuple}.
     * Returns {@code null} if any key component is {@link Value#NULL}, enforcing the SQL invariant
     * that {@code NULL = NULL} never matches in a join condition.
     */
    private List<Value> evaluateJoinKeys(Tuple tuple, boolean isLeftTuple) {
        List<BoundExpr> keyExprs = isLeftTuple ? leftKeys : rightKeys;
        Tuple evalContext = isLeftTuple
                ? tuple
                : NestedLoopJoinExecutor.combineTuples(null, tuple, leftColCount, rightColCount);

        List<Value> keyValues = new ArrayList<>(keyExprs.size());
        for (BoundExpr expr : keyExprs) {
            Value v = ExpressionEvaluator.evaluate(expr, evalContext);
            if (v == null || v.isNull()) {
                return null;
            }
            keyValues.add(v);
        }
        return List.copyOf(keyValues);
    }

    @Override
    public Tuple next() {
        if (hashTable == null) {
            throw new IllegalStateException("HashJoinExecutor.init() must be called before next()");
        }
        long start = System.nanoTime();
        Executor probeExec = buildLeft ? rightChild : leftChild;
        try {
            while (currentProbeTuple != null) {
                while (bucketCursor < currentBucket.size()) {
                    Tuple buildTuple = currentBucket.get(bucketCursor++);
                    Tuple leftTuple = buildLeft ? buildTuple : currentProbeTuple;
                    Tuple rightTuple = buildLeft ? currentProbeTuple : buildTuple;
                    Tuple combined = NestedLoopJoinExecutor.combineTuples(
                            leftTuple, rightTuple, leftColCount, rightColCount);
                    if (residualPredicate == null
                            || ExpressionEvaluator.evaluatePredicate(residualPredicate, combined)) {
                        currentLeftMatched = true;
                        actualRows++;
                        return combined;
                    }
                }

                if (joinType == JoinType.LEFT && !currentLeftMatched) {
                    Tuple nullPadded = NestedLoopJoinExecutor.combineTuples(
                            currentProbeTuple, null, leftColCount, rightColCount);
                    advanceProbe(probeExec);
                    actualRows++;
                    return nullPadded;
                }

                advanceProbe(probeExec);
            }
            return null;
        } finally {
            elapsedNanos += System.nanoTime() - start;
        }
    }

    private void advanceProbe(Executor probeExec) {
        this.currentProbeTuple = probeExec.next();
        this.currentBucket = prepareBucketForCurrentProbe();
        this.bucketCursor = 0;
        this.currentLeftMatched = false;
    }

    @Override
    public void close() {
        hashTable = null;
        currentProbeTuple = null;
        currentBucket = List.of();
        leftChild.close();
        rightChild.close();
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    @Override
    public double getEstimatedCost() {
        return estimatedCost;
    }

    @Override
    public long getEstimatedRows() {
        return estimatedRows;
    }

    @Override
    public void setEstimates(double estimatedCost, long estimatedRows) {
        this.estimatedCost = estimatedCost;
        this.estimatedRows = estimatedRows;
    }

    @Override
    public long getActualRows() {
        return actualRows;
    }

    @Override
    public long getElapsedNanos() {
        return elapsedNanos;
    }

    @Override
    public boolean isExecuted() {
        return executed;
    }

    @Override
    public String explain(int indent) {
        String prefix = "  ".repeat(Math.max(0, indent));
        StringBuilder condSb = new StringBuilder();
        for (int i = 0; i < leftKeys.size(); i++) {
            if (i > 0) {
                condSb.append(" AND ");
            }
            condSb.append(formatKeyExpr(leftKeys.get(i)))
                    .append(" = ")
                    .append(formatKeyExpr(rightKeys.get(i)));
        }
        String buildSide = buildLeft ? "left" : "right";
        return prefix + "HashJoin(type=" + joinType.name()
                + ", condition=" + condSb
                + ", build=" + buildSide + ")"
                + formatMetrics()
                + "\n" + leftChild.explain(indent + 1)
                + "\n" + rightChild.explain(indent + 1);
    }

    private static String formatKeyExpr(BoundExpr expr) {
        if (expr instanceof BoundExpr.BoundColumnRef(int ignored, String colName, var ignoredType)) {
            return colName;
        }
        if (expr instanceof BoundExpr.BoundCastExpr(BoundExpr operand, var ignoredType)) {
            return formatKeyExpr(operand);
        }
        return expr.toString();
    }
}
