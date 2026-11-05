package io.nexusdb.core.executor;

import io.nexusdb.core.ast.JoinType;
import io.nexusdb.core.planner.BoundExpr;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Physical nested-loop join operator supporting {@link JoinType#INNER} and {@link JoinType#LEFT}
 * for arbitrary (including non-equi) join predicates.
 *
 * <p>SQL Join Semantics Invariant:
 * <ul>
 *   <li>A pair of left and right tuples matches if and only if {@code condition} evaluates to
 *       {@code TRUE} under three-valued SQL logic ({@code FALSE} and {@code NULL} do not match,
 *       so {@code NULL = NULL} never matches).</li>
 *   <li>For {@link JoinType#LEFT}, any left tuple that matches zero right tuples is emitted once
 *       padded on the right with {@link Value#NULL} for all right-side columns.</li>
 * </ul>
 */
public final class NestedLoopJoinExecutor implements Executor {
    private final Executor leftChild;
    private final Executor rightChild;
    private final JoinType joinType;
    private final BoundExpr condition;
    private final Schema outputSchema;
    private final int leftColCount;
    private final int rightColCount;

    private List<Tuple> rightTuples;
    private Tuple currentLeft;
    private int rightCursor;
    private boolean currentLeftMatched;

    private double estimatedCost;
    private long estimatedRows;
    private long actualRows;
    private long elapsedNanos;
    private boolean executed;

    public NestedLoopJoinExecutor(
            Executor leftChild,
            Executor rightChild,
            JoinType joinType,
            BoundExpr condition) {
        this.leftChild = Objects.requireNonNull(leftChild, "leftChild must not be null");
        this.rightChild = Objects.requireNonNull(rightChild, "rightChild must not be null");
        this.joinType = Objects.requireNonNull(joinType, "joinType must not be null");
        this.condition = Objects.requireNonNull(condition, "condition must not be null");
        this.leftColCount = leftChild.getOutputSchema().getColumnCount();
        this.rightColCount = rightChild.getOutputSchema().getColumnCount();
        this.outputSchema = buildJoinedSchema(leftChild.getOutputSchema(), rightChild.getOutputSchema());

        long leftRows = leftChild.getEstimatedRows();
        long rightRows = rightChild.getEstimatedRows();
        long crossRows = Math.max(0L, leftRows * rightRows);
        if (joinType == JoinType.LEFT) {
            this.estimatedRows = Math.max(leftRows, Math.round(crossRows * 0.25));
        } else {
            this.estimatedRows = crossRows == 0L ? 0L : Math.max(1L, Math.round(crossRows * 0.25));
        }
        this.estimatedCost = leftChild.getEstimatedCost()
                + rightChild.getEstimatedCost()
                + crossRows * 0.05;
    }

    static Schema buildJoinedSchema(Schema leftSchema, Schema rightSchema) {
        List<Column> combined = new ArrayList<>(leftSchema.getColumnCount() + rightSchema.getColumnCount());
        int seq = 0;
        for (Column c : leftSchema.getColumns()) {
            combined.add(new Column("c" + (seq++) + "_" + c.name(), c.type(), c.maxLength(), true, false));
        }
        for (Column c : rightSchema.getColumns()) {
            combined.add(new Column("c" + (seq++) + "_" + c.name(), c.type(), c.maxLength(), true, false));
        }
        return new Schema(combined);
    }

    static Tuple combineTuples(Tuple left, Tuple right, int leftCols, int rightCols) {
        List<Value> values = new ArrayList<>(leftCols + rightCols);
        if (left != null) {
            values.addAll(left.getValues());
        } else {
            values.addAll(Collections.nCopies(leftCols, Value.NULL));
        }
        if (right != null) {
            values.addAll(right.getValues());
        } else {
            values.addAll(Collections.nCopies(rightCols, Value.NULL));
        }
        return new Tuple(values);
    }

    @Override
    public void init() {
        long start = System.nanoTime();
        this.actualRows = 0L;
        this.executed = true;

        rightChild.init();
        List<Tuple> materializedRight = new ArrayList<>();
        try {
            Tuple r;
            while ((r = rightChild.next()) != null) {
                materializedRight.add(r);
            }
        } finally {
            rightChild.close();
        }
        this.rightTuples = materializedRight;

        leftChild.init();
        this.currentLeft = leftChild.next();
        this.rightCursor = 0;
        this.currentLeftMatched = false;
        this.elapsedNanos = System.nanoTime() - start;
    }

    @Override
    public Tuple next() {
        if (rightTuples == null) {
            throw new IllegalStateException("NestedLoopJoinExecutor.init() must be called before next()");
        }
        long start = System.nanoTime();
        try {
            while (currentLeft != null) {
                while (rightCursor < rightTuples.size()) {
                    Tuple right = rightTuples.get(rightCursor++);
                    Tuple candidate = combineTuples(currentLeft, right, leftColCount, rightColCount);
                    if (ExpressionEvaluator.evaluatePredicate(condition, candidate)) {
                        currentLeftMatched = true;
                        actualRows++;
                        return candidate;
                    }
                }
                if (joinType == JoinType.LEFT && !currentLeftMatched) {
                    Tuple nullPadded = combineTuples(currentLeft, null, leftColCount, rightColCount);
                    currentLeft = leftChild.next();
                    rightCursor = 0;
                    currentLeftMatched = false;
                    actualRows++;
                    return nullPadded;
                }
                currentLeft = leftChild.next();
                rightCursor = 0;
                currentLeftMatched = false;
            }
            return null;
        } finally {
            elapsedNanos += System.nanoTime() - start;
        }
    }

    @Override
    public void close() {
        rightTuples = null;
        currentLeft = null;
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
        return prefix + "NestedLoopJoin(type=" + joinType.name() + ", condition=" + condition + ")"
                + formatMetrics()
                + "\n" + leftChild.explain(indent + 1)
                + "\n" + rightChild.explain(indent + 1);
    }
}
