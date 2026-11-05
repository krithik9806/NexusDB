package io.nexusdb.core.executor;

import io.nexusdb.core.planner.BoundStatement;

import java.util.Objects;

/**
 * Logical-to-physical execution planner that translates a validated {@link BoundStatement}
 * into a Volcano {@link Executor} pipeline.
 */
public final class ExecutionPlanner {

    public ExecutionPlanner() {
    }

    public Executor plan(BoundStatement statement) {
        Objects.requireNonNull(statement, "statement must not be null");
        return switch (statement) {
            case BoundStatement.BoundInsert insert ->
                    new InsertExecutor(insert.table(), insert.rows());
            case BoundStatement.BoundSelect select -> planSelect(select);
            case BoundStatement.BoundUpdate update -> {
                Executor scan = new SeqScanExecutor(update.table());
                if (update.where() != null) {
                    scan = new FilterExecutor(scan, update.where());
                }
                yield new UpdateExecutor(update.table(), scan, update.assignments());
            }
            case BoundStatement.BoundDelete delete -> {
                Executor scan = new SeqScanExecutor(delete.table());
                if (delete.where() != null) {
                    scan = new FilterExecutor(scan, delete.where());
                }
                yield new DeleteExecutor(delete.table(), scan);
            }
            default -> throw new IllegalArgumentException(
                    "Statement type does not produce a physical tuple Executor directly: "
                            + statement.getClass().getSimpleName());
        };
    }

    private Executor planSelect(BoundStatement.BoundSelect select) {
        Executor plan = new SeqScanExecutor(select.table());
        if (select.where() != null) {
            plan = new FilterExecutor(plan, select.where());
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
}
