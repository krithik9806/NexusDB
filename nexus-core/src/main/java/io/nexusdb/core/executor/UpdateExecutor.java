package io.nexusdb.core.executor;

import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.planner.BoundStatement;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Physical {@code UPDATE} operator that updates matching tuples in a {@link TableHeap} and
 * emits a single tuple containing the number of updated rows.
 *
 * <p>Halloween Problem & Pinning Invariant:
 * When an updated variable-length tuple grows beyond its current page's capacity,
 * {@link TableHeap#update} deletes the old slot and appends the tuple to the tail page of the heap.
 * If {@code UpdateExecutor} mutated the heap while a forward {@link SeqScanExecutor} was still scanning,
 * the scan could visit the relocated tuple a second time (the Halloween Problem). Therefore, {@link #next()}
 * materializes all matching tuples from {@code child} and closes the scan before applying updates.
 */
public final class UpdateExecutor implements Executor {
    private final TableMetadata table;
    private final Executor child;
    private final List<BoundStatement.BoundAssignment> assignments;
    private boolean executed;

    public UpdateExecutor(
            TableMetadata table,
            Executor child,
            List<BoundStatement.BoundAssignment> assignments) {
        this.table = Objects.requireNonNull(table, "table must not be null");
        this.child = Objects.requireNonNull(child, "child must not be null");
        this.assignments = List.copyOf(Objects.requireNonNull(assignments, "assignments must not be null"));
    }

    @Override
    public void init() {
        this.executed = false;
        child.init();
    }

    @Override
    public Tuple next() {
        if (executed) {
            return null;
        }
        executed = true;

        List<Tuple> targets = new ArrayList<>();
        try {
            Tuple t;
            while ((t = child.next()) != null) {
                targets.add(t);
            }
        } finally {
            child.close();
        }

        Schema schema = table.getSchema();
        TableHeap heap = table.getTableHeap();
        int updatedCount = 0;

        for (Tuple existing : targets) {
            List<Value> newValues = new ArrayList<>(existing.getValues());
            for (BoundStatement.BoundAssignment assignment : assignments) {
                int colIdx = assignment.columnIndex();
                Column col = schema.getColumn(colIdx);
                Value evaluated = ExpressionEvaluator.evaluate(assignment.value(), existing);
                Value validated = InsertExecutor.validateAndCoerceColumnValue(evaluated, col);
                newValues.set(colIdx, validated);
            }
            Tuple updatedTuple = new Tuple(newValues, existing.getRecordId());
            heap.update(existing.getRecordId(), updatedTuple);
            updatedCount++;
        }

        return new Tuple(Value.ofInt(updatedCount));
    }

    @Override
    public void close() {
        child.close();
    }

    @Override
    public Schema getOutputSchema() {
        return InsertExecutor.DML_OUTPUT_SCHEMA;
    }

    @Override
    public String explain(int indent) {
        String prefix = "  ".repeat(Math.max(0, indent));
        return prefix + "Update(table=" + table.getName() + ")\n" + child.explain(indent + 1);
    }
}
