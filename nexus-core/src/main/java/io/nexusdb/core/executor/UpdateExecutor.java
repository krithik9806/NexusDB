package io.nexusdb.core.executor;

import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.index.BPlusTree;
import io.nexusdb.core.index.DuplicateKeyException;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.planner.BoundStatement;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Physical {@code UPDATE} operator that updates matching tuples in a {@link TableHeap},
 * maintains all table {@link BPlusTree} indexes (including when tuples relocate across pages),
 * and emits a single tuple containing the number of updated rows.
 *
 * <p>Halloween Problem & Index Maintenance Invariant:
 * <ol>
 *   <li>All matching tuples from {@code child} are materialized and {@code child} is closed before
 *       any heap or index mutation occurs, preventing a scan from re-visiting relocated tuples.</li>
 *   <li>All unique indexes are pre-validated across the updated tuple set before mutating the heap
 *       or any index, guaranteeing no partial updates on {@link DuplicateKeyException}.</li>
 *   <li>When {@link TableHeap#update} relocates a growing variable-length tuple to a new
 *       {@link RecordId}, all indexes on the table (even those whose key value did not change)
 *       replace the old {@link RecordId} with the relocated {@link RecordId}.</li>
 * </ol>
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

        if (targets.isEmpty()) {
            return new Tuple(Value.ofInt(0));
        }

        Schema schema = table.getSchema();
        TableHeap heap = table.getTableHeap();
        List<IndexMetadata> indexes = table.getIndexes();

        List<Tuple> newTuples = new ArrayList<>(targets.size());
        Set<RecordId> targetRidSet = new HashSet<>(targets.size() * 2);

        for (Tuple existing : targets) {
            targetRidSet.add(existing.getRecordId());
            List<Value> newValues = new ArrayList<>(existing.getValues());
            for (BoundStatement.BoundAssignment assignment : assignments) {
                int colIdx = assignment.columnIndex();
                Column col = schema.getColumn(colIdx);
                Value evaluated = ExpressionEvaluator.evaluate(assignment.value(), existing);
                Value validated = InsertExecutor.validateAndCoerceColumnValue(evaluated, col);
                newValues.set(colIdx, validated);
            }
            newTuples.add(new Tuple(newValues, existing.getRecordId()));
        }

        // Pre-validate all unique indexes before mutating the heap or any index
        for (IndexMetadata idx : indexes) {
            if (!idx.unique()) {
                continue;
            }
            int colIdx = schema.getColumnIndex(idx.columnName());
            BPlusTree tree = table.getIndexTree(idx);
            Set<Value> updatedKeyBatch = new HashSet<>();

            for (Tuple updated : newTuples) {
                Value newKey = updated.getValue(colIdx);
                if (!updatedKeyBatch.add(newKey)) {
                    throw new DuplicateKeyException(
                            "Duplicate key '" + newKey + "' in UPDATE violates unique index '"
                                    + idx.indexName() + "' on column '" + idx.columnName() + "'");
                }
                List<RecordId> existingMatches = tree.search(newKey);
                for (RecordId matchRid : existingMatches) {
                    if (!targetRidSet.contains(matchRid)) {
                        throw new DuplicateKeyException(
                                "Duplicate key '" + newKey + "' in UPDATE violates unique index '"
                                        + idx.indexName() + "' on column '" + idx.columnName() + "'");
                    }
                }
            }
        }

        // Remove old index entries for all target rows before applying heap updates
        for (int i = 0; i < targets.size(); i++) {
            Tuple oldTuple = targets.get(i);
            RecordId oldRid = oldTuple.getRecordId();
            for (IndexMetadata idx : indexes) {
                int colIdx = schema.getColumnIndex(idx.columnName());
                BPlusTree tree = table.getIndexTree(idx);
                tree.delete(oldTuple.getValue(colIdx), oldRid);
            }
        }

        // Apply heap updates and re-insert index entries with the (possibly relocated) RecordId
        int updatedCount = 0;
        for (int i = 0; i < targets.size(); i++) {
            Tuple oldTuple = targets.get(i);
            Tuple updatedTuple = newTuples.get(i);
            RecordId newRid = heap.update(oldTuple.getRecordId(), updatedTuple);
            for (IndexMetadata idx : indexes) {
                int colIdx = schema.getColumnIndex(idx.columnName());
                BPlusTree tree = table.getIndexTree(idx);
                tree.insert(updatedTuple.getValue(colIdx), newRid);
            }
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
