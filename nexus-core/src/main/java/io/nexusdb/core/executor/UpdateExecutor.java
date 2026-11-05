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
import io.nexusdb.core.wal.LogManager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Physical {@code UPDATE} operator that updates matching tuples in a {@link TableHeap},
 * maintains all table {@link BPlusTree} indexes (including when tuples relocate across pages),
 * and emits a single tuple containing the number of updated rows.
 */
public final class UpdateExecutor implements Executor {
    private final TableMetadata table;
    private final Executor child;
    private final List<BoundStatement.BoundAssignment> assignments;
    private boolean consumed;

    private double estimatedCost;
    private long estimatedRows;
    private long actualRows;
    private long elapsedNanos;
    private boolean executed;

    public UpdateExecutor(
            TableMetadata table,
            Executor child,
            List<BoundStatement.BoundAssignment> assignments) {
        this.table = Objects.requireNonNull(table, "table must not be null");
        this.child = Objects.requireNonNull(child, "child must not be null");
        this.assignments = List.copyOf(Objects.requireNonNull(assignments, "assignments must not be null"));
        this.estimatedRows = child.getEstimatedRows();
        this.estimatedCost = child.getEstimatedCost() + this.estimatedRows * (1.0 + table.getIndexes().size() * 0.5);
    }

    @Override
    public void init() {
        long start = System.nanoTime();
        this.consumed = false;
        this.actualRows = 0L;
        this.executed = true;
        child.init();
        this.elapsedNanos = System.nanoTime() - start;
    }

    @Override
    public Tuple next() {
        if (consumed) {
            return null;
        }
        consumed = true;
        long start = System.nanoTime();
        try {
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
                this.actualRows = 0L;
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

            LogManager lm = heap.getBufferPoolManager().getLogManager();
            boolean managedByTxn = lm != null && lm.getThreadTransactionContext() != null;

            if (!managedByTxn) {
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
                        for (RecordId matchRid : tree.search(newKey)) {
                            if (!targetRidSet.contains(matchRid)) {
                                throw new DuplicateKeyException(
                                        "Duplicate key '" + newKey + "' in UPDATE violates unique index '"
                                                + idx.indexName() + "' on column '" + idx.columnName() + "'");
                            }
                        }
                    }
                }
            }

            // Apply updates to heap and check unique index constraints as each row is updated
            List<RecordId> updatedRids = new ArrayList<>(targets.size());
            for (int i = 0; i < targets.size(); i++) {
                Tuple oldTuple = targets.get(i);
                Tuple updatedTuple = newTuples.get(i);
                RecordId newRid = heap.update(oldTuple.getRecordId(), updatedTuple);
                updatedRids.add(newRid);

                if (managedByTxn) {
                    for (IndexMetadata idx : indexes) {
                        if (!idx.unique()) {
                            continue;
                        }
                        int colIdx = schema.getColumnIndex(idx.columnName());
                        Value newKey = updatedTuple.getValue(colIdx);
                        for (int prev = 0; prev < i; prev++) {
                            if (newTuples.get(prev).getValue(colIdx).equals(newKey)) {
                                throw new DuplicateKeyException(
                                        "Duplicate key '" + newKey + "' in UPDATE violates unique index '"
                                                + idx.indexName() + "' on column '" + idx.columnName() + "'");
                            }
                        }
                        BPlusTree tree = table.getIndexTree(idx);
                        for (RecordId matchRid : tree.search(newKey)) {
                            if (!targetRidSet.contains(matchRid)) {
                                throw new DuplicateKeyException(
                                        "Duplicate key '" + newKey + "' in UPDATE violates unique index '"
                                                + idx.indexName() + "' on column '" + idx.columnName() + "'");
                            }
                        }
                    }
                }
            }

            for (int i = 0; i < targets.size(); i++) {
                Tuple oldTuple = targets.get(i);
                RecordId oldRid = oldTuple.getRecordId();
                for (IndexMetadata idx : indexes) {
                    int colIdx = schema.getColumnIndex(idx.columnName());
                    BPlusTree tree = table.getIndexTree(idx);
                    tree.delete(oldTuple.getValue(colIdx), oldRid);
                }
            }

            for (int i = 0; i < targets.size(); i++) {
                Tuple updatedTuple = newTuples.get(i);
                RecordId newRid = updatedRids.get(i);
                for (IndexMetadata idx : indexes) {
                    int colIdx = schema.getColumnIndex(idx.columnName());
                    BPlusTree tree = table.getIndexTree(idx);
                    tree.insert(updatedTuple.getValue(colIdx), newRid);
                }
            }

            this.actualRows = targets.size();
            return new Tuple(Value.ofInt(targets.size()));
        } finally {
            elapsedNanos += System.nanoTime() - start;
        }
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
        return prefix + "Update(table=" + table.getName() + ")" + formatMetrics()
                + "\n" + child.explain(indent + 1);
    }
}
