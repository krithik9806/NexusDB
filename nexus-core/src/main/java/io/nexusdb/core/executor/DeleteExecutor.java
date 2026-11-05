package io.nexusdb.core.executor;

import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.index.BPlusTree;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Physical {@code DELETE} operator that removes matching tuples from a {@link TableHeap},
 * deletes their corresponding entries from all table {@link BPlusTree} indexes, and emits
 * a single tuple containing the number of deleted rows.
 */
public final class DeleteExecutor implements Executor {
    private final TableMetadata table;
    private final Executor child;
    private boolean executed;

    public DeleteExecutor(TableMetadata table, Executor child) {
        this.table = Objects.requireNonNull(table, "table must not be null");
        this.child = Objects.requireNonNull(child, "child must not be null");
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

        List<Tuple> targetTuples = new ArrayList<>();
        try {
            Tuple t;
            while ((t = child.next()) != null) {
                if (t.getRecordId() == null) {
                    throw new IllegalStateException("Cannot delete tuple without RecordId");
                }
                targetTuples.add(t);
            }
        } finally {
            child.close();
        }

        Schema schema = table.getSchema();
        TableHeap heap = table.getTableHeap();
        List<IndexMetadata> indexes = table.getIndexes();
        int deletedCount = 0;

        for (Tuple tuple : targetTuples) {
            RecordId rid = tuple.getRecordId();
            if (heap.delete(rid)) {
                for (IndexMetadata idx : indexes) {
                    int colIdx = schema.getColumnIndex(idx.columnName());
                    BPlusTree tree = table.getIndexTree(idx);
                    tree.delete(tuple.getValue(colIdx), rid);
                }
                deletedCount++;
            }
        }

        return new Tuple(Value.ofInt(deletedCount));
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
        return prefix + "Delete(table=" + table.getName() + ")\n" + child.explain(indent + 1);
    }
}
