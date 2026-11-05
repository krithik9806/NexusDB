package io.nexusdb.core.executor;

import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Physical {@code DELETE} operator that removes matching tuples from a {@link TableHeap}
 * and emits a single tuple containing the number of deleted rows.
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

        List<RecordId> targetRids = new ArrayList<>();
        try {
            Tuple t;
            while ((t = child.next()) != null) {
                if (t.getRecordId() == null) {
                    throw new IllegalStateException("Cannot delete tuple without RecordId");
                }
                targetRids.add(t.getRecordId());
            }
        } finally {
            child.close();
        }

        TableHeap heap = table.getTableHeap();
        int deletedCount = 0;
        for (RecordId rid : targetRids) {
            if (heap.delete(rid)) {
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
