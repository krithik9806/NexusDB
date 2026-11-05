package io.nexusdb.core.executor;

import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.TableIterator;
import io.nexusdb.core.record.Tuple;

import java.util.Objects;

/**
 * Physical sequential scan operator iterating over all live tuples in a {@link TableHeap}.
 */
public final class SeqScanExecutor implements Executor {
    private final String tableName;
    private final TableHeap tableHeap;
    private TableIterator iterator;

    public SeqScanExecutor(TableMetadata tableMetadata) {
        Objects.requireNonNull(tableMetadata, "tableMetadata must not be null");
        this.tableName = tableMetadata.getName();
        this.tableHeap = tableMetadata.getTableHeap();
    }

    public SeqScanExecutor(String tableName, TableHeap tableHeap) {
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        this.tableHeap = Objects.requireNonNull(tableHeap, "tableHeap must not be null");
    }

    @Override
    public void init() {
        if (iterator != null) {
            iterator.close();
        }
        iterator = tableHeap.scan();
    }

    @Override
    public Tuple next() {
        if (iterator == null) {
            throw new IllegalStateException("SeqScanExecutor.init() must be called before next()");
        }
        if (!iterator.hasNext()) {
            return null;
        }
        return iterator.next();
    }

    @Override
    public void close() {
        if (iterator != null) {
            iterator.close();
            iterator = null;
        }
    }

    @Override
    public Schema getOutputSchema() {
        return tableHeap.getSchema();
    }

    @Override
    public String explain(int indent) {
        return "  ".repeat(Math.max(0, indent)) + "SeqScan(table=" + tableName + ")";
    }
}
