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

    private double estimatedCost;
    private long estimatedRows;
    private long actualRows;
    private long elapsedNanos;
    private boolean executed;

    public SeqScanExecutor(TableMetadata tableMetadata) {
        Objects.requireNonNull(tableMetadata, "tableMetadata must not be null");
        this.tableName = tableMetadata.getName();
        this.tableHeap = tableMetadata.getTableHeap();
        this.estimatedRows = tableHeap.getLiveRowCount();
        this.estimatedCost = tableHeap.getPageCount() * 1.0 + this.estimatedRows * 0.10;
    }

    public SeqScanExecutor(String tableName, TableHeap tableHeap) {
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        this.tableHeap = Objects.requireNonNull(tableHeap, "tableHeap must not be null");
        this.estimatedRows = tableHeap.getLiveRowCount();
        this.estimatedCost = tableHeap.getPageCount() * 1.0 + this.estimatedRows * 0.10;
    }

    @Override
    public void init() {
        long start = System.nanoTime();
        if (iterator != null) {
            iterator.close();
        }
        iterator = tableHeap.scan();
        this.actualRows = 0L;
        this.executed = true;
        this.elapsedNanos = System.nanoTime() - start;
    }

    @Override
    public Tuple next() {
        if (iterator == null) {
            throw new IllegalStateException("SeqScanExecutor.init() must be called before next()");
        }
        long start = System.nanoTime();
        try {
            if (!iterator.hasNext()) {
                return null;
            }
            Tuple tuple = iterator.next();
            if (tuple != null) {
                actualRows++;
            }
            return tuple;
        } finally {
            elapsedNanos += System.nanoTime() - start;
        }
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
        return "  ".repeat(Math.max(0, indent))
                + "SeqScan(table=" + tableName + ")"
                + formatMetrics();
    }
}
