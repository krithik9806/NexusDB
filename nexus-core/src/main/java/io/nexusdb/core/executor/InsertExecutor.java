package io.nexusdb.core.executor;

import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.index.BPlusTree;
import io.nexusdb.core.index.DuplicateKeyException;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.planner.BindException;
import io.nexusdb.core.planner.BoundExpr;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import io.nexusdb.core.wal.LogManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Physical {@code INSERT} operator that evaluates bound row expressions, validates runtime
 * and unique-index constraints, inserts tuples into the target {@link TableHeap}, maintains
 * all registered {@link BPlusTree} indexes, and emits a single tuple containing the number
 * of inserted rows.
 */
public final class InsertExecutor implements Executor {
    public static final Schema DML_OUTPUT_SCHEMA = new Schema(List.of(
            new Column("rows_affected", DataType.INT, false)
    ));

    private final TableMetadata table;
    private final List<List<BoundExpr>> rows;
    private boolean consumed;

    private double estimatedCost;
    private long estimatedRows;
    private long actualRows;
    private long elapsedNanos;
    private boolean executed;

    public InsertExecutor(TableMetadata table, List<List<BoundExpr>> rows) {
        this.table = Objects.requireNonNull(table, "table must not be null");
        this.rows = rows.stream().map(List::copyOf).toList();
        this.estimatedRows = this.rows.size();
        this.estimatedCost = this.rows.size() * (1.0 + table.getIndexes().size() * 0.5);
    }

    @Override
    public void init() {
        this.consumed = false;
        this.actualRows = 0L;
        this.elapsedNanos = 0L;
        this.executed = true;
    }

    @Override
    public Tuple next() {
        if (consumed) {
            return null;
        }
        consumed = true;
        long start = System.nanoTime();
        try {
            Schema schema = table.getSchema();
            TableHeap heap = table.getTableHeap();
            List<IndexMetadata> indexes = table.getIndexes();
            List<Tuple> tuplesToInsert = new ArrayList<>(rows.size());

            for (List<BoundExpr> rowExprs : rows) {
                List<Value> values = new ArrayList<>(schema.getColumnCount());
                for (int colIdx = 0; colIdx < schema.getColumnCount(); colIdx++) {
                    Column col = schema.getColumn(colIdx);
                    Value val = ExpressionEvaluator.evaluate(rowExprs.get(colIdx), null);
                    val = validateAndCoerceColumnValue(val, col);
                    values.add(val);
                }
                tuplesToInsert.add(new Tuple(values));
            }

            LogManager lm = heap.getBufferPoolManager().getLogManager();
            boolean managedByTxn = lm != null && lm.getThreadTransactionContext() != null;

            List<InsertedRowRecord> localInserted = managedByTxn ? null : new ArrayList<>(tuplesToInsert.size());
            int count = 0;
            try {
                for (Tuple tuple : tuplesToInsert) {
                    RecordId rid = heap.insert(tuple);
                    int idxPos = 0;
                    try {
                        for (; idxPos < indexes.size(); idxPos++) {
                            IndexMetadata idx = indexes.get(idxPos);
                            int colIdx = schema.getColumnIndex(idx.columnName());
                            BPlusTree tree = table.getIndexTree(idx);
                            tree.insert(tuple.getValue(colIdx), rid);
                        }
                    } catch (DuplicateKeyException dup) {
                        // Remove from any indexes on the current row that already succeeded before this index failed
                        for (int j = 0; j < idxPos; j++) {
                            IndexMetadata prevIdx = indexes.get(j);
                            int prevColIdx = schema.getColumnIndex(prevIdx.columnName());
                            table.getIndexTree(prevIdx).delete(tuple.getValue(prevColIdx), rid);
                        }
                        if (!managedByTxn) {
                            heap.delete(rid);
                        }
                        throw dup;
                    }
                    if (localInserted != null) {
                        localInserted.add(new InsertedRowRecord(rid, tuple));
                    }
                    count++;
                }
            } catch (RuntimeException e) {
                if (localInserted != null) {
                    for (int i = localInserted.size() - 1; i >= 0; i--) {
                        InsertedRowRecord rec = localInserted.get(i);
                        heap.delete(rec.rid());
                        for (IndexMetadata idx : indexes) {
                            int colIdx = schema.getColumnIndex(idx.columnName());
                            table.getIndexTree(idx).delete(rec.tuple().getValue(colIdx), rec.rid());
                        }
                    }
                }
                throw e;
            }

            this.actualRows = count;
            return new Tuple(Value.ofInt(count));
        } finally {
            elapsedNanos += System.nanoTime() - start;
        }
    }

    private record InsertedRowRecord(RecordId rid, Tuple tuple) {}

    static Value validateAndCoerceColumnValue(Value val, Column col) {
        if (val == null || val.isNull()) {
            if (!col.nullable()) {
                throw new BindException("Column '" + col.name() + "' is NOT NULL");
            }
            return Value.NULL;
        }
        Value coerced = ExpressionEvaluator.coerceValue(val, col.type());
        if (col.type() == DataType.VARCHAR && coerced instanceof Value.VarcharValue(String s)) {
            if (s.length() > col.maxLength()) {
                throw new BindException(
                        "VARCHAR value length " + s.length() + " exceeds maximum length "
                                + col.maxLength() + " for column '" + col.name() + "'");
            }
        }
        return coerced;
    }

    @Override
    public void close() {
    }

    @Override
    public Schema getOutputSchema() {
        return DML_OUTPUT_SCHEMA;
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
                + "Insert(table=" + table.getName() + ", rows=" + rows.size() + ")"
                + formatMetrics();
    }
}
