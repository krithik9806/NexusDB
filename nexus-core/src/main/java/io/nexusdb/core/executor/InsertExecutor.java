package io.nexusdb.core.executor;

import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.planner.BindException;
import io.nexusdb.core.planner.BoundExpr;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Physical {@code INSERT} operator that evaluates bound row expressions, validates runtime
 * constraints, inserts tuples into the target {@link TableHeap}, and emits a single tuple
 * containing the number of inserted rows.
 */
public final class InsertExecutor implements Executor {
    public static final Schema DML_OUTPUT_SCHEMA = new Schema(List.of(
            new Column("rows_affected", DataType.INT, false)
    ));

    private final TableMetadata table;
    private final List<List<BoundExpr>> rows;
    private boolean executed;

    public InsertExecutor(TableMetadata table, List<List<BoundExpr>> rows) {
        this.table = Objects.requireNonNull(table, "table must not be null");
        this.rows = rows.stream().map(List::copyOf).toList();
    }

    @Override
    public void init() {
        this.executed = false;
    }

    @Override
    public Tuple next() {
        if (executed) {
            return null;
        }
        executed = true;

        Schema schema = table.getSchema();
        TableHeap heap = table.getTableHeap();
        int count = 0;

        for (List<BoundExpr> rowExprs : rows) {
            List<Value> values = new ArrayList<>(schema.getColumnCount());
            for (int colIdx = 0; colIdx < schema.getColumnCount(); colIdx++) {
                Column col = schema.getColumn(colIdx);
                Value val = ExpressionEvaluator.evaluate(rowExprs.get(colIdx), null);
                val = validateAndCoerceColumnValue(val, col);
                values.add(val);
            }
            heap.insert(new Tuple(values));
            count++;
        }

        return new Tuple(Value.ofInt(count));
    }

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
    public String explain(int indent) {
        return "  ".repeat(Math.max(0, indent))
                + "Insert(table=" + table.getName() + ", rows=" + rows.size() + ")";
    }
}
