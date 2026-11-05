package io.nexusdb.core.engine;

import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.Tuple;

import java.util.List;
import java.util.Objects;

/**
 * Immutable result of executing a SQL statement via {@link Database#execute(String)}.
 */
public record ResultSet(
        List<Column> columns,
        List<Tuple> rows,
        int rowsAffected,
        long executionTimeNanos,
        String statusMessage) {

    public ResultSet {
        columns = List.copyOf(Objects.requireNonNull(columns, "columns must not be null"));
        rows = List.copyOf(Objects.requireNonNull(rows, "rows must not be null"));
        Objects.requireNonNull(statusMessage, "statusMessage must not be null");
    }

    public ResultSet(List<Column> columns, List<Tuple> rows, int rowsAffected, long executionTimeNanos) {
        this(columns, rows, rowsAffected, executionTimeNanos, "");
    }

    public double executionTimeMs() {
        return executionTimeNanos / 1_000_000.0;
    }

    public boolean hasResultSet() {
        return !columns.isEmpty();
    }

    public String formatAsciiTable() {
        if (!columns.isEmpty()) {
            return ResultFormatter.formatTable(columns, rows);
        }
        return statusMessage;
    }
}
