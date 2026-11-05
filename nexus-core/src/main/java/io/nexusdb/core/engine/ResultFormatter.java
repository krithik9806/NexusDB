package io.nexusdb.core.engine;

import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;

import java.util.List;
import java.util.Objects;

/**
 * Formats query {@link ResultSet} columns and rows into a clean ASCII box table.
 */
public final class ResultFormatter {

    private ResultFormatter() {
    }

    public static String formatTable(List<Column> columns, List<Tuple> rows) {
        Objects.requireNonNull(columns, "columns must not be null");
        Objects.requireNonNull(rows, "rows must not be null");
        if (columns.isEmpty()) {
            return "";
        }

        int colCount = columns.size();
        int[] widths = new int[colCount];
        String[] headers = new String[colCount];
        for (int c = 0; c < colCount; c++) {
            headers[c] = columns.get(c).name();
            widths[c] = headers[c].length();
        }

        String[][] cells = new String[rows.size()][colCount];
        for (int r = 0; r < rows.size(); r++) {
            Tuple tuple = rows.get(r);
            for (int c = 0; c < colCount; c++) {
                String formatted = formatValue(tuple.getValue(c));
                cells[r][c] = formatted;
                if (formatted.length() > widths[c]) {
                    widths[c] = formatted.length();
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        String separator = buildSeparator(widths);
        sb.append(separator).append('\n');
        sb.append(buildRow(headers, widths)).append('\n');
        sb.append(separator).append('\n');
        for (String[] row : cells) {
            sb.append(buildRow(row, widths)).append('\n');
        }
        sb.append(separator).append('\n');
        sb.append('(').append(rows.size()).append(rows.size() == 1 ? " row)" : " rows)");
        return sb.toString();
    }

    public static String formatValue(Value value) {
        if (value == null || value.isNull()) {
            return "NULL";
        }
        return switch (value) {
            case Value.IntValue(int v) -> Integer.toString(v);
            case Value.BigIntValue(long v) -> Long.toString(v);
            case Value.DoubleValue(double v) -> Double.toString(v);
            case Value.BooleanValue(boolean v) -> Boolean.toString(v);
            case Value.VarcharValue(String s) -> s;
            case Value.NullValue ignored -> "NULL";
        };
    }

    private static String buildSeparator(int[] widths) {
        StringBuilder sb = new StringBuilder("+");
        for (int w : widths) {
            sb.append("-".repeat(w + 2)).append('+');
        }
        return sb.toString();
    }

    private static String buildRow(String[] values, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < widths.length; i++) {
            String val = values[i];
            sb.append(' ').append(val).append(" ".repeat(widths[i] - val.length() + 1)).append('|');
        }
        return sb.toString();
    }
}
