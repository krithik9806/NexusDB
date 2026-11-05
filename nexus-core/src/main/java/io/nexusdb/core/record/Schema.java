package io.nexusdb.core.record;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable relational table schema consisting of an ordered list of {@link Column}s
 * with $O(1)$ case-insensitive column lookup.
 */
public final class Schema {
    private final List<Column> columns;
    private final Map<String, Integer> nameToIndex;
    private final int primaryKeyIndex;

    public Schema(List<Column> columns) {
        Objects.requireNonNull(columns, "columns must not be null");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Schema must contain at least one column");
        }
        this.columns = List.copyOf(columns);
        Map<String, Integer> map = new HashMap<>(columns.size() * 2);
        int pkIdx = -1;
        for (int i = 0; i < this.columns.size(); i++) {
            Column col = this.columns.get(i);
            String normalized = col.name().toLowerCase(Locale.ROOT);
            if (map.put(normalized, i) != null) {
                throw new IllegalArgumentException("Duplicate column name in schema: " + col.name());
            }
            if (col.primaryKey()) {
                if (pkIdx != -1) {
                    throw new IllegalArgumentException("Multiple primary key columns are not supported");
                }
                pkIdx = i;
            }
        }
        this.nameToIndex = Collections.unmodifiableMap(map);
        this.primaryKeyIndex = pkIdx;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public int getColumnCount() {
        return columns.size();
    }

    public Column getColumn(int index) {
        if (index < 0 || index >= columns.size()) {
            throw new IndexOutOfBoundsException("Column index out of bounds: " + index);
        }
        return columns.get(index);
    }

    public int getColumnIndex(String name) {
        Objects.requireNonNull(name, "Column name must not be null");
        Integer idx = nameToIndex.get(name.toLowerCase(Locale.ROOT));
        return idx == null ? -1 : idx;
    }

    public Column getColumn(String name) {
        int idx = getColumnIndex(name);
        if (idx < 0) {
            throw new IllegalArgumentException("Unknown column: " + name);
        }
        return columns.get(idx);
    }

    public boolean hasColumn(String name) {
        return getColumnIndex(name) >= 0;
    }

    public int getPrimaryKeyIndex() {
        return primaryKeyIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Schema that)) {
            return false;
        }
        return columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        return columns.hashCode();
    }

    @Override
    public String toString() {
        return "Schema" + columns;
    }
}
