package io.nexusdb.core.ast;

import java.util.Objects;

/**
 * Table reference in a {@code FROM} clause with an optional table alias.
 */
public record TableRef(String tableName, String alias) {
    public TableRef {
        Objects.requireNonNull(tableName, "tableName must not be null");
        if (tableName.isBlank()) {
            throw new IllegalArgumentException("tableName must not be blank");
        }
    }

    public TableRef(String tableName) {
        this(tableName, null);
    }

    public String effectiveName() {
        return alias != null ? alias : tableName;
    }
}
