package io.nexusdb.core.ast;

import java.util.Objects;

/**
 * Column assignment ({@code columnName = value}) in an {@code UPDATE ... SET} clause.
 */
public record Assignment(String columnName, Expr value) {
    public Assignment {
        Objects.requireNonNull(columnName, "columnName must not be null");
        if (columnName.isBlank()) {
            throw new IllegalArgumentException("columnName must not be blank");
        }
        Objects.requireNonNull(value, "value must not be null");
    }
}
