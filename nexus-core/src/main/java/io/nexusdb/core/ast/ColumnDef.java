package io.nexusdb.core.ast;

import io.nexusdb.core.record.DataType;

import java.util.Objects;

/**
 * AST node representing a column definition inside a {@code CREATE TABLE} statement.
 */
public record ColumnDef(
        String name,
        DataType dataType,
        int maxLength,
        boolean nullable,
        boolean primaryKey) {

    public ColumnDef {
        Objects.requireNonNull(name, "name must not be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Column name must not be blank");
        }
        Objects.requireNonNull(dataType, "dataType must not be null");
        if (dataType == DataType.VARCHAR) {
            if (maxLength <= 0 || maxLength > Short.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "VARCHAR length must be between 1 and " + Short.MAX_VALUE + ", got: " + maxLength);
            }
        } else {
            maxLength = dataType.getFixedSize();
        }
        if (primaryKey) {
            nullable = false;
        }
    }
}
