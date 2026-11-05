package io.nexusdb.core.record;

import java.util.Objects;

/**
 * Metadata definition for a single table column.
 */
public record Column(
        String name,
        DataType type,
        int maxLength,
        boolean nullable,
        boolean primaryKey) {

    public Column {
        Objects.requireNonNull(name, "Column name must not be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Column name must not be blank");
        }
        Objects.requireNonNull(type, "Column type must not be null");
        if (type == DataType.VARCHAR) {
            if (maxLength <= 0 || maxLength > Short.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "VARCHAR maxLength must be in 1.." + Short.MAX_VALUE + ", got: " + maxLength);
            }
        } else {
            maxLength = type.getFixedSize();
        }
        if (primaryKey) {
            nullable = false;
        }
    }

    public Column(String name, DataType type) {
        this(name, type, type == DataType.VARCHAR ? 255 : type.getFixedSize(), true, false);
    }

    public Column(String name, DataType type, boolean nullable) {
        this(name, type, type == DataType.VARCHAR ? 255 : type.getFixedSize(), nullable, false);
    }

    public Column(String name, DataType type, int maxLength, boolean nullable) {
        this(name, type, maxLength, nullable, false);
    }

    public static Column varchar(String name, int maxLength, boolean nullable) {
        return new Column(name, DataType.VARCHAR, maxLength, nullable, false);
    }

    public static Column primaryKey(String name, DataType type) {
        return new Column(name, type, type == DataType.VARCHAR ? 255 : type.getFixedSize(), false, true);
    }

    public static Column primaryKeyVarchar(String name, int maxLength) {
        return new Column(name, DataType.VARCHAR, maxLength, false, true);
    }
}
