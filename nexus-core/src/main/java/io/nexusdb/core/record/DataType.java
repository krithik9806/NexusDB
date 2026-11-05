package io.nexusdb.core.record;

/**
 * Supported relational column data types in NexusDB.
 */
public enum DataType {
    INT(4),
    BIGINT(8),
    DOUBLE(8),
    BOOLEAN(1),
    VARCHAR(-1);

    private final int fixedSize;

    DataType(int fixedSize) {
        this.fixedSize = fixedSize;
    }

    public boolean isFixedLength() {
        return fixedSize > 0;
    }

    public int getFixedSize() {
        return fixedSize;
    }
}
