package io.nexusdb.core.wal;

/**
 * Types of records stored in the NexusDB Write-Ahead Log (WAL).
 */
public enum LogRecordType {
    BEGIN,
    COMMIT,
    ABORT,
    UPDATE,
    INSERT,
    DELETE,
    CLR,
    CHECKPOINT_BEGIN,
    CHECKPOINT_END;

    private static final LogRecordType[] VALUES = values();

    public static LogRecordType fromByte(int ordinal) {
        if (ordinal < 0 || ordinal >= VALUES.length) {
            throw new IllegalArgumentException("Invalid LogRecordType ordinal: " + ordinal);
        }
        return VALUES[ordinal];
    }
}
