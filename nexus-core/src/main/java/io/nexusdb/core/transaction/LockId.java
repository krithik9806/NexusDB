package io.nexusdb.core.transaction;

import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.record.Value;

import java.util.Locale;
import java.util.Objects;

/**
 * Identifies a lockable resource at table, physical row ({@link RecordId}), or logical index-key
 * granularity inside {@link LockManager}.
 */
public record LockId(
        Granularity granularity,
        String tableName,
        RecordId recordId,
        String indexName,
        Value keyValue) {

    public enum Granularity {
        TABLE,
        ROW,
        KEY
    }

    public LockId {
        Objects.requireNonNull(granularity, "granularity must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        tableName = tableName.toLowerCase(Locale.ROOT);
        indexName = indexName == null ? "" : indexName.toLowerCase(Locale.ROOT);
        if (granularity == Granularity.ROW) {
            Objects.requireNonNull(recordId, "recordId must not be null for ROW lock");
        } else if (granularity == Granularity.KEY) {
            Objects.requireNonNull(keyValue, "keyValue must not be null for KEY lock");
        }
    }

    public static LockId table(String tableName) {
        return new LockId(Granularity.TABLE, tableName, null, "", null);
    }

    public static LockId row(String tableName, RecordId recordId) {
        return new LockId(Granularity.ROW, tableName, recordId, "", null);
    }

    public static LockId key(String tableName, String indexName, Value keyValue) {
        return new LockId(Granularity.KEY, tableName, null, indexName, keyValue);
    }

    @Override
    public String toString() {
        return switch (granularity) {
            case TABLE -> "LockId[TABLE:" + tableName + "]";
            case ROW -> "LockId[ROW:" + tableName + "@" + recordId + "]";
            case KEY -> "LockId[KEY:" + tableName + "." + indexName + "=" + keyValue + "]";
        };
    }
}
