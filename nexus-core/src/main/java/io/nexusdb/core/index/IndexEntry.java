package io.nexusdb.core.index;

import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.record.Value;

import java.util.Objects;

/**
 * Immutable B+Tree leaf entry mapping an indexed {@link Value} key to a tuple's {@link RecordId}.
 */
public record IndexEntry(Value key, RecordId recordId) implements Comparable<IndexEntry> {
    public IndexEntry {
        Objects.requireNonNull(key, "key must not be null (use Value.NULL for SQL NULL)");
        Objects.requireNonNull(recordId, "recordId must not be null");
    }

    public static int compare(Value k1, RecordId r1, Value k2, RecordId r2, boolean unique) {
        int cmp = k1.compareTo(k2);
        if (cmp != 0 || unique) {
            return cmp;
        }
        return r1.compareTo(r2);
    }

    @Override
    public int compareTo(IndexEntry other) {
        int cmp = this.key.compareTo(other.key);
        return cmp != 0 ? cmp : this.recordId.compareTo(other.recordId);
    }
}
