package io.nexusdb.core.catalog;

import java.util.Objects;

/**
 * Persistent metadata describing a secondary or primary-key B+Tree index on a table column.
 */
public record IndexMetadata(
        String indexName,
        String tableName,
        String columnName,
        int rootPageId,
        boolean unique) {

    public IndexMetadata {
        Objects.requireNonNull(indexName, "indexName must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        Objects.requireNonNull(columnName, "columnName must not be null");
        Catalog.validateIdentifier(indexName, "index name");
        Catalog.validateIdentifier(tableName, "table name");
        Catalog.validateIdentifier(columnName, "column name");
        if (rootPageId <= 0) {
            throw new IllegalArgumentException("rootPageId must be > 0, got: " + rootPageId);
        }
    }
}
