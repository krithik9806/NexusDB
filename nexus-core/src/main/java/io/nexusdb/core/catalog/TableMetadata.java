package io.nexusdb.core.catalog;

import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;

import java.util.List;
import java.util.Objects;

/**
 * Catalog descriptor for a table, binding its logical {@link Schema}, physical {@link TableHeap},
 * and registered {@link IndexMetadata} definitions.
 */
public final class TableMetadata {
    private final String name;
    private final Schema schema;
    private final int firstPageId;
    private final TableHeap tableHeap;
    private final List<IndexMetadata> indexes;
    private final RecordId catalogRecordId;

    public TableMetadata(
            String name,
            Schema schema,
            int firstPageId,
            TableHeap tableHeap,
            List<IndexMetadata> indexes,
            RecordId catalogRecordId) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        if (firstPageId <= 0) {
            throw new IllegalArgumentException("firstPageId must be > 0, got: " + firstPageId);
        }
        this.firstPageId = firstPageId;
        this.tableHeap = Objects.requireNonNull(tableHeap, "tableHeap must not be null");
        this.indexes = List.copyOf(Objects.requireNonNull(indexes, "indexes must not be null"));
        this.catalogRecordId = catalogRecordId;
    }

    public String getName() {
        return name;
    }

    public Schema getSchema() {
        return schema;
    }

    public int getFirstPageId() {
        return firstPageId;
    }

    public TableHeap getTableHeap() {
        return tableHeap;
    }

    public List<IndexMetadata> getIndexes() {
        return indexes;
    }

    public RecordId getCatalogRecordId() {
        return catalogRecordId;
    }

    TableMetadata withCatalogRecordId(RecordId newRecordId) {
        return new TableMetadata(name, schema, firstPageId, tableHeap, indexes, newRecordId);
    }

    TableMetadata withIndexes(List<IndexMetadata> newIndexes, RecordId newRecordId) {
        return new TableMetadata(name, schema, firstPageId, tableHeap, newIndexes, newRecordId);
    }
}
