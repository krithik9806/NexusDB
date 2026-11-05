package io.nexusdb.core.catalog;

import io.nexusdb.core.index.BPlusTree;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Catalog descriptor for a table, binding its logical {@link Schema}, physical {@link TableHeap},
 * registered {@link IndexMetadata} definitions, and opened {@link BPlusTree} index handles.
 */
public final class TableMetadata {
    private final String name;
    private final Schema schema;
    private final int firstPageId;
    private final TableHeap tableHeap;
    private final List<IndexMetadata> indexes;
    private final RecordId catalogRecordId;
    private final Map<String, BPlusTree> indexTrees;

    public TableMetadata(
            String name,
            Schema schema,
            int firstPageId,
            TableHeap tableHeap,
            List<IndexMetadata> indexes,
            RecordId catalogRecordId) {
        this(name, schema, firstPageId, tableHeap, indexes, catalogRecordId, new ConcurrentHashMap<>());
    }

    TableMetadata(
            String name,
            Schema schema,
            int firstPageId,
            TableHeap tableHeap,
            List<IndexMetadata> indexes,
            RecordId catalogRecordId,
            Map<String, BPlusTree> indexTrees) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        if (firstPageId <= 0) {
            throw new IllegalArgumentException("firstPageId must be > 0, got: " + firstPageId);
        }
        this.firstPageId = firstPageId;
        this.tableHeap = Objects.requireNonNull(tableHeap, "tableHeap must not be null");
        this.indexes = List.copyOf(Objects.requireNonNull(indexes, "indexes must not be null"));
        this.catalogRecordId = catalogRecordId;
        this.indexTrees = Objects.requireNonNull(indexTrees, "indexTrees must not be null");
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

    public IndexMetadata getIndex(String indexName) {
        Objects.requireNonNull(indexName, "indexName must not be null");
        for (IndexMetadata idx : indexes) {
            if (idx.indexName().equalsIgnoreCase(indexName)) {
                return idx;
            }
        }
        return null;
    }

    public List<IndexMetadata> getIndexesForColumn(String columnName) {
        Objects.requireNonNull(columnName, "columnName must not be null");
        return indexes.stream()
                .filter(idx -> idx.columnName().equalsIgnoreCase(columnName))
                .toList();
    }

    /**
     * Returns (lazily opening if necessary) the {@link BPlusTree} backing {@code indexMeta}.
     */
    public BPlusTree getIndexTree(IndexMetadata indexMeta) {
        Objects.requireNonNull(indexMeta, "indexMeta must not be null");
        String normName = indexMeta.indexName().toLowerCase(Locale.ROOT);
        return indexTrees.computeIfAbsent(normName, ignored -> {
            Column col = schema.getColumn(indexMeta.columnName());
            return BPlusTree.open(
                    tableHeap.getBufferPoolManager(),
                    indexMeta.rootPageId(),
                    col.type(),
                    indexMeta.unique());
        });
    }

    public BPlusTree getIndexTree(String indexName) {
        IndexMetadata meta = getIndex(indexName);
        if (meta == null) {
            throw new IllegalArgumentException("Unknown index '" + indexName + "' on table '" + name + "'");
        }
        return getIndexTree(meta);
    }

    void putIndexTree(String indexName, BPlusTree tree) {
        indexTrees.put(indexName.toLowerCase(Locale.ROOT), tree);
    }

    void removeIndexTree(String indexName) {
        indexTrees.remove(indexName.toLowerCase(Locale.ROOT));
    }

    TableMetadata withCatalogRecordId(RecordId newRecordId) {
        return new TableMetadata(name, schema, firstPageId, tableHeap, indexes, newRecordId, indexTrees);
    }

    TableMetadata withIndexes(List<IndexMetadata> newIndexes, RecordId newRecordId) {
        return new TableMetadata(name, schema, firstPageId, tableHeap, newIndexes, newRecordId, indexTrees);
    }
}
