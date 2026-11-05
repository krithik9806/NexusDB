package io.nexusdb.core.catalog;

import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.page.Page;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.page.SlottedPage;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.storage.DiskManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Persistent system catalog storing table schemas, heap root page IDs, and index metadata
 * in a chain of slotted pages rooted at {@link DiskManager#getCatalogMetaPageId()}.
 *
 * <p>Concurrency & Crash-Safety Invariants:
 * <ul>
 *   <li>All DDL operations ({@link #createTable}, {@link #dropTable}, {@link #registerIndex},
 *       {@link #dropIndex}) are serialized on {@code catalogLock}.</li>
 *   <li>When initializing a brand-new database file, the root catalog page is formatted and unpinned
 *       before its page ID is recorded in Page 0 via {@link DiskManager#setCatalogMetaPageId(int)}.</li>
 *   <li>Each catalog page is pinned only for the duration of an individual read/write step and
 *       protected by its page reader/writer latch.</li>
 * </ul>
 */
public final class Catalog {
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]{0,127}$");
    private static final DataType[] DATA_TYPES = DataType.values();

    private final BufferPoolManager bufferPoolManager;
    private final int catalogFirstPageId;
    private int catalogLastPageId;
    private final Map<String, TableMetadata> tables = new LinkedHashMap<>();
    private final Object catalogLock = new Object();

    public Catalog(BufferPoolManager bufferPoolManager) {
        this.bufferPoolManager = Objects.requireNonNull(bufferPoolManager, "bufferPoolManager must not be null");
        DiskManager dm = bufferPoolManager.getDiskManager();
        int metaPageId = dm.getCatalogMetaPageId();
        if (metaPageId == -1) {
            Page rootPage = bufferPoolManager.newPage();
            int newPageId = rootPage.getPageId();
            rootPage.wlatch();
            try {
                SlottedPage sp = new SlottedPage(rootPage, true);
                sp.setPrevPageId(-1);
                sp.setNextPageId(-1);
            } finally {
                rootPage.wunlatch();
                bufferPoolManager.unpinPage(newPageId, true);
            }
            bufferPoolManager.flushPage(newPageId);
            dm.setCatalogMetaPageId(newPageId);
            this.catalogFirstPageId = newPageId;
            this.catalogLastPageId = newPageId;
        } else {
            this.catalogFirstPageId = metaPageId;
            this.catalogLastPageId = metaPageId;
            loadFromDisk();
        }
    }

    public static Catalog open(BufferPoolManager bufferPoolManager) {
        return new Catalog(bufferPoolManager);
    }

    public static void validateIdentifier(String identifier, String kind) {
        if (identifier == null || !IDENTIFIER_PATTERN.matcher(identifier).matches()) {
            throw new IllegalArgumentException(
                    "Invalid " + kind + " identifier: '" + identifier
                            + "' (must match [A-Za-z_][A-Za-z0-9_]{0,127})");
        }
    }

    private static String normalize(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    private void loadFromDisk() {
        synchronized (catalogLock) {
            tables.clear();
            int currPageId = catalogFirstPageId;
            int tailPageId = catalogFirstPageId;

            while (currPageId != -1) {
                tailPageId = currPageId;
                List< RawCatalogSlot > rawSlots = new ArrayList<>();
                int nextPageId;

                Page page = bufferPoolManager.fetchPage(currPageId);
                page.rlatch();
                try {
                    SlottedPage sp = SlottedPage.wrap(page);
                    int slotCount = sp.getSlotCount();
                    for (int slotId = 0; slotId < slotCount; slotId++) {
                        if (sp.isValidSlot(slotId)) {
                            byte[] bytes = sp.get(slotId);
                            if (bytes != null) {
                                rawSlots.add(new RawCatalogSlot(new RecordId(currPageId, slotId), bytes));
                            }
                        }
                    }
                    nextPageId = sp.getNextPageId();
                } finally {
                    page.runlatch();
                    bufferPoolManager.unpinPage(currPageId, false);
                }

                // Deserialize and open TableHeap outside catalog page pin so tiny buffer pools never starve
                for (RawCatalogSlot slot : rawSlots) {
                    TableMetadata meta = deserializeEntry(slot.bytes(), slot.recordId());
                    tables.put(normalize(meta.getName()), meta);
                }
                currPageId = nextPageId;
            }
            this.catalogLastPageId = tailPageId;
        }
    }

    private record RawCatalogSlot(RecordId recordId, byte[] bytes) {}

    /**
     * Creates a new persistent table with the given name and schema, allocating its initial
     * {@link TableHeap} page and recording the metadata in the system catalog.
     */
    public TableMetadata createTable(String name, Schema schema) {
        validateIdentifier(name, "table name");
        Objects.requireNonNull(schema, "schema must not be null");
        for (Column col : schema.getColumns()) {
            validateIdentifier(col.name(), "column name");
        }

        synchronized (catalogLock) {
            String key = normalize(name);
            if (tables.containsKey(key)) {
                throw new IllegalArgumentException("Table already exists: " + name);
            }

            TableHeap heap = TableHeap.create(bufferPoolManager, schema);
            TableMetadata provisional = new TableMetadata(
                    name, schema, heap.getFirstPageId(), heap, List.of(), null);
            byte[] encoded = serializeEntry(provisional);
            RecordId catalogRid = insertCatalogRecordLocked(encoded);
            TableMetadata persisted = provisional.withCatalogRecordId(catalogRid);
            tables.put(key, persisted);
            return persisted;
        }
    }

    /**
     * Drops the named table from the catalog and deallocates all pages belonging to its {@link TableHeap}.
     *
     * @return {@code true} if the table existed and was dropped, {@code false} if no such table exists
     */
    public boolean dropTable(String name) {
        Objects.requireNonNull(name, "name must not be null");
        synchronized (catalogLock) {
            String key = normalize(name);
            TableMetadata meta = tables.remove(key);
            if (meta == null) {
                return false;
            }
            deleteCatalogRecordLocked(meta.getCatalogRecordId());
            freeHeapPagesLocked(meta.getFirstPageId());
            return true;
        }
    }

    public TableMetadata getTable(String name) {
        Objects.requireNonNull(name, "name must not be null");
        synchronized (catalogLock) {
            return tables.get(normalize(name));
        }
    }

    public boolean hasTable(String name) {
        return getTable(name) != null;
    }

    public List<TableMetadata> listTables() {
        synchronized (catalogLock) {
            return List.copyOf(tables.values());
        }
    }

    public List<String> listTableNames() {
        synchronized (catalogLock) {
            List<String> names = new ArrayList<>(tables.size());
            for (TableMetadata meta : tables.values()) {
                names.add(meta.getName());
            }
            return List.copyOf(names);
        }
    }

    /**
     * Registers a new index on an existing table and persists the updated catalog entry.
     */
    public IndexMetadata registerIndex(
            String tableName,
            String indexName,
            String columnName,
            int rootPageId,
            boolean unique) {
        synchronized (catalogLock) {
            TableMetadata meta = getTable(tableName);
            if (meta == null) {
                throw new IllegalArgumentException("Unknown table: " + tableName);
            }
            if (!meta.getSchema().hasColumn(columnName)) {
                throw new IllegalArgumentException(
                        "Column '" + columnName + "' does not exist in table '" + meta.getName() + "'");
            }
            for (IndexMetadata existing : meta.getIndexes()) {
                if (existing.indexName().equalsIgnoreCase(indexName)) {
                    throw new IllegalArgumentException("Index already exists: " + indexName);
                }
            }

            Column canonicalCol = meta.getSchema().getColumn(columnName);
            IndexMetadata indexMeta = new IndexMetadata(
                    indexName, meta.getName(), canonicalCol.name(), rootPageId, unique);
            List<IndexMetadata> updatedIndexes = new ArrayList<>(meta.getIndexes());
            updatedIndexes.add(indexMeta);

            TableMetadata updatedMeta = updateCatalogMetadataLocked(meta, updatedIndexes);
            tables.put(normalize(meta.getName()), updatedMeta);
            return indexMeta;
        }
    }

    public boolean dropIndex(String tableName, String indexName) {
        Objects.requireNonNull(tableName, "tableName must not be null");
        Objects.requireNonNull(indexName, "indexName must not be null");
        synchronized (catalogLock) {
            TableMetadata meta = getTable(tableName);
            if (meta == null) {
                return false;
            }
            List<IndexMetadata> updatedIndexes = new ArrayList<>(meta.getIndexes());
            boolean removed = updatedIndexes.removeIf(idx -> idx.indexName().equalsIgnoreCase(indexName));
            if (!removed) {
                return false;
            }
            TableMetadata updatedMeta = updateCatalogMetadataLocked(meta, updatedIndexes);
            tables.put(normalize(meta.getName()), updatedMeta);
            return true;
        }
    }

    private TableMetadata updateCatalogMetadataLocked(TableMetadata oldMeta, List<IndexMetadata> newIndexes) {
        TableMetadata candidate = oldMeta.withIndexes(newIndexes, oldMeta.getCatalogRecordId());
        byte[] encoded = serializeEntry(candidate);
        RecordId oldRid = oldMeta.getCatalogRecordId();
        int pageId = oldRid.pageId();

        Page page = bufferPoolManager.fetchPage(pageId);
        page.wlatch();
        boolean updatedInPlace;
        try {
            SlottedPage sp = SlottedPage.wrap(page);
            updatedInPlace = sp.update(oldRid.slotId(), encoded);
            if (!updatedInPlace) {
                sp.delete(oldRid.slotId());
            }
        } finally {
            page.wunlatch();
            bufferPoolManager.unpinPage(pageId, true);
        }

        if (updatedInPlace) {
            return candidate;
        }
        RecordId newRid = insertCatalogRecordLocked(encoded);
        return candidate.withCatalogRecordId(newRid);
    }

    private RecordId insertCatalogRecordLocked(byte[] encoded) {
        int maxPayload = bufferPoolManager.getDiskManager().getPageSize()
                - SlottedPage.HEADER_SIZE
                - SlottedPage.SLOT_SIZE;
        if (encoded.length > maxPayload) {
            throw new IllegalArgumentException(
                    "Catalog entry size " + encoded.length + " exceeds single page capacity " + maxPayload);
        }

        Page tailPage = bufferPoolManager.fetchPage(catalogLastPageId);
        tailPage.wlatch();
        try {
            SlottedPage sp = SlottedPage.wrap(tailPage);
            int slotId = sp.insert(encoded);
            if (slotId >= 0) {
                RecordId rid = new RecordId(catalogLastPageId, slotId);
                tailPage.wunlatch();
                bufferPoolManager.unpinPage(catalogLastPageId, true);
                return rid;
            }
        } catch (RuntimeException e) {
            tailPage.wunlatch();
            bufferPoolManager.unpinPage(catalogLastPageId, false);
            throw e;
        }
        tailPage.wunlatch();
        bufferPoolManager.unpinPage(catalogLastPageId, false);

        int oldLastPageId = catalogLastPageId;
        Page newPage = bufferPoolManager.newPage();
        int newPageId = newPage.getPageId();
        int assignedSlot;
        newPage.wlatch();
        try {
            SlottedPage newSp = new SlottedPage(newPage, true);
            newSp.setPrevPageId(oldLastPageId);
            newSp.setNextPageId(-1);
            assignedSlot = newSp.insert(encoded);
        } finally {
            newPage.wunlatch();
            bufferPoolManager.unpinPage(newPageId, true);
        }

        Page prevPage = bufferPoolManager.fetchPage(oldLastPageId);
        prevPage.wlatch();
        try {
            SlottedPage prevSp = SlottedPage.wrap(prevPage);
            prevSp.setNextPageId(newPageId);
        } finally {
            prevPage.wunlatch();
            bufferPoolManager.unpinPage(oldLastPageId, true);
        }

        this.catalogLastPageId = newPageId;
        return new RecordId(newPageId, assignedSlot);
    }

    private void deleteCatalogRecordLocked(RecordId rid) {
        int pageId = rid.pageId();
        Page page = bufferPoolManager.fetchPage(pageId);
        page.wlatch();
        try {
            SlottedPage sp = SlottedPage.wrap(page);
            sp.delete(rid.slotId());
        } finally {
            page.wunlatch();
            bufferPoolManager.unpinPage(pageId, true);
        }
    }

    private void freeHeapPagesLocked(int firstPageId) {
        List<Integer> pageIds = new ArrayList<>();
        int curr = firstPageId;
        while (curr != -1) {
            pageIds.add(curr);
            Page page = bufferPoolManager.fetchPage(curr);
            page.rlatch();
            try {
                SlottedPage sp = SlottedPage.wrap(page);
                curr = sp.getNextPageId();
            } finally {
                page.runlatch();
                bufferPoolManager.unpinPage(page.getPageId(), false);
            }
        }
        for (int pid : pageIds) {
            bufferPoolManager.deletePage(pid);
        }
    }

    private byte[] serializeEntry(TableMetadata meta) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
            DataOutputStream out = new DataOutputStream(baos);

            writeString(out, meta.getName());
            out.writeInt(meta.getFirstPageId());

            List<Column> cols = meta.getSchema().getColumns();
            out.writeInt(cols.size());
            for (Column col : cols) {
                writeString(out, col.name());
                out.writeByte(col.type().ordinal());
                out.writeInt(col.maxLength());
                out.writeBoolean(col.nullable());
                out.writeBoolean(col.primaryKey());
            }

            List<IndexMetadata> indexes = meta.getIndexes();
            out.writeInt(indexes.size());
            for (IndexMetadata idx : indexes) {
                writeString(out, idx.indexName());
                writeString(out, idx.columnName());
                out.writeInt(idx.rootPageId());
                out.writeBoolean(idx.unique());
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected I/O error serializing catalog entry", e);
        }
    }

    private TableMetadata deserializeEntry(byte[] bytes, RecordId catalogRid) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            String tableName = readString(in);
            int firstHeapPageId = in.readInt();

            int colCount = in.readInt();
            List<Column> columns = new ArrayList<>(colCount);
            for (int i = 0; i < colCount; i++) {
                String colName = readString(in);
                int typeOrdinal = in.readUnsignedByte();
                DataType type = DATA_TYPES[typeOrdinal];
                int maxLength = in.readInt();
                boolean nullable = in.readBoolean();
                boolean primaryKey = in.readBoolean();
                columns.add(new Column(colName, type, maxLength, nullable, primaryKey));
            }
            Schema schema = new Schema(columns);

            int indexCount = in.readInt();
            List<IndexMetadata> indexes = new ArrayList<>(indexCount);
            for (int i = 0; i < indexCount; i++) {
                String indexName = readString(in);
                String columnName = readString(in);
                int rootPageId = in.readInt();
                boolean unique = in.readBoolean();
                indexes.add(new IndexMetadata(indexName, tableName, columnName, rootPageId, unique));
            }

            TableHeap heap = TableHeap.open(bufferPoolManager, schema, firstHeapPageId);
            return new TableMetadata(tableName, schema, firstHeapPageId, heap, indexes, catalogRid);
        } catch (IOException e) {
            throw new IllegalStateException("Corrupted catalog entry at " + catalogRid, e);
        }
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
        out.writeShort(utf8.length);
        out.write(utf8);
    }

    private static String readString(DataInputStream in) throws IOException {
        int len = in.readUnsignedShort();
        byte[] utf8 = in.readNBytes(len);
        return new String(utf8, StandardCharsets.UTF_8);
    }
}
