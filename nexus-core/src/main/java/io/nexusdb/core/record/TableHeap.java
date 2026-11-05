package io.nexusdb.core.record;

import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.page.Page;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.page.SlottedPage;
import io.nexusdb.core.wal.LogManager;
import io.nexusdb.core.wal.LogRecord;

import java.util.Iterator;
import java.util.Objects;

/**
 * Manages a doubly-linked chain of {@link SlottedPage}s storing serialized {@link Tuple}s
 * via a {@link BufferPoolManager}.
 *
 * <p>Relocation Behavior on Update:
 * {@link #update(RecordId, Tuple)} first attempts an in-page update (in-place if the serialized
 * tuple does not grow, or via in-page compaction if sufficient free space exists on the same page),
 * preserving the original {@link RecordId}. If the updated tuple grows beyond the current page's
 * free capacity, the old slot is deleted and the new tuple is inserted into a page with sufficient
 * space, returning a new relocated {@link RecordId}. Any secondary indexes referencing the row must
 * be updated when the returned {@link RecordId} differs from the input {@link RecordId}.
 *
 * <p>Concurrency, Pinning & WAL Invariants:
 * <ul>
 *   <li>At most one buffer pool frame is pinned at any instant during {@link #insert}, {@link #get},
 *       {@link #update}, and {@link #delete}, preventing self-deadlock even on a 1-frame buffer pool.</li>
 *   <li>Every accessed page is protected with {@link Page#rlatch()} or {@link Page#wlatch()} for the
 *       duration of the in-memory page access and unlatched before unpinning.</li>
 *   <li>Every page mutation appends a WAL record to {@link LogManager} (when attached to
 *       {@link BufferPoolManager}), stamps {@link Page#setPageLsn(long)}, and updates the buffer pool's
 *       Dirty Page Table before unpinning.</li>
 * </ul>
 */
public final class TableHeap implements Iterable<Tuple> {
    private final BufferPoolManager bufferPoolManager;
    private final Schema schema;
    private final String tableName;
    private final int firstPageId;
    private int lastPageId;
    private long liveRowCount;
    private int pageCount;
    private final Object heapLock = new Object();

    /**
     * Creates a new {@link TableHeap} by allocating and initializing its first slotted page.
     */
    public static TableHeap create(BufferPoolManager bufferPoolManager, Schema schema) {
        return create(bufferPoolManager, schema, "");
    }

    public static TableHeap create(BufferPoolManager bufferPoolManager, Schema schema, String tableName) {
        Objects.requireNonNull(bufferPoolManager, "bufferPoolManager must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        Page firstPage = bufferPoolManager.newPage();
        int firstId = firstPage.getPageId();
        firstPage.wlatch();
        try {
            SlottedPage sp = new SlottedPage(firstPage, true);
            sp.setPrevPageId(-1);
            sp.setNextPageId(-1);
            bufferPoolManager.logPageWrite(firstPage);
        } finally {
            firstPage.wunlatch();
            bufferPoolManager.unpinPage(firstId, true);
        }
        return new TableHeap(bufferPoolManager, schema, tableName == null ? "" : tableName, firstId, firstId, 0L, 1);
    }

    /**
     * Opens an existing {@link TableHeap} rooted at {@code firstPageId}, walking the page chain
     * to discover {@code lastPageId}, {@code pageCount}, and {@code liveRowCount}.
     */
    public static TableHeap open(BufferPoolManager bufferPoolManager, Schema schema, int firstPageId) {
        return open(bufferPoolManager, schema, firstPageId, "");
    }

    public static TableHeap open(
            BufferPoolManager bufferPoolManager,
            Schema schema,
            int firstPageId,
            String tableName) {
        Objects.requireNonNull(bufferPoolManager, "bufferPoolManager must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        if (firstPageId <= 0) {
            throw new IllegalArgumentException("firstPageId must be > 0, got: " + firstPageId);
        }
        int curr = firstPageId;
        int tail = firstPageId;
        long rows = 0L;
        int pages = 0;
        while (curr != -1) {
            tail = curr;
            pages++;
            Page page = bufferPoolManager.fetchPage(curr);
            page.rlatch();
            try {
                SlottedPage sp = SlottedPage.wrap(page);
                int slotCount = sp.getSlotCount();
                for (int slotId = 0; slotId < slotCount; slotId++) {
                    if (sp.isValidSlot(slotId)) {
                        rows++;
                    }
                }
                curr = sp.getNextPageId();
            } finally {
                page.runlatch();
                bufferPoolManager.unpinPage(tail, false);
            }
        }
        return new TableHeap(
                bufferPoolManager,
                schema,
                tableName == null ? "" : tableName,
                firstPageId,
                tail,
                rows,
                pages);
    }

    /**
     * Opens an existing {@link TableHeap} with known metadata without scanning its page chain,
     * preserving a completely cold {@link BufferPoolManager}.
     */
    public static TableHeap openWithoutScan(
            BufferPoolManager bufferPoolManager,
            Schema schema,
            int firstPageId,
            int lastPageId,
            long liveRowCount,
            int pageCount) {
        Objects.requireNonNull(bufferPoolManager, "bufferPoolManager must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        if (firstPageId <= 0 || lastPageId <= 0) {
            throw new IllegalArgumentException("Page IDs must be > 0");
        }
        return new TableHeap(bufferPoolManager, schema, "", firstPageId, lastPageId, liveRowCount, pageCount);
    }

    private TableHeap(
            BufferPoolManager bufferPoolManager,
            Schema schema,
            String tableName,
            int firstPageId,
            int lastPageId,
            long liveRowCount,
            int pageCount) {
        this.bufferPoolManager = bufferPoolManager;
        this.schema = schema;
        this.tableName = tableName == null ? "" : tableName;
        this.firstPageId = firstPageId;
        this.lastPageId = lastPageId;
        this.liveRowCount = liveRowCount;
        this.pageCount = pageCount;
    }

    public String getTableName() {
        return tableName;
    }

    public int getFirstPageId() {
        return firstPageId;
    }

    public int getLastPageId() {
        synchronized (heapLock) {
            return lastPageId;
        }
    }

    public long getLiveRowCount() {
        synchronized (heapLock) {
            return liveRowCount;
        }
    }

    public int getPageCount() {
        synchronized (heapLock) {
            return pageCount;
        }
    }

    public Schema getSchema() {
        return schema;
    }

    public BufferPoolManager getBufferPoolManager() {
        return bufferPoolManager;
    }

    /**
     * Re-scans the heap page chain to refresh {@code lastPageId}, {@code pageCount}, and {@code liveRowCount}.
     */
    public void refreshMetadata() {
        synchronized (heapLock) {
            int curr = firstPageId;
            int tail = firstPageId;
            long rows = 0L;
            int pages = 0;
            while (curr != -1) {
                tail = curr;
                pages++;
                Page page = bufferPoolManager.fetchPage(curr);
                page.rlatch();
                try {
                    SlottedPage sp = SlottedPage.wrap(page);
                    int slotCount = sp.getSlotCount();
                    for (int slotId = 0; slotId < slotCount; slotId++) {
                        if (sp.isValidSlot(slotId)) {
                            rows++;
                        }
                    }
                    curr = sp.getNextPageId();
                } finally {
                    page.runlatch();
                    bufferPoolManager.unpinPage(tail, false);
                }
            }
            this.lastPageId = tail;
            this.liveRowCount = rows;
            this.pageCount = pages;
        }
    }

    private void validateTupleFitsPage(byte[] bytes) {
        int maxPayload = bufferPoolManager.getDiskManager().getPageSize()
                - SlottedPage.HEADER_SIZE
                - SlottedPage.SLOT_SIZE;
        if (bytes.length > maxPayload) {
            throw new IllegalArgumentException(
                    "Serialized tuple length " + bytes.length + " exceeds maximum page payload " + maxPayload);
        }
    }

    private void logTupleInsert(Page page, int slotId, byte[] afterBytes) {
        LogManager lm = bufferPoolManager.getLogManager();
        if (lm == null) {
            return;
        }
        LogManager.TransactionLogContext ctx = lm.getThreadTransactionContext();
        long txnId = ctx != null ? ctx.getTxnId() : 0L;
        long prevLsn = ctx != null ? ctx.getLastLsn() : 0L;
        LogRecord rec = lm.appendRecord(
                LogRecord.insert(0L, txnId, prevLsn, tableName, page.getPageId(), slotId, afterBytes));
        if (ctx != null) {
            ctx.onRecordLogged(rec);
        }
        bufferPoolManager.recordPageModification(page, rec.lsn());
    }

    private void logTupleDelete(Page page, int slotId, byte[] beforeBytes) {
        LogManager lm = bufferPoolManager.getLogManager();
        if (lm == null) {
            return;
        }
        LogManager.TransactionLogContext ctx = lm.getThreadTransactionContext();
        long txnId = ctx != null ? ctx.getTxnId() : 0L;
        long prevLsn = ctx != null ? ctx.getLastLsn() : 0L;
        LogRecord rec = lm.appendRecord(
                LogRecord.delete(0L, txnId, prevLsn, tableName, page.getPageId(), slotId, beforeBytes));
        if (ctx != null) {
            ctx.onRecordLogged(rec);
        }
        bufferPoolManager.recordPageModification(page, rec.lsn());
    }

    private void logTupleUpdate(Page page, int slotId, byte[] beforeBytes, byte[] afterBytes) {
        LogManager lm = bufferPoolManager.getLogManager();
        if (lm == null) {
            return;
        }
        LogManager.TransactionLogContext ctx = lm.getThreadTransactionContext();
        long txnId = ctx != null ? ctx.getTxnId() : 0L;
        long prevLsn = ctx != null ? ctx.getLastLsn() : 0L;
        LogRecord rec = lm.appendRecord(
                LogRecord.update(0L, txnId, prevLsn, tableName, page.getPageId(), slotId, beforeBytes, afterBytes));
        if (ctx != null) {
            ctx.onRecordLogged(rec);
        }
        bufferPoolManager.recordPageModification(page, rec.lsn());
    }

    /**
     * Serializes and inserts {@code tuple} into the table heap, allocating and linking a new page
     * at the tail of the heap if the current tail page cannot accommodate it.
     */
    public RecordId insert(Tuple tuple) {
        Objects.requireNonNull(tuple, "tuple must not be null");
        byte[] bytes = tuple.toBytes(schema);
        validateTupleFitsPage(bytes);

        synchronized (heapLock) {
            // Try inserting into the current tail page first
            Page tailPage = bufferPoolManager.fetchPage(lastPageId);
            tailPage.wlatch();
            try {
                SlottedPage sp = SlottedPage.wrap(tailPage);
                int slotId = sp.insert(bytes);
                if (slotId >= 0) {
                    logTupleInsert(tailPage, slotId, bytes);
                    RecordId rid = new RecordId(lastPageId, slotId);
                    liveRowCount++;
                    tailPage.wunlatch();
                    bufferPoolManager.unpinPage(lastPageId, true);
                    return rid;
                }
            } catch (RuntimeException e) {
                tailPage.wunlatch();
                bufferPoolManager.unpinPage(lastPageId, false);
                throw e;
            }
            tailPage.wunlatch();
            bufferPoolManager.unpinPage(lastPageId, false);

            // Tail page is full; allocate a new page while holding zero page pins
            int oldLastPageId = lastPageId;
            Page newRawPage = bufferPoolManager.newPage();
            int newPageId = newRawPage.getPageId();
            int assignedSlot;
            newRawPage.wlatch();
            try {
                SlottedPage newSp = new SlottedPage(newRawPage, true);
                newSp.setPrevPageId(oldLastPageId);
                newSp.setNextPageId(-1);
                // Log structural page header setup before tuple insert
                bufferPoolManager.logPageWrite(newRawPage);
                assignedSlot = newSp.insert(bytes);
                if (assignedSlot < 0) {
                    throw new IllegalStateException("Newly initialized page failed to insert tuple of length " + bytes.length);
                }
                logTupleInsert(newRawPage, assignedSlot, bytes);
            } finally {
                newRawPage.wunlatch();
                bufferPoolManager.unpinPage(newPageId, true);
            }

            // Link oldLastPage -> newPage
            Page prevPage = bufferPoolManager.fetchPage(oldLastPageId);
            prevPage.wlatch();
            try {
                SlottedPage prevSp = SlottedPage.wrap(prevPage);
                prevSp.setNextPageId(newPageId);
                bufferPoolManager.logPageWrite(prevPage);
            } finally {
                prevPage.wunlatch();
                bufferPoolManager.unpinPage(oldLastPageId, true);
            }

            this.lastPageId = newPageId;
            this.pageCount++;
            this.liveRowCount++;
            return new RecordId(newPageId, assignedSlot);
        }
    }

    /**
     * Reads and deserializes the tuple at {@code recordId}, or returns {@code null} if deleted.
     */
    public Tuple get(RecordId recordId) {
        Objects.requireNonNull(recordId, "recordId must not be null");
        int pageId = recordId.pageId();
        Page page = bufferPoolManager.fetchPage(pageId);
        page.rlatch();
        try {
            SlottedPage sp = SlottedPage.wrap(page);
            if (!sp.isValidSlot(recordId.slotId())) {
                return null;
            }
            byte[] raw = sp.get(recordId.slotId());
            return raw == null ? null : Tuple.fromBytes(schema, raw, recordId);
        } finally {
            page.runlatch();
            bufferPoolManager.unpinPage(pageId, false);
        }
    }

    /**
     * Updates the tuple at {@code recordId}. If the updated tuple fits in the same page (either
     * in-place or after in-page compaction), returns {@code recordId}. Otherwise deletes the
     * record from its old page and inserts it into the heap, returning the new {@link RecordId}.
     */
    public RecordId update(RecordId recordId, Tuple updatedTuple) {
        Objects.requireNonNull(recordId, "recordId must not be null");
        Objects.requireNonNull(updatedTuple, "updatedTuple must not be null");
        byte[] newBytes = updatedTuple.toBytes(schema);
        validateTupleFitsPage(newBytes);

        synchronized (heapLock) {
            int pageId = recordId.pageId();
            Page page = bufferPoolManager.fetchPage(pageId);
            page.wlatch();
            boolean updatedInPage;
            try {
                SlottedPage sp = SlottedPage.wrap(page);
                if (!sp.isValidSlot(recordId.slotId())) {
                    throw new IllegalArgumentException("Cannot update non-existent or deleted slot: " + recordId);
                }
                byte[] oldBytes = sp.get(recordId.slotId());
                updatedInPage = sp.update(recordId.slotId(), newBytes);
                if (updatedInPage) {
                    logTupleUpdate(page, recordId.slotId(), oldBytes, newBytes);
                } else {
                    // Delete from current page before relocating
                    sp.delete(recordId.slotId());
                    logTupleDelete(page, recordId.slotId(), oldBytes);
                }
            } catch (RuntimeException e) {
                page.wunlatch();
                bufferPoolManager.unpinPage(pageId, false);
                throw e;
            }
            page.wunlatch();
            bufferPoolManager.unpinPage(pageId, true);

            if (updatedInPage) {
                return recordId;
            }
            liveRowCount--;
            return insert(updatedTuple);
        }
    }

    /**
     * Deletes the tuple at {@code recordId}.
     *
     * @return {@code true} if the slot was live and deleted, {@code false} if already deleted or invalid
     */
    public boolean delete(RecordId recordId) {
        Objects.requireNonNull(recordId, "recordId must not be null");
        synchronized (heapLock) {
            int pageId = recordId.pageId();
            Page page = bufferPoolManager.fetchPage(pageId);
            page.wlatch();
            boolean deleted = false;
            try {
                SlottedPage sp = SlottedPage.wrap(page);
                if (sp.isValidSlot(recordId.slotId())) {
                    byte[] oldBytes = sp.get(recordId.slotId());
                    deleted = sp.delete(recordId.slotId());
                    if (deleted) {
                        logTupleDelete(page, recordId.slotId(), oldBytes);
                    }
                }
            } finally {
                page.wunlatch();
                bufferPoolManager.unpinPage(pageId, deleted);
            }
            if (deleted) {
                liveRowCount--;
            }
            return deleted;
        }
    }

    /**
     * Restores or overwrites {@code recordId} with {@code rawTupleBytes} during transaction Undo
     * or WAL Redo, stamping {@code clrOrRedoLsn} on the page.
     */
    public void restoreSlotForUndoOrRedo(RecordId recordId, byte[] rawTupleBytes, long clrOrRedoLsn) {
        Objects.requireNonNull(recordId, "recordId must not be null");
        Objects.requireNonNull(rawTupleBytes, "rawTupleBytes must not be null");
        synchronized (heapLock) {
            int pageId = recordId.pageId();
            Page page = bufferPoolManager.fetchPage(pageId);
            page.wlatch();
            boolean wasValid;
            try {
                SlottedPage sp = SlottedPage.wrap(page);
                wasValid = sp.isValidSlot(recordId.slotId());
                if (!sp.putSlot(recordId.slotId(), rawTupleBytes)) {
                    throw new IllegalStateException("Failed to restore slot " + recordId + " during undo/redo");
                }
                if (clrOrRedoLsn > 0L) {
                    bufferPoolManager.recordPageModification(page, clrOrRedoLsn);
                }
            } finally {
                page.wunlatch();
                bufferPoolManager.unpinPage(pageId, true);
            }
            if (!wasValid) {
                liveRowCount++;
            }
        }
    }

    /**
     * Deletes {@code recordId} during transaction Undo or WAL Redo, stamping {@code clrOrRedoLsn} on the page.
     */
    public void deleteSlotForUndoOrRedo(RecordId recordId, long clrOrRedoLsn) {
        Objects.requireNonNull(recordId, "recordId must not be null");
        synchronized (heapLock) {
            int pageId = recordId.pageId();
            Page page = bufferPoolManager.fetchPage(pageId);
            page.wlatch();
            boolean wasValid;
            try {
                SlottedPage sp = SlottedPage.wrap(page);
                wasValid = sp.isValidSlot(recordId.slotId());
                if (wasValid) {
                    sp.delete(recordId.slotId());
                }
                if (clrOrRedoLsn > 0L) {
                    bufferPoolManager.recordPageModification(page, clrOrRedoLsn);
                }
            } finally {
                page.wunlatch();
                bufferPoolManager.unpinPage(pageId, true);
            }
            if (wasValid) {
                liveRowCount--;
            }
        }
    }

    public TableIterator scan() {
        return new TableIterator(bufferPoolManager, schema, firstPageId);
    }

    @Override
    public Iterator<Tuple> iterator() {
        return scan();
    }
}
