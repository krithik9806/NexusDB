package io.nexusdb.core.record;

import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.page.Page;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.page.SlottedPage;

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
 * <p>Concurrency & Pinning Invariants:
 * <ul>
 *   <li>At most one buffer pool frame is pinned at any instant during {@link #insert}, {@link #get},
 *       {@link #update}, and {@link #delete}, preventing self-deadlock even on a 1-frame buffer pool.</li>
 *   <li>Every accessed page is protected with {@link Page#rlatch()} or {@link Page#wlatch()} for the
 *       duration of the in-memory page access and unlatched before unpinning.</li>
 * </ul>
 */
public final class TableHeap implements Iterable<Tuple> {
    private final BufferPoolManager bufferPoolManager;
    private final Schema schema;
    private final int firstPageId;
    private int lastPageId;
    private final Object heapLock = new Object();

    /**
     * Creates a new {@link TableHeap} by allocating and initializing its first slotted page.
     */
    public static TableHeap create(BufferPoolManager bufferPoolManager, Schema schema) {
        Objects.requireNonNull(bufferPoolManager, "bufferPoolManager must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        Page firstPage = bufferPoolManager.newPage();
        int firstId = firstPage.getPageId();
        firstPage.wlatch();
        try {
            SlottedPage sp = new SlottedPage(firstPage, true);
            sp.setPrevPageId(-1);
            sp.setNextPageId(-1);
        } finally {
            firstPage.wunlatch();
            bufferPoolManager.unpinPage(firstId, true);
        }
        return new TableHeap(bufferPoolManager, schema, firstId, firstId);
    }

    /**
     * Opens an existing {@link TableHeap} rooted at {@code firstPageId}, walking the page chain
     * to discover {@code lastPageId}.
     */
    public static TableHeap open(BufferPoolManager bufferPoolManager, Schema schema, int firstPageId) {
        Objects.requireNonNull(bufferPoolManager, "bufferPoolManager must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        if (firstPageId <= 0) {
            throw new IllegalArgumentException("firstPageId must be > 0, got: " + firstPageId);
        }
        int curr = firstPageId;
        int tail = firstPageId;
        while (curr != -1) {
            tail = curr;
            Page page = bufferPoolManager.fetchPage(curr);
            page.rlatch();
            try {
                SlottedPage sp = SlottedPage.wrap(page);
                curr = sp.getNextPageId();
            } finally {
                page.runlatch();
                bufferPoolManager.unpinPage(tail, false);
            }
        }
        return new TableHeap(bufferPoolManager, schema, firstPageId, tail);
    }

    private TableHeap(BufferPoolManager bufferPoolManager, Schema schema, int firstPageId, int lastPageId) {
        this.bufferPoolManager = bufferPoolManager;
        this.schema = schema;
        this.firstPageId = firstPageId;
        this.lastPageId = lastPageId;
    }

    public int getFirstPageId() {
        return firstPageId;
    }

    public int getLastPageId() {
        synchronized (heapLock) {
            return lastPageId;
        }
    }

    public Schema getSchema() {
        return schema;
    }

    public BufferPoolManager getBufferPoolManager() {
        return bufferPoolManager;
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
                    RecordId rid = new RecordId(lastPageId, slotId);
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
                assignedSlot = newSp.insert(bytes);
                if (assignedSlot < 0) {
                    throw new IllegalStateException("Newly initialized page failed to insert tuple of length " + bytes.length);
                }
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
            } finally {
                prevPage.wunlatch();
                bufferPoolManager.unpinPage(oldLastPageId, true);
            }

            this.lastPageId = newPageId;
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
                updatedInPage = sp.update(recordId.slotId(), newBytes);
                if (!updatedInPage) {
                    // Delete from current page before relocating
                    sp.delete(recordId.slotId());
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
                    deleted = sp.delete(recordId.slotId());
                }
            } finally {
                page.wunlatch();
                bufferPoolManager.unpinPage(pageId, deleted);
            }
            return deleted;
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
