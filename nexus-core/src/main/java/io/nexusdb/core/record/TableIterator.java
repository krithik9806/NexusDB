package io.nexusdb.core.record;

import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.page.Page;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.page.SlottedPage;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Forward sequential scan iterator over a {@link TableHeap}'s doubly-linked slotted page chain.
 *
 * <p>Pinning Invariant: Each page is fetched, read-latched, inspected, unlatched, and unpinned
 * within {@link #advance()}, so the iterator holds zero buffer pool pins between {@link #next()} calls.
 */
public final class TableIterator implements Iterator<Tuple>, AutoCloseable {
    private final BufferPoolManager bufferPoolManager;
    private final Schema schema;
    private int currentPageId;
    private int nextSlotId;
    private Tuple nextTuple;
    private boolean closed;

    public TableIterator(BufferPoolManager bufferPoolManager, Schema schema, int firstPageId) {
        this.bufferPoolManager = Objects.requireNonNull(bufferPoolManager, "bufferPoolManager must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        this.currentPageId = firstPageId;
        this.nextSlotId = 0;
        advance();
    }

    private void advance() {
        nextTuple = null;
        while (!closed && currentPageId != -1 && nextTuple == null) {
            int pageId = currentPageId;
            Page page = bufferPoolManager.fetchPage(pageId);
            page.rlatch();
            try {
                SlottedPage sp = SlottedPage.wrap(page);
                int slotCount = sp.getSlotCount();
                while (nextSlotId < slotCount) {
                    int slot = nextSlotId++;
                    if (sp.isValidSlot(slot)) {
                        byte[] raw = sp.get(slot);
                        if (raw != null) {
                            RecordId rid = new RecordId(pageId, slot);
                            nextTuple = Tuple.fromBytes(schema, raw, rid);
                            return;
                        }
                    }
                }
                currentPageId = sp.getNextPageId();
                nextSlotId = 0;
            } finally {
                page.runlatch();
                bufferPoolManager.unpinPage(pageId, false);
            }
        }
    }

    @Override
    public boolean hasNext() {
        return !closed && nextTuple != null;
    }

    @Override
    public Tuple next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more tuples in TableIterator");
        }
        Tuple result = nextTuple;
        advance();
        return result;
    }

    @Override
    public void close() {
        closed = true;
        nextTuple = null;
    }
}
