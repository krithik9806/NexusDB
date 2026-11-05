package io.nexusdb.core.page;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory wrapper around a fixed-size page {@link ByteBuffer} with a reader-writer latch
 * and WAL {@code pageLSN} accessors at byte offset 0..7.
 *
 * <p>Concurrency Invariant: Callers must acquire {@link #rlatch()} before reading page contents
 * and {@link #wlatch()} before mutating page contents. Page latches are held only for the duration
 * of in-memory page operations and never across blocking lock requests.
 */
public class Page {
    public static final int LSN_OFFSET = 0;

    private final int pageId;
    private final ByteBuffer buffer;
    private final ReentrantReadWriteLock latch = new ReentrantReadWriteLock();

    public Page(int pageId, int pageSize) {
        this(pageId, ByteBuffer.allocate(pageSize));
    }

    public Page(int pageId, byte[] bytes) {
        this(pageId, ByteBuffer.wrap(bytes));
    }

    public Page(int pageId, ByteBuffer buffer) {
        this.pageId = pageId;
        this.buffer = buffer.order(ByteOrder.BIG_ENDIAN);
    }

    public int getPageId() {
        return pageId;
    }

    public int getPageSize() {
        return buffer.capacity();
    }

    public ByteBuffer getBuffer() {
        return buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
    }

    public byte[] getData() {
        if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.capacity() == buffer.array().length) {
            return buffer.array();
        }
        byte[] copy = new byte[buffer.capacity()];
        ByteBuffer dup = getBuffer();
        dup.position(0);
        dup.get(copy);
        return copy;
    }

    public long getPageLsn() {
        return buffer.getLong(LSN_OFFSET);
    }

    public void setPageLsn(long lsn) {
        buffer.putLong(LSN_OFFSET, lsn);
    }

    public void rlatch() {
        latch.readLock().lock();
    }

    public void runlatch() {
        latch.readLock().unlock();
    }

    public void wlatch() {
        latch.writeLock().lock();
    }

    public void wunlatch() {
        latch.writeLock().unlock();
    }

    public ReentrantReadWriteLock getLatch() {
        return latch;
    }
}
