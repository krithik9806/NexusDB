package io.nexusdb.core.buffer;

import io.nexusdb.core.page.Page;
import io.nexusdb.core.storage.DiskManager;
import io.nexusdb.core.storage.PageAllocator;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * Fixed-capacity page cache mediating between execution/index layers and {@link DiskManager}.
 *
 * <p>Concurrency & Crash-Safety Invariants:
 * <ul>
 *   <li>Page table, frame descriptors, free-frame list, and {@link LruReplacer} state transitions
 *       are protected by {@code poolLock}.</li>
 *   <li>Individual page contents are protected by per-page {@link Page#rlatch()} and {@link Page#wlatch()}
 *       reader-writer latches. Callers must never acquire {@code poolLock} while waiting on a blocking
 *       transaction lock; page latches are held only for short in-memory operations.</li>
 *   <li>Pinned pages ({@code pinCount > 0}) are excluded from {@link LruReplacer} and can never be evicted.</li>
 *   <li>Before writing any dirty page to disk (on eviction or explicit flush), {@code walFlushHook}
 *       (when registered) is invoked with {@link Page#getPageLsn()} to enforce the Write-Ahead Logging rule.</li>
 * </ul>
 */
public final class BufferPoolManager implements AutoCloseable {
    private final int poolSize;
    private final DiskManager diskManager;
    private final PageAllocator pageAllocator;
    private final LruReplacer replacer;
    private final Frame[] frames;
    private final Deque<Integer> freeFrames;
    private final Map<Integer, Integer> pageTable;
    private final Object poolLock = new Object();

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();
    private volatile LongConsumer walFlushHook;

    private static final class Frame {
        Page page;
        int pinCount;
        boolean dirty;

        void reset() {
            this.page = null;
            this.pinCount = 0;
            this.dirty = false;
        }
    }

    public BufferPoolManager(int poolSize, DiskManager diskManager) {
        this(poolSize, diskManager, new PageAllocator(diskManager));
    }

    public BufferPoolManager(int poolSize, DiskManager diskManager, PageAllocator pageAllocator) {
        if (poolSize <= 0) {
            throw new IllegalArgumentException("poolSize must be > 0, got: " + poolSize);
        }
        this.poolSize = poolSize;
        this.diskManager = Objects.requireNonNull(diskManager, "diskManager must not be null");
        this.pageAllocator = Objects.requireNonNull(pageAllocator, "pageAllocator must not be null");
        this.replacer = new LruReplacer(poolSize);
        this.frames = new Frame[poolSize];
        this.freeFrames = new ArrayDeque<>(poolSize);
        this.pageTable = new HashMap<>(poolSize * 2);

        for (int i = 0; i < poolSize; i++) {
            frames[i] = new Frame();
            freeFrames.addLast(i);
        }
    }

    /**
     * Registers a WAL flush callback (e.g. {@code walManager::flushUpTo}) invoked with {@code pageLsn}
     * before any dirty page is written to disk.
     */
    public void setWalFlushHook(LongConsumer walFlushHook) {
        this.walFlushHook = walFlushHook;
    }

    /**
     * Fetches the requested page into the buffer pool and increments its pin count.
     *
     * @throws BufferPoolException if the page is not cached and all frames are currently pinned
     */
    public Page fetchPage(int pageId) {
        if (pageId <= 0) {
            throw new IllegalArgumentException("Cannot fetch pageId <= 0 into buffer pool: " + pageId);
        }
        synchronized (poolLock) {
            Integer existingFrameId = pageTable.get(pageId);
            if (existingFrameId != null) {
                Frame frame = frames[existingFrameId];
                frame.pinCount++;
                replacer.pin(existingFrameId);
                hits.incrementAndGet();
                return frame.page;
            }

            misses.incrementAndGet();
            int frameId = allocateFrameLocked();
            byte[] pageData = diskManager.readPage(pageId);
            Page page = new Page(pageId, pageData);

            Frame frame = frames[frameId];
            frame.page = page;
            frame.pinCount = 1;
            frame.dirty = false;
            replacer.pin(frameId);
            pageTable.put(pageId, frameId);
            return page;
        }
    }

    /**
     * Allocates a new page via {@link PageAllocator}, loads it into a buffer frame with {@code pinCount = 1}
     * and {@code dirty = true}, and returns the pinned {@link Page}.
     *
     * @throws BufferPoolException if all frames in the buffer pool are currently pinned
     */
    public Page newPage() {
        synchronized (poolLock) {
            int frameId = allocateFrameLocked();
            int pageId = pageAllocator.allocatePage();
            Page page = new Page(pageId, diskManager.getPageSize());

            Frame frame = frames[frameId];
            frame.page = page;
            frame.pinCount = 1;
            frame.dirty = true;
            replacer.pin(frameId);
            pageTable.put(pageId, frameId);
            return page;
        }
    }

    /**
     * Decrements the pin count of {@code pageId} and records whether the caller dirtied the page.
     *
     * @return {@code true} if unpinned, {@code false} if the page is not in the pool or its pinCount is already 0
     */
    public boolean unpinPage(int pageId, boolean isDirty) {
        synchronized (poolLock) {
            Integer frameId = pageTable.get(pageId);
            if (frameId == null) {
                return false;
            }
            Frame frame = frames[frameId];
            if (frame.pinCount <= 0) {
                return false;
            }
            if (isDirty) {
                frame.dirty = true;
            }
            frame.pinCount--;
            if (frame.pinCount == 0) {
                replacer.unpin(frameId);
            }
            return true;
        }
    }

    /**
     * Flushes a specific page to disk regardless of its pin state, clearing its dirty flag.
     *
     * @return {@code true} if the page was found in the pool and flushed, {@code false} otherwise
     */
    public boolean flushPage(int pageId) {
        synchronized (poolLock) {
            Integer frameId = pageTable.get(pageId);
            if (frameId == null) {
                return false;
            }
            Frame frame = frames[frameId];
            writeBackLocked(frame);
            return true;
        }
    }

    /**
     * Flushes all dirty pages in the buffer pool to disk and forces an fsync on {@link DiskManager}.
     */
    public void flushAll() {
        synchronized (poolLock) {
            for (Frame frame : frames) {
                if (frame.page != null && frame.dirty) {
                    writeBackLocked(frame);
                }
            }
            diskManager.fsync();
        }
    }

    /**
     * Deletes a page from the buffer pool and returns it to the {@link PageAllocator} free list.
     *
     * @throws BufferPoolException if the page is currently pinned ({@code pinCount > 0})
     */
    public boolean deletePage(int pageId) {
        synchronized (poolLock) {
            Integer frameId = pageTable.get(pageId);
            if (frameId != null) {
                Frame frame = frames[frameId];
                if (frame.pinCount > 0) {
                    throw new BufferPoolException(
                            "Cannot delete page " + pageId + " while pinned (pinCount=" + frame.pinCount + ")");
                }
                replacer.pin(frameId);
                pageTable.remove(pageId);
                frame.reset();
                freeFrames.addLast(frameId);
            }
            pageAllocator.deallocatePage(pageId);
            return true;
        }
    }

    private int allocateFrameLocked() {
        if (!freeFrames.isEmpty()) {
            return freeFrames.removeFirst();
        }
        int victimFrameId = replacer.victim();
        if (victimFrameId < 0) {
            throw new BufferPoolException(
                    "Buffer pool exhausted: all " + poolSize + " frames are currently pinned");
        }
        Frame victim = frames[victimFrameId];
        if (victim.dirty) {
            writeBackLocked(victim);
        }
        pageTable.remove(victim.page.getPageId());
        victim.reset();
        evictions.incrementAndGet();
        return victimFrameId;
    }

    private void writeBackLocked(Frame frame) {
        Page page = frame.page;
        LongConsumer hook = this.walFlushHook;
        if (hook != null) {
            long pageLsn = page.getPageLsn();
            if (pageLsn > 0) {
                hook.accept(pageLsn);
            }
        }
        diskManager.writePage(page.getPageId(), page.getData());
        frame.dirty = false;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public DiskManager getDiskManager() {
        return diskManager;
    }

    public PageAllocator getPageAllocator() {
        return pageAllocator;
    }

    public int getPinCount(int pageId) {
        synchronized (poolLock) {
            Integer frameId = pageTable.get(pageId);
            return frameId == null ? 0 : frames[frameId].pinCount;
        }
    }

    public boolean isDirty(int pageId) {
        synchronized (poolLock) {
            Integer frameId = pageTable.get(pageId);
            return frameId != null && frames[frameId].dirty;
        }
    }

    public long getHits() {
        return hits.get();
    }

    public long getMisses() {
        return misses.get();
    }

    public long getEvictions() {
        return evictions.get();
    }

    public double getHitRatio() {
        long h = hits.get();
        long total = h + misses.get();
        return total == 0 ? 0.0 : (double) h / total;
    }

    @Override
    public void close() {
        flushAll();
    }
}
