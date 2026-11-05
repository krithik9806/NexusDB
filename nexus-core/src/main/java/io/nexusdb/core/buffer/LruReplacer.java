package io.nexusdb.core.buffer;

import java.util.Iterator;
import java.util.LinkedHashSet;

/**
 * Tracks unpinned buffer pool frame IDs in Least-Recently-Used (LRU) eviction order.
 *
 * <p>Concurrency Invariant: All operations are synchronized on {@code this} (and typically
 * invoked while holding the {@link BufferPoolManager} lock), providing $O(1)$ {@link #pin(int)},
 * {@link #unpin(int)}, and {@link #victim()} selection.
 */
public final class LruReplacer {
    private final int capacity;
    private final LinkedHashSet<Integer> unpinnedFrames;

    public LruReplacer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be > 0, got: " + capacity);
        }
        this.capacity = capacity;
        this.unpinnedFrames = new LinkedHashSet<>(capacity);
    }

    /**
     * Marks a frame as pinned, removing it from eviction candidates.
     */
    public synchronized void pin(int frameId) {
        unpinnedFrames.remove(frameId);
    }

    /**
     * Marks a frame as unpinned and eligible for eviction at the MRU tail.
     */
    public synchronized void unpin(int frameId) {
        if (unpinnedFrames.contains(frameId)) {
            return;
        }
        if (unpinnedFrames.size() >= capacity) {
            throw new IllegalStateException("LruReplacer capacity exceeded: " + capacity);
        }
        unpinnedFrames.add(frameId);
    }

    /**
     * Removes and returns the least-recently-unpinned frame ID, or {@code -1} if no frame is evictable.
     */
    public synchronized int victim() {
        Iterator<Integer> it = unpinnedFrames.iterator();
        if (!it.hasNext()) {
            return -1;
        }
        int frameId = it.next();
        it.remove();
        return frameId;
    }

    public synchronized int size() {
        return unpinnedFrames.size();
    }
}
