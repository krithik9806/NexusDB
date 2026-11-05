package io.nexusdb.core.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Manages page allocation and deallocation on top of {@link DiskManager} using a persistent
 * LIFO free-page list stored directly inside freed pages and rooted at Page 0's {@code freeListHead}.
 *
 * <p>On-disk Free Page Format (Big-Endian):
 * <ul>
 *   <li>Bytes {@code 0..3}: {@link #FREE_PAGE_MARKER} ({@code 0xFFFFFFFF})</li>
 *   <li>Bytes {@code 4..7}: {@code nextFreePageId} ({@code int}, {@code -1} if end of list)</li>
 * </ul>
 *
 * <p>Concurrency & Crash-Safety Invariants:
 * <ul>
 *   <li>All allocation and deallocation mutations are serialized on {@code lock}.</li>
 *   <li>On deallocation, the freed page's {@code FREE_PAGE_MARKER} and {@code nextFreePageId} pointer
 *       are written to disk <em>before</em> Page 0's {@code freeListHead} is updated, preventing
 *       dangling free-list head pointers on crash.</li>
 *   <li>On allocation from the free list, Page 0's {@code freeListHead} is advanced to {@code nextFreePageId}
 *       before the recycled page is zeroed and handed out, ensuring a crash never leaves an allocated page
 *       reachable from the free list.</li>
 * </ul>
 */
public final class PageAllocator {
    public static final int FREE_PAGE_MARKER = 0xFFFFFFFF;
    public static final int MARKER_OFFSET = 0;
    public static final int NEXT_FREE_OFFSET = 4;

    private final DiskManager diskManager;
    private final Set<Integer> freePages = new HashSet<>();
    private final Object lock = new Object();

    public PageAllocator(DiskManager diskManager) {
        this.diskManager = Objects.requireNonNull(diskManager, "diskManager must not be null");
        rebuildFreeSetFromDisk();
    }

    private void rebuildFreeSetFromDisk() {
        synchronized (lock) {
            freePages.clear();
            int current = diskManager.getFreeListHead();
            int pageCount = diskManager.getPageCount();
            ByteBuffer buf = ByteBuffer.allocate(diskManager.getPageSize()).order(ByteOrder.BIG_ENDIAN);

            while (current != -1) {
                if (current <= 0 || current >= pageCount) {
                    throw new StorageException("Corrupted free list: invalid pageId " + current);
                }
                if (!freePages.add(current)) {
                    throw new StorageException("Corrupted free list: cycle detected at pageId " + current);
                }
                buf.clear();
                diskManager.readPage(current, buf);
                int marker = buf.getInt(MARKER_OFFSET);
                if (marker != FREE_PAGE_MARKER) {
                    throw new StorageException(String.format(
                            "Corrupted free list: page %d missing free marker (got 0x%08X)", current, marker));
                }
                current = buf.getInt(NEXT_FREE_OFFSET);
            }
        }
    }

    /**
     * Allocates a zeroed page, reusing the most recently freed page (LIFO) if available,
     * or extending the database file via {@link DiskManager#allocatePage()} otherwise.
     */
    public int allocatePage() {
        synchronized (lock) {
            int head = diskManager.getFreeListHead();
            if (head != -1) {
                int pageSize = diskManager.getPageSize();
                ByteBuffer buf = ByteBuffer.allocate(pageSize).order(ByteOrder.BIG_ENDIAN);
                diskManager.readPage(head, buf);

                int marker = buf.getInt(MARKER_OFFSET);
                if (marker != FREE_PAGE_MARKER) {
                    throw new StorageException(String.format(
                            "Corrupted free page %d: expected marker 0x%08X, got 0x%08X",
                            head, FREE_PAGE_MARKER, marker));
                }
                int nextFree = buf.getInt(NEXT_FREE_OFFSET);

                // Advance freeListHead first, then zero the recycled page on disk
                diskManager.setFreeListHead(nextFree);
                freePages.remove(head);

                byte[] zeroPage = new byte[pageSize];
                diskManager.writePage(head, zeroPage);
                return head;
            }
            return diskManager.allocatePage();
        }
    }

    public int allocate() {
        return allocatePage();
    }

    /**
     * Deallocates the given {@code pageId} and pushes it onto the persistent LIFO free list.
     *
     * @throws IllegalArgumentException if {@code pageId <= 0}, out of bounds, or already free
     */
    public void deallocatePage(int pageId) {
        synchronized (lock) {
            if (pageId == 0) {
                throw new IllegalArgumentException("Cannot deallocate header page 0");
            }
            int pageCount = diskManager.getPageCount();
            if (pageId < 0 || pageId >= pageCount) {
                throw new IllegalArgumentException(
                        "Cannot deallocate out-of-range pageId " + pageId + " (pageCount=" + pageCount + ")");
            }
            if (freePages.contains(pageId)) {
                throw new IllegalArgumentException("Page " + pageId + " is already deallocated");
            }

            int currentHead = diskManager.getFreeListHead();
            ByteBuffer freeBuf = ByteBuffer.allocate(diskManager.getPageSize()).order(ByteOrder.BIG_ENDIAN);
            freeBuf.putInt(MARKER_OFFSET, FREE_PAGE_MARKER);
            freeBuf.putInt(NEXT_FREE_OFFSET, currentHead);

            // Persist free page link before updating header's freeListHead
            diskManager.writePage(pageId, freeBuf);
            diskManager.setFreeListHead(pageId);
            freePages.add(pageId);
        }
    }

    public void deallocate(int pageId) {
        deallocatePage(pageId);
    }

    public boolean isFree(int pageId) {
        synchronized (lock) {
            return freePages.contains(pageId);
        }
    }

    public int getFreePageCount() {
        synchronized (lock) {
            return freePages.size();
        }
    }

    public DiskManager getDiskManager() {
        return diskManager;
    }
}
