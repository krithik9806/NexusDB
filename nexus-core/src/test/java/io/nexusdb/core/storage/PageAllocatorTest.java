package io.nexusdb.core.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class PageAllocatorTest {

    @TempDir
    Path tempDir;

    @Test
    void allocateFreeAndReopenPersistence() {
        Path dbFile = tempDir.resolve("allocator.db");

        try (DiskManager dm = DiskManager.open(dbFile)) {
            PageAllocator allocator = new PageAllocator(dm);

            int p1 = allocator.allocatePage();
            int p2 = allocator.allocatePage();
            int p3 = allocator.allocatePage();
            assertEquals(1, p1);
            assertEquals(2, p2);
            assertEquals(3, p3);
            assertEquals(4, dm.getPageCount());
            assertEquals(-1, dm.getFreeListHead());

            // Deallocate p1 then p2 (LIFO free list: head -> 2 -> 1 -> -1)
            allocator.deallocatePage(p1);
            allocator.deallocatePage(p2);
            assertEquals(2, dm.getFreeListHead());
            assertEquals(2, allocator.getFreePageCount());

            // Verify on-disk free page header of p2
            ByteBuffer buf = ByteBuffer.wrap(dm.readPage(p2)).order(ByteOrder.BIG_ENDIAN);
            assertEquals(PageAllocator.FREE_PAGE_MARKER, buf.getInt(PageAllocator.MARKER_OFFSET));
            assertEquals(p1, buf.getInt(PageAllocator.NEXT_FREE_OFFSET));
        }

        // Reopen and verify free list is recovered and LIFO order is preserved
        try (DiskManager dm = DiskManager.open(dbFile)) {
            PageAllocator allocator = new PageAllocator(dm);
            assertEquals(2, allocator.getFreePageCount());
            assertTrue(allocator.isFree(1));
            assertTrue(allocator.isFree(2));
            assertFalse(allocator.isFree(3));

            // Double deallocation after reopen must be rejected
            assertThrows(IllegalArgumentException.class, () -> allocator.deallocatePage(2));
            assertThrows(IllegalArgumentException.class, () -> allocator.deallocatePage(1));

            // Allocate pops 2 then 1 without growing file
            assertEquals(2, allocator.allocatePage());
            assertEquals(1, allocator.allocatePage());
            assertEquals(0, allocator.getFreePageCount());
            assertEquals(-1, dm.getFreeListHead());
            assertEquals(4, dm.getPageCount());

            // Next allocation extends the file with page 4
            assertEquals(4, allocator.allocatePage());
            assertEquals(5, dm.getPageCount());
        }
    }

    @Test
    void preventsDeallocatingPageZeroAndDoubleDeallocation() {
        Path dbFile = tempDir.resolve("guards.db");
        try (DiskManager dm = DiskManager.open(dbFile)) {
            PageAllocator allocator = new PageAllocator(dm);
            int p1 = allocator.allocatePage();

            assertThrows(IllegalArgumentException.class, () -> allocator.deallocatePage(0));
            assertThrows(IllegalArgumentException.class, () -> allocator.deallocatePage(-1));
            assertThrows(IllegalArgumentException.class, () -> allocator.deallocatePage(99));

            allocator.deallocatePage(p1);
            assertThrows(IllegalArgumentException.class, () -> allocator.deallocatePage(p1));
        }
    }

    @Test
    void tenThousandRandomOperationsMatchSetModel() {
        Path dbFile = tempDir.resolve("random_10k.db");
        Set<Integer> activePages = new HashSet<>();
        Random rng = new Random(42L);

        try (DiskManager dm = DiskManager.open(dbFile, 512)) {
            PageAllocator allocator = new PageAllocator(dm);

            for (int i = 0; i < 10_000; i++) {
                boolean doAllocate = activePages.isEmpty() || rng.nextInt(100) < 55;
                if (doAllocate) {
                    int pageId = allocator.allocatePage();
                    assertTrue(pageId > 0, "Allocated pageId must be > 0");
                    assertTrue(activePages.add(pageId), "Double allocation detected for page " + pageId);
                    assertFalse(allocator.isFree(pageId));

                    // Write non-zero data to ensure recycled pages are properly zeroed later
                    byte[] data = new byte[512];
                    ByteBuffer.wrap(data).putInt(0, pageId * 31 + i);
                    dm.writePage(pageId, data);
                } else {
                    List<Integer> list = new ArrayList<>(activePages);
                    int victim = list.get(rng.nextInt(list.size()));
                    allocator.deallocatePage(victim);
                    assertTrue(activePages.remove(victim));
                    assertTrue(allocator.isFree(victim));
                }

                // Invariant: total pages on disk == 1 (page 0) + activePages.size() + freePageCount
                assertEquals(1 + activePages.size() + allocator.getFreePageCount(), dm.getPageCount());
            }
        }

        // Reopen and verify persisted free list matches the model
        try (DiskManager dm = DiskManager.open(dbFile, 512)) {
            PageAllocator allocator = new PageAllocator(dm);
            assertEquals(1 + activePages.size() + allocator.getFreePageCount(), dm.getPageCount());
            for (int active : activePages) {
                assertFalse(allocator.isFree(active));
            }
            int freeCount = allocator.getFreePageCount();
            for (int i = 0; i < freeCount; i++) {
                int reused = allocator.allocatePage();
                assertTrue(activePages.add(reused), "Reopened allocator handed out already-active page " + reused);
            }
            assertEquals(0, allocator.getFreePageCount());
            assertEquals(-1, dm.getFreeListHead());
        }
    }
}
