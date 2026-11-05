package io.nexusdb.core.buffer;

import io.nexusdb.core.page.Page;
import io.nexusdb.core.page.SlottedPage;
import io.nexusdb.core.storage.DiskManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class BufferPoolManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void lruEvictionOrderAndHitMissMetrics() {
        Path dbFile = tempDir.resolve("lru.db");
        try (DiskManager dm = DiskManager.open(dbFile, 512);
             BufferPoolManager bpm = new BufferPoolManager(3, dm)) {

            Page p1 = bpm.newPage();
            Page p2 = bpm.newPage();
            Page p3 = bpm.newPage();
            assertEquals(1, p1.getPageId());
            assertEquals(2, p2.getPageId());
            assertEquals(3, p3.getPageId());

            // Unpin in order 1, 2, 3 -> LRU order: 1 (oldest), 2, 3 (newest)
            assertTrue(bpm.unpinPage(1, false));
            assertTrue(bpm.unpinPage(2, false));
            assertTrue(bpm.unpinPage(3, false));

            // Fetch page 1 -> cache hit! Unpin 1 -> now LRU order is 2 (oldest), 3, 1 (newest)
            Page p1Again = bpm.fetchPage(1);
            assertSame(p1, p1Again);
            assertEquals(1L, bpm.getHits());
            assertEquals(0L, bpm.getMisses());
            assertTrue(bpm.unpinPage(1, false));

            // Allocate p4 -> must evict page 2
            Page p4 = bpm.newPage();
            assertEquals(4, p4.getPageId());
            assertEquals(1L, bpm.getEvictions());
            assertTrue(bpm.unpinPage(4, false));

            // Page 3 and Page 1 must still be cached (hits)
            bpm.fetchPage(3);
            bpm.unpinPage(3, false);
            bpm.fetchPage(1);
            bpm.unpinPage(1, false);
            assertEquals(3L, bpm.getHits());
            assertEquals(0L, bpm.getMisses());

            // Fetching evicted page 2 is a miss and evicts page 4 (the oldest unpinned)
            bpm.fetchPage(2);
            bpm.unpinPage(2, false);
            assertEquals(3L, bpm.getHits());
            assertEquals(1L, bpm.getMisses());
            assertEquals(2L, bpm.getEvictions());
            assertEquals(0.75, bpm.getHitRatio(), 1e-6);
        }
    }

    @Test
    void pinnedPagesNeverEvictedAndThrowsWhenAllFramesPinned() {
        Path dbFile = tempDir.resolve("pinned.db");
        try (DiskManager dm = DiskManager.open(dbFile, 512);
             BufferPoolManager bpm = new BufferPoolManager(2, dm)) {

            Page p1 = bpm.newPage();
            Page p2 = bpm.newPage();
            assertEquals(1, bpm.getPinCount(p1.getPageId()));
            assertEquals(1, bpm.getPinCount(p2.getPageId()));

            // Both frames are pinned -> newPage() must throw BufferPoolException
            BufferPoolException ex = assertThrows(BufferPoolException.class, bpm::newPage);
            assertTrue(ex.getMessage().contains("pinned"));

            // Cannot delete a pinned page
            assertThrows(BufferPoolException.class, () -> bpm.deletePage(p1.getPageId()));

            // Unpin p2 only; p1 stays pinned even if it was allocated first
            bpm.unpinPage(p2.getPageId(), false);
            Page p3 = bpm.newPage();
            assertEquals(3, p3.getPageId());
            assertEquals(1, bpm.getPinCount(p1.getPageId()));
            assertEquals(0, bpm.getPinCount(p2.getPageId()));

            bpm.unpinPage(p1.getPageId(), false);
            bpm.unpinPage(p3.getPageId(), false);
        }
    }

    @Test
    void dirtyWriteBackOnEvictionAndReopenPersistence() {
        Path dbFile = tempDir.resolve("dirty_persist.db");
        AtomicLong lastFlushedWalLsn = new AtomicLong(0L);

        try (DiskManager dm = DiskManager.open(dbFile, 512);
             BufferPoolManager bpm = new BufferPoolManager(2, dm)) {
            bpm.setWalFlushHook(lastFlushedWalLsn::set);

            for (int i = 1; i <= 5; i++) {
                Page page = bpm.newPage();
                page.wlatch();
                try {
                    SlottedPage sp = SlottedPage.wrap(page);
                    sp.setPageLsn(i * 100L);
                    sp.insert(("record-on-page-" + i).getBytes());
                } finally {
                    page.wunlatch();
                }
                bpm.unpinPage(page.getPageId(), true);
            }
            // Evictions should have triggered WAL flush hook for evicted pages
            assertTrue(lastFlushedWalLsn.get() >= 300L);
        }

        // Reopen with fresh DiskManager and BufferPoolManager and verify all 5 pages persisted
        try (DiskManager dm = DiskManager.open(dbFile, 512);
             BufferPoolManager bpm = new BufferPoolManager(2, dm)) {
            for (int i = 1; i <= 5; i++) {
                Page page = bpm.fetchPage(i);
                page.rlatch();
                try {
                    SlottedPage sp = SlottedPage.wrap(page);
                    assertEquals(i * 100L, sp.getPageLsn());
                    assertArrayEquals(("record-on-page-" + i).getBytes(), sp.get(0));
                } finally {
                    page.runlatch();
                }
                bpm.unpinPage(i, false);
            }
        }
    }

    @Test
    void multiThreadedStressTestMaintainsPageInvariants() throws Exception {
        Path dbFile = tempDir.resolve("mt_stress.db");
        final int numPages = 32;
        final int poolSize = 8; // Smaller than working set to force heavy concurrent eviction
        final int numThreads = 8;
        final int opsPerThread = 500;

        try (DiskManager dm = DiskManager.open(dbFile, 512);
             BufferPoolManager bpm = new BufferPoolManager(poolSize, dm)) {

            for (int i = 1; i <= numPages; i++) {
                Page p = bpm.newPage();
                assertEquals(i, p.getPageId());
                ByteBuffer buf = p.getBuffer();
                buf.putInt(16, i); // pageId tag at offset 16
                buf.putInt(20, 0); // counter at offset 20
                bpm.unpinPage(i, true);
            }

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            try {
                List<Callable<Integer>> workers = new ArrayList<>();
                for (int t = 0; t < numThreads; t++) {
                    final long seed = 1000L + t;
                    workers.add(() -> {
                        Random rng = new Random(seed);
                        int successfulUpdates = 0;
                        for (int op = 0; op < opsPerThread; op++) {
                            int pageId = rng.nextInt(1, numPages + 1);
                            Page page = null;
                            while (page == null) {
                                try {
                                    page = bpm.fetchPage(pageId);
                                } catch (BufferPoolException exhausted) {
                                    Thread.yield();
                                }
                            }
                            page.wlatch();
                            try {
                                ByteBuffer buf = page.getBuffer();
                                assertEquals(pageId, buf.getInt(16), "Frame corruption: wrong pageId");
                                int current = buf.getInt(20);
                                buf.putInt(20, current + 1);
                                successfulUpdates++;
                            } finally {
                                page.wunlatch();
                                bpm.unpinPage(pageId, true);
                            }
                        }
                        return successfulUpdates;
                    });
                }

                int totalExpected = 0;
                for (Future<Integer> f : executor.invokeAll(workers)) {
                    totalExpected += f.get();
                }

                int actualTotal = 0;
                for (int pageId = 1; pageId <= numPages; pageId++) {
                    assertEquals(0, bpm.getPinCount(pageId), "Page " + pageId + " left pinned");
                    Page page = bpm.fetchPage(pageId);
                    page.rlatch();
                    try {
                        ByteBuffer buf = page.getBuffer();
                        assertEquals(pageId, buf.getInt(16));
                        actualTotal += buf.getInt(20);
                    } finally {
                        page.runlatch();
                        bpm.unpinPage(pageId, false);
                    }
                }
                assertEquals(totalExpected, actualTotal, "Lost update across evictions/fetches");
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
