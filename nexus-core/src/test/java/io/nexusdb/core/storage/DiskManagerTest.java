package io.nexusdb.core.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class DiskManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void pageRoundTripAndCustomPageSize() {
        Path dbFile = tempDir.resolve("roundtrip.db");
        try (DiskManager dm = DiskManager.open(dbFile, 1024)) {
            assertEquals(1024, dm.getPageSize());
            assertEquals(1, dm.getPageCount());

            int pageId = dm.allocatePage();
            assertEquals(1, pageId);
            assertEquals(2, dm.getPageCount());

            byte[] payload = new byte[1024];
            Arrays.fill(payload, (byte) 0x5A);
            byte[] msg = "NexusDB DiskManager".getBytes(StandardCharsets.UTF_8);
            System.arraycopy(msg, 0, payload, 0, msg.length);

            dm.writePage(pageId, payload);
            dm.fsync();

            byte[] readBack = dm.readPage(pageId);
            assertArrayEquals(payload, readBack);
        }
    }

    @Test
    void reopenPersistencePreservesHeaderAndPages() {
        Path dbFile = tempDir.resolve("persist.db");
        byte[] page1Data = new byte[DiskManager.DEFAULT_PAGE_SIZE];
        byte[] page2Data = new byte[DiskManager.DEFAULT_PAGE_SIZE];
        Arrays.fill(page1Data, (byte) 11);
        Arrays.fill(page2Data, (byte) 22);

        try (DiskManager dm = DiskManager.open(dbFile)) {
            int p1 = dm.allocatePage();
            int p2 = dm.allocatePage();
            assertEquals(1, p1);
            assertEquals(2, p2);
            dm.writePage(p1, page1Data);
            dm.writePage(p2, page2Data);
            dm.setFreeListHead(p2);
            dm.setCatalogMetaPageId(p1);
            dm.setCheckpointLsn(987654321L);
        }

        try (DiskManager reopened = DiskManager.open(dbFile)) {
            assertEquals(DiskManager.DEFAULT_PAGE_SIZE, reopened.getPageSize());
            assertEquals(3, reopened.getPageCount());
            assertEquals(2, reopened.getFreeListHead());
            assertEquals(1, reopened.getCatalogMetaPageId());
            assertEquals(987654321L, reopened.getCheckpointLsn());
            assertArrayEquals(page1Data, reopened.readPage(1));
            assertArrayEquals(page2Data, reopened.readPage(2));
        }
    }

    @Test
    void corruptedHeaderIsRejectedOnOpen() throws IOException {
        Path badMagicFile = tempDir.resolve("bad_magic.db");
        try (DiskManager dm = DiskManager.open(badMagicFile)) {
            dm.allocatePage();
        }
        byte[] bytes = Files.readAllBytes(badMagicFile);
        bytes[0] = 0x00; // Corrupt magic number
        Files.write(badMagicFile, bytes);

        StorageException ex = assertThrows(StorageException.class, () -> DiskManager.open(badMagicFile));
        assertTrue(ex.getMessage().contains("magic"));

        Path badVersionFile = tempDir.resolve("bad_version.db");
        try (DiskManager dm = DiskManager.open(badVersionFile)) {
            dm.allocatePage();
        }
        byte[] vBytes = Files.readAllBytes(badVersionFile);
        ByteBuffer.wrap(vBytes).putInt(DiskManager.HEADER_VERSION_OFFSET, 99);
        Files.write(badVersionFile, vBytes);

        StorageException vex = assertThrows(StorageException.class, () -> DiskManager.open(badVersionFile));
        assertTrue(vex.getMessage().contains("version"));
    }

    @Test
    void concurrentWritesToDifferentPagesAreThreadSafe() throws Exception {
        Path dbFile = tempDir.resolve("concurrent.db");
        int numPages = 64;
        try (DiskManager dm = DiskManager.open(dbFile)) {
            for (int i = 0; i < numPages; i++) {
                dm.allocatePage();
            }

            ExecutorService pool = Executors.newFixedThreadPool(16);
            try {
                List<Callable<Void>> tasks = new ArrayList<>();
                for (int pageId = 1; pageId <= numPages; pageId++) {
                    final int pid = pageId;
                    tasks.add(() -> {
                        ByteBuffer buf = ByteBuffer.allocate(dm.getPageSize());
                        for (int offset = 0; offset < dm.getPageSize(); offset += 4) {
                            buf.putInt(offset, pid * 100_000 + offset);
                        }
                        dm.writePage(pid, buf);
                        return null;
                    });
                }
                for (Future<Void> f : pool.invokeAll(tasks)) {
                    f.get();
                }
            } finally {
                pool.shutdownNow();
            }

            for (int pageId = 1; pageId <= numPages; pageId++) {
                ByteBuffer readBuf = ByteBuffer.wrap(dm.readPage(pageId));
                for (int offset = 0; offset < dm.getPageSize(); offset += 4) {
                    assertEquals(pageId * 100_000 + offset, readBuf.getInt(offset));
                }
            }
        }
    }
}
