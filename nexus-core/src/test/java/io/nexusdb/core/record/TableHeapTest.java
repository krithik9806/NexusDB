package io.nexusdb.core.record;

import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.storage.DiskManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TableHeapTest {

    @TempDir
    Path tempDir;

    private static Schema userSchema() {
        return new Schema(List.of(
                Column.primaryKey("id", DataType.INT),
                Column.varchar("name", 128, false),
                new Column("score", DataType.BIGINT, false),
                new Column("active", DataType.BOOLEAN, false)));
    }

    @Test
    void fiftyThousandInsertsSpanningManyPagesAndFullScan() {
        Path dbFile = tempDir.resolve("heap_50k.db");
        Schema schema = userSchema();
        final int totalRows = 50_000;

        int firstPageId;
        try (DiskManager dm = DiskManager.open(dbFile, 4096);
             BufferPoolManager bpm = new BufferPoolManager(64, dm)) {

            TableHeap heap = TableHeap.create(bpm, schema);
            firstPageId = heap.getFirstPageId();

            for (int i = 0; i < totalRows; i++) {
                Tuple t = new Tuple(
                        Value.ofInt(i),
                        Value.ofVarchar("user_" + i),
                        Value.ofBigInt(i * 10L),
                        Value.ofBoolean((i & 1) == 0));
                RecordId rid = heap.insert(t);
                assertNotNull(rid);
            }

            assertTrue(dm.getPageCount() > 300, "50k tuples should span hundreds of pages");

            int count = 0;
            for (Tuple t : heap) {
                assertNotNull(t.getRecordId());
                assertEquals(Value.ofInt(count), t.getValue(0));
                assertEquals(Value.ofVarchar("user_" + count), t.getValue(1));
                assertEquals(Value.ofBigInt(count * 10L), t.getValue(2));
                assertEquals(Value.ofBoolean((count & 1) == 0), t.getValue(3));
                count++;
            }
            assertEquals(totalRows, count);
        }

        // Reopen from disk and scan all 50k tuples
        try (DiskManager dm = DiskManager.open(dbFile, 4096);
             BufferPoolManager bpm = new BufferPoolManager(32, dm)) {
            TableHeap reopened = TableHeap.open(bpm, schema, firstPageId);
            int count = 0;
            try (TableIterator it = reopened.scan()) {
                while (it.hasNext()) {
                    Tuple t = it.next();
                    assertEquals(Value.ofInt(count), t.getValue(0));
                    count++;
                }
            }
            assertEquals(totalRows, count);
        }
    }

    @Test
    void updateInPlaceRelocationAndDeleteWithTinyBufferPool() {
        Path dbFile = tempDir.resolve("tiny_pool.db");
        Schema schema = userSchema();
        Map<RecordId, Tuple> expected = new HashMap<>();

        // Use a tiny 4-frame buffer pool to force heavy eviction on every scan/update
        try (DiskManager dm = DiskManager.open(dbFile, 512);
             BufferPoolManager bpm = new BufferPoolManager(4, dm)) {

            TableHeap heap = TableHeap.create(bpm, schema);
            List<RecordId> rids = new java.util.ArrayList<>();

            for (int i = 0; i < 300; i++) {
                Tuple t = new Tuple(
                        Value.ofInt(i),
                        Value.ofVarchar("short_" + i),
                        Value.ofBigInt(i),
                        Value.ofBoolean(true));
                RecordId rid = heap.insert(t);
                rids.add(rid);
                expected.put(rid, t);
            }
            assertTrue(bpm.getEvictions() > 10, "4-frame buffer pool must evict heavily across 300 inserts");

            // Delete every 3rd tuple
            for (int i = 0; i < 300; i += 3) {
                RecordId rid = rids.get(i);
                assertTrue(heap.delete(rid));
                assertFalse(heap.delete(rid)); // second delete returns false
                assertNull(heap.get(rid));
                expected.remove(rid);
            }

            // Update remaining even indices with a much longer VARCHAR that forces cross-page relocation
            int relocatedCount = 0;
            for (int i = 0; i < 300; i++) {
                if (i % 3 == 0) {
                    continue;
                }
                RecordId oldRid = rids.get(i);
                String newName = (i % 2 == 0)
                        ? "relocated_long_varchar_payload_that_forces_page_split_" + "X".repeat(60) + "_" + i
                        : "in_place_" + i;
                Tuple updated = new Tuple(
                        Value.ofInt(i),
                        Value.ofVarchar(newName),
                        Value.ofBigInt(i * 100L),
                        Value.ofBoolean(false));
                RecordId newRid = heap.update(oldRid, updated);
                if (!newRid.equals(oldRid)) {
                    relocatedCount++;
                    assertNull(heap.get(oldRid), "Old slot must be deleted after cross-page relocation");
                }
                expected.remove(oldRid);
                expected.put(newRid, updated);
                assertEquals(updated, heap.get(newRid));
            }
            assertTrue(relocatedCount > 0, "Expected at least one growing update to relocate to a new page");

            // Full scan must match expected map exactly
            Map<RecordId, Tuple> scanned = new HashMap<>();
            for (Tuple t : heap) {
                scanned.put(t.getRecordId(), t);
            }
            assertEquals(expected, scanned);
        }
    }
}
