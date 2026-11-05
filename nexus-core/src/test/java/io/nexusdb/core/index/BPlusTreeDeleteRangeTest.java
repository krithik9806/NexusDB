package io.nexusdb.core.index;

import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Value;
import io.nexusdb.core.storage.DiskManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BPlusTreeDeleteRangeTest {

    private static final int TWO_HUNDRED_K = 200_000;

    @TempDir
    Path tempDir;

    @Test
    void randomizedDifferentialTest200kMixedOperationsAgainstTreeMapAndPersistence() {
        Path dbFile = tempDir.resolve("btree-diff-200k.db");
        TreeMap<Integer, RecordId> oracle = new TreeMap<>();
        Random rng = new Random(20260305L);
        int keyDomain = 15_000;
        int rootPageId;

        try (DiskManager dm = DiskManager.open(dbFile);
             BufferPoolManager bpm = new BufferPoolManager(512, dm)) {
            BPlusTree tree = BPlusTree.create(bpm, DataType.INT, true);
            rootPageId = tree.getRootPageId();

            for (int op = 1; op <= TWO_HUNDRED_K; op++) {
                int action = rng.nextInt(100);
                int k = rng.nextInt(keyDomain);
                Value keyVal = Value.ofInt(k);

                if (action < 45) {
                    // Insert (if not already in unique oracle, or idempotent re-insert)
                    RecordId existing = oracle.get(k);
                    if (existing == null) {
                        RecordId rid = new RecordId(1 + (op / 100), op % 100);
                        tree.insert(keyVal, rid);
                        oracle.put(k, rid);
                    } else {
                        tree.insert(keyVal, existing);
                    }
                } else if (action < 75) {
                    // Delete
                    RecordId existing = oracle.remove(k);
                    if (existing != null) {
                        if (rng.nextBoolean()) {
                            assertTrue(tree.delete(keyVal, existing));
                        } else {
                            assertEquals(1, tree.deleteKey(keyVal));
                        }
                    } else {
                        assertFalse(tree.delete(keyVal, new RecordId(999, 999)));
                    }
                } else if (action < 95) {
                    // Point search
                    List<RecordId> actual = tree.search(keyVal);
                    RecordId expected = oracle.get(k);
                    if (expected == null) {
                        assertTrue(actual.isEmpty());
                    } else {
                        assertEquals(List.of(expected), actual);
                    }
                } else {
                    // Range scan (sampled frequency so 200k ops complete quickly)
                    if (op % 25 == 0) {
                        verifyRandomRangeScan(tree, oracle, rng, keyDomain);
                    }
                }

                if (op % 50_000 == 0) {
                    tree.verifyInvariants();
                }
            }

            tree.verifyInvariants();
        }

        // Reopen DiskManager and BufferPoolManager and verify persistence of the B+Tree
        try (DiskManager dm = DiskManager.open(dbFile);
             BufferPoolManager bpm = new BufferPoolManager(256, dm)) {
            BPlusTree reopened = BPlusTree.open(bpm, rootPageId);
            assertEquals(DataType.INT, reopened.getKeyType());
            assertTrue(reopened.isUnique());
            reopened.verifyInvariants();

            List<IndexEntry> allEntries = reopened.rangeScan(null, true, null, true).toList();
            assertEquals(oracle.size(), allEntries.size());
            int idx = 0;
            for (Map.Entry<Integer, RecordId> expected : oracle.entrySet()) {
                IndexEntry actual = allEntries.get(idx++);
                assertEquals(Value.ofInt(expected.getKey()), actual.key());
                assertEquals(expected.getValue(), actual.recordId());
            }

            for (int i = 0; i < 50; i++) {
                verifyRandomRangeScan(reopened, oracle, rng, keyDomain);
            }
        }
    }

    private void verifyRandomRangeScan(
            BPlusTree tree,
            TreeMap<Integer, RecordId> oracle,
            Random rng,
            int keyDomain) {
        int mode = rng.nextInt(6);
        Integer low = null;
        Integer high = null;
        boolean lowInc = rng.nextBoolean();
        boolean highInc = rng.nextBoolean();

        if (mode == 0) {
            // Bounded small range
            int a = rng.nextInt(keyDomain);
            int b = Math.min(keyDomain - 1, a + rng.nextInt(300));
            low = a;
            high = b;
        } else if (mode == 1) {
            // Unbounded low, bounded near bottom
            high = rng.nextInt(400);
        } else if (mode == 2) {
            // Bounded near top, unbounded high
            low = Math.max(0, keyDomain - 1 - rng.nextInt(400));
        } else if (mode == 3) {
            // Single key range [k, k]
            int k = rng.nextInt(keyDomain);
            low = k;
            high = k;
        } else if (mode == 4) {
            // Inverted empty range low > high
            low = 500;
            high = 400;
        } else {
            // Medium range
            int a = rng.nextInt(keyDomain);
            int b = Math.min(keyDomain - 1, a + rng.nextInt(1000));
            low = a;
            high = b;
        }

        List<RecordId> actual = tree.rangeScan(
                low == null ? null : Value.ofInt(low),
                lowInc,
                high == null ? null : Value.ofInt(high),
                highInc).recordIds();

        NavigableMap<Integer, RecordId> sub;
        if (low != null && high != null) {
            if (low > high || (low.equals(high) && (!lowInc || !highInc))) {
                sub = Collections.emptyNavigableMap();
            } else {
                sub = oracle.subMap(low, lowInc, high, highInc);
            }
        } else if (low != null) {
            sub = oracle.tailMap(low, lowInc);
        } else if (high != null) {
            sub = oracle.headMap(high, highInc);
        } else {
            sub = oracle;
        }

        assertEquals(new ArrayList<>(sub.values()), actual);
    }

    @Test
    void nonUniqueDeleteRedistributeMergeAndRootCollapseToSingleLeaf() {
        Path dbFile = tempDir.resolve("btree-nonunique-collapse.db");
        try (DiskManager dm = DiskManager.open(dbFile);
             BufferPoolManager bpm = new BufferPoolManager(64, dm)) {
            BPlusTree tree = BPlusTree.create(bpm, DataType.INT, false, 4);
            TreeSet<IndexEntry> oracle = new TreeSet<>();
            Random rng = new Random(77L);

            for (int i = 0; i < 400; i++) {
                Value k = Value.ofInt(i % 40);
                RecordId rid = new RecordId(1 + (i / 20), i % 20);
                tree.insert(k, rid);
                oracle.add(new IndexEntry(k, rid));
                if (i % 40 == 0) {
                    tree.verifyInvariants();
                }
            }
            assertTrue(tree.getHeight() >= 4, "Expected multi-level B+Tree before deletions");

            List<IndexEntry> toDelete = new ArrayList<>(oracle);
            Collections.shuffle(toDelete, rng);

            for (int i = 0; i < toDelete.size(); i++) {
                IndexEntry entry = toDelete.get(i);
                assertTrue(tree.delete(entry.key(), entry.recordId()));
                oracle.remove(entry);
                tree.verifyInvariants();
                assertEquals(
                        new ArrayList<>(oracle),
                        tree.rangeScan(null, true, null, true).toList());
            }

            assertEquals(1, tree.getHeight(), "Root must shrink back to height 1 when all entries are deleted");
            assertTrue(tree.rangeScan(null, true, null, true).toList().isEmpty());
        }
    }
}
