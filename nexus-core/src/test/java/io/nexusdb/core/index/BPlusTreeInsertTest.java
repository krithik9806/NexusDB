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
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BPlusTreeInsertTest {

    private static final int HUNDRED_K = 100_000;

    @TempDir
    Path tempDir;

    @Test
    void sequentialInsert100kUniqueKeysAndVerifyInvariants() {
        Path dbFile = tempDir.resolve("btree-seq-unique.db");
        try (DiskManager dm = DiskManager.open(dbFile);
             BufferPoolManager bpm = new BufferPoolManager(512, dm)) {
            BPlusTree tree = BPlusTree.create(bpm, DataType.INT, true);

            for (int i = 0; i < HUNDRED_K; i++) {
                tree.insert(Value.ofInt(i), new RecordId(i / 100 + 1, i % 100));
            }

            assertTrue(tree.getHeight() >= 2, "100k keys must span multiple B+Tree levels");
            tree.verifyInvariants();

            for (int i = 0; i < HUNDRED_K; i += 97) {
                List<RecordId> found = tree.search(Value.ofInt(i));
                assertEquals(List.of(new RecordId(i / 100 + 1, i % 100)), found);
            }
            assertTrue(tree.search(Value.ofInt(-1)).isEmpty());
            assertTrue(tree.search(Value.ofInt(HUNDRED_K + 1)).isEmpty());
        }
    }

    @Test
    void reverseInsert100kUniqueKeysAndVerifyInvariants() {
        Path dbFile = tempDir.resolve("btree-rev-unique.db");
        try (DiskManager dm = DiskManager.open(dbFile);
             BufferPoolManager bpm = new BufferPoolManager(512, dm)) {
            BPlusTree tree = BPlusTree.create(bpm, DataType.INT, true);

            for (int i = HUNDRED_K - 1; i >= 0; i--) {
                tree.insert(Value.ofInt(i), new RecordId(i / 100 + 1, i % 100));
            }

            assertTrue(tree.getHeight() >= 2);
            tree.verifyInvariants();

            for (int i = 0; i < HUNDRED_K; i += 101) {
                List<RecordId> found = tree.search(Value.ofInt(i));
                assertEquals(List.of(new RecordId(i / 100 + 1, i % 100)), found);
            }
        }
    }

    @Test
    void randomInsert100kUniqueKeysAndVerifyInvariants() {
        Path dbFile = tempDir.resolve("btree-rnd-unique.db");
        List<Integer> keys = new ArrayList<>(HUNDRED_K);
        for (int i = 0; i < HUNDRED_K; i++) {
            keys.add(i);
        }
        Collections.shuffle(keys, new Random(42L));

        try (DiskManager dm = DiskManager.open(dbFile);
             BufferPoolManager bpm = new BufferPoolManager(512, dm)) {
            BPlusTree tree = BPlusTree.create(bpm, DataType.INT, true);

            for (int k : keys) {
                tree.insert(Value.ofInt(k), new RecordId(k / 64 + 1, k % 64));
            }

            assertTrue(tree.getHeight() >= 2);
            tree.verifyInvariants();

            for (int i = 0; i < HUNDRED_K; i += 89) {
                assertEquals(List.of(new RecordId(i / 64 + 1, i % 64)), tree.search(Value.ofInt(i)));
            }
        }
    }

    @Test
    void sequentialReverseAndRandomInsert100kNonUniqueKeysAndVerifyInvariants() {
        Path dbFile = tempDir.resolve("btree-nonunique.db");
        try (DiskManager dm = DiskManager.open(dbFile);
             BufferPoolManager bpm = new BufferPoolManager(512, dm)) {
            BPlusTree tree = BPlusTree.create(bpm, DataType.INT, false);

            // Insert 100k entries across 5,000 distinct keys (20 duplicates per key)
            // Mix sequential (first 40k), reverse (next 30k), and random (final 30k)
            int distinctKeys = 5_000;
            for (int i = 0; i < 40_000; i++) {
                int k = i % distinctKeys;
                tree.insert(Value.ofInt(k), new RecordId(1 + (i / distinctKeys), i));
            }
            for (int i = 69_999; i >= 40_000; i--) {
                int k = i % distinctKeys;
                tree.insert(Value.ofInt(k), new RecordId(1 + (i / distinctKeys), i));
            }
            List<Integer> tail = new ArrayList<>(30_000);
            for (int i = 70_000; i < HUNDRED_K; i++) {
                tail.add(i);
            }
            Collections.shuffle(tail, new Random(99L));
            for (int i : tail) {
                int k = i % distinctKeys;
                tree.insert(Value.ofInt(k), new RecordId(1 + (i / distinctKeys), i));
            }

            tree.verifyInvariants();

            for (int k = 0; k < distinctKeys; k += 137) {
                List<RecordId> found = tree.search(Value.ofInt(k));
                assertEquals(20, found.size(), "Expected 20 RecordIds for key " + k);
                for (int dup = 0; dup < 20; dup++) {
                    int seq = dup * distinctKeys + k;
                    assertEquals(new RecordId(1 + dup, seq), found.get(dup));
                }
            }
        }
    }

    @Test
    void uniqueIndexRejectsDuplicateKeyAndAllowsIdempotentEntry() {
        Path dbFile = tempDir.resolve("btree-dup.db");
        try (DiskManager dm = DiskManager.open(dbFile);
             BufferPoolManager bpm = new BufferPoolManager(32, dm)) {
            BPlusTree tree = BPlusTree.create(bpm, DataType.VARCHAR, true, 4);

            RecordId rid1 = new RecordId(1, 0);
            RecordId rid2 = new RecordId(1, 1);
            tree.insert(Value.ofVarchar("alice"), rid1);
            // Idempotent re-insert of identical (key, RecordId) succeeds
            tree.insert(Value.ofVarchar("alice"), rid1);
            assertEquals(List.of(rid1), tree.search(Value.ofVarchar("alice")));

            // Conflicting RecordId for same key throws DuplicateKeyException
            assertThrows(
                    DuplicateKeyException.class,
                    () -> tree.insert(Value.ofVarchar("alice"), rid2));

            // Null keys and multi-level splits with maxKeys=4
            tree.insert(Value.NULL, new RecordId(2, 0));
            assertThrows(
                    DuplicateKeyException.class,
                    () -> tree.insert(Value.NULL, new RecordId(2, 1)));

            for (int i = 0; i < 60; i++) {
                tree.insert(Value.ofVarchar("user_" + String.format("%03d", i)), new RecordId(3, i));
            }
            assertTrue(tree.getHeight() >= 3);
            tree.verifyInvariants();
        }
    }
}
