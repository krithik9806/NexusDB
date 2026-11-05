package io.nexusdb.core.page;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class SlottedPageTest {

    @Test
    void fillPageToCapacityAndVerifyAllRecords() {
        SlottedPage page = SlottedPage.create(1, 512);
        page.setPageLsn(42L);
        assertEquals(42L, page.getPageLsn());

        List<byte[]> inserted = new ArrayList<>();
        int slot = 0;
        while (true) {
            byte[] payload = new byte[20];
            Arrays.fill(payload, (byte) (slot + 1));
            int assigned = page.insert(payload);
            if (assigned < 0) {
                break;
            }
            assertEquals(slot, assigned);
            inserted.add(payload);
            slot++;
        }

        assertTrue(inserted.size() >= 18);
        assertTrue(page.getTotalFreeSpace() < 24);

        for (int i = 0; i < inserted.size(); i++) {
            assertArrayEquals(inserted.get(i), page.get(i));
        }
    }

    @Test
    void deleteThenInsertReusesSlotAndCompactsSpace() {
        SlottedPage page = SlottedPage.create(2, 512);
        Map<Integer, byte[]> expected = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            byte[] data = new byte[40];
            Arrays.fill(data, (byte) (i + 10));
            int slotId = page.insert(data);
            expected.put(slotId, data);
        }

        int freeBeforeDeletes = page.getTotalFreeSpace();
        // Delete odd slots (slot 9 is at freeSpacePointer so its 40B returns to contiguous space; 1,3,5,7 add 160B fragmented)
        for (int i = 1; i < 10; i += 2) {
            assertTrue(page.delete(i));
            assertNull(page.get(i));
            expected.remove(i);
        }
        assertEquals(4 * 40, page.getFragmentedBytes());
        assertEquals(freeBeforeDeletes + 5 * 40, page.getTotalFreeSpace());

        // Insert new records that require slot reuse and compaction
        for (int i = 1; i < 10; i += 2) {
            byte[] replacement = new byte[40];
            Arrays.fill(replacement, (byte) (i + 100));
            int reusedSlot = page.insert(replacement);
            assertEquals(i, reusedSlot);
            expected.put(reusedSlot, replacement);
        }

        assertEquals(10, page.getSlotCount());
        for (Map.Entry<Integer, byte[]> entry : expected.entrySet()) {
            assertArrayEquals(entry.getValue(), page.get(entry.getKey()));
        }
    }

    @Test
    void variableLengthUpdatesInPlaceAndRelocatedPreserveSlotIds() {
        SlottedPage page = SlottedPage.create(3, 512);
        int s0 = page.insert("alpha".getBytes());
        int s1 = page.insert("bravo-charlie".getBytes());
        int s2 = page.insert("delta".getBytes());

        // Shrink s1 in place
        assertTrue(page.update(s1, "bravo".getBytes()));
        assertArrayEquals("bravo".getBytes(), page.get(s1));

        // Grow s0 (relocates inside page)
        byte[] larger = "alpha-extended-variable-length-payload".getBytes();
        assertTrue(page.update(s0, larger));
        assertArrayEquals(larger, page.get(s0));
        assertArrayEquals("bravo".getBytes(), page.get(s1));
        assertArrayEquals("delta".getBytes(), page.get(s2));

        // Attempt to update with an oversized payload that exceeds page capacity
        byte[] huge = new byte[600];
        assertFalse(page.update(s2, huge));
        // Original value of s2 must remain intact
        assertArrayEquals("delta".getBytes(), page.get(s2));
    }

    @Test
    void randomizedOperationsMatchMapModel() {
        SlottedPage page = SlottedPage.create(7, 4096);
        Map<Integer, byte[]> model = new HashMap<>();
        Random rng = new Random(123456789L);

        for (int step = 0; step < 5000; step++) {
            int op = rng.nextInt(100);
            if (op < 45) {
                int len = rng.nextInt(1, 120);
                byte[] bytes = new byte[len];
                rng.nextBytes(bytes);
                int slotId = page.insert(bytes);
                if (slotId >= 0) {
                    model.put(slotId, bytes);
                } else {
                    assertFalse(page.canInsert(len));
                }
            } else if (op < 75 && !model.isEmpty()) {
                List<Integer> keys = new ArrayList<>(model.keySet());
                int slotId = keys.get(rng.nextInt(keys.size()));
                int newLen = rng.nextInt(1, 120);
                byte[] newBytes = new byte[newLen];
                rng.nextBytes(newBytes);
                boolean updated = page.update(slotId, newBytes);
                if (updated) {
                    model.put(slotId, newBytes);
                }
            } else if (op < 95 && !model.isEmpty()) {
                List<Integer> keys = new ArrayList<>(model.keySet());
                int slotId = keys.get(rng.nextInt(keys.size()));
                assertTrue(page.delete(slotId));
                model.remove(slotId);
            } else {
                page.compact();
                assertEquals(0, page.getFragmentedBytes());
            }

            for (Map.Entry<Integer, byte[]> entry : model.entrySet()) {
                assertArrayEquals(entry.getValue(), page.get(entry.getKey()), "Mismatch at step " + step);
            }
        }
    }
}
