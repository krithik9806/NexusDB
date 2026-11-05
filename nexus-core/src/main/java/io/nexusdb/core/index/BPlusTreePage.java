package io.nexusdb.core.index;

import io.nexusdb.core.page.Page;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Value;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Binary layout view over a {@link Page} representing a B+Tree Leaf or Internal node.
 *
 * <p>On-Disk Node Header Layout (32 bytes, Big-Endian):
 * <pre>
 *  [0..7]   pageLSN           (long, 8B)
 *  [8]      nodeType          (byte, 1B: 1 = LEAF, 2 = INTERNAL)
 *  [9]      keyType           (byte, 1B: DataType ordinal)
 *  [10]     unique            (byte, 1B: 1 = unique, 0 = non-unique)
 *  [11]     reserved          (byte, 1B: 0)
 *  [12..13] keyCount          (short, 2B: unsigned entry count m)
 *  [14..15] freeSpacePointer  (short, 2B: start offset of upward-growing payload area)
 *  [16..17] fragmentedBytes   (short, 2B: dead payload bytes from deletions/updates)
 *  [18..19] reserved2         (short, 2B: 0)
 *  [20..23] prevLeafPageId    (int, 4B: -1 if leftmost leaf or internal node)
 *  [24..27] nextLeafPageId    (int, 4B: -1 if rightmost leaf or internal node)
 *  [28..31] firstChildPageId  (int, 4B: child C_0 for internal node, -1 for leaf node)
 * </pre>
 *
 * <p>Sorted Slot Directory (starts at byte 32, 2 bytes per entry):
 * Slot {@code i} at offset {@code 32 + 2 * i} stores the unsigned 16-bit byte offset of entry {@code i}.
 *
 * <p>Entry Payload Layout:
 * <ul>
 *   <li>Leaf entry: {@code [recordPageId: 4B][recordSlotId: 4B][nullFlag: 1B][keyBytes...]}</li>
 *   <li>Internal entry: {@code [rightChildPageId: 4B][sepRecordPageId: 4B][sepRecordSlotId: 4B][nullFlag: 1B][keyBytes...]}</li>
 * </ul>
 */
public final class BPlusTreePage {
    public static final byte NODE_TYPE_LEAF = 1;
    public static final byte NODE_TYPE_INTERNAL = 2;

    public static final int HEADER_SIZE = 32;
    public static final int SLOT_SIZE = 2;

    private static final int NODE_TYPE_OFFSET = 8;
    private static final int KEY_TYPE_OFFSET = 9;
    private static final int UNIQUE_OFFSET = 10;
    private static final int KEY_COUNT_OFFSET = 12;
    private static final int FREE_SPACE_OFFSET = 14;
    private static final int FRAGMENTED_BYTES_OFFSET = 16;
    private static final int PREV_LEAF_OFFSET = 20;
    private static final int NEXT_LEAF_OFFSET = 24;
    private static final int FIRST_CHILD_OFFSET = 28;

    private static final DataType[] DATA_TYPES = DataType.values();

    private final Page page;
    private final ByteBuffer buf;
    private final byte[] raw;

    public record InternalEntry(Value key, RecordId recordId, int rightChildPageId) {
        public InternalEntry {
            Objects.requireNonNull(key, "key must not be null");
            Objects.requireNonNull(recordId, "recordId must not be null");
        }
    }

    private BPlusTreePage(Page page) {
        this.page = Objects.requireNonNull(page, "page must not be null");
        this.buf = page.getBuffer();
        this.raw = page.getData();
    }

    public static BPlusTreePage wrap(Page page) {
        return new BPlusTreePage(page);
    }

    public static BPlusTreePage initLeaf(Page page, DataType keyType, boolean unique) {
        BPlusTreePage node = new BPlusTreePage(page);
        node.resetHeader(NODE_TYPE_LEAF, keyType, unique, -1, -1, -1);
        return node;
    }

    public static BPlusTreePage initInternal(Page page, DataType keyType, boolean unique, int firstChildPageId) {
        BPlusTreePage node = new BPlusTreePage(page);
        node.resetHeader(NODE_TYPE_INTERNAL, keyType, unique, -1, -1, firstChildPageId);
        return node;
    }

    public void resetHeader(
            byte nodeType,
            DataType keyType,
            boolean unique,
            int prevLeafPageId,
            int nextLeafPageId,
            int firstChildPageId) {
        buf.put(NODE_TYPE_OFFSET, nodeType);
        buf.put(KEY_TYPE_OFFSET, (byte) keyType.ordinal());
        buf.put(UNIQUE_OFFSET, (byte) (unique ? 1 : 0));
        buf.put(11, (byte) 0);
        buf.putShort(KEY_COUNT_OFFSET, (short) 0);
        buf.putShort(FREE_SPACE_OFFSET, (short) page.getPageSize());
        buf.putShort(FRAGMENTED_BYTES_OFFSET, (short) 0);
        buf.putShort(18, (short) 0);
        buf.putInt(PREV_LEAF_OFFSET, prevLeafPageId);
        buf.putInt(NEXT_LEAF_OFFSET, nextLeafPageId);
        buf.putInt(FIRST_CHILD_OFFSET, firstChildPageId);
    }

    public Page getPage() {
        return page;
    }

    public int getPageId() {
        return page.getPageId();
    }

    public int getPageSize() {
        return page.getPageSize();
    }

    public int getMaxPayloadBytes() {
        return page.getPageSize() - HEADER_SIZE;
    }

    public byte getNodeType() {
        return buf.get(NODE_TYPE_OFFSET);
    }

    public boolean isLeaf() {
        return getNodeType() == NODE_TYPE_LEAF;
    }

    public boolean isInternal() {
        return getNodeType() == NODE_TYPE_INTERNAL;
    }

    public DataType getKeyType() {
        return DATA_TYPES[Byte.toUnsignedInt(buf.get(KEY_TYPE_OFFSET))];
    }

    public boolean isUnique() {
        return buf.get(UNIQUE_OFFSET) != 0;
    }

    public int getKeyCount() {
        return Short.toUnsignedInt(buf.getShort(KEY_COUNT_OFFSET));
    }

    private void setKeyCount(int count) {
        buf.putShort(KEY_COUNT_OFFSET, (short) count);
    }

    public int getFreeSpacePointer() {
        return Short.toUnsignedInt(buf.getShort(FREE_SPACE_OFFSET));
    }

    private void setFreeSpacePointer(int offset) {
        buf.putShort(FREE_SPACE_OFFSET, (short) offset);
    }

    public int getFragmentedBytes() {
        return Short.toUnsignedInt(buf.getShort(FRAGMENTED_BYTES_OFFSET));
    }

    private void setFragmentedBytes(int bytes) {
        buf.putShort(FRAGMENTED_BYTES_OFFSET, (short) bytes);
    }

    public int getPrevLeafPageId() {
        return buf.getInt(PREV_LEAF_OFFSET);
    }

    public void setPrevLeafPageId(int pageId) {
        buf.putInt(PREV_LEAF_OFFSET, pageId);
    }

    public int getNextLeafPageId() {
        return buf.getInt(NEXT_LEAF_OFFSET);
    }

    public void setNextLeafPageId(int pageId) {
        buf.putInt(NEXT_LEAF_OFFSET, pageId);
    }

    public int getFirstChildPageId() {
        return buf.getInt(FIRST_CHILD_OFFSET);
    }

    public void setFirstChildPageId(int pageId) {
        buf.putInt(FIRST_CHILD_OFFSET, pageId);
    }

    public int getSlotArrayEnd() {
        return HEADER_SIZE + getKeyCount() * SLOT_SIZE;
    }

    public int getContiguousFreeSpace() {
        return getFreeSpacePointer() - getSlotArrayEnd();
    }

    public int getTotalFreeSpace() {
        return getContiguousFreeSpace() + getFragmentedBytes();
    }

    public int getUsedBytes() {
        return getMaxPayloadBytes() - getTotalFreeSpace();
    }

    private int getSlotOffset(int index) {
        return Short.toUnsignedInt(buf.getShort(HEADER_SIZE + index * SLOT_SIZE));
    }

    private void setSlotOffset(int index, int offset) {
        buf.putShort(HEADER_SIZE + index * SLOT_SIZE, (short) offset);
    }

    public static int keyPayloadBytes(Value key) {
        if (key.isNull()) {
            return 1;
        }
        return switch (key) {
            case Value.IntValue ignored -> 1 + 4;
            case Value.BigIntValue ignored -> 1 + 8;
            case Value.DoubleValue ignored -> 1 + 8;
            case Value.BooleanValue ignored -> 1 + 1;
            case Value.VarcharValue(String s) -> 1 + 2 + s.getBytes(StandardCharsets.UTF_8).length;
            case Value.NullValue ignored -> 1;
        };
    }

    public static int leafEntryPayloadBytes(Value key) {
        return 8 + keyPayloadBytes(key);
    }

    public static int internalEntryPayloadBytes(Value key) {
        return 12 + keyPayloadBytes(key);
    }

    public boolean canFitLeafEntry(Value key) {
        int needed = SLOT_SIZE + leafEntryPayloadBytes(key);
        return getTotalFreeSpace() >= needed;
    }

    public boolean canFitInternalEntry(Value key) {
        int needed = SLOT_SIZE + internalEntryPayloadBytes(key);
        return getTotalFreeSpace() >= needed;
    }

    private int entryPayloadLengthAtOffset(int entryOffset) {
        int base = entryOffset + (isInternal() ? 4 : 0);
        int headerPrefix = isInternal() ? 12 : 8;
        byte nullFlag = buf.get(base + 8);
        if (nullFlag == 0) {
            return headerPrefix + 1;
        }
        return switch (getKeyType()) {
            case INT -> headerPrefix + 1 + 4;
            case BIGINT, DOUBLE -> headerPrefix + 1 + 8;
            case BOOLEAN -> headerPrefix + 1 + 1;
            case VARCHAR -> {
                int utf8Len = Short.toUnsignedInt(buf.getShort(base + 9));
                yield headerPrefix + 1 + 2 + utf8Len;
            }
        };
    }

    public Value getKey(int index) {
        int entryOffset = getSlotOffset(index);
        int base = entryOffset + (isInternal() ? 4 : 0);
        byte nullFlag = buf.get(base + 8);
        if (nullFlag == 0) {
            return Value.NULL;
        }
        return switch (getKeyType()) {
            case INT -> Value.ofInt(buf.getInt(base + 9));
            case BIGINT -> Value.ofBigInt(buf.getLong(base + 9));
            case DOUBLE -> Value.ofDouble(buf.getDouble(base + 9));
            case BOOLEAN -> Value.ofBoolean(buf.get(base + 9) != 0);
            case VARCHAR -> {
                int utf8Len = Short.toUnsignedInt(buf.getShort(base + 9));
                yield Value.ofVarchar(new String(raw, base + 11, utf8Len, StandardCharsets.UTF_8));
            }
        };
    }

    public RecordId getRecordId(int index) {
        int entryOffset = getSlotOffset(index);
        int base = entryOffset + (isInternal() ? 4 : 0);
        return new RecordId(buf.getInt(base), buf.getInt(base + 4));
    }

    public IndexEntry getLeafEntry(int index) {
        int entryOffset = getSlotOffset(index);
        RecordId rid = new RecordId(buf.getInt(entryOffset), buf.getInt(entryOffset + 4));
        Value key = getKey(index);
        return new IndexEntry(key, rid);
    }

    public int getRightChildPageId(int index) {
        int entryOffset = getSlotOffset(index);
        return buf.getInt(entryOffset);
    }

    public void setRightChildPageId(int index, int childPageId) {
        int entryOffset = getSlotOffset(index);
        buf.putInt(entryOffset, childPageId);
    }

    /**
     * Returns child pointer {@code childIndex} in {@code [0..keyCount]} for an internal node.
     */
    public int getChildPageId(int childIndex) {
        if (childIndex == 0) {
            return getFirstChildPageId();
        }
        return getRightChildPageId(childIndex - 1);
    }

    public void setChildPageId(int childIndex, int childPageId) {
        if (childIndex == 0) {
            setFirstChildPageId(childPageId);
        } else {
            setRightChildPageId(childIndex - 1, childPageId);
        }
    }

    public InternalEntry getInternalEntry(int index) {
        int entryOffset = getSlotOffset(index);
        int rightChild = buf.getInt(entryOffset);
        RecordId rid = new RecordId(buf.getInt(entryOffset + 4), buf.getInt(entryOffset + 8));
        Value key = getKey(index);
        return new InternalEntry(key, rid, rightChild);
    }

    /**
     * Compares entry {@code index} in this node against {@code (targetKey, targetPageId, targetSlotId)}
     * directly in the page buffer without allocating {@link Value} or {@link RecordId} wrappers.
     */
    public int compareEntryAt(
            int index,
            Value targetKey,
            int targetPageId,
            int targetSlotId,
            boolean compareRecordId) {
        int entryOffset = getSlotOffset(index);
        int base = entryOffset + (isInternal() ? 4 : 0);
        byte nullFlag = buf.get(base + 8);

        int cmp;
        if (targetKey.isNull()) {
            cmp = (nullFlag == 0) ? 0 : 1;
        } else if (nullFlag == 0) {
            cmp = -1;
        } else {
            cmp = switch (targetKey) {
                case Value.IntValue(int tv) -> Integer.compare(buf.getInt(base + 9), tv);
                case Value.BigIntValue(long tv) -> Long.compare(buf.getLong(base + 9), tv);
                case Value.DoubleValue(double tv) -> Double.compare(buf.getDouble(base + 9), tv);
                case Value.BooleanValue(boolean tv) -> Boolean.compare(buf.get(base + 9) != 0, tv);
                case Value.VarcharValue(String ts) -> {
                    int utf8Len = Short.toUnsignedInt(buf.getShort(base + 9));
                    String es = new String(raw, base + 11, utf8Len, StandardCharsets.UTF_8);
                    yield es.compareTo(ts);
                }
                case Value.NullValue ignored -> 0;
            };
        }

        if (cmp != 0 || !compareRecordId) {
            return cmp;
        }
        int cmpPage = Integer.compare(buf.getInt(base), targetPageId);
        if (cmpPage != 0) {
            return cmpPage;
        }
        return Integer.compare(buf.getInt(base + 4), targetSlotId);
    }

    /**
     * Binary searches this node for {@code (key, recordId)}.
     *
     * @return {@code >= 0} if found at that index, or {@code -(insertionPoint + 1)} if not found
     */
    public int binarySearch(Value key, RecordId recordId, boolean compareRecordId) {
        int low = 0;
        int high = getKeyCount() - 1;
        int rPage = recordId.pageId();
        int rSlot = recordId.slotId();

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = compareEntryAt(mid, key, rPage, rSlot, compareRecordId);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

    /**
     * Finds the child index {@code [0..keyCount]} to follow when descending an internal node
     * for {@code (key, recordId)}.
     */
    public int findChildIndex(Value key, RecordId recordId, boolean compareRecordId) {
        int bs = binarySearch(key, recordId, compareRecordId);
        return bs >= 0 ? (bs + 1) : (-bs - 1);
    }

    public int findChildPageId(Value key, RecordId recordId, boolean compareRecordId) {
        return getChildPageId(findChildIndex(key, recordId, compareRecordId));
    }

    /**
     * Inserts {@code (key, recordId)} into this leaf node at {@code insertIndex}.
     */
    public void insertLeafAt(int insertIndex, Value key, RecordId recordId) {
        int payloadLen = leafEntryPayloadBytes(key);
        ensureContiguousSpace(SLOT_SIZE + payloadLen);

        int newFreePtr = getFreeSpacePointer() - payloadLen;
        writeKeyAndRecordId(newFreePtr, key, recordId);
        setFreeSpacePointer(newFreePtr);

        int count = getKeyCount();
        if (insertIndex < count) {
            int srcOff = HEADER_SIZE + insertIndex * SLOT_SIZE;
            System.arraycopy(raw, srcOff, raw, srcOff + SLOT_SIZE, (count - insertIndex) * SLOT_SIZE);
        }
        setSlotOffset(insertIndex, newFreePtr);
        setKeyCount(count + 1);
    }

    /**
     * Inserts {@code (key, recordId, rightChildPageId)} into this internal node at {@code insertIndex}.
     */
    public void insertInternalAt(int insertIndex, Value key, RecordId recordId, int rightChildPageId) {
        int payloadLen = internalEntryPayloadBytes(key);
        ensureContiguousSpace(SLOT_SIZE + payloadLen);

        int newFreePtr = getFreeSpacePointer() - payloadLen;
        buf.putInt(newFreePtr, rightChildPageId);
        writeKeyAndRecordId(newFreePtr + 4, key, recordId);
        setFreeSpacePointer(newFreePtr);

        int count = getKeyCount();
        if (insertIndex < count) {
            int srcOff = HEADER_SIZE + insertIndex * SLOT_SIZE;
            System.arraycopy(raw, srcOff, raw, srcOff + SLOT_SIZE, (count - insertIndex) * SLOT_SIZE);
        }
        setSlotOffset(insertIndex, newFreePtr);
        setKeyCount(count + 1);
    }

    /**
     * Removes the entry at {@code removeIndex} from this leaf or internal node.
     */
    public void removeAt(int removeIndex) {
        int count = getKeyCount();
        if (removeIndex < 0 || removeIndex >= count) {
            throw new IndexOutOfBoundsException("removeIndex=" + removeIndex + ", keyCount=" + count);
        }
        int entryOffset = getSlotOffset(removeIndex);
        int payloadLen = entryPayloadLengthAtOffset(entryOffset);

        if (removeIndex < count - 1) {
            int srcOff = HEADER_SIZE + (removeIndex + 1) * SLOT_SIZE;
            System.arraycopy(raw, srcOff, raw, srcOff - SLOT_SIZE, (count - 1 - removeIndex) * SLOT_SIZE);
        }
        setKeyCount(count - 1);
        setFragmentedBytes(getFragmentedBytes() + payloadLen);
    }

    /**
     * Checks whether replacing the separator key at {@code index} with {@code newKey} fits in this internal page.
     */
    public boolean canReplaceInternalKeyAt(int index, Value newKey) {
        int currentOffset = getSlotOffset(index);
        int currentLen = entryPayloadLengthAtOffset(currentOffset);
        int newLen = internalEntryPayloadBytes(newKey);
        return getTotalFreeSpace() + currentLen >= newLen;
    }

    /**
     * Replaces the separator key and RecordId at {@code index} in an internal node while preserving
     * its {@code rightChildPageId}.
     */
    public void setInternalKeyAt(int index, Value newKey, RecordId newRecordId) {
        int rightChild = getRightChildPageId(index);
        removeAt(index);
        insertInternalAt(index, newKey, newRecordId, rightChild);
    }

    /**
     * Copies the entire B+Tree node state (header, slot directory, and payload area) from {@code source}.
     */
    public void copyNodeContentsFrom(BPlusTreePage source) {
        System.arraycopy(source.raw, NODE_TYPE_OFFSET, this.raw, NODE_TYPE_OFFSET, getPageSize() - NODE_TYPE_OFFSET);
    }

    public List<IndexEntry> getAllLeafEntries() {
        int count = getKeyCount();
        List<IndexEntry> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(getLeafEntry(i));
        }
        return list;
    }

    public List<InternalEntry> getAllInternalEntries() {
        int count = getKeyCount();
        List<InternalEntry> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(getInternalEntry(i));
        }
        return list;
    }

    public void setAllLeafEntries(List<IndexEntry> entries) {
        int prev = getPrevLeafPageId();
        int next = getNextLeafPageId();
        resetHeader(NODE_TYPE_LEAF, getKeyType(), isUnique(), prev, next, -1);
        for (int i = 0; i < entries.size(); i++) {
            IndexEntry e = entries.get(i);
            insertLeafAt(i, e.key(), e.recordId());
        }
    }

    public void setAllInternalEntries(int firstChildPageId, List<InternalEntry> entries) {
        resetHeader(NODE_TYPE_INTERNAL, getKeyType(), isUnique(), -1, -1, firstChildPageId);
        for (int i = 0; i < entries.size(); i++) {
            InternalEntry e = entries.get(i);
            insertInternalAt(i, e.key(), e.recordId(), e.rightChildPageId());
        }
    }

    private void ensureContiguousSpace(int requiredBytes) {
        if (getContiguousFreeSpace() >= requiredBytes) {
            return;
        }
        if (getTotalFreeSpace() < requiredBytes) {
            throw new IllegalStateException(
                    "Insufficient space on BPlusTreePage " + getPageId()
                            + ": required=" + requiredBytes + ", totalFree=" + getTotalFreeSpace());
        }
        compact();
    }

    public void compact() {
        int count = getKeyCount();
        int pageSize = page.getPageSize();
        byte[] scratch = new byte[pageSize];
        int writePtr = pageSize;

        for (int i = 0; i < count; i++) {
            int oldOffset = getSlotOffset(i);
            int len = entryPayloadLengthAtOffset(oldOffset);
            writePtr -= len;
            System.arraycopy(raw, oldOffset, scratch, writePtr, len);
            setSlotOffset(i, writePtr);
        }

        int payloadSpan = pageSize - writePtr;
        if (payloadSpan > 0) {
            System.arraycopy(scratch, writePtr, raw, writePtr, payloadSpan);
        }
        setFreeSpacePointer(writePtr);
        setFragmentedBytes(0);
    }

    private void writeKeyAndRecordId(int baseOffset, Value key, RecordId recordId) {
        buf.putInt(baseOffset, recordId.pageId());
        buf.putInt(baseOffset + 4, recordId.slotId());
        if (key.isNull()) {
            buf.put(baseOffset + 8, (byte) 0);
            return;
        }
        buf.put(baseOffset + 8, (byte) 1);
        switch (key) {
            case Value.IntValue(int v) -> buf.putInt(baseOffset + 9, v);
            case Value.BigIntValue(long v) -> buf.putLong(baseOffset + 9, v);
            case Value.DoubleValue(double v) -> buf.putDouble(baseOffset + 9, v);
            case Value.BooleanValue(boolean v) -> buf.put(baseOffset + 9, (byte) (v ? 1 : 0));
            case Value.VarcharValue(String s) -> {
                byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
                buf.putShort(baseOffset + 9, (short) utf8.length);
                System.arraycopy(utf8, 0, raw, baseOffset + 11, utf8.length);
            }
            case Value.NullValue ignored -> {
            }
        }
    }
}
