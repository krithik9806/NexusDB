package io.nexusdb.core.page;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Slotted-page layout implementation over a {@link Page}.
 *
 * <p>Header layout (24 bytes):
 * <ul>
 *   <li>[0..7]   pageLSN (long)</li>
 *   <li>[8..9]   pageFlags (short, bit 0 = initialized)</li>
 *   <li>[10..11] slotCount (short)</li>
 *   <li>[12..13] freeSpacePointer (unsigned short)</li>
 *   <li>[14..15] fragmentedBytes (unsigned short)</li>
 *   <li>[16..19] prevPageId (int, -1 if none)</li>
 *   <li>[20..23] nextPageId (int, -1 if none)</li>
 * </ul>
 * Slot array begins at offset 24 ({@code 4 bytes} per slot: {@code short offset, short length}).
 * Deleted slots have {@code offset == -1 (0xFFFF)} and {@code length == 0}.
 */
public final class SlottedPage {
    public static final int HEADER_SIZE = 24;
    public static final int SLOT_SIZE = 4;

    private static final int FLAGS_OFFSET = 8;
    private static final int SLOT_COUNT_OFFSET = 10;
    private static final int FREE_SPACE_PTR_OFFSET = 12;
    private static final int FRAGMENTED_BYTES_OFFSET = 14;
    private static final int PREV_PAGE_ID_OFFSET = 16;
    private static final int NEXT_PAGE_ID_OFFSET = 20;

    private static final short FLAG_INITIALIZED = 0x0001;
    private static final short DELETED_SLOT_OFFSET = (short) -1;

    private final Page page;
    private final ByteBuffer buf;

    public SlottedPage(Page page, boolean init) {
        this.page = page;
        this.buf = page.getBuffer().order(ByteOrder.BIG_ENDIAN);
        if (init) {
            initPage();
        }
    }

    public static SlottedPage create(int pageId, int pageSize) {
        return new SlottedPage(new Page(pageId, pageSize), true);
    }

    public static SlottedPage wrap(Page page) {
        ByteBuffer b = page.getBuffer();
        short flags = b.getShort(FLAGS_OFFSET);
        boolean needsInit = (flags & FLAG_INITIALIZED) == 0 && b.getShort(FREE_SPACE_PTR_OFFSET) == 0;
        return new SlottedPage(page, needsInit);
    }

    private void initPage() {
        buf.putLong(Page.LSN_OFFSET, 0L);
        buf.putShort(FLAGS_OFFSET, FLAG_INITIALIZED);
        buf.putShort(SLOT_COUNT_OFFSET, (short) 0);
        buf.putShort(FREE_SPACE_PTR_OFFSET, (short) page.getPageSize());
        buf.putShort(FRAGMENTED_BYTES_OFFSET, (short) 0);
        buf.putInt(PREV_PAGE_ID_OFFSET, -1);
        buf.putInt(NEXT_PAGE_ID_OFFSET, -1);
    }

    public Page getPage() {
        return page;
    }

    public int getPageId() {
        return page.getPageId();
    }

    public long getPageLsn() {
        return page.getPageLsn();
    }

    public void setPageLsn(long lsn) {
        page.setPageLsn(lsn);
    }

    public int getPrevPageId() {
        return buf.getInt(PREV_PAGE_ID_OFFSET);
    }

    public void setPrevPageId(int prevPageId) {
        buf.putInt(PREV_PAGE_ID_OFFSET, prevPageId);
    }

    public int getNextPageId() {
        return buf.getInt(NEXT_PAGE_ID_OFFSET);
    }

    public void setNextPageId(int nextPageId) {
        buf.putInt(NEXT_PAGE_ID_OFFSET, nextPageId);
    }

    public int getSlotCount() {
        return Short.toUnsignedInt(buf.getShort(SLOT_COUNT_OFFSET));
    }

    private void setSlotCount(int count) {
        buf.putShort(SLOT_COUNT_OFFSET, (short) count);
    }

    public int getFreeSpacePointer() {
        return Short.toUnsignedInt(buf.getShort(FREE_SPACE_PTR_OFFSET));
    }

    private void setFreeSpacePointer(int ptr) {
        buf.putShort(FREE_SPACE_PTR_OFFSET, (short) ptr);
    }

    public int getFragmentedBytes() {
        return Short.toUnsignedInt(buf.getShort(FRAGMENTED_BYTES_OFFSET));
    }

    private void setFragmentedBytes(int bytes) {
        buf.putShort(FRAGMENTED_BYTES_OFFSET, (short) bytes);
    }

    public int getSlotArrayEnd() {
        return HEADER_SIZE + getSlotCount() * SLOT_SIZE;
    }

    public int getContiguousFreeSpace() {
        return Math.max(0, getFreeSpacePointer() - getSlotArrayEnd());
    }

    public int getTotalFreeSpace() {
        return getContiguousFreeSpace() + getFragmentedBytes();
    }

    public boolean canInsert(int recordLength) {
        int reusableSlot = findReusableSlot();
        int extraSlotBytes = (reusableSlot >= 0) ? 0 : SLOT_SIZE;
        return recordLength + extraSlotBytes <= getTotalFreeSpace();
    }

    /**
     * Inserts a tuple into the page, reusing a deleted slot if available or compacting if needed.
     *
     * @param record tuple bytes
     * @return assigned {@code slotId}, or {@code -1} if insufficient space
     */
    public int insert(byte[] record) {
        if (record == null) {
            throw new IllegalArgumentException("Record must not be null");
        }
        int recordLen = record.length;
        int slotId = findReusableSlot();
        int extraSlotBytes = (slotId >= 0) ? 0 : SLOT_SIZE;

        if (recordLen + extraSlotBytes > getTotalFreeSpace()) {
            return -1;
        }
        if (recordLen + extraSlotBytes > getContiguousFreeSpace()) {
            compact();
        }
        if (slotId < 0) {
            slotId = getSlotCount();
            setSlotCount(slotId + 1);
        }
        int newFreePtr = getFreeSpacePointer() - recordLen;
        setFreeSpacePointer(newFreePtr);
        writeBytes(newFreePtr, record);
        setSlot(slotId, newFreePtr, recordLen);
        return slotId;
    }

    public RecordId insertRecord(byte[] record) {
        int slotId = insert(record);
        return slotId < 0 ? null : new RecordId(getPageId(), slotId);
    }

    public boolean isValidSlot(int slotId) {
        if (slotId < 0 || slotId >= getSlotCount()) {
            return false;
        }
        return getSlotOffsetRaw(slotId) != DELETED_SLOT_OFFSET;
    }

    public byte[] get(int slotId) {
        checkSlotBounds(slotId);
        short rawOffset = getSlotOffsetRaw(slotId);
        if (rawOffset == DELETED_SLOT_OFFSET) {
            return null;
        }
        int offset = Short.toUnsignedInt(rawOffset);
        int length = getSlotLength(slotId);
        byte[] data = new byte[length];
        ByteBuffer dup = buf.duplicate();
        dup.position(offset);
        dup.get(data);
        return data;
    }

    public boolean delete(int slotId) {
        checkSlotBounds(slotId);
        short rawOffset = getSlotOffsetRaw(slotId);
        if (rawOffset == DELETED_SLOT_OFFSET) {
            return false;
        }
        int offset = Short.toUnsignedInt(rawOffset);
        int length = getSlotLength(slotId);
        setSlotDeleted(slotId);

        if (offset == getFreeSpacePointer()) {
            setFreeSpacePointer(offset + length);
        } else {
            setFragmentedBytes(getFragmentedBytes() + length);
        }
        return true;
    }

    /**
     * Updates an existing slot in place if {@code newData.length <= oldLength}, or relocates it
     * within the page (compacting if required) while preserving {@code slotId}.
     *
     * @return {@code true} if updated in this page, {@code false} if insufficient free space
     */
    public boolean update(int slotId, byte[] newData) {
        if (newData == null) {
            throw new IllegalArgumentException("Updated record must not be null");
        }
        checkSlotBounds(slotId);
        short rawOffset = getSlotOffsetRaw(slotId);
        if (rawOffset == DELETED_SLOT_OFFSET) {
            throw new IllegalArgumentException("Cannot update deleted slot " + slotId);
        }
        int oldOffset = Short.toUnsignedInt(rawOffset);
        int oldLength = getSlotLength(slotId);
        int newLength = newData.length;

        if (newLength <= oldLength) {
            writeBytes(oldOffset, newData);
            setSlot(slotId, oldOffset, newLength);
            int diff = oldLength - newLength;
            if (diff > 0) {
                setFragmentedBytes(getFragmentedBytes() + diff);
            }
            return true;
        }

        int additionalNeeded = newLength - oldLength;
        if (additionalNeeded > getTotalFreeSpace()) {
            return false;
        }

        // Reclaim old tuple space and place new tuple
        if (oldOffset == getFreeSpacePointer()) {
            setFreeSpacePointer(oldOffset + oldLength);
        } else {
            setFragmentedBytes(getFragmentedBytes() + oldLength);
        }
        setSlotDeleted(slotId);

        if (newLength > getContiguousFreeSpace()) {
            compact();
        }
        int newFreePtr = getFreeSpacePointer() - newLength;
        setFreeSpacePointer(newFreePtr);
        writeBytes(newFreePtr, newData);
        setSlot(slotId, newFreePtr, newLength);
        return true;
    }

    /**
     * Compacts all live tuples toward the end of the page, eliminating fragmented space
     * while strictly preserving every tuple's {@code slotId}.
     */
    public void compact() {
        int pageSize = page.getPageSize();
        int slotCount = getSlotCount();
        byte[] temp = new byte[pageSize];
        ByteBuffer tempBuf = ByteBuffer.wrap(temp).order(ByteOrder.BIG_ENDIAN);

        int writeCursor = pageSize;
        for (int slotId = 0; slotId < slotCount; slotId++) {
            short rawOffset = getSlotOffsetRaw(slotId);
            if (rawOffset != DELETED_SLOT_OFFSET) {
                int oldOffset = Short.toUnsignedInt(rawOffset);
                int length = getSlotLength(slotId);
                writeCursor -= length;
                ByteBuffer src = buf.duplicate();
                src.position(oldOffset).limit(oldOffset + length);
                tempBuf.position(writeCursor);
                tempBuf.put(src);
                setSlot(slotId, writeCursor, length);
            }
        }

        int liveBytes = pageSize - writeCursor;
        if (liveBytes > 0) {
            ByteBuffer dst = buf.duplicate();
            dst.position(writeCursor);
            dst.put(temp, writeCursor, liveBytes);
        }
        setFreeSpacePointer(writeCursor);
        setFragmentedBytes(0);
    }

    private int findReusableSlot() {
        int slotCount = getSlotCount();
        for (int i = 0; i < slotCount; i++) {
            if (getSlotOffsetRaw(i) == DELETED_SLOT_OFFSET) {
                return i;
            }
        }
        return -1;
    }

    private void checkSlotBounds(int slotId) {
        if (slotId < 0 || slotId >= getSlotCount()) {
            throw new IllegalArgumentException("Slot ID out of bounds: " + slotId + " (slotCount=" + getSlotCount() + ")");
        }
    }

    private int slotEntryOffset(int slotId) {
        return HEADER_SIZE + slotId * SLOT_SIZE;
    }

    private short getSlotOffsetRaw(int slotId) {
        return buf.getShort(slotEntryOffset(slotId));
    }

    private int getSlotLength(int slotId) {
        return Short.toUnsignedInt(buf.getShort(slotEntryOffset(slotId) + 2));
    }

    private void setSlot(int slotId, int tupleOffset, int tupleLength) {
        int pos = slotEntryOffset(slotId);
        buf.putShort(pos, (short) tupleOffset);
        buf.putShort(pos + 2, (short) tupleLength);
    }

    private void setSlotDeleted(int slotId) {
        int pos = slotEntryOffset(slotId);
        buf.putShort(pos, DELETED_SLOT_OFFSET);
        buf.putShort(pos + 2, (short) 0);
    }

    private void writeBytes(int offset, byte[] data) {
        ByteBuffer dup = buf.duplicate();
        dup.position(offset);
        dup.put(data);
    }
}

