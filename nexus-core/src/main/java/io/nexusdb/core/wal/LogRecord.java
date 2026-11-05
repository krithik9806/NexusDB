package io.nexusdb.core.wal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * Immutable Write-Ahead Log (WAL) record with big-endian binary serialization and CRC32 framing.
 *
 * <p>On-Disk Record Framing (Big-Endian):
 * <ul>
 *   <li>Bytes {@code 0..3}: {@link #MAGIC} ({@code 0x57414C52}, ASCII {@code "WALR"})</li>
 *   <li>Bytes {@code 4..7}: {@code payloadLength} ({@code int}, bytes following the 12-byte header)</li>
 *   <li>Bytes {@code 8..11}: {@code crc32} ({@code int}, CRC32 over {@code magic + payloadLength + payload})</li>
 *   <li>Bytes {@code 12..12+payloadLength-1}: record payload ({@code lsn}, {@code txnId}, {@code prevLsn},
 *       {@code type}, and type-specific fields)</li>
 * </ul>
 */
public final class LogRecord {
    public static final int MAGIC = 0x57414C52; // "WALR"
    public static final int HEADER_SIZE = 12;
    public static final int MIN_PAYLOAD_SIZE = 25; // lsn(8) + txnId(8) + prevLsn(8) + type(1)
    public static final int MAX_PAYLOAD_SIZE = 16 * 1024 * 1024; // 16 MB sanity limit
    public static final long INVALID_LSN = 0L;

    private static final byte[] EMPTY_BYTES = new byte[0];

    private final long lsn;
    private final long txnId;
    private final long prevLsn;
    private final LogRecordType type;
    private final String tableName;
    private final int pageId;
    private final int slotId;
    private final byte[] beforeImage;
    private final byte[] afterImage;
    private final long undoNextLsn;
    private final Map<Integer, Long> dirtyPageTable;
    private final Map<Long, Long> activeTransactionTable;

    public LogRecord(
            long lsn,
            long txnId,
            long prevLsn,
            LogRecordType type,
            String tableName,
            int pageId,
            int slotId,
            byte[] beforeImage,
            byte[] afterImage,
            long undoNextLsn,
            Map<Integer, Long> dirtyPageTable,
            Map<Long, Long> activeTransactionTable) {
        this.lsn = lsn;
        this.txnId = txnId;
        this.prevLsn = prevLsn;
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.tableName = tableName == null ? "" : tableName;
        this.pageId = pageId;
        this.slotId = slotId;
        this.beforeImage = beforeImage == null || beforeImage.length == 0
                ? EMPTY_BYTES
                : Arrays.copyOf(beforeImage, beforeImage.length);
        this.afterImage = afterImage == null || afterImage.length == 0
                ? EMPTY_BYTES
                : Arrays.copyOf(afterImage, afterImage.length);
        this.undoNextLsn = undoNextLsn;
        this.dirtyPageTable = dirtyPageTable == null || dirtyPageTable.isEmpty()
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(dirtyPageTable));
        this.activeTransactionTable = activeTransactionTable == null || activeTransactionTable.isEmpty()
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(activeTransactionTable));
    }

    public static LogRecord begin(long lsn, long txnId) {
        return new LogRecord(
                lsn, txnId, INVALID_LSN, LogRecordType.BEGIN, "", -1, -1, EMPTY_BYTES, EMPTY_BYTES, INVALID_LSN, Map.of(), Map.of());
    }

    public static LogRecord commit(long lsn, long txnId, long prevLsn) {
        return new LogRecord(
                lsn, txnId, prevLsn, LogRecordType.COMMIT, "", -1, -1, EMPTY_BYTES, EMPTY_BYTES, INVALID_LSN, Map.of(), Map.of());
    }

    public static LogRecord abort(long lsn, long txnId, long prevLsn) {
        return new LogRecord(
                lsn, txnId, prevLsn, LogRecordType.ABORT, "", -1, -1, EMPTY_BYTES, EMPTY_BYTES, INVALID_LSN, Map.of(), Map.of());
    }

    public static LogRecord insert(
            long lsn,
            long txnId,
            long prevLsn,
            String tableName,
            int pageId,
            int slotId,
            byte[] afterImage) {
        Objects.requireNonNull(afterImage, "afterImage must not be null for INSERT");
        return new LogRecord(
                lsn, txnId, prevLsn, LogRecordType.INSERT, tableName, pageId, slotId, EMPTY_BYTES, afterImage, INVALID_LSN, Map.of(), Map.of());
    }

    public static LogRecord delete(
            long lsn,
            long txnId,
            long prevLsn,
            String tableName,
            int pageId,
            int slotId,
            byte[] beforeImage) {
        Objects.requireNonNull(beforeImage, "beforeImage must not be null for DELETE");
        return new LogRecord(
                lsn, txnId, prevLsn, LogRecordType.DELETE, tableName, pageId, slotId, beforeImage, EMPTY_BYTES, INVALID_LSN, Map.of(), Map.of());
    }

    public static LogRecord update(
            long lsn,
            long txnId,
            long prevLsn,
            String tableName,
            int pageId,
            int slotId,
            byte[] beforeImage,
            byte[] afterImage) {
        Objects.requireNonNull(beforeImage, "beforeImage must not be null for UPDATE");
        Objects.requireNonNull(afterImage, "afterImage must not be null for UPDATE");
        return new LogRecord(
                lsn, txnId, prevLsn, LogRecordType.UPDATE, tableName, pageId, slotId, beforeImage, afterImage, INVALID_LSN, Map.of(), Map.of());
    }

    /**
     * Creates a full-page physical write log record ({@code slotId == -1}) used for structural page
     * changes such as heap page-chain linking, catalog updates, and B+Tree node modifications.
     */
    public static LogRecord pageWrite(
            long lsn,
            long txnId,
            long prevLsn,
            int pageId,
            byte[] beforeImage,
            byte[] afterImage) {
        Objects.requireNonNull(afterImage, "afterImage must not be null for pageWrite");
        return new LogRecord(
                lsn,
                txnId,
                prevLsn,
                LogRecordType.UPDATE,
                "",
                pageId,
                -1,
                beforeImage == null ? EMPTY_BYTES : beforeImage,
                afterImage,
                INVALID_LSN,
                Map.of(),
                Map.of());
    }

    public static LogRecord clr(
            long lsn,
            long txnId,
            long prevLsn,
            String tableName,
            int pageId,
            int slotId,
            byte[] afterImage,
            long undoNextLsn) {
        return new LogRecord(
                lsn,
                txnId,
                prevLsn,
                LogRecordType.CLR,
                tableName,
                pageId,
                slotId,
                EMPTY_BYTES,
                afterImage == null ? EMPTY_BYTES : afterImage,
                undoNextLsn,
                Map.of(),
                Map.of());
    }

    public static LogRecord checkpointBegin(long lsn) {
        return new LogRecord(
                lsn, 0L, INVALID_LSN, LogRecordType.CHECKPOINT_BEGIN, "", -1, -1, EMPTY_BYTES, EMPTY_BYTES, INVALID_LSN, Map.of(), Map.of());
    }

    public static LogRecord checkpointEnd(
            long lsn,
            Map<Integer, Long> dirtyPageTable,
            Map<Long, Long> activeTransactionTable) {
        return new LogRecord(
                lsn,
                0L,
                INVALID_LSN,
                LogRecordType.CHECKPOINT_END,
                "",
                -1,
                -1,
                EMPTY_BYTES,
                EMPTY_BYTES,
                INVALID_LSN,
                dirtyPageTable,
                activeTransactionTable);
    }

    public LogRecord withLsn(long newLsn) {
        if (this.lsn == newLsn) {
            return this;
        }
        return new LogRecord(
                newLsn,
                txnId,
                prevLsn,
                type,
                tableName,
                pageId,
                slotId,
                beforeImage,
                afterImage,
                undoNextLsn,
                dirtyPageTable,
                activeTransactionTable);
    }

    public long lsn() {
        return lsn;
    }

    public long txnId() {
        return txnId;
    }

    public long prevLsn() {
        return prevLsn;
    }

    public LogRecordType type() {
        return type;
    }

    public String tableName() {
        return tableName;
    }

    public int pageId() {
        return pageId;
    }

    public int slotId() {
        return slotId;
    }

    public byte[] beforeImage() {
        return beforeImage.length == 0 ? EMPTY_BYTES : Arrays.copyOf(beforeImage, beforeImage.length);
    }

    public byte[] afterImage() {
        return afterImage.length == 0 ? EMPTY_BYTES : Arrays.copyOf(afterImage, afterImage.length);
    }

    public long undoNextLsn() {
        return undoNextLsn;
    }

    public Map<Integer, Long> dirtyPageTable() {
        return dirtyPageTable;
    }

    public Map<Long, Long> activeTransactionTable() {
        return activeTransactionTable;
    }

    /**
     * Serializes this record including its 12-byte framing header and CRC32 checksum.
     */
    public byte[] toBytes() {
        byte[] tableBytes = tableName.getBytes(StandardCharsets.UTF_8);
        int payloadLen = MIN_PAYLOAD_SIZE;
        switch (type) {
            case BEGIN, COMMIT, ABORT, CHECKPOINT_BEGIN -> {
                // No additional payload fields
            }
            case INSERT -> payloadLen += 2 + tableBytes.length + 4 + 4 + 4 + afterImage.length;
            case DELETE -> payloadLen += 2 + tableBytes.length + 4 + 4 + 4 + beforeImage.length;
            case UPDATE -> payloadLen += 2 + tableBytes.length + 4 + 4 + 4 + beforeImage.length + 4 + afterImage.length;
            case CLR -> payloadLen += 2 + tableBytes.length + 4 + 4 + 8 + 4 + afterImage.length;
            case CHECKPOINT_END -> payloadLen += 4 + dirtyPageTable.size() * 12 + 4 + activeTransactionTable.size() * 16;
        }

        byte[] out = new byte[HEADER_SIZE + payloadLen];
        ByteBuffer buf = ByteBuffer.wrap(out).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(MAGIC);
        buf.putInt(payloadLen);
        buf.putInt(0); // CRC32 placeholder at offset 8..11

        buf.putLong(lsn);
        buf.putLong(txnId);
        buf.putLong(prevLsn);
        buf.put((byte) type.ordinal());

        switch (type) {
            case BEGIN, COMMIT, ABORT, CHECKPOINT_BEGIN -> {
                // No additional fields
            }
            case INSERT -> {
                putStringBytes(buf, tableBytes);
                buf.putInt(pageId);
                buf.putInt(slotId);
                buf.putInt(afterImage.length);
                buf.put(afterImage);
            }
            case DELETE -> {
                putStringBytes(buf, tableBytes);
                buf.putInt(pageId);
                buf.putInt(slotId);
                buf.putInt(beforeImage.length);
                buf.put(beforeImage);
            }
            case UPDATE -> {
                putStringBytes(buf, tableBytes);
                buf.putInt(pageId);
                buf.putInt(slotId);
                buf.putInt(beforeImage.length);
                buf.put(beforeImage);
                buf.putInt(afterImage.length);
                buf.put(afterImage);
            }
            case CLR -> {
                putStringBytes(buf, tableBytes);
                buf.putInt(pageId);
                buf.putInt(slotId);
                buf.putLong(undoNextLsn);
                buf.putInt(afterImage.length);
                buf.put(afterImage);
            }
            case CHECKPOINT_END -> {
                buf.putInt(dirtyPageTable.size());
                for (Map.Entry<Integer, Long> entry : dirtyPageTable.entrySet()) {
                    buf.putInt(entry.getKey());
                    buf.putLong(entry.getValue());
                }
                buf.putInt(activeTransactionTable.size());
                for (Map.Entry<Long, Long> entry : activeTransactionTable.entrySet()) {
                    buf.putLong(entry.getKey());
                    buf.putLong(entry.getValue());
                }
            }
        }

        int crc = computeCrc32(out, payloadLen);
        buf.putInt(8, crc);
        return out;
    }

    /**
     * Deserializes a framed {@link LogRecord} from {@code bytes}, verifying magic number,
     * payload length, and CRC32 checksum.
     */
    public static LogRecord fromBytes(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        if (bytes.length < HEADER_SIZE + MIN_PAYLOAD_SIZE) {
            throw new IllegalArgumentException("LogRecord buffer too short: " + bytes.length + " bytes");
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        int magic = buf.getInt(0);
        if (magic != MAGIC) {
            throw new IllegalArgumentException(String.format("Invalid WAL record magic: 0x%08X", magic));
        }
        int payloadLen = buf.getInt(4);
        if (payloadLen < MIN_PAYLOAD_SIZE || payloadLen > MAX_PAYLOAD_SIZE || HEADER_SIZE + payloadLen > bytes.length) {
            throw new IllegalArgumentException("Invalid WAL payload length: " + payloadLen);
        }
        int expectedCrc = buf.getInt(8);
        int actualCrc = computeCrc32(bytes, payloadLen);
        if (expectedCrc != actualCrc) {
            throw new IllegalArgumentException(
                    String.format("WAL record CRC32 mismatch: expected 0x%08X, got 0x%08X", expectedCrc, actualCrc));
        }
        return deserializePayload(buf, HEADER_SIZE, payloadLen);
    }

    static LogRecord deserializePayload(ByteBuffer buf, int payloadOffset, int payloadLen) {
        ByteBuffer slice = buf.duplicate().order(ByteOrder.BIG_ENDIAN);
        slice.position(payloadOffset).limit(payloadOffset + payloadLen);

        long lsn = slice.getLong();
        long txnId = slice.getLong();
        long prevLsn = slice.getLong();
        LogRecordType type = LogRecordType.fromByte(Byte.toUnsignedInt(slice.get()));

        return switch (type) {
            case BEGIN -> begin(lsn, txnId);
            case COMMIT -> commit(lsn, txnId, prevLsn);
            case ABORT -> abort(lsn, txnId, prevLsn);
            case CHECKPOINT_BEGIN -> checkpointBegin(lsn);
            case INSERT -> {
                String table = readString(slice);
                int pageId = slice.getInt();
                int slotId = slice.getInt();
                byte[] after = readByteArray(slice);
                yield insert(lsn, txnId, prevLsn, table, pageId, slotId, after);
            }
            case DELETE -> {
                String table = readString(slice);
                int pageId = slice.getInt();
                int slotId = slice.getInt();
                byte[] before = readByteArray(slice);
                yield delete(lsn, txnId, prevLsn, table, pageId, slotId, before);
            }
            case UPDATE -> {
                String table = readString(slice);
                int pageId = slice.getInt();
                int slotId = slice.getInt();
                byte[] before = readByteArray(slice);
                byte[] after = readByteArray(slice);
                yield update(lsn, txnId, prevLsn, table, pageId, slotId, before, after);
            }
            case CLR -> {
                String table = readString(slice);
                int pageId = slice.getInt();
                int slotId = slice.getInt();
                long undoNextLsn = slice.getLong();
                byte[] after = readByteArray(slice);
                yield clr(lsn, txnId, prevLsn, table, pageId, slotId, after, undoNextLsn);
            }
            case CHECKPOINT_END -> {
                int dptCount = slice.getInt();
                if (dptCount < 0 || dptCount * 12 > slice.remaining()) {
                    throw new IllegalArgumentException("Corrupted CHECKPOINT_END dptCount: " + dptCount);
                }
                Map<Integer, Long> dpt = new LinkedHashMap<>(dptCount * 2);
                for (int i = 0; i < dptCount; i++) {
                    int pid = slice.getInt();
                    long recLsn = slice.getLong();
                    dpt.put(pid, recLsn);
                }
                int attCount = slice.getInt();
                if (attCount < 0 || attCount * 16 > slice.remaining()) {
                    throw new IllegalArgumentException("Corrupted CHECKPOINT_END attCount: " + attCount);
                }
                Map<Long, Long> att = new LinkedHashMap<>(attCount * 2);
                for (int i = 0; i < attCount; i++) {
                    long tid = slice.getLong();
                    long lastLsn = slice.getLong();
                    att.put(tid, lastLsn);
                }
                yield checkpointEnd(lsn, dpt, att);
            }
        };
    }

    static int computeCrc32(byte[] framedRecord, int payloadLen) {
        CRC32 crc = new CRC32();
        crc.update(framedRecord, 0, 8); // magic + payloadLen
        crc.update(framedRecord, HEADER_SIZE, payloadLen);
        return (int) crc.getValue();
    }

    static int computeCrc32(ByteBuffer headerMagicAndLen, ByteBuffer payload) {
        CRC32 crc = new CRC32();
        ByteBuffer hDup = headerMagicAndLen.duplicate();
        hDup.position(0).limit(8);
        crc.update(hDup);
        ByteBuffer pDup = payload.duplicate();
        crc.update(pDup);
        return (int) crc.getValue();
    }

    private static void putStringBytes(ByteBuffer buf, byte[] utf8) {
        if (utf8.length > 0xFFFF) {
            throw new IllegalArgumentException("Identifier UTF-8 length exceeds 65535 bytes: " + utf8.length);
        }
        buf.putShort((short) utf8.length);
        buf.put(utf8);
    }

    private static String readString(ByteBuffer buf) {
        int len = Short.toUnsignedInt(buf.getShort());
        if (len < 0 || len > buf.remaining()) {
            throw new IllegalArgumentException("Invalid UTF-8 string length in WAL record: " + len);
        }
        if (len == 0) {
            return "";
        }
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] readByteArray(ByteBuffer buf) {
        int len = buf.getInt();
        if (len < 0 || len > buf.remaining()) {
            throw new IllegalArgumentException("Invalid byte array length in WAL record: " + len);
        }
        if (len == 0) {
            return EMPTY_BYTES;
        }
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LogRecord that)) {
            return false;
        }
        return lsn == that.lsn
                && txnId == that.txnId
                && prevLsn == that.prevLsn
                && type == that.type
                && pageId == that.pageId
                && slotId == that.slotId
                && undoNextLsn == that.undoNextLsn
                && Objects.equals(tableName, that.tableName)
                && Arrays.equals(beforeImage, that.beforeImage)
                && Arrays.equals(afterImage, that.afterImage)
                && Objects.equals(dirtyPageTable, that.dirtyPageTable)
                && Objects.equals(activeTransactionTable, that.activeTransactionTable);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                lsn, txnId, prevLsn, type, tableName, pageId, slotId, undoNextLsn, dirtyPageTable, activeTransactionTable);
        result = 31 * result + Arrays.hashCode(beforeImage);
        result = 31 * result + Arrays.hashCode(afterImage);
        return result;
    }

    @Override
    public String toString() {
        return "LogRecord{"
                + "lsn=" + lsn
                + ", txnId=" + txnId
                + ", prevLsn=" + prevLsn
                + ", type=" + type
                + ", tableName='" + tableName + '\''
                + ", pageId=" + pageId
                + ", slotId=" + slotId
                + ", beforeLen=" + beforeImage.length
                + ", afterLen=" + afterImage.length
                + ", undoNextLsn=" + undoNextLsn
                + ", dpt=" + dirtyPageTable
                + ", att=" + activeTransactionTable
                + '}';
    }
}
