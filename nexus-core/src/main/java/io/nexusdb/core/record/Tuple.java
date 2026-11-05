package io.nexusdb.core.record;

import io.nexusdb.core.page.RecordId;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Immutable row tuple holding column {@link Value}s and an optional physical {@link RecordId}.
 *
 * <p>Binary Serialization Format (Big-Endian, per {@code docs/storage-engine.md}):
 * <ul>
 *   <li>Null bitmap: {@code ceil(columnCount / 8)} bytes. Bit {@code i == 1} indicates column {@code i} is NULL.</li>
 *   <li>Column payloads in schema order for every non-null column {@code i}:
 *     <ul>
 *       <li>{@code INT}: 4-byte signed int</li>
 *       <li>{@code BIGINT}: 8-byte signed long</li>
 *       <li>{@code DOUBLE}: 8-byte IEEE 754 double</li>
 *       <li>{@code BOOLEAN}: 1-byte ({@code 0x01} = true, {@code 0x00} = false)</li>
 *       <li>{@code VARCHAR(n)}: 2-byte unsigned short UTF-8 byte length + UTF-8 bytes</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public final class Tuple {
    private final List<Value> values;
    private final RecordId recordId;

    public Tuple(List<Value> values) {
        this(values, null);
    }

    public Tuple(Value... values) {
        this(Arrays.asList(values), null);
    }

    public Tuple(List<Value> values, RecordId recordId) {
        Objects.requireNonNull(values, "values must not be null");
        List<Value> normalized = new ArrayList<>(values.size());
        for (Value v : values) {
            normalized.add(v == null ? Value.NULL : v);
        }
        this.values = List.copyOf(normalized);
        this.recordId = recordId;
    }

    public List<Value> getValues() {
        return values;
    }

    public Value getValue(int index) {
        return values.get(index);
    }

    public int size() {
        return values.size();
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public Tuple withRecordId(RecordId newRecordId) {
        return new Tuple(this.values, newRecordId);
    }

    public static int nullBitmapBytes(int columnCount) {
        return (columnCount + 7) >>> 3;
    }

    /**
     * Validates this tuple against {@code schema} and computes its exact serialized byte length.
     */
    public int serializedSize(Schema schema) {
        Objects.requireNonNull(schema, "schema must not be null");
        int colCount = schema.getColumnCount();
        if (values.size() != colCount) {
            throw new IllegalArgumentException(
                    "Tuple value count " + values.size() + " does not match schema column count " + colCount);
        }

        int totalBytes = nullBitmapBytes(colCount);
        for (int i = 0; i < colCount; i++) {
            Column col = schema.getColumn(i);
            Value val = values.get(i);

            if (val.isNull()) {
                if (!col.nullable()) {
                    throw new IllegalArgumentException(
                            "Column '" + col.name() + "' at index " + i + " is NOT NULL");
                }
                continue;
            }

            if (val.dataType() != col.type()) {
                throw new IllegalArgumentException(
                        "Type mismatch for column '" + col.name() + "' at index " + i
                                + ": expected " + col.type() + ", got " + val.dataType());
            }

            switch (val) {
                case Value.IntValue ignored -> totalBytes += 4;
                case Value.BigIntValue ignored -> totalBytes += 8;
                case Value.DoubleValue ignored -> totalBytes += 8;
                case Value.BooleanValue ignored -> totalBytes += 1;
                case Value.VarcharValue(String str) -> {
                    if (str.length() > col.maxLength()) {
                        throw new IllegalArgumentException(
                                "VARCHAR value for column '" + col.name() + "' exceeds maxLength "
                                        + col.maxLength() + " (length=" + str.length() + ")");
                    }
                    int utf8Len = str.getBytes(StandardCharsets.UTF_8).length;
                    if (utf8Len > 0xFFFF) {
                        throw new IllegalArgumentException(
                                "VARCHAR UTF-8 byte length exceeds 65535 for column '" + col.name() + "'");
                    }
                    totalBytes += 2 + utf8Len;
                }
                case Value.NullValue ignored -> {
                }
            }
        }
        return totalBytes;
    }

    /**
     * Serializes this tuple according to {@code schema}.
     */
    public byte[] toBytes(Schema schema) {
        int byteSize = serializedSize(schema);
        int colCount = schema.getColumnCount();
        int bitmapLen = nullBitmapBytes(colCount);

        byte[] result = new byte[byteSize];
        ByteBuffer buf = ByteBuffer.wrap(result).order(ByteOrder.BIG_ENDIAN);
        buf.position(bitmapLen);

        for (int i = 0; i < colCount; i++) {
            Value val = values.get(i);
            if (val.isNull()) {
                result[i >>> 3] |= (byte) (1 << (i & 7));
                continue;
            }
            switch (val) {
                case Value.IntValue(int v) -> buf.putInt(v);
                case Value.BigIntValue(long v) -> buf.putLong(v);
                case Value.DoubleValue(double v) -> buf.putDouble(v);
                case Value.BooleanValue(boolean v) -> buf.put((byte) (v ? 1 : 0));
                case Value.VarcharValue(String s) -> {
                    byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
                    buf.putShort((short) utf8.length);
                    buf.put(utf8);
                }
                case Value.NullValue ignored -> {
                }
            }
        }
        return result;
    }

    /**
     * Deserializes a tuple from its binary representation according to {@code schema}.
     */
    public static Tuple fromBytes(Schema schema, byte[] bytes) {
        return fromBytes(schema, bytes, null);
    }

    public static Tuple fromBytes(Schema schema, byte[] bytes, RecordId recordId) {
        Objects.requireNonNull(schema, "schema must not be null");
        Objects.requireNonNull(bytes, "bytes must not be null");
        int colCount = schema.getColumnCount();
        int bitmapLen = nullBitmapBytes(colCount);
        if (bytes.length < bitmapLen) {
            throw new IllegalArgumentException(
                    "Corrupted tuple bytes: length " + bytes.length + " < bitmap length " + bitmapLen);
        }

        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        buf.position(bitmapLen);
        List<Value> decoded = new ArrayList<>(colCount);

        for (int i = 0; i < colCount; i++) {
            Column col = schema.getColumn(i);
            boolean isNull = (bytes[i >>> 3] & (1 << (i & 7))) != 0;
            if (isNull) {
                if (!col.nullable()) {
                    throw new IllegalArgumentException(
                            "Corrupted tuple: NULL bit set for non-nullable column '" + col.name() + "'");
                }
                decoded.add(Value.NULL);
                continue;
            }

            switch (col.type()) {
                case INT -> decoded.add(new Value.IntValue(buf.getInt()));
                case BIGINT -> decoded.add(new Value.BigIntValue(buf.getLong()));
                case DOUBLE -> decoded.add(new Value.DoubleValue(buf.getDouble()));
                case BOOLEAN -> decoded.add(new Value.BooleanValue(buf.get() != 0));
                case VARCHAR -> {
                    int utf8Len = Short.toUnsignedInt(buf.getShort());
                    byte[] strBytes = new byte[utf8Len];
                    buf.get(strBytes);
                    String str = new String(strBytes, StandardCharsets.UTF_8);
                    if (str.length() > col.maxLength()) {
                        throw new IllegalArgumentException(
                                "Decoded VARCHAR exceeds maxLength for column '" + col.name() + "'");
                    }
                    decoded.add(new Value.VarcharValue(str));
                }
            }
        }
        if (buf.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes in serialized tuple: " + buf.remaining());
        }
        return new Tuple(decoded, recordId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Tuple that)) {
            return false;
        }
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return recordId == null ? "Tuple" + values : "Tuple@" + recordId + values;
    }
}
