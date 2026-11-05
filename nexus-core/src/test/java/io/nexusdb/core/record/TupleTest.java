package io.nexusdb.core.record;

import io.nexusdb.core.page.RecordId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class TupleTest {

    private static Schema sampleSchema() {
        return new Schema(List.of(
                Column.primaryKey("id", DataType.INT),
                new Column("account_id", DataType.BIGINT, false),
                new Column("balance", DataType.DOUBLE, true),
                new Column("active", DataType.BOOLEAN, false),
                Column.varchar("username", 16, false),
                Column.varchar("bio", 64, true),
                new Column("loyalty_pts", DataType.INT, true),
                new Column("tier_score", DataType.DOUBLE, true),
                Column.varchar("notes", 32, true) // 9th column exercises 2-byte null bitmap
        ));
    }

    @Test
    void roundTripAllDataTypesAndCaseInsensitiveSchemaLookup() {
        Schema schema = sampleSchema();
        assertEquals(0, schema.getColumnIndex("ID"));
        assertEquals(4, schema.getColumnIndex("UserName"));
        assertEquals(0, schema.getPrimaryKeyIndex());

        Tuple tuple = new Tuple(
                List.of(
                        Value.ofInt(42),
                        Value.ofBigInt(9_876_543_210L),
                        Value.ofDouble(12345.6789),
                        Value.ofBoolean(true),
                        Value.ofVarchar("krithik"),
                        Value.ofVarchar("Database engineer & systems builder 🚀"),
                        Value.ofInt(-99),
                        Value.ofDouble(-0.0001),
                        Value.ofVarchar("tier-gold")),
                new RecordId(3, 7));

        byte[] bytes = tuple.toBytes(schema);
        assertEquals(tuple.serializedSize(schema), bytes.length);

        Tuple decoded = Tuple.fromBytes(schema, bytes, tuple.getRecordId());
        assertEquals(tuple, decoded);
        assertEquals(new RecordId(3, 7), decoded.getRecordId());
        for (int i = 0; i < schema.getColumnCount(); i++) {
            assertEquals(0, tuple.getValue(i).compareTo(decoded.getValue(i)));
        }
    }

    @Test
    void nullBitmapSavesSpaceAndPreservesNullPositions() {
        Schema schema = sampleSchema();
        Tuple withNulls = new Tuple(
                Value.ofInt(1),
                Value.ofBigInt(100L),
                Value.NULL,
                Value.ofBoolean(false),
                Value.ofVarchar("alice"),
                Value.NULL,
                Value.NULL,
                Value.NULL,
                Value.NULL);

        byte[] bytes = withNulls.toBytes(schema);
        // 2-byte bitmap + 4 (id) + 8 (account_id) + 1 (active) + (2 + 5) (username) = 22 bytes
        assertEquals(22, bytes.length);

        Tuple decoded = Tuple.fromBytes(schema, bytes);
        assertEquals(withNulls, decoded);
        assertTrue(decoded.getValue(2).isNull());
        assertTrue(decoded.getValue(5).isNull());
        assertTrue(decoded.getValue(6).isNull());
        assertTrue(decoded.getValue(7).isNull());
        assertTrue(decoded.getValue(8).isNull());
    }

    @Test
    void enforcesMaxLengthNullabilityAndTypeValidation() {
        Schema schema = sampleSchema();

        // Exceeds VARCHAR(16) on username
        Tuple tooLong = new Tuple(
                Value.ofInt(1),
                Value.ofBigInt(10L),
                Value.NULL,
                Value.ofBoolean(true),
                Value.ofVarchar("this_username_is_way_too_long"),
                Value.NULL,
                Value.NULL,
                Value.NULL,
                Value.NULL);
        assertThrows(IllegalArgumentException.class, () -> tooLong.toBytes(schema));

        // NULL in NOT NULL primary key column
        Tuple nullPk = new Tuple(
                Value.NULL,
                Value.ofBigInt(10L),
                Value.NULL,
                Value.ofBoolean(true),
                Value.ofVarchar("bob"),
                Value.NULL,
                Value.NULL,
                Value.NULL,
                Value.NULL);
        assertThrows(IllegalArgumentException.class, () -> nullPk.toBytes(schema));

        // Wrong type for column 0 (VARCHAR instead of INT)
        Tuple wrongType = new Tuple(
                Value.ofVarchar("not-an-int"),
                Value.ofBigInt(10L),
                Value.NULL,
                Value.ofBoolean(true),
                Value.ofVarchar("bob"),
                Value.NULL,
                Value.NULL,
                Value.NULL,
                Value.NULL);
        assertThrows(IllegalArgumentException.class, () -> wrongType.toBytes(schema));

        // Wrong column count
        Tuple tooFew = new Tuple(Value.ofInt(1));
        assertThrows(IllegalArgumentException.class, () -> tooFew.toBytes(schema));
    }

    @Test
    void randomizedTupleSerializationRoundTrips() {
        Schema schema = sampleSchema();
        Random rng = new Random(2026L);

        for (int i = 0; i < 2000; i++) {
            List<Value> vals = new ArrayList<>(schema.getColumnCount());
            vals.add(Value.ofInt(rng.nextInt()));
            vals.add(Value.ofBigInt(rng.nextLong()));
            vals.add(rng.nextBoolean() ? Value.NULL : Value.ofDouble(rng.nextDouble() * 1e6));
            vals.add(Value.ofBoolean(rng.nextBoolean()));
            vals.add(Value.ofVarchar(randomString(rng, 16)));
            vals.add(rng.nextBoolean() ? Value.NULL : Value.ofVarchar(randomString(rng, 64)));
            vals.add(rng.nextBoolean() ? Value.NULL : Value.ofInt(rng.nextInt()));
            vals.add(rng.nextBoolean() ? Value.NULL : Value.ofDouble(rng.nextGaussian()));
            vals.add(rng.nextBoolean() ? Value.NULL : Value.ofVarchar(randomString(rng, 32)));

            Tuple original = new Tuple(vals);
            byte[] encoded = original.toBytes(schema);
            assertEquals(original.serializedSize(schema), encoded.length);
            Tuple restored = Tuple.fromBytes(schema, encoded);
            assertEquals(original, restored);
        }
    }

    private static String randomString(Random rng, int maxLen) {
        int len = rng.nextInt(maxLen + 1);
        StringBuilder sb = new StringBuilder(len);
        String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_αβγ";
        for (int i = 0; i < len; i++) {
            sb.append(alphabet.charAt(rng.nextInt(alphabet.length())));
        }
        return sb.toString();
    }
}
