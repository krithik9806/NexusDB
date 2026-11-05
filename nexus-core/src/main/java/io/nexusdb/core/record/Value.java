package io.nexusdb.core.record;

import java.util.Objects;

/**
 * Sealed representation of a runtime column value in NexusDB.
 *
 * <p>Natural ordering sorts {@link NullValue} before any non-null value, and compares
 * matching runtime types by their underlying values. Comparing incompatible non-null types
 * throws {@link IllegalArgumentException}.
 */
public sealed interface Value extends Comparable<Value>
        permits Value.IntValue,
                Value.BigIntValue,
                Value.DoubleValue,
                Value.BooleanValue,
                Value.VarcharValue,
                Value.NullValue {

    NullValue NULL = new NullValue();

    default boolean isNull() {
        return this instanceof NullValue;
    }

    DataType dataType();

    static IntValue ofInt(int value) {
        return new IntValue(value);
    }

    static BigIntValue ofBigInt(long value) {
        return new BigIntValue(value);
    }

    static DoubleValue ofDouble(double value) {
        return new DoubleValue(value);
    }

    static BooleanValue ofBoolean(boolean value) {
        return new BooleanValue(value);
    }

    static VarcharValue ofVarchar(String value) {
        return new VarcharValue(value);
    }

    static NullValue ofNull() {
        return NULL;
    }

    record IntValue(int value) implements Value {
        @Override
        public DataType dataType() {
            return DataType.INT;
        }

        @Override
        public int compareTo(Value other) {
            if (other instanceof NullValue) {
                return 1;
            }
            if (other instanceof IntValue(int v)) {
                return Integer.compare(this.value, v);
            }
            if (other instanceof BigIntValue(long v)) {
                return Long.compare(this.value, v);
            }
            throw new IllegalArgumentException("Cannot compare INT with " + other.dataType());
        }
    }

    record BigIntValue(long value) implements Value {
        @Override
        public DataType dataType() {
            return DataType.BIGINT;
        }

        @Override
        public int compareTo(Value other) {
            if (other instanceof NullValue) {
                return 1;
            }
            if (other instanceof BigIntValue(long v)) {
                return Long.compare(this.value, v);
            }
            if (other instanceof IntValue(int v)) {
                return Long.compare(this.value, v);
            }
            throw new IllegalArgumentException("Cannot compare BIGINT with " + other.dataType());
        }
    }

    record DoubleValue(double value) implements Value {
        @Override
        public DataType dataType() {
            return DataType.DOUBLE;
        }

        @Override
        public int compareTo(Value other) {
            if (other instanceof NullValue) {
                return 1;
            }
            if (other instanceof DoubleValue(double v)) {
                return Double.compare(this.value, v);
            }
            throw new IllegalArgumentException("Cannot compare DOUBLE with " + other.dataType());
        }
    }

    record BooleanValue(boolean value) implements Value {
        @Override
        public DataType dataType() {
            return DataType.BOOLEAN;
        }

        @Override
        public int compareTo(Value other) {
            if (other instanceof NullValue) {
                return 1;
            }
            if (other instanceof BooleanValue(boolean v)) {
                return Boolean.compare(this.value, v);
            }
            throw new IllegalArgumentException("Cannot compare BOOLEAN with " + other.dataType());
        }
    }

    record VarcharValue(String value) implements Value {
        public VarcharValue {
            Objects.requireNonNull(value, "VarcharValue string must not be null (use NullValue)");
        }

        @Override
        public DataType dataType() {
            return DataType.VARCHAR;
        }

        @Override
        public int compareTo(Value other) {
            if (other instanceof NullValue) {
                return 1;
            }
            if (other instanceof VarcharValue(String v)) {
                return this.value.compareTo(v);
            }
            throw new IllegalArgumentException("Cannot compare VARCHAR with " + other.dataType());
        }
    }

    record NullValue() implements Value {
        @Override
        public DataType dataType() {
            return null;
        }

        @Override
        public int compareTo(Value other) {
            return (other instanceof NullValue) ? 0 : -1;
        }
    }
}
