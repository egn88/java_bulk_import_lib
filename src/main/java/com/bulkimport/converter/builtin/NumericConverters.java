package com.bulkimport.converter.builtin;

import com.bulkimport.config.NullHandling;
import com.bulkimport.converter.TypeConverter;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Converters for numeric types.
 */
public final class NumericConverters {

    private NumericConverters() {
    }

    public static class IntegerConverter implements TypeConverter<Integer> {
        @Override
        public String toCsvValue(Integer value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.toString();
        }

        @Override
        public Class<Integer> supportedType() {
            return Integer.class;
        }
    }

    public static class LongConverter implements TypeConverter<Long> {
        @Override
        public String toCsvValue(Long value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.toString();
        }

        @Override
        public Class<Long> supportedType() {
            return Long.class;
        }
    }

    public static class DoubleConverter implements TypeConverter<Double> {
        @Override
        public String toCsvValue(Double value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            // Handle special cases
            if (value.isNaN()) {
                return "NaN";
            }
            if (value.isInfinite()) {
                return value > 0 ? "Infinity" : "-Infinity";
            }
            return value.toString();
        }

        @Override
        public Class<Double> supportedType() {
            return Double.class;
        }
    }

    public static class FloatConverter implements TypeConverter<Float> {
        @Override
        public String toCsvValue(Float value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            // Handle special cases
            if (value.isNaN()) {
                return "NaN";
            }
            if (value.isInfinite()) {
                return value > 0 ? "Infinity" : "-Infinity";
            }
            return value.toString();
        }

        @Override
        public Class<Float> supportedType() {
            return Float.class;
        }
    }

    public static class BigDecimalConverter implements TypeConverter<BigDecimal> {
        @Override
        public String toCsvValue(BigDecimal value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.toPlainString();
        }

        @Override
        public Class<BigDecimal> supportedType() {
            return BigDecimal.class;
        }
    }

    public static class BigIntegerConverter implements TypeConverter<BigInteger> {
        @Override
        public String toCsvValue(BigInteger value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.toString();
        }

        @Override
        public Class<BigInteger> supportedType() {
            return BigInteger.class;
        }
    }

    public static class ShortConverter implements TypeConverter<Short> {
        @Override
        public String toCsvValue(Short value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.toString();
        }

        @Override
        public Class<Short> supportedType() {
            return Short.class;
        }
    }

    public static class ByteConverter implements TypeConverter<Byte> {
        @Override
        public String toCsvValue(Byte value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.toString();
        }

        @Override
        public Class<Byte> supportedType() {
            return Byte.class;
        }
    }
}
