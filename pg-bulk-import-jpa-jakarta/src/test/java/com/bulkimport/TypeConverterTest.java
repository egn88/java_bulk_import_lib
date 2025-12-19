package com.bulkimport;

import com.bulkimport.config.NullHandling;
import com.bulkimport.converter.TypeConverter;
import com.bulkimport.converter.TypeConverterRegistry;
import com.bulkimport.converter.builtin.BooleanConverter;
import com.bulkimport.converter.builtin.ByteArrayConverter;
import com.bulkimport.converter.builtin.DateTimeConverters;
import com.bulkimport.converter.builtin.JsonConverter;
import com.bulkimport.converter.builtin.NumericConverters;
import com.bulkimport.converter.builtin.StringConverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TypeConverterTest {

    private final TypeConverterRegistry registry = TypeConverterRegistry.getDefault();

    @Test
    void shouldConvertStrings() {
        StringConverter converter = new StringConverter();

        assertThat(converter.toCsvValue("hello", NullHandling.EMPTY_STRING))
            .isEqualTo("hello");

        assertThat(converter.toCsvValue(null, NullHandling.EMPTY_STRING))
            .isEqualTo("");

        assertThat(converter.toCsvValue(null, NullHandling.LITERAL_NULL))
            .isEqualTo("\\N");
    }

    @Test
    void shouldReturnRawStringsForFastCsvEscaping() {
        // Converters return raw values - FastCSV handles CSV escaping
        StringConverter converter = new StringConverter();

        // Commas - returned raw, FastCSV will quote
        assertThat(converter.toCsvValue("a,b,c", NullHandling.EMPTY_STRING))
            .isEqualTo("a,b,c");

        // Quotes - returned raw, FastCSV will escape
        assertThat(converter.toCsvValue("say \"hello\"", NullHandling.EMPTY_STRING))
            .isEqualTo("say \"hello\"");

        // Newlines - returned raw, FastCSV will quote
        assertThat(converter.toCsvValue("line1\nline2", NullHandling.EMPTY_STRING))
            .isEqualTo("line1\nline2");
    }

    @Test
    void shouldConvertIntegers() {
        NumericConverters.IntegerConverter converter = new NumericConverters.IntegerConverter();

        assertThat(converter.toCsvValue(42, NullHandling.EMPTY_STRING))
            .isEqualTo("42");

        assertThat(converter.toCsvValue(-100, NullHandling.EMPTY_STRING))
            .isEqualTo("-100");

        assertThat(converter.toCsvValue(null, NullHandling.EMPTY_STRING))
            .isEqualTo("");
    }

    @Test
    void shouldConvertLongs() {
        NumericConverters.LongConverter converter = new NumericConverters.LongConverter();

        assertThat(converter.toCsvValue(9223372036854775807L, NullHandling.EMPTY_STRING))
            .isEqualTo("9223372036854775807");
    }

    @Test
    void shouldConvertDoubles() {
        NumericConverters.DoubleConverter converter = new NumericConverters.DoubleConverter();

        assertThat(converter.toCsvValue(3.14159, NullHandling.EMPTY_STRING))
            .isEqualTo("3.14159");

        assertThat(converter.toCsvValue(Double.NaN, NullHandling.EMPTY_STRING))
            .isEqualTo("NaN");

        assertThat(converter.toCsvValue(Double.POSITIVE_INFINITY, NullHandling.EMPTY_STRING))
            .isEqualTo("Infinity");
    }

    @Test
    void shouldConvertBigDecimal() {
        NumericConverters.BigDecimalConverter converter = new NumericConverters.BigDecimalConverter();

        assertThat(converter.toCsvValue(new BigDecimal("123.456789"), NullHandling.EMPTY_STRING))
            .isEqualTo("123.456789");

        assertThat(converter.toCsvValue(new BigDecimal("1E+10"), NullHandling.EMPTY_STRING))
            .isEqualTo("10000000000");
    }

    @Test
    void shouldConvertBooleans() {
        BooleanConverter converter = new BooleanConverter();

        assertThat(converter.toCsvValue(true, NullHandling.EMPTY_STRING))
            .isEqualTo("true");

        assertThat(converter.toCsvValue(false, NullHandling.EMPTY_STRING))
            .isEqualTo("false");
    }

    @Test
    void shouldConvertDates() {
        DateTimeConverters.LocalDateConverter converter = new DateTimeConverters.LocalDateConverter();

        assertThat(converter.toCsvValue(LocalDate.of(2024, 1, 15), NullHandling.EMPTY_STRING))
            .isEqualTo("2024-01-15");
    }

    @Test
    void shouldConvertDateTimes() {
        DateTimeConverters.LocalDateTimeConverter converter = new DateTimeConverters.LocalDateTimeConverter();

        assertThat(converter.toCsvValue(LocalDateTime.of(2024, 1, 15, 10, 30, 45), NullHandling.EMPTY_STRING))
            .isEqualTo("2024-01-15T10:30:45");
    }

    @Test
    void shouldConvertUUIDs() {
        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");

        String result = registry.convert(uuid, NullHandling.EMPTY_STRING);

        assertThat(result).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
    }

    @Test
    void shouldConvertEnums() {
        enum Status { ACTIVE, INACTIVE, PENDING }

        String result = registry.convert(Status.ACTIVE, NullHandling.EMPTY_STRING);

        assertThat(result).isEqualTo("ACTIVE");
    }

    @Test
    void shouldConvertArrays() {
        String[] array = {"a", "b", "c"};

        String result = registry.convert(array, NullHandling.EMPTY_STRING);

        // PostgreSQL array format
        assertThat(result).isEqualTo("{a,b,c}");
    }

    @Test
    void shouldConvertLists() {
        List<String> list = List.of("x", "y", "z");

        String result = registry.convert(list, NullHandling.EMPTY_STRING);

        assertThat(result).isEqualTo("{x,y,z}");
    }

    @Test
    void shouldFindConverterForPrimitiveWrappers() {
        // Registry should handle primitive types via their wrappers
        TypeConverter<Integer> intConverter = registry.getConverter(Integer.class);
        TypeConverter<Long> longConverter = registry.getConverter(Long.class);

        assertThat(intConverter).isNotNull();
        assertThat(longConverter).isNotNull();
    }

    @Test
    void shouldAllowCustomConverters() {
        // Given
        TypeConverterRegistry customRegistry = new TypeConverterRegistry();
        customRegistry.register(Integer.class, new TypeConverter<>() {
            @Override
            public String toCsvValue(Integer value, NullHandling nullHandling) {
                return value == null ? "NULL" : "INT:" + value;
            }

            @Override
            public Class<Integer> supportedType() {
                return Integer.class;
            }
        });

        // When
        String result = customRegistry.convert(42, NullHandling.EMPTY_STRING);

        // Then
        assertThat(result).isEqualTo("INT:42");
    }

    @Test
    void shouldUseFallbackForUnknownTypes() {
        // Given - a custom class without explicit converter
        record CustomType(String value) {
            @Override
            public String toString() {
                return "custom:" + value;
            }
        }

        // When
        String result = registry.convert(new CustomType("test"), NullHandling.EMPTY_STRING);

        // Then - should use toString()
        assertThat(result).isEqualTo("custom:test");
    }

    @Nested
    class ByteArrayConverterTests {

        private final ByteArrayConverter converter = new ByteArrayConverter();

        @Test
        void shouldConvertByteArrayToHexFormat() {
            byte[] data = {0x48, 0x65, 0x6c, 0x6c, 0x6f}; // "Hello" in bytes

            String result = converter.toCsvValue(data, NullHandling.EMPTY_STRING);

            // PostgreSQL hex format with escaped backslash for CSV
            assertThat(result).isEqualTo("\\\\x48656c6c6f");
        }

        @Test
        void shouldConvertEmptyByteArray() {
            byte[] data = new byte[0];

            String result = converter.toCsvValue(data, NullHandling.EMPTY_STRING);

            assertThat(result).isEqualTo("\\\\x");
        }

        @Test
        void shouldConvertSingleByte() {
            byte[] data = {(byte) 0xFF};

            String result = converter.toCsvValue(data, NullHandling.EMPTY_STRING);

            assertThat(result).isEqualTo("\\\\xff");
        }

        @Test
        void shouldConvertZeroByte() {
            byte[] data = {0x00};

            String result = converter.toCsvValue(data, NullHandling.EMPTY_STRING);

            assertThat(result).isEqualTo("\\\\x00");
        }

        @Test
        void shouldHandleNullByteArray() {
            assertThat(converter.toCsvValue(null, NullHandling.EMPTY_STRING))
                .isEqualTo("");

            assertThat(converter.toCsvValue(null, NullHandling.LITERAL_NULL))
                .isEqualTo("\\N");

            assertThat(converter.toCsvValue(null, NullHandling.NULL_STRING))
                .isEqualTo("NULL");
        }

        @Test
        void shouldConvertBinaryData() {
            // Binary data with various byte values
            byte[] data = {0x00, 0x01, 0x7F, (byte) 0x80, (byte) 0xFE, (byte) 0xFF};

            String result = converter.toCsvValue(data, NullHandling.EMPTY_STRING);

            assertThat(result).isEqualTo("\\\\x00017f80feff");
        }
    }

    @Nested
    class JsonConverterTests {

        private final JsonConverter converter = new JsonConverter();
        private final ObjectMapper mapper = new ObjectMapper();

        @Test
        void shouldConvertJsonObject() throws Exception {
            JsonNode json = mapper.readTree("{\"name\":\"John\",\"age\":30}");

            String result = converter.toCsvValue(json, NullHandling.EMPTY_STRING);

            assertThat(result).isEqualTo("{\"name\":\"John\",\"age\":30}");
        }

        @Test
        void shouldConvertJsonArray() throws Exception {
            JsonNode json = mapper.readTree("[1,2,3]");

            String result = converter.toCsvValue(json, NullHandling.EMPTY_STRING);

            assertThat(result).isEqualTo("[1,2,3]");
        }

        @Test
        void shouldConvertNestedJson() throws Exception {
            JsonNode json = mapper.readTree("{\"user\":{\"name\":\"John\"},\"tags\":[\"a\",\"b\"]}");

            String result = converter.toCsvValue(json, NullHandling.EMPTY_STRING);

            assertThat(result).contains("\"user\":{\"name\":\"John\"}");
            assertThat(result).contains("\"tags\":[\"a\",\"b\"]");
        }

        @Test
        void shouldHandleNullJsonNode() {
            assertThat(converter.toCsvValue(null, NullHandling.EMPTY_STRING))
                .isEqualTo("");

            assertThat(converter.toCsvValue(null, NullHandling.LITERAL_NULL))
                .isEqualTo("\\N");
        }

        @Test
        void shouldHandleNullNodeValue() {
            JsonNode nullNode = NullNode.getInstance();

            assertThat(converter.toCsvValue(nullNode, NullHandling.EMPTY_STRING))
                .isEqualTo("");
        }

        @Test
        void shouldConvertJsonWithSpecialCharacters() throws Exception {
            JsonNode json = mapper.readTree("{\"message\":\"Hello\\nWorld\"}");

            String result = converter.toCsvValue(json, NullHandling.EMPTY_STRING);

            // JSON keeps the escaped newline
            assertThat(result).contains("Hello\\nWorld");
        }
    }

    @Nested
    class NullHandlingTests {

        @ParameterizedTest
        @EnumSource(NullHandling.class)
        void shouldHandleAllNullStrategiesForStrings(NullHandling handling) {
            StringConverter converter = new StringConverter();

            String result = converter.toCsvValue(null, handling);

            assertThat(result).isEqualTo(handling.getRepresentation());
        }

        @ParameterizedTest
        @EnumSource(NullHandling.class)
        void shouldHandleAllNullStrategiesForIntegers(NullHandling handling) {
            NumericConverters.IntegerConverter converter = new NumericConverters.IntegerConverter();

            String result = converter.toCsvValue(null, handling);

            assertThat(result).isEqualTo(handling.getRepresentation());
        }

        @ParameterizedTest
        @EnumSource(NullHandling.class)
        void shouldHandleAllNullStrategiesForBooleans(NullHandling handling) {
            BooleanConverter converter = new BooleanConverter();

            String result = converter.toCsvValue(null, handling);

            assertThat(result).isEqualTo(handling.getRepresentation());
        }

        @Test
        void shouldReturnCorrectRepresentations() {
            assertThat(NullHandling.EMPTY_STRING.getRepresentation()).isEqualTo("");
            assertThat(NullHandling.LITERAL_NULL.getRepresentation()).isEqualTo("\\N");
            assertThat(NullHandling.NULL_STRING.getRepresentation()).isEqualTo("NULL");
        }
    }

    @Nested
    class ArrayConverterEdgeCases {

        @Test
        void shouldConvertArrayWithNullElements() {
            String[] array = {"a", null, "c"};

            String result = registry.convert(array, NullHandling.EMPTY_STRING);

            // Null elements are represented as NULL in PostgreSQL array format
            assertThat(result).isEqualTo("{a,NULL,c}");
        }

        @Test
        void shouldConvertEmptyArray() {
            String[] array = {};

            String result = registry.convert(array, NullHandling.EMPTY_STRING);

            assertThat(result).isEqualTo("{}");
        }

        @Test
        void shouldConvertArrayWithSpecialCharacters() {
            String[] array = {"a,b", "c\"d", "e\\f"};

            String result = registry.convert(array, NullHandling.EMPTY_STRING);

            // Array elements with special characters should be quoted
            assertThat(result).contains("a,b");
        }

        @Test
        void shouldConvertIntegerArray() {
            Integer[] array = {1, 2, 3};

            String result = registry.convert(array, NullHandling.EMPTY_STRING);

            assertThat(result).isEqualTo("{1,2,3}");
        }
    }
}
