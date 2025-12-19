package com.bulkimport.converter;

import com.bulkimport.config.NullHandling;
import com.bulkimport.converter.builtin.ArrayConverter;
import com.bulkimport.converter.builtin.BooleanConverter;
import com.bulkimport.converter.builtin.ByteArrayConverter;
import com.bulkimport.converter.builtin.DateTimeConverters;
import com.bulkimport.converter.builtin.EnumConverter;
import com.bulkimport.converter.builtin.JsonConverter;
import com.bulkimport.converter.builtin.NumericConverters;
import com.bulkimport.converter.builtin.StringConverter;
import com.bulkimport.converter.builtin.UUIDConverter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for type converters.
 * Maintains a mapping from Java types to their converters.
 */
public class TypeConverterRegistry {

    private static final TypeConverterRegistry DEFAULT_INSTANCE = createDefault();

    private final Map<Class<?>, TypeConverter<?>> converters;
    private final TypeConverter<Object> fallbackConverter;

    /**
     * Creates a new registry with no converters.
     */
    public TypeConverterRegistry() {
        this.converters = new ConcurrentHashMap<>();
        this.fallbackConverter = new FallbackConverter();
    }

    /**
     * Creates a registry with all built-in converters registered.
     */
    public static TypeConverterRegistry createDefault() {
        TypeConverterRegistry registry = new TypeConverterRegistry();
        registry.registerBuiltInConverters();
        return registry;
    }

    /**
     * Gets the shared default registry instance.
     */
    public static TypeConverterRegistry getDefault() {
        return DEFAULT_INSTANCE;
    }

    /**
     * Registers a type converter.
     *
     * @param type the Java type
     * @param converter the converter for that type
     */
    public <T> void register(Class<T> type, TypeConverter<T> converter) {
        converters.put(type, converter);
    }

    /**
     * Registers a type converter with raw types (for generic types like List).
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void registerRaw(Class type, TypeConverter converter) {
        converters.put(type, converter);
    }

    /**
     * Gets the converter for a type.
     * If no exact match is found, tries to find a converter for a supertype.
     *
     * @param type the Java type
     * @return the converter, or the fallback converter if none found
     */
    @SuppressWarnings("unchecked")
    public <T> TypeConverter<T> getConverter(Class<T> type) {
        // Check for exact match
        TypeConverter<?> converter = converters.get(type);
        if (converter != null) {
            return (TypeConverter<T>) converter;
        }

        // Check for enum types
        if (type.isEnum()) {
            return (TypeConverter<T>) new EnumConverter();
        }

        // Check for array types
        if (type.isArray()) {
            return (TypeConverter<T>) converters.get(Object[].class);
        }

        // Check for List types
        if (List.class.isAssignableFrom(type)) {
            return (TypeConverter<T>) converters.get(List.class);
        }

        // Check primitive wrappers
        Class<?> wrapper = getWrapperType(type);
        if (wrapper != null) {
            converter = converters.get(wrapper);
            if (converter != null) {
                return (TypeConverter<T>) converter;
            }
        }

        // Check supertypes
        for (Map.Entry<Class<?>, TypeConverter<?>> entry : converters.entrySet()) {
            if (entry.getKey().isAssignableFrom(type)) {
                return (TypeConverter<T>) entry.getValue();
            }
        }

        // Return fallback converter
        return (TypeConverter<T>) fallbackConverter;
    }

    /**
     * Converts a value to CSV string using the appropriate converter.
     */
    @SuppressWarnings("unchecked")
    public <T> String convert(T value, NullHandling nullHandling) {
        if (value == null) {
            return nullHandling.getRepresentation();
        }

        TypeConverter<T> converter = (TypeConverter<T>) getConverter(value.getClass());
        return converter.toCsvValue(value, nullHandling);
    }

    /**
     * Checks if a converter is registered for the given type.
     */
    public boolean hasConverter(Class<?> type) {
        return converters.containsKey(type) ||
               type.isEnum() ||
               type.isArray() ||
               List.class.isAssignableFrom(type);
    }

    private void registerBuiltInConverters() {
        // String
        register(String.class, new StringConverter());

        // Numeric types
        register(Integer.class, new NumericConverters.IntegerConverter());
        register(Long.class, new NumericConverters.LongConverter());
        register(Double.class, new NumericConverters.DoubleConverter());
        register(Float.class, new NumericConverters.FloatConverter());
        register(BigDecimal.class, new NumericConverters.BigDecimalConverter());
        register(BigInteger.class, new NumericConverters.BigIntegerConverter());
        register(Short.class, new NumericConverters.ShortConverter());
        register(Byte.class, new NumericConverters.ByteConverter());

        // Boolean
        register(Boolean.class, new BooleanConverter());

        // Date/Time
        register(LocalDate.class, new DateTimeConverters.LocalDateConverter());
        register(LocalDateTime.class, new DateTimeConverters.LocalDateTimeConverter());
        register(LocalTime.class, new DateTimeConverters.LocalTimeConverter());
        register(Instant.class, new DateTimeConverters.InstantConverter());
        register(ZonedDateTime.class, new DateTimeConverters.ZonedDateTimeConverter());
        register(OffsetDateTime.class, new DateTimeConverters.OffsetDateTimeConverter());
        register(java.sql.Date.class, new DateTimeConverters.SqlDateConverter());
        register(java.sql.Timestamp.class, new DateTimeConverters.SqlTimestampConverter());
        register(java.util.Date.class, new DateTimeConverters.UtilDateConverter());

        // UUID
        register(UUID.class, new UUIDConverter());

        // Binary
        register(byte[].class, new ByteArrayConverter());

        // Arrays and Lists
        register(Object[].class, new ArrayConverter());
        registerRaw(List.class, new ArrayConverter.ListConverter());

        // JSON (if Jackson is available)
        try {
            Class.forName("com.fasterxml.jackson.databind.JsonNode");
            register(com.fasterxml.jackson.databind.JsonNode.class, new JsonConverter());
        } catch (ClassNotFoundException e) {
            // Jackson not available, skip JSON converter
        }
    }

    private Class<?> getWrapperType(Class<?> primitiveType) {
        if (primitiveType == int.class) return Integer.class;
        if (primitiveType == long.class) return Long.class;
        if (primitiveType == double.class) return Double.class;
        if (primitiveType == float.class) return Float.class;
        if (primitiveType == boolean.class) return Boolean.class;
        if (primitiveType == short.class) return Short.class;
        if (primitiveType == byte.class) return Byte.class;
        if (primitiveType == char.class) return Character.class;
        return null;
    }

    /**
     * Fallback converter that uses toString().
     */
    private static class FallbackConverter implements TypeConverter<Object> {
        @Override
        public String toCsvValue(Object value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            // Return raw value - FastCSV handles CSV escaping
            return value.toString();
        }

        @Override
        public Class<Object> supportedType() {
            return Object.class;
        }
    }
}
