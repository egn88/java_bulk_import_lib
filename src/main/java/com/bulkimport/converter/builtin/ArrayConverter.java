package com.bulkimport.converter.builtin;

import com.bulkimport.config.NullHandling;
import com.bulkimport.converter.TypeConverter;
import com.bulkimport.converter.TypeConverterRegistry;

import java.util.List;

/**
 * Converter for arrays (PostgreSQL array format: {val1,val2,val3}).
 */
public class ArrayConverter implements TypeConverter<Object[]> {

    @Override
    public String toCsvValue(Object[] value, NullHandling nullHandling) {
        if (value == null) {
            return handleNull(nullHandling);
        }

        return formatArray(value, nullHandling);
    }

    @Override
    public Class<Object[]> supportedType() {
        return Object[].class;
    }

    private String formatArray(Object[] array, NullHandling nullHandling) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');

        TypeConverterRegistry registry = TypeConverterRegistry.getDefault();

        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                sb.append(',');
            }

            Object element = array[i];
            if (element == null) {
                sb.append("NULL");
            } else {
                String converted = registry.convert(element, nullHandling);
                // For array elements, we need to quote strings containing special chars
                if (needsQuoting(converted)) {
                    sb.append('"');
                    sb.append(escapeArrayElement(converted));
                    sb.append('"');
                } else {
                    sb.append(converted);
                }
            }
        }

        sb.append('}');
        // Return raw PostgreSQL array format - FastCSV handles CSV escaping
        return sb.toString();
    }

    private boolean needsQuoting(String value) {
        if (value.isEmpty()) {
            return true;
        }
        for (char c : value.toCharArray()) {
            if (c == ',' || c == '{' || c == '}' || c == '"' || c == '\\' ||
                Character.isWhitespace(c)) {
                return true;
            }
        }
        return false;
    }

    private String escapeArrayElement(String value) {
        StringBuilder sb = new StringBuilder();
        for (char c : value.toCharArray()) {
            if (c == '"' || c == '\\') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Converter for List types.
     */
    public static class ListConverter implements TypeConverter<List<?>> {

        @Override
        public String toCsvValue(List<?> value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }

            return new ArrayConverter().toCsvValue(value.toArray(), nullHandling);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<List<?>> supportedType() {
            return (Class<List<?>>) (Class<?>) List.class;
        }
    }
}
