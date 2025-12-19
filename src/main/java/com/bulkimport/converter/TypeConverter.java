package com.bulkimport.converter;

import com.bulkimport.config.NullHandling;

/**
 * Interface for converting Java types to CSV-compatible strings for PostgreSQL COPY.
 *
 * @param <T> the Java type this converter handles
 */
public interface TypeConverter<T> {

    /**
     * Converts a value to its CSV string representation.
     *
     * @param value the value to convert, may be null
     * @param nullHandling how to handle null values
     * @return the CSV string representation
     */
    String toCsvValue(T value, NullHandling nullHandling);

    /**
     * Gets the Java type this converter handles.
     *
     * @return the supported type
     */
    Class<T> supportedType();

    /**
     * Handles null values according to the null handling strategy.
     */
    default String handleNull(NullHandling nullHandling) {
        return nullHandling.getRepresentation();
    }

    /**
     * Escapes a string value for CSV format.
     * Handles quotes, newlines, and other special characters.
     */
    default String escapeCsvValue(String value) {
        if (value == null) {
            return "";
        }

        // Check if escaping is needed
        boolean needsQuoting = value.contains(",") ||
                               value.contains("\"") ||
                               value.contains("\n") ||
                               value.contains("\r") ||
                               value.contains("\\");

        if (!needsQuoting) {
            return value;
        }

        // Escape quotes by doubling them and wrap in quotes
        StringBuilder escaped = new StringBuilder();
        escaped.append('"');
        for (char c : value.toCharArray()) {
            if (c == '"') {
                escaped.append("\"\"");
            } else {
                escaped.append(c);
            }
        }
        escaped.append('"');

        return escaped.toString();
    }
}
