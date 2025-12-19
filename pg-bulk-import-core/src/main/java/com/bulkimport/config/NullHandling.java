package com.bulkimport.config;

/**
 * Defines how null values are represented in CSV output for PostgreSQL COPY.
 */
public enum NullHandling {

    /**
     * Represents null values as empty strings.
     * PostgreSQL COPY with CSV format treats empty strings as NULL by default.
     */
    EMPTY_STRING(""),

    /**
     * Represents null values as the literal string "\N".
     * This is PostgreSQL's default NULL representation for text format.
     */
    LITERAL_NULL("\\N"),

    /**
     * Represents null values as the literal string "NULL".
     * Some systems prefer this explicit representation.
     */
    NULL_STRING("NULL");

    private final String representation;

    NullHandling(String representation) {
        this.representation = representation;
    }

    /**
     * Gets the string representation for null values.
     */
    public String getRepresentation() {
        return representation;
    }
}
