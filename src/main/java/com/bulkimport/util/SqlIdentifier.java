package com.bulkimport.util;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class for safe SQL identifier handling.
 * Provides validation and quoting to prevent SQL injection attacks.
 *
 * <p>SQL identifiers (table names, column names) cannot be parameterized
 * in prepared statements, so we use validation + quoting as the industry
 * standard approach for security.</p>
 */
public final class SqlIdentifier {

    /**
     * Valid SQL identifier pattern: starts with letter or underscore,
     * followed by letters, digits, or underscores.
     */
    private static final Pattern VALID_IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    /**
     * Maximum length for SQL identifiers (PostgreSQL limit is 63).
     */
    private static final int MAX_IDENTIFIER_LENGTH = 63;

    private SqlIdentifier() {
        // Utility class
    }

    /**
     * Validates and quotes a SQL identifier (table name, column name).
     *
     * <p>This method provides defense-in-depth against SQL injection:
     * <ol>
     *   <li>Validates the identifier matches a strict pattern</li>
     *   <li>Quotes the identifier using PostgreSQL double-quote syntax</li>
     * </ol>
     * </p>
     *
     * @param identifier the identifier to validate and quote
     * @return the quoted identifier, safe for use in SQL
     * @throws IllegalArgumentException if the identifier is invalid
     */
    public static String quote(String identifier) {
        validate(identifier);
        return "\"" + identifier + "\"";
    }

    /**
     * Validates and quotes a qualified identifier (schema.table or just table).
     *
     * @param schemaName the schema name (can be null)
     * @param tableName the table name
     * @return the quoted qualified identifier
     * @throws IllegalArgumentException if any identifier is invalid
     */
    public static String quoteQualified(String schemaName, String tableName) {
        if (schemaName != null && !schemaName.isEmpty()) {
            return quote(schemaName) + "." + quote(tableName);
        }
        return quote(tableName);
    }

    /**
     * Quotes a list of column names and joins them with commas.
     *
     * @param columnNames the column names to quote
     * @return comma-separated quoted column names
     * @throws IllegalArgumentException if any identifier is invalid
     */
    public static String quoteAndJoin(List<String> columnNames) {
        return columnNames.stream()
                .map(SqlIdentifier::quote)
                .collect(Collectors.joining(", "));
    }

    /**
     * Validates a SQL identifier without quoting it.
     *
     * @param identifier the identifier to validate
     * @throws IllegalArgumentException if the identifier is invalid
     */
    public static void validate(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException("SQL identifier cannot be null or empty");
        }

        if (identifier.length() > MAX_IDENTIFIER_LENGTH) {
            throw new IllegalArgumentException(
                    "SQL identifier exceeds maximum length of " + MAX_IDENTIFIER_LENGTH +
                    " characters: " + identifier);
        }

        if (!VALID_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException(
                    "Invalid SQL identifier: '" + identifier + "'. " +
                    "Identifiers must start with a letter or underscore, " +
                    "and contain only letters, digits, or underscores.");
        }
    }

    /**
     * Checks if a string is a valid SQL identifier.
     *
     * @param identifier the identifier to check
     * @return true if valid, false otherwise
     */
    public static boolean isValid(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return false;
        }
        if (identifier.length() > MAX_IDENTIFIER_LENGTH) {
            return false;
        }
        return VALID_IDENTIFIER.matcher(identifier).matches();
    }
}
