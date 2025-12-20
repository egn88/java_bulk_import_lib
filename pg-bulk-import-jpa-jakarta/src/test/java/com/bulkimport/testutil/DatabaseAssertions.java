package com.bulkimport.testutil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Utility class for database assertions in tests.
 * Provides static methods for common database queries used in test validations.
 */
public final class DatabaseAssertions {

    private DatabaseAssertions() {
        // Utility class
    }

    /**
     * Counts the number of rows in a table.
     *
     * @param connection the database connection
     * @param table the table name
     * @return the row count
     */
    public static long countRows(Connection connection, String table) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /**
     * Gets a string column value by ID.
     *
     * @param connection the database connection
     * @param table the table name
     * @param column the column to retrieve
     * @param idColumn the ID column name
     * @param id the ID value
     * @return the string value, or null if not found
     */
    public static String getString(Connection connection, String table, String column,
                                   String idColumn, Object id) throws SQLException {
        String sql = "SELECT " + column + " FROM " + table + " WHERE " + idColumn + " = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setObject(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null;
            }
        }
    }

    /**
     * Gets an integer column value by ID.
     *
     * @param connection the database connection
     * @param table the table name
     * @param column the column to retrieve
     * @param idColumn the ID column name
     * @param id the ID value
     * @return the integer value, or null if not found
     */
    public static Integer getInteger(Connection connection, String table, String column,
                                     String idColumn, Object id) throws SQLException {
        String sql = "SELECT " + column + " FROM " + table + " WHERE " + idColumn + " = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setObject(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    int value = rs.getInt(1);
                    return rs.wasNull() ? null : value;
                }
                return null;
            }
        }
    }

    /**
     * Gets a long column value by ID.
     *
     * @param connection the database connection
     * @param table the table name
     * @param column the column to retrieve
     * @param idColumn the ID column name
     * @param id the ID value
     * @return the long value, or null if not found
     */
    public static Long getLong(Connection connection, String table, String column,
                               String idColumn, Object id) throws SQLException {
        String sql = "SELECT " + column + " FROM " + table + " WHERE " + idColumn + " = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setObject(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    long value = rs.getLong(1);
                    return rs.wasNull() ? null : value;
                }
                return null;
            }
        }
    }

    /**
     * Truncates a table.
     *
     * @param connection the database connection
     * @param table the table name
     */
    public static void truncateTable(Connection connection, String table) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("TRUNCATE TABLE " + table);
        }
    }
}
