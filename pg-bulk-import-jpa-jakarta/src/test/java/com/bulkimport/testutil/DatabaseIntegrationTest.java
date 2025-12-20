package com.bulkimport.testutil;

import com.bulkimport.BulkImporter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Base class for database integration tests.
 * Provides common setup for PostgreSQL test container and connection management.
 */
@Testcontainers
public abstract class DatabaseIntegrationTest {

    @Container
    protected static final PostgreSQLContainer<?> postgres = PostgresTestContainer.getInstance();

    protected Connection connection;
    protected BulkImporter importer;

    @BeforeEach
    void setUpConnection() throws SQLException {
        connection = PostgresTestContainer.getConnection();
        importer = BulkImporter.create(connection);
    }

    @AfterEach
    void tearDownConnection() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // Convenience methods delegating to DatabaseAssertions

    /**
     * Counts rows in a table.
     */
    protected long countRows(String table) throws SQLException {
        return DatabaseAssertions.countRows(connection, table);
    }

    /**
     * Gets a string column value by ID.
     */
    protected String getString(String table, String column, String idColumn, Object id) throws SQLException {
        return DatabaseAssertions.getString(connection, table, column, idColumn, id);
    }

    /**
     * Gets an integer column value by ID.
     */
    protected Integer getInteger(String table, String column, String idColumn, Object id) throws SQLException {
        return DatabaseAssertions.getInteger(connection, table, column, idColumn, id);
    }

    /**
     * Gets a long column value by ID.
     */
    protected Long getLong(String table, String column, String idColumn, Object id) throws SQLException {
        return DatabaseAssertions.getLong(connection, table, column, idColumn, id);
    }

    /**
     * Truncates a table.
     */
    protected void truncateTable(String table) throws SQLException {
        DatabaseAssertions.truncateTable(connection, table);
    }
}
