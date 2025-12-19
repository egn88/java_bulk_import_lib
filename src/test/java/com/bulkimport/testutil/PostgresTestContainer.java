package com.bulkimport.testutil;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Shared PostgreSQL test container for integration tests.
 */
public class PostgresTestContainer {

    private static final String POSTGRES_IMAGE = "postgres:16-alpine";

    private static PostgreSQLContainer<?> container;

    private PostgresTestContainer() {
    }

    /**
     * Gets the shared PostgreSQL container, starting it if necessary.
     */
    public static synchronized PostgreSQLContainer<?> getInstance() {
        if (container == null) {
            container = new PostgreSQLContainer<>(DockerImageName.parse(POSTGRES_IMAGE))
                .withDatabaseName("testdb")
                .withUsername("test")
                .withPassword("test");
            container.start();
        }
        return container;
    }

    /**
     * Creates a new connection to the test database.
     */
    public static Connection getConnection() throws SQLException {
        PostgreSQLContainer<?> pg = getInstance();
        return DriverManager.getConnection(
            pg.getJdbcUrl(),
            pg.getUsername(),
            pg.getPassword()
        );
    }

    /**
     * Executes SQL statements on the test database.
     */
    public static void executeSql(String... statements) throws SQLException {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            for (String sql : statements) {
                stmt.execute(sql);
            }
        }
    }

    /**
     * Creates a test table for users.
     */
    public static void createUsersTable() throws SQLException {
        executeSql(
            "DROP TABLE IF EXISTS users CASCADE",
            """
            CREATE TABLE users (
                id BIGINT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE,
                age INTEGER,
                active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata JSONB
            )
            """
        );
    }

    /**
     * Creates a test table for products with arrays.
     */
    public static void createProductsTable() throws SQLException {
        executeSql(
            "DROP TABLE IF EXISTS products CASCADE",
            """
            CREATE TABLE products (
                id UUID PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                price NUMERIC(10,2),
                tags TEXT[],
                data BYTEA
            )
            """
        );
    }

    /**
     * Drops all test tables.
     */
    public static void dropTables() throws SQLException {
        executeSql(
            "DROP TABLE IF EXISTS users CASCADE",
            "DROP TABLE IF EXISTS products CASCADE"
        );
    }
}
