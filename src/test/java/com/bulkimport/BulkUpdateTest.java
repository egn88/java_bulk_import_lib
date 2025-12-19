package com.bulkimport;

import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.testutil.PostgresTestContainer;
import com.bulkimport.testutil.TestEntities.JpaUser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class BulkUpdateTest {

    @Container
    private static final org.testcontainers.containers.PostgreSQLContainer<?> postgres =
        PostgresTestContainer.getInstance();

    private Connection connection;
    private BulkImporter importer;

    @BeforeAll
    static void setUpDatabase() throws SQLException {
        PostgresTestContainer.createUsersTable();
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = PostgresTestContainer.getConnection();
        importer = BulkImporter.create(connection);

        // Clear and insert initial data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("TRUNCATE TABLE users");
            stmt.execute("""
                INSERT INTO users (id, name, email, age, active) VALUES
                (1, 'Alice', 'alice@example.com', 30, true),
                (2, 'Bob', 'bob@example.com', 25, true),
                (3, 'Charlie', 'charlie@example.com', 35, false)
                """);
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    void shouldUpdateExistingRows() throws SQLException {
        // Given
        List<JpaUser> users = List.of(
            new JpaUser(1L, "Alice Updated", "alice.new@example.com", 31, true),
            new JpaUser(2L, "Bob Updated", "bob.new@example.com", 26, false)
        );

        // When
        int updated = importer.update(JpaUser.class, users);

        // Then
        assertThat(updated).isEqualTo(2);
        assertThat(getUserName(1L)).isEqualTo("Alice Updated");
        assertThat(getUserName(2L)).isEqualTo("Bob Updated");
        assertThat(getUserName(3L)).isEqualTo("Charlie"); // Unchanged
    }

    @Test
    void shouldUpdateWithSpecificMatchColumns() throws SQLException {
        // Given - Update by email instead of ID
        BulkImportConfig config = BulkImportConfig.builder()
            .matchColumns("email")
            .build();

        BulkImporter configuredImporter = BulkImporter.create(connection)
            .withConfig(config);

        List<JpaUser> users = List.of(
            new JpaUser(999L, "Alice Via Email", "alice@example.com", 32, true)
        );

        // When
        int updated = configuredImporter.update(JpaUser.class, users);

        // Then
        assertThat(updated).isEqualTo(1);
        assertThat(getUserName(1L)).isEqualTo("Alice Via Email");
        assertThat(getUserAge(1L)).isEqualTo(32);
    }

    @Test
    void shouldUpdateWithSpecificColumns() throws SQLException {
        // Given - Only update name column
        BulkImportConfig config = BulkImportConfig.builder()
            .updateColumns("name")
            .build();

        BulkImporter configuredImporter = BulkImporter.create(connection)
            .withConfig(config);

        List<JpaUser> users = List.of(
            new JpaUser(1L, "Alice Only Name", "ignored@example.com", 99, false)
        );

        // When
        int updated = configuredImporter.update(JpaUser.class, users);

        // Then
        assertThat(updated).isEqualTo(1);
        assertThat(getUserName(1L)).isEqualTo("Alice Only Name");
        assertThat(getUserEmail(1L)).isEqualTo("alice@example.com"); // Unchanged
        assertThat(getUserAge(1L)).isEqualTo(30); // Unchanged
    }

    @Test
    void shouldNotUpdateNonExistingRows() throws SQLException {
        // Given
        List<JpaUser> users = List.of(
            new JpaUser(999L, "Non Existing", "none@example.com", 50, true)
        );

        // When
        int updated = importer.update(JpaUser.class, users);

        // Then
        assertThat(updated).isEqualTo(0);
        assertThat(countRows()).isEqualTo(3); // Original count unchanged
    }

    @Test
    void shouldHandleEmptyList() {
        // Given
        List<JpaUser> users = List.of();

        // When
        int updated = importer.update(JpaUser.class, users);

        // Then
        assertThat(updated).isEqualTo(0);
    }

    private int countRows() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM users")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private String getUserName(Long id) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM users WHERE id = " + id)) {
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        }
    }

    private String getUserEmail(Long id) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT email FROM users WHERE id = " + id)) {
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        }
    }

    private Integer getUserAge(Long id) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT age FROM users WHERE id = " + id)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return null;
        }
    }
}
