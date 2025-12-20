package com.bulkimport;

import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.mapping.jpa.JpaEntityMapper;
import com.bulkimport.testutil.DatabaseIntegrationTest;
import com.bulkimport.testutil.PostgresTestContainer;
import com.bulkimport.testutil.TestEntities.JpaUser;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BulkUpdateTest extends DatabaseIntegrationTest {

    private static final String TABLE_NAME = "users";

    @BeforeAll
    static void setUpDatabase() throws SQLException {
        JpaEntityMapper.register();
        PostgresTestContainer.createUsersTable();
    }

    @BeforeEach
    void setUp() throws SQLException {
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
        assertThat(countRows(TABLE_NAME)).isEqualTo(3); // Original count unchanged
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

    private String getUserName(Long id) throws SQLException {
        return getString(TABLE_NAME, "name", "id", id);
    }

    private String getUserEmail(Long id) throws SQLException {
        return getString(TABLE_NAME, "email", "id", id);
    }

    private Integer getUserAge(Long id) throws SQLException {
        return getInteger(TABLE_NAME, "age", "id", id);
    }
}
