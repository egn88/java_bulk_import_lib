package com.bulkimport;

import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.config.ConflictStrategy;
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

class BulkUpsertTest extends DatabaseIntegrationTest {

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
                (2, 'Bob', 'bob@example.com', 25, true)
                """);
        }
    }

    @Test
    void shouldUpsertWithDoNothing() throws SQLException {
        // Given
        BulkImportConfig config = BulkImportConfig.builder()
            .conflictStrategy(ConflictStrategy.DO_NOTHING)
            .conflictColumns("id")
            .build();

        BulkImporter upsertImporter = BulkImporter.create(connection)
            .withConfig(config);

        List<JpaUser> users = List.of(
            new JpaUser(1L, "Alice Updated", "alice.new@example.com", 31, true), // Existing
            new JpaUser(3L, "Charlie", "charlie@example.com", 35, false) // New
        );

        // When
        int affected = upsertImporter.upsert(JpaUser.class, users);

        // Then
        assertThat(affected).isEqualTo(1); // Only new row inserted
        assertThat(countRows(TABLE_NAME)).isEqualTo(3);
        assertThat(getUserName(1L)).isEqualTo("Alice"); // Not updated
        assertThat(getUserName(3L)).isEqualTo("Charlie"); // Inserted
    }

    @Test
    void shouldUpsertWithUpdateAll() throws SQLException {
        // Given
        BulkImportConfig config = BulkImportConfig.builder()
            .conflictStrategy(ConflictStrategy.UPDATE_ALL)
            .conflictColumns("id")
            .build();

        BulkImporter upsertImporter = BulkImporter.create(connection)
            .withConfig(config);

        List<JpaUser> users = List.of(
            new JpaUser(1L, "Alice Updated", "alice.new@example.com", 31, false), // Existing
            new JpaUser(3L, "Charlie", "charlie@example.com", 35, false) // New
        );

        // When
        int affected = upsertImporter.upsert(JpaUser.class, users);

        // Then
        assertThat(affected).isEqualTo(2);
        assertThat(countRows(TABLE_NAME)).isEqualTo(3);
        assertThat(getUserName(1L)).isEqualTo("Alice Updated"); // Updated
        assertThat(getUserEmail(1L)).isEqualTo("alice.new@example.com"); // Updated
        assertThat(getUserName(3L)).isEqualTo("Charlie"); // Inserted
    }

    @Test
    void shouldUpsertWithUpdateSpecified() throws SQLException {
        // Given - Only update name on conflict
        BulkImportConfig config = BulkImportConfig.builder()
            .conflictStrategy(ConflictStrategy.UPDATE_SPECIFIED)
            .conflictColumns("id")
            .updateColumns("name")
            .build();

        BulkImporter upsertImporter = BulkImporter.create(connection)
            .withConfig(config);

        List<JpaUser> users = List.of(
            new JpaUser(1L, "Alice Updated", "alice.new@example.com", 99, false)
        );

        // When
        int affected = upsertImporter.upsert(JpaUser.class, users);

        // Then
        assertThat(affected).isEqualTo(1);
        assertThat(getUserName(1L)).isEqualTo("Alice Updated"); // Updated
        assertThat(getUserEmail(1L)).isEqualTo("alice@example.com"); // Not updated
        assertThat(getUserAge(1L)).isEqualTo(30); // Not updated
    }

    @Test
    void shouldUpsertOnUniqueColumn() throws SQLException {
        // Given - Use email as conflict column (unique constraint)
        BulkImportConfig config = BulkImportConfig.builder()
            .conflictStrategy(ConflictStrategy.UPDATE_ALL)
            .conflictColumns("email")
            .build();

        BulkImporter upsertImporter = BulkImporter.create(connection)
            .withConfig(config);

        List<JpaUser> users = List.of(
            new JpaUser(999L, "Alice By Email", "alice@example.com", 31, false), // Matches existing email
            new JpaUser(3L, "Charlie", "charlie@example.com", 35, false) // New
        );

        // When
        int affected = upsertImporter.upsert(JpaUser.class, users);

        // Then
        assertThat(affected).isEqualTo(2);
        // ID=1 row should be updated (matched by email)
        assertThat(getUserName(1L)).isEqualTo("Alice By Email");
    }

    @Test
    void shouldInsertAllWhenNoConflicts() throws SQLException {
        // Given
        BulkImportConfig config = BulkImportConfig.builder()
            .conflictStrategy(ConflictStrategy.UPDATE_ALL)
            .conflictColumns("id")
            .build();

        BulkImporter upsertImporter = BulkImporter.create(connection)
            .withConfig(config);

        List<JpaUser> users = List.of(
            new JpaUser(3L, "Charlie", "charlie@example.com", 35, false),
            new JpaUser(4L, "David", "david@example.com", 40, true)
        );

        // When
        int affected = upsertImporter.upsert(JpaUser.class, users);

        // Then
        assertThat(affected).isEqualTo(2);
        assertThat(countRows(TABLE_NAME)).isEqualTo(4);
    }

    @Test
    void shouldHandleEmptyList() {
        // Given
        BulkImportConfig config = BulkImportConfig.builder()
            .conflictStrategy(ConflictStrategy.UPDATE_ALL)
            .conflictColumns("id")
            .build();

        BulkImporter upsertImporter = BulkImporter.create(connection)
            .withConfig(config);

        List<JpaUser> users = List.of();

        // When
        int affected = upsertImporter.upsert(JpaUser.class, users);

        // Then
        assertThat(affected).isEqualTo(0);
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
