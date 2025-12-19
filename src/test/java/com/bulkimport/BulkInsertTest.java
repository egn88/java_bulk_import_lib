package com.bulkimport;

import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.mapping.TableMapping;
import com.bulkimport.testutil.PostgresTestContainer;
import com.bulkimport.testutil.TestEntities.BulkUser;
import com.bulkimport.testutil.TestEntities.JpaUser;
import com.bulkimport.testutil.TestEntities.SimpleUser;

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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class BulkInsertTest {

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

        // Clear table before each test
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("TRUNCATE TABLE users");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    void shouldInsertWithJpaAnnotations() throws SQLException {
        // Given
        List<JpaUser> users = List.of(
            new JpaUser(1L, "Alice", "alice@example.com", 30, true),
            new JpaUser(2L, "Bob", "bob@example.com", 25, true),
            new JpaUser(3L, "Charlie", "charlie@example.com", 35, false)
        );

        // When
        int inserted = importer.insert(JpaUser.class, users);

        // Then
        assertThat(inserted).isEqualTo(3);
        assertThat(countRows()).isEqualTo(3);
        assertThat(getUserName(1L)).isEqualTo("Alice");
    }

    @Test
    void shouldInsertWithCustomAnnotations() throws SQLException {
        // Given
        List<BulkUser> users = List.of(
            new BulkUser(1L, "Alice", "alice@example.com", 30, true),
            new BulkUser(2L, "Bob", "bob@example.com", 25, true)
        );

        // When
        int inserted = importer.insert(BulkUser.class, users);

        // Then
        assertThat(inserted).isEqualTo(2);
        assertThat(countRows()).isEqualTo(2);
    }

    @Test
    void shouldInsertWithFluentMapping() throws SQLException {
        // Given
        TableMapping<SimpleUser> mapping = TableMapping.<SimpleUser>builder("users")
            .id("id", SimpleUser::getId)
            .column("name", SimpleUser::getName)
            .column("email", SimpleUser::getEmail)
            .build();

        List<SimpleUser> users = List.of(
            new SimpleUser(1L, "Alice", "alice@example.com"),
            new SimpleUser(2L, "Bob", "bob@example.com")
        );

        // When
        int inserted = importer.insert(mapping, users);

        // Then
        assertThat(inserted).isEqualTo(2);
        assertThat(countRows()).isEqualTo(2);
    }

    @Test
    void shouldInsertLargeBatch() throws SQLException {
        // Given
        int batchSize = 10_000;
        List<JpaUser> users = new ArrayList<>(batchSize);
        for (int i = 1; i <= batchSize; i++) {
            users.add(new JpaUser((long) i, "User" + i, "user" + i + "@example.com", 20 + (i % 50), true));
        }

        // When
        int inserted = importer.insert(JpaUser.class, users);

        // Then
        assertThat(inserted).isEqualTo(batchSize);
        assertThat(countRows()).isEqualTo(batchSize);
    }

    @Test
    void shouldInsertFromStream() throws SQLException {
        // Given
        var userStream = IntStream.rangeClosed(1, 100)
            .mapToObj(i -> new JpaUser((long) i, "User" + i, "user" + i + "@example.com", 25, true));

        // When
        int inserted = importer.insert(JpaUser.class, userStream);

        // Then
        assertThat(inserted).isEqualTo(100);
        assertThat(countRows()).isEqualTo(100);
    }

    @Test
    void shouldHandleNullValues() throws SQLException {
        // Given
        List<JpaUser> users = List.of(
            new JpaUser(1L, "Alice", null, null, null)
        );

        // When
        int inserted = importer.insert(JpaUser.class, users);

        // Then
        assertThat(inserted).isEqualTo(1);
        assertThat(getUserEmail(1L)).isNull();
    }

    @Test
    void shouldHandleSpecialCharacters() throws SQLException {
        // Given
        List<JpaUser> users = List.of(
            new JpaUser(1L, "O'Brien", "o'brien@example.com", 30, true),
            new JpaUser(2L, "User,With,Commas", "comma@example.com", 25, true),
            new JpaUser(3L, "Quote\"Test", "quote@example.com", 35, true),
            new JpaUser(4L, "Tab\tTest", "tab@example.com", 40, true),
            new JpaUser(5L, "Newline\nTest", "newline@example.com", 45, true)
        );

        // When
        int inserted = importer.insert(JpaUser.class, users);

        // Then
        assertThat(inserted).isEqualTo(5);
        assertThat(getUserName(1L)).isEqualTo("O'Brien");
        assertThat(getUserName(2L)).isEqualTo("User,With,Commas");
        assertThat(getUserName(3L)).isEqualTo("Quote\"Test");
    }

    @Test
    void shouldHandleEmptyList() {
        // Given
        List<JpaUser> users = List.of();

        // When
        int inserted = importer.insert(JpaUser.class, users);

        // Then
        assertThat(inserted).isEqualTo(0);
    }

    @Test
    void shouldRespectConfiguration() throws SQLException {
        // Given
        BulkImportConfig config = BulkImportConfig.builder()
            .batchSize(100)
            .build();

        BulkImporter configuredImporter = BulkImporter.create(connection)
            .withConfig(config);

        List<JpaUser> users = List.of(
            new JpaUser(1L, "Alice", "alice@example.com", 30, true)
        );

        // When
        int inserted = configuredImporter.insert(JpaUser.class, users);

        // Then
        assertThat(inserted).isEqualTo(1);
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
}
