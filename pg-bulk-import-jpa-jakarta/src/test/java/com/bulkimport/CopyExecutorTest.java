package com.bulkimport;

import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.config.NullHandling;
import com.bulkimport.executor.CopyExecutor;
import com.bulkimport.mapping.TableMapping;
import com.bulkimport.testutil.PostgresTestContainer;
import com.bulkimport.testutil.TestEntities.JpaUser;
import com.bulkimport.testutil.TestEntities.SimpleUser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for CopyExecutor focusing on the stream-only implementation.
 */
@Testcontainers
class CopyExecutorTest {

    @Container
    private static final org.testcontainers.containers.PostgreSQLContainer<?> postgres =
        PostgresTestContainer.getInstance();

    private Connection connection;

    @BeforeAll
    static void setUpDatabase() throws SQLException {
        PostgresTestContainer.createUsersTable();
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = PostgresTestContainer.getConnection();
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

    @Nested
    class ListToStreamConversion {

        @Test
        void shouldCopyEmptyList() throws SQLException {
            // Given
            TableMapping<SimpleUser> mapping = createSimpleUserMapping();
            CopyExecutor<SimpleUser> executor = new CopyExecutor<>(
                connection, mapping, BulkImportConfig.defaults());

            List<SimpleUser> emptyList = Collections.emptyList();

            // When
            long result = executor.copyIn(emptyList);

            // Then
            assertThat(result).isEqualTo(0);
            assertThat(countRows()).isEqualTo(0);
        }

        @Test
        void shouldCopyListWithSingleElement() throws SQLException {
            // Given
            TableMapping<SimpleUser> mapping = createSimpleUserMapping();
            CopyExecutor<SimpleUser> executor = new CopyExecutor<>(
                connection, mapping, BulkImportConfig.defaults());

            List<SimpleUser> singleItem = List.of(
                new SimpleUser(1L, "Alice", "alice@example.com")
            );

            // When
            long result = executor.copyIn(singleItem);

            // Then
            assertThat(result).isEqualTo(1);
            assertThat(countRows()).isEqualTo(1);
        }

        @Test
        void shouldCopyListConvertedToStream() throws SQLException {
            // Given
            TableMapping<SimpleUser> mapping = createSimpleUserMapping();
            CopyExecutor<SimpleUser> executor = new CopyExecutor<>(
                connection, mapping, BulkImportConfig.defaults());

            List<SimpleUser> users = List.of(
                new SimpleUser(1L, "Alice", "alice@example.com"),
                new SimpleUser(2L, "Bob", "bob@example.com"),
                new SimpleUser(3L, "Charlie", "charlie@example.com")
            );

            // When
            long result = executor.copyIn(users);

            // Then
            assertThat(result).isEqualTo(3);
            assertThat(countRows()).isEqualTo(3);
        }
    }

    @Nested
    class StreamProcessing {

        @Test
        void shouldCopyFromStream() throws SQLException {
            // Given
            TableMapping<SimpleUser> mapping = createSimpleUserMapping();
            CopyExecutor<SimpleUser> executor = new CopyExecutor<>(
                connection, mapping, BulkImportConfig.defaults());

            Stream<SimpleUser> userStream = IntStream.rangeClosed(1, 50)
                .mapToObj(i -> new SimpleUser((long) i, "User" + i, "user" + i + "@example.com"));

            // When
            long result = executor.copyIn(userStream);

            // Then
            assertThat(result).isEqualTo(50);
            assertThat(countRows()).isEqualTo(50);
        }

        @Test
        void shouldCopyEmptyStream() throws SQLException {
            // Given
            TableMapping<SimpleUser> mapping = createSimpleUserMapping();
            CopyExecutor<SimpleUser> executor = new CopyExecutor<>(
                connection, mapping, BulkImportConfig.defaults());

            Stream<SimpleUser> emptyStream = Stream.empty();

            // When
            long result = executor.copyIn(emptyStream);

            // Then
            assertThat(result).isEqualTo(0);
            assertThat(countRows()).isEqualTo(0);
        }

        @Test
        void shouldCopyLargeStreamEfficiently() throws SQLException {
            // Given
            TableMapping<SimpleUser> mapping = createSimpleUserMapping();
            CopyExecutor<SimpleUser> executor = new CopyExecutor<>(
                connection, mapping, BulkImportConfig.defaults());

            int count = 5000;
            Stream<SimpleUser> largeStream = IntStream.rangeClosed(1, count)
                .mapToObj(i -> new SimpleUser((long) i, "User" + i, "user" + i + "@example.com"));

            // When
            long result = executor.copyIn(largeStream);

            // Then
            assertThat(result).isEqualTo(count);
            assertThat(countRows()).isEqualTo(count);
        }

        @Test
        void shouldCopyStreamGeneratedOnDemand() throws SQLException {
            // Given - a stream that generates items lazily
            TableMapping<SimpleUser> mapping = createSimpleUserMapping();
            CopyExecutor<SimpleUser> executor = new CopyExecutor<>(
                connection, mapping, BulkImportConfig.defaults());

            List<Integer> processedIds = new ArrayList<>();
            Stream<SimpleUser> lazyStream = IntStream.rangeClosed(1, 10)
                .mapToObj(i -> {
                    processedIds.add(i);
                    return new SimpleUser((long) i, "User" + i, "user" + i + "@example.com");
                });

            // When
            long result = executor.copyIn(lazyStream);

            // Then - verify stream was actually processed
            assertThat(result).isEqualTo(10);
            assertThat(processedIds).hasSize(10);
            assertThat(countRows()).isEqualTo(10);
        }
    }

    @Nested
    class CopyInToStagingTable {

        private static final String STAGING_TABLE = "bulk_staging_users";

        @BeforeEach
        void createStagingTable() throws SQLException {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + STAGING_TABLE);
                stmt.execute("""
                    CREATE TABLE bulk_staging_users (
                        id BIGINT,
                        name VARCHAR(255),
                        email VARCHAR(255)
                    )
                    """);
            }
        }

        @AfterEach
        void dropStagingTable() throws SQLException {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + STAGING_TABLE);
            }
        }

        @Test
        void shouldCopyListToStagingTable() throws SQLException {
            // Given
            TableMapping<SimpleUser> mapping = createSimpleUserMapping();
            CopyExecutor<SimpleUser> executor = new CopyExecutor<>(
                connection, mapping, BulkImportConfig.defaults());

            List<SimpleUser> users = List.of(
                new SimpleUser(1L, "Alice", "alice@example.com"),
                new SimpleUser(2L, "Bob", "bob@example.com")
            );

            // When
            long result = executor.copyInTo(STAGING_TABLE, users);

            // Then
            assertThat(result).isEqualTo(2);
            assertThat(countRowsInTable(STAGING_TABLE)).isEqualTo(2);
        }

        @Test
        void shouldCopyStreamToStagingTable() throws SQLException {
            // Given
            TableMapping<SimpleUser> mapping = createSimpleUserMapping();
            CopyExecutor<SimpleUser> executor = new CopyExecutor<>(
                connection, mapping, BulkImportConfig.defaults());

            Stream<SimpleUser> userStream = IntStream.rangeClosed(1, 20)
                .mapToObj(i -> new SimpleUser((long) i, "User" + i, "user" + i + "@example.com"));

            // When
            long result = executor.copyInTo(STAGING_TABLE, userStream);

            // Then
            assertThat(result).isEqualTo(20);
            assertThat(countRowsInTable(STAGING_TABLE)).isEqualTo(20);
        }

        @Test
        void shouldCopyEmptyListToStagingTable() throws SQLException {
            // Given
            TableMapping<SimpleUser> mapping = createSimpleUserMapping();
            CopyExecutor<SimpleUser> executor = new CopyExecutor<>(
                connection, mapping, BulkImportConfig.defaults());

            List<SimpleUser> emptyList = List.of();

            // When
            long result = executor.copyInTo(STAGING_TABLE, emptyList);

            // Then
            assertThat(result).isEqualTo(0);
            assertThat(countRowsInTable(STAGING_TABLE)).isEqualTo(0);
        }
    }

    @Nested
    class NullHandlingConfiguration {

        @Test
        void shouldUseConfiguredNullHandling() throws SQLException {
            // Given
            BulkImportConfig config = BulkImportConfig.builder()
                .nullHandling(NullHandling.LITERAL_NULL)
                .build();

            TableMapping<SimpleUser> mapping = createSimpleUserMapping();
            CopyExecutor<SimpleUser> executor = new CopyExecutor<>(
                connection, mapping, config);

            List<SimpleUser> users = List.of(
                new SimpleUser(1L, "Alice", null)
            );

            // When
            long result = executor.copyIn(users);

            // Then
            assertThat(result).isEqualTo(1);
            assertThat(getEmail(1L)).isNull();
        }
    }

    private TableMapping<SimpleUser> createSimpleUserMapping() {
        return TableMapping.<SimpleUser>builder("users")
            .id("id", SimpleUser::getId)
            .column("name", SimpleUser::getName)
            .column("email", SimpleUser::getEmail)
            .build();
    }

    private int countRows() throws SQLException {
        return countRowsInTable("users");
    }

    private int countRowsInTable(String tableName) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private String getEmail(Long id) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT email FROM users WHERE id = " + id)) {
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        }
    }
}
