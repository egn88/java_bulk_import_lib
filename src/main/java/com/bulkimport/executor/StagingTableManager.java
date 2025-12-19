package com.bulkimport.executor;

import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.exception.ExecutionException;
import com.bulkimport.mapping.ColumnMapping;
import com.bulkimport.mapping.TableMapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Manages temporary staging tables for bulk UPDATE and UPSERT operations.
 *
 * @param <T> the entity type
 */
public class StagingTableManager<T> {

    private static final Logger log = LoggerFactory.getLogger(StagingTableManager.class);

    private final Connection connection;
    private final TableMapping<T> mapping;
    private final BulkImportConfig config;
    private String stagingTableName;

    /**
     * Creates a new staging table manager.
     */
    public StagingTableManager(Connection connection, TableMapping<T> mapping, BulkImportConfig config) {
        this.connection = connection;
        this.mapping = mapping;
        this.config = config;
    }

    /**
     * Creates the staging table with the same structure as the target table.
     *
     * @return the name of the created staging table
     */
    public String createStagingTable() {
        this.stagingTableName = generateStagingTableName();

        String createTableSql = buildCreateTableSql();
        log.debug("Creating staging table: {}", createTableSql);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
            log.info("Created staging table: {}", stagingTableName);
            return stagingTableName;
        } catch (SQLException e) {
            throw ExecutionException.stagingTableCreationFailed(stagingTableName, e);
        }
    }

    /**
     * Drops the staging table.
     */
    public void dropStagingTable() {
        if (stagingTableName == null) {
            return;
        }

        if (!config.isAutoCleanupStaging()) {
            log.debug("Skipping staging table cleanup (autoCleanupStaging=false)");
            return;
        }

        String dropSql = "DROP TABLE IF EXISTS " + stagingTableName;
        log.debug("Dropping staging table: {}", dropSql);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(dropSql);
            log.debug("Dropped staging table: {}", stagingTableName);
        } catch (SQLException e) {
            // Log but don't fail - dropping staging table is not critical
            log.warn("Failed to drop staging table '{}': {}", stagingTableName, e.getMessage());
        }
    }

    /**
     * Gets the name of the staging table.
     *
     * @return the staging table name, or null if not created
     */
    public String getStagingTableName() {
        return stagingTableName;
    }

    private String generateStagingTableName() {
        String prefix = config.getStagingTablePrefix();
        String uniqueId = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        return prefix + mapping.getTableName() + "_" + uniqueId;
    }

    private String buildCreateTableSql() {
        StringBuilder sql = new StringBuilder();

        // Use UNLOGGED table for better performance (no WAL logging)
        if (config.isUseUnloggedTables()) {
            sql.append("CREATE UNLOGGED TABLE ");
        } else {
            sql.append("CREATE TEMP TABLE ");
        }

        sql.append(stagingTableName).append(" (");

        // Add column definitions
        List<String> columnDefs = mapping.getColumns().stream()
            .map(this::buildColumnDefinition)
            .collect(Collectors.toList());

        sql.append(String.join(", ", columnDefs));
        sql.append(")");

        return sql.toString();
    }

    private String buildColumnDefinition(ColumnMapping<T, ?> column) {
        StringBuilder def = new StringBuilder();
        def.append(column.getColumnName());
        def.append(" ");
        def.append(mapJavaTypeToSqlType(column.getValueType()));

        return def.toString();
    }

    /**
     * Maps Java types to PostgreSQL types.
     */
    private String mapJavaTypeToSqlType(Class<?> javaType) {
        if (javaType == null || javaType == Object.class) {
            return "TEXT";
        }

        // String types
        if (javaType == String.class) {
            return "TEXT";
        }

        // Numeric types
        if (javaType == Integer.class || javaType == int.class) {
            return "INTEGER";
        }
        if (javaType == Long.class || javaType == long.class) {
            return "BIGINT";
        }
        if (javaType == Short.class || javaType == short.class) {
            return "SMALLINT";
        }
        if (javaType == java.math.BigDecimal.class) {
            return "NUMERIC";
        }
        if (javaType == java.math.BigInteger.class) {
            return "NUMERIC";
        }
        if (javaType == Double.class || javaType == double.class) {
            return "DOUBLE PRECISION";
        }
        if (javaType == Float.class || javaType == float.class) {
            return "REAL";
        }

        // Boolean
        if (javaType == Boolean.class || javaType == boolean.class) {
            return "BOOLEAN";
        }

        // Date/Time
        if (javaType == java.time.LocalDate.class) {
            return "DATE";
        }
        if (javaType == java.time.LocalDateTime.class) {
            return "TIMESTAMP";
        }
        if (javaType == java.time.LocalTime.class) {
            return "TIME";
        }
        if (javaType == java.time.Instant.class ||
            javaType == java.time.ZonedDateTime.class ||
            javaType == java.time.OffsetDateTime.class) {
            return "TIMESTAMP WITH TIME ZONE";
        }
        if (javaType == java.sql.Date.class) {
            return "DATE";
        }
        if (javaType == java.sql.Timestamp.class) {
            return "TIMESTAMP";
        }
        if (javaType == java.util.Date.class) {
            return "TIMESTAMP WITH TIME ZONE";
        }

        // UUID
        if (javaType == java.util.UUID.class) {
            return "UUID";
        }

        // Binary
        if (javaType == byte[].class) {
            return "BYTEA";
        }

        // Arrays and Lists
        if (javaType.isArray()) {
            Class<?> componentType = javaType.getComponentType();
            return mapJavaTypeToSqlType(componentType) + "[]";
        }
        if (java.util.List.class.isAssignableFrom(javaType)) {
            return "TEXT[]";
        }

        // JSON (check for Jackson JsonNode)
        if (javaType.getName().equals("com.fasterxml.jackson.databind.JsonNode")) {
            return "JSONB";
        }

        // Enum
        if (javaType.isEnum()) {
            return "TEXT";
        }

        // Default to TEXT
        return "TEXT";
    }
}
