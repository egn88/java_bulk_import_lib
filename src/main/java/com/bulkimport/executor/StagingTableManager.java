package com.bulkimport.executor;

import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.exception.ExecutionException;
import com.bulkimport.mapping.TableMapping;
import com.bulkimport.util.SqlIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

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

            // Make all columns nullable in staging table to allow partial data
            // This is needed when ID columns are auto-generated and not in the mapping
            makeColumnsNullable(stmt);

            log.info("Created staging table: {}", stagingTableName);
            return stagingTableName;
        } catch (SQLException e) {
            throw ExecutionException.stagingTableCreationFailed(stagingTableName, e);
        }
    }

    private void makeColumnsNullable(Statement stmt) throws SQLException {
        // Get all columns that have NOT NULL constraint and drop it
        // Use parameterized query for the table name lookup
        String query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
            AND is_nullable = 'NO'
            """;

        try (PreparedStatement pstmt = connection.prepareStatement(query)) {
            pstmt.setString(1, stagingTableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    // Validate and quote identifiers to prevent SQL injection
                    String alterSql = "ALTER TABLE " + SqlIdentifier.quote(stagingTableName) +
                                     " ALTER COLUMN " + SqlIdentifier.quote(columnName) + " DROP NOT NULL";
                    log.debug("Dropping NOT NULL constraint: {}", alterSql);
                    stmt.execute(alterSql);
                }
            }
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

        String dropSql = "DROP TABLE IF EXISTS " + SqlIdentifier.quote(stagingTableName);
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

        // Quote staging table name (already validated in generateStagingTableName)
        sql.append(SqlIdentifier.quote(stagingTableName));

        // Use LIKE to copy column definitions from target table
        // This ensures correct types regardless of how mapping was defined
        // Quote the source table name for safety
        String sourceTable = SqlIdentifier.quoteQualified(
                mapping.getSchemaName(), mapping.getTableName());
        sql.append(" (LIKE ").append(sourceTable).append(")");

        return sql.toString();
    }
}
