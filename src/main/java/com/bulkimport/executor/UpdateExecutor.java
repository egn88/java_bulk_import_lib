package com.bulkimport.executor;

import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.config.ConflictStrategy;
import com.bulkimport.exception.ExecutionException;
import com.bulkimport.mapping.TableMapping;
import com.bulkimport.util.SqlIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Executes UPDATE and UPSERT operations from staging tables.
 *
 * @param <T> the entity type
 */
public class UpdateExecutor<T> {

    private static final Logger log = LoggerFactory.getLogger(UpdateExecutor.class);

    private final Connection connection;
    private final TableMapping<T> mapping;
    private final BulkImportConfig config;

    /**
     * Creates a new update executor.
     */
    public UpdateExecutor(Connection connection, TableMapping<T> mapping, BulkImportConfig config) {
        this.connection = connection;
        this.mapping = mapping;
        this.config = config;
    }

    /**
     * Executes an UPDATE from the staging table to the target table.
     *
     * @param stagingTableName the staging table name
     * @return the number of rows updated
     */
    public int executeUpdate(String stagingTableName) {
        String updateSql = buildUpdateSql(stagingTableName);
        log.debug("Executing UPDATE: {}", updateSql);

        try (Statement stmt = connection.createStatement()) {
            int rowsUpdated = stmt.executeUpdate(updateSql);
            log.debug("UPDATE completed: {} rows", rowsUpdated);
            return rowsUpdated;
        } catch (SQLException e) {
            throw ExecutionException.updateFailed(mapping.getTableName(), e);
        }
    }

    /**
     * Executes an UPSERT (INSERT ... ON CONFLICT) from the staging table.
     *
     * @param stagingTableName the staging table name
     * @return the number of rows affected
     */
    public int executeUpsert(String stagingTableName) {
        String upsertSql = buildUpsertSql(stagingTableName);
        log.debug("Executing UPSERT: {}", upsertSql);

        try (Statement stmt = connection.createStatement()) {
            int rowsAffected = stmt.executeUpdate(upsertSql);
            log.debug("UPSERT completed: {} rows", rowsAffected);
            return rowsAffected;
        } catch (SQLException e) {
            throw ExecutionException.upsertFailed(mapping.getTableName(), e);
        }
    }

    private String buildUpdateSql(String stagingTableName) {
        String targetTable = getQuotedTargetTableName();
        List<String> matchColumns = getMatchColumns();
        List<String> updateColumns = getUpdateColumns();

        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(targetTable).append(" AS t SET ");

        // SET clause - quote column names
        String setClause = updateColumns.stream()
            .map(col -> SqlIdentifier.quote(col) + " = s." + SqlIdentifier.quote(col))
            .collect(Collectors.joining(", "));
        sql.append(setClause);

        // FROM clause - quote staging table name
        sql.append(" FROM ").append(SqlIdentifier.quote(stagingTableName)).append(" AS s");

        // WHERE clause for matching - quote column names
        sql.append(" WHERE ");
        String whereClause = matchColumns.stream()
            .map(col -> "t." + SqlIdentifier.quote(col) + " = s." + SqlIdentifier.quote(col))
            .collect(Collectors.joining(" AND "));
        sql.append(whereClause);

        return sql.toString();
    }

    private String buildUpsertSql(String stagingTableName) {
        String targetTable = getQuotedTargetTableName();
        List<String> allColumns = mapping.getColumnNames();
        List<String> conflictColumns = getConflictColumns();

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(targetTable);
        sql.append(" (").append(SqlIdentifier.quoteAndJoin(allColumns)).append(")");

        // SELECT from staging - quote column and table names
        sql.append(" SELECT ").append(SqlIdentifier.quoteAndJoin(allColumns));
        sql.append(" FROM ").append(SqlIdentifier.quote(stagingTableName));

        // ON CONFLICT clause
        ConflictStrategy strategy = config.getConflictStrategy();
        if (strategy == ConflictStrategy.DO_NOTHING) {
            sql.append(" ON CONFLICT (").append(SqlIdentifier.quoteAndJoin(conflictColumns)).append(")");
            sql.append(" DO NOTHING");
        } else if (strategy == ConflictStrategy.UPDATE_ALL || strategy == ConflictStrategy.UPDATE_SPECIFIED) {
            sql.append(" ON CONFLICT (").append(SqlIdentifier.quoteAndJoin(conflictColumns)).append(")");
            sql.append(" DO UPDATE SET ");

            List<String> updateColumns = getUpdateColumnsForUpsert();
            String updateClause = updateColumns.stream()
                .map(col -> SqlIdentifier.quote(col) + " = EXCLUDED." + SqlIdentifier.quote(col))
                .collect(Collectors.joining(", "));
            sql.append(updateClause);
        }
        // FAIL strategy: no ON CONFLICT clause, let it throw on constraint violation

        return sql.toString();
    }

    private String getQuotedTargetTableName() {
        String schema = config.getSchemaName();
        if (schema != null && !schema.isEmpty()) {
            return SqlIdentifier.quoteQualified(schema, mapping.getTableName());
        }
        return SqlIdentifier.quoteQualified(mapping.getSchemaName(), mapping.getTableName());
    }

    private List<String> getMatchColumns() {
        // Use explicitly configured match columns if available
        if (config.hasExplicitMatchColumns()) {
            return config.getMatchColumns();
        }

        // Default to ID columns
        List<String> idColumns = mapping.getIdColumnNames();
        if (idColumns.isEmpty()) {
            throw new IllegalStateException(
                "No match columns specified and no ID columns found. " +
                "Configure matchColumns() or ensure entity has @Id/@BulkId annotated fields."
            );
        }

        return idColumns;
    }

    private List<String> getConflictColumns() {
        // Use explicitly configured conflict columns if available
        if (config.hasConflictColumns()) {
            return config.getConflictColumns();
        }

        // Default to ID columns
        List<String> idColumns = mapping.getIdColumnNames();
        if (idColumns.isEmpty()) {
            throw new IllegalStateException(
                "No conflict columns specified and no ID columns found. " +
                "Configure conflictColumns() or ensure entity has @Id/@BulkId annotated fields."
            );
        }

        return idColumns;
    }

    private List<String> getUpdateColumns() {
        // Use explicitly configured update columns if available
        if (config.hasUpdateColumns()) {
            return config.getUpdateColumns();
        }

        // Default to all non-ID columns
        return mapping.getNonIdColumnNames();
    }

    private List<String> getUpdateColumnsForUpsert() {
        if (config.getConflictStrategy() == ConflictStrategy.UPDATE_SPECIFIED) {
            return config.getUpdateColumns();
        }

        // UPDATE_ALL: update all non-ID columns
        return mapping.getNonIdColumnNames();
    }
}
