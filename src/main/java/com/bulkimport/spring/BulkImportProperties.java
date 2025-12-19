package com.bulkimport.spring;

import com.bulkimport.config.ConflictStrategy;
import com.bulkimport.config.NullHandling;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Configuration properties for bulk import operations.
 * Can be configured in application.properties or application.yml:
 *
 * <pre>
 * bulkimport.batch-size=5000
 * bulkimport.conflict-strategy=UPDATE_ALL
 * bulkimport.use-unlogged-tables=true
 * </pre>
 */
@ConfigurationProperties(prefix = "bulkimport")
public class BulkImportProperties {

    /**
     * Batch size for streaming operations.
     */
    private int batchSize = 10_000;

    /**
     * Strategy for handling conflicts during upsert operations.
     */
    private ConflictStrategy conflictStrategy = ConflictStrategy.FAIL;

    /**
     * Columns to use for conflict detection (ON CONFLICT clause).
     */
    private List<String> conflictColumns;

    /**
     * Columns to update when a conflict occurs.
     */
    private List<String> updateColumns;

    /**
     * Columns to use for matching rows during UPDATE operations.
     */
    private List<String> matchColumns;

    /**
     * Whether to use UNLOGGED tables for staging.
     */
    private boolean useUnloggedTables = true;

    /**
     * Prefix for staging table names.
     */
    private String stagingTablePrefix = "bulk_staging_";

    /**
     * Whether to automatically drop staging tables after use.
     */
    private boolean autoCleanupStaging = true;

    /**
     * How null values should be represented in CSV.
     */
    private NullHandling nullHandling = NullHandling.EMPTY_STRING;

    /**
     * Default schema name for tables.
     */
    private String schemaName;

    /**
     * Whether to enable the auto-configuration.
     */
    private boolean enabled = true;

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public ConflictStrategy getConflictStrategy() {
        return conflictStrategy;
    }

    public void setConflictStrategy(ConflictStrategy conflictStrategy) {
        this.conflictStrategy = conflictStrategy;
    }

    public List<String> getConflictColumns() {
        return conflictColumns;
    }

    public void setConflictColumns(List<String> conflictColumns) {
        this.conflictColumns = conflictColumns;
    }

    public List<String> getUpdateColumns() {
        return updateColumns;
    }

    public void setUpdateColumns(List<String> updateColumns) {
        this.updateColumns = updateColumns;
    }

    public List<String> getMatchColumns() {
        return matchColumns;
    }

    public void setMatchColumns(List<String> matchColumns) {
        this.matchColumns = matchColumns;
    }

    public boolean isUseUnloggedTables() {
        return useUnloggedTables;
    }

    public void setUseUnloggedTables(boolean useUnloggedTables) {
        this.useUnloggedTables = useUnloggedTables;
    }

    public String getStagingTablePrefix() {
        return stagingTablePrefix;
    }

    public void setStagingTablePrefix(String stagingTablePrefix) {
        this.stagingTablePrefix = stagingTablePrefix;
    }

    public boolean isAutoCleanupStaging() {
        return autoCleanupStaging;
    }

    public void setAutoCleanupStaging(boolean autoCleanupStaging) {
        this.autoCleanupStaging = autoCleanupStaging;
    }

    public NullHandling getNullHandling() {
        return nullHandling;
    }

    public void setNullHandling(NullHandling nullHandling) {
        this.nullHandling = nullHandling;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
