package com.bulkimport.config;

import com.bulkimport.exception.ConfigurationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Configuration for bulk import operations.
 * Use the builder pattern to create instances.
 */
public class BulkImportConfig {

    private final int batchSize;
    private final ConflictStrategy conflictStrategy;
    private final List<String> conflictColumns;
    private final List<String> updateColumns;
    private final List<String> matchColumns;
    private final boolean useUnloggedTables;
    private final String stagingTablePrefix;
    private final boolean autoCleanupStaging;
    private final NullHandling nullHandling;
    private final String schemaName;

    private BulkImportConfig(Builder builder) {
        this.batchSize = builder.batchSize;
        this.conflictStrategy = builder.conflictStrategy;
        this.conflictColumns = Collections.unmodifiableList(new ArrayList<>(builder.conflictColumns));
        this.updateColumns = Collections.unmodifiableList(new ArrayList<>(builder.updateColumns));
        this.matchColumns = Collections.unmodifiableList(new ArrayList<>(builder.matchColumns));
        this.useUnloggedTables = builder.useUnloggedTables;
        this.stagingTablePrefix = builder.stagingTablePrefix;
        this.autoCleanupStaging = builder.autoCleanupStaging;
        this.nullHandling = builder.nullHandling;
        this.schemaName = builder.schemaName;
    }

    /**
     * Creates a new builder with default values.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a default configuration.
     */
    public static BulkImportConfig defaults() {
        return builder().build();
    }

    public int getBatchSize() {
        return batchSize;
    }

    public ConflictStrategy getConflictStrategy() {
        return conflictStrategy;
    }

    public List<String> getConflictColumns() {
        return conflictColumns;
    }

    public List<String> getUpdateColumns() {
        return updateColumns;
    }

    public List<String> getMatchColumns() {
        return matchColumns;
    }

    public boolean isUseUnloggedTables() {
        return useUnloggedTables;
    }

    public String getStagingTablePrefix() {
        return stagingTablePrefix;
    }

    public boolean isAutoCleanupStaging() {
        return autoCleanupStaging;
    }

    public NullHandling getNullHandling() {
        return nullHandling;
    }

    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Returns true if match columns are explicitly specified.
     */
    public boolean hasExplicitMatchColumns() {
        return !matchColumns.isEmpty();
    }

    /**
     * Returns true if conflict columns are specified.
     */
    public boolean hasConflictColumns() {
        return !conflictColumns.isEmpty();
    }

    /**
     * Returns true if update columns are specified.
     */
    public boolean hasUpdateColumns() {
        return !updateColumns.isEmpty();
    }

    /**
     * Builder for BulkImportConfig.
     */
    public static class Builder {
        private int batchSize = 10_000;
        private ConflictStrategy conflictStrategy = ConflictStrategy.FAIL;
        private List<String> conflictColumns = new ArrayList<>();
        private List<String> updateColumns = new ArrayList<>();
        private List<String> matchColumns = new ArrayList<>();
        private boolean useUnloggedTables = true;
        private String stagingTablePrefix = "bulk_staging_";
        private boolean autoCleanupStaging = true;
        private NullHandling nullHandling = NullHandling.EMPTY_STRING;
        private String schemaName = null;

        private Builder() {
        }

        /**
         * Sets the batch size for streaming operations.
         * Default: 10,000
         *
         * @param batchSize must be greater than 0
         */
        public Builder batchSize(int batchSize) {
            if (batchSize <= 0) {
                throw ConfigurationException.invalidBatchSize(batchSize);
            }
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets the conflict resolution strategy for upsert operations.
         * Default: FAIL
         */
        public Builder conflictStrategy(ConflictStrategy strategy) {
            this.conflictStrategy = Objects.requireNonNull(strategy, "conflictStrategy cannot be null");
            return this;
        }

        /**
         * Sets the columns to use for conflict detection (ON CONFLICT clause).
         * These are typically the primary key or unique constraint columns.
         */
        public Builder conflictColumns(String... columns) {
            this.conflictColumns = List.of(columns);
            return this;
        }

        /**
         * Sets the columns to use for conflict detection (ON CONFLICT clause).
         */
        public Builder conflictColumns(List<String> columns) {
            this.conflictColumns = new ArrayList<>(columns);
            return this;
        }

        /**
         * Sets the columns to update when a conflict occurs.
         * Only used with UPDATE_SPECIFIED strategy.
         */
        public Builder updateColumns(String... columns) {
            this.updateColumns = List.of(columns);
            return this;
        }

        /**
         * Sets the columns to update when a conflict occurs.
         */
        public Builder updateColumns(List<String> columns) {
            this.updateColumns = new ArrayList<>(columns);
            return this;
        }

        /**
         * Sets the columns to use for matching rows during UPDATE operations.
         * If not specified, ID columns from the mapping will be used.
         */
        public Builder matchColumns(String... columns) {
            this.matchColumns = List.of(columns);
            return this;
        }

        /**
         * Sets the columns to use for matching rows during UPDATE operations.
         */
        public Builder matchColumns(List<String> columns) {
            this.matchColumns = new ArrayList<>(columns);
            return this;
        }

        /**
         * Sets whether to use UNLOGGED tables for staging.
         * UNLOGGED tables are faster but not crash-safe.
         * Default: true
         */
        public Builder useUnloggedTables(boolean useUnlogged) {
            this.useUnloggedTables = useUnlogged;
            return this;
        }

        /**
         * Sets the prefix for staging table names.
         * Default: "bulk_staging_"
         */
        public Builder stagingTablePrefix(String prefix) {
            this.stagingTablePrefix = Objects.requireNonNull(prefix, "stagingTablePrefix cannot be null");
            return this;
        }

        /**
         * Sets whether to automatically drop staging tables after use.
         * Default: true
         */
        public Builder autoCleanupStaging(boolean autoCleanup) {
            this.autoCleanupStaging = autoCleanup;
            return this;
        }

        /**
         * Sets how null values should be represented in CSV.
         * Default: EMPTY_STRING
         */
        public Builder nullHandling(NullHandling handling) {
            this.nullHandling = Objects.requireNonNull(handling, "nullHandling cannot be null");
            return this;
        }

        /**
         * Sets the schema name for tables.
         * If null, the default schema is used.
         */
        public Builder schemaName(String schema) {
            this.schemaName = schema;
            return this;
        }

        /**
         * Builds the configuration.
         *
         * @throws ConfigurationException if the configuration is invalid
         */
        public BulkImportConfig build() {
            validate();
            return new BulkImportConfig(this);
        }

        private void validate() {
            if (conflictStrategy == ConflictStrategy.UPDATE_ALL ||
                conflictStrategy == ConflictStrategy.UPDATE_SPECIFIED) {
                if (conflictColumns.isEmpty()) {
                    throw ConfigurationException.missingConflictColumns();
                }
            }

            if (conflictStrategy == ConflictStrategy.UPDATE_SPECIFIED && updateColumns.isEmpty()) {
                throw ConfigurationException.missingUpdateColumns();
            }
        }
    }
}
