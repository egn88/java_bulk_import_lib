package com.bulkimport.config;

/**
 * Defines the strategy for handling conflicts during upsert operations.
 */
public enum ConflictStrategy {

    /**
     * Throws an exception if a conflict occurs (default behavior).
     * No ON CONFLICT clause is added; PostgreSQL will raise a unique constraint violation.
     */
    FAIL,

    /**
     * Ignores rows that would cause a conflict.
     * Uses: ON CONFLICT DO NOTHING
     */
    DO_NOTHING,

    /**
     * Updates all non-ID columns when a conflict occurs.
     * Uses: ON CONFLICT (conflict_columns) DO UPDATE SET col1 = EXCLUDED.col1, ...
     */
    UPDATE_ALL,

    /**
     * Updates only the specified columns when a conflict occurs.
     * Requires updateColumns to be specified in BulkImportConfig.
     * Uses: ON CONFLICT (conflict_columns) DO UPDATE SET col1 = EXCLUDED.col1, ...
     */
    UPDATE_SPECIFIED
}
