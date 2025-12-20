package com.bulkimport.exception;

/**
 * Exception thrown during bulk import execution.
 */
public class ExecutionException extends BulkImportException {

    public ExecutionException(String message) {
        super(message);
    }

    public ExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates an exception for COPY command failure.
     */
    public static ExecutionException copyFailed(String tableName, Throwable cause) {
        return new ExecutionException(
            String.format("COPY command failed for table '%s': %s", tableName, getMessageOrDefault(cause)),
            cause
        );
    }

    /**
     * Creates an exception for staging table creation failure.
     */
    public static ExecutionException stagingTableCreationFailed(String tableName, Throwable cause) {
        return new ExecutionException(
            String.format("Failed to create staging table '%s': %s", tableName, getMessageOrDefault(cause)),
            cause
        );
    }

    /**
     * Creates an exception for staging table cleanup failure.
     */
    public static ExecutionException stagingTableCleanupFailed(String tableName, Throwable cause) {
        return new ExecutionException(
            String.format("Failed to drop staging table '%s': %s", tableName, getMessageOrDefault(cause)),
            cause
        );
    }

    /**
     * Creates an exception for update operation failure.
     */
    public static ExecutionException updateFailed(String tableName, Throwable cause) {
        return new ExecutionException(
            String.format("UPDATE operation failed for table '%s': %s", tableName, getMessageOrDefault(cause)),
            cause
        );
    }

    /**
     * Creates an exception for upsert operation failure.
     */
    public static ExecutionException upsertFailed(String tableName, Throwable cause) {
        return new ExecutionException(
            String.format("UPSERT operation failed for table '%s': %s", tableName, getMessageOrDefault(cause)),
            cause
        );
    }

    /**
     * Creates an exception for connection/transaction errors.
     */
    public static ExecutionException connectionError(String operation, Throwable cause) {
        return new ExecutionException(
            String.format("Database connection error during %s: %s", operation, getMessageOrDefault(cause)),
            cause
        );
    }

    /**
     * Creates an exception for CSV generation errors.
     */
    public static ExecutionException csvGenerationFailed(Throwable cause) {
        return new ExecutionException(
            String.format("Failed to generate CSV data: %s", getMessageOrDefault(cause)),
            cause
        );
    }

    /**
     * Gets the message from a throwable, or a default message if null.
     */
    private static String getMessageOrDefault(Throwable cause) {
        if (cause == null) {
            return "(no cause)";
        }
        String message = cause.getMessage();
        if (message == null || message.isEmpty()) {
            return cause.getClass().getName();
        }
        return message;
    }

    /**
     * Creates an exception when the connection is not a PostgreSQL connection.
     */
    public static ExecutionException notPostgresConnection() {
        return new ExecutionException(
            "The provided connection is not a PostgreSQL connection. " +
            "This library requires a PostgreSQL database with COPY command support."
        );
    }
}
