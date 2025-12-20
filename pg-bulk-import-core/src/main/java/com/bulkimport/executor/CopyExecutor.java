package com.bulkimport.executor;

import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.converter.TypeConverterRegistry;
import com.bulkimport.csv.CsvStreamWriter;
import com.bulkimport.exception.ExecutionException;
import com.bulkimport.mapping.TableMapping;
import com.bulkimport.util.SqlIdentifier;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Executes PostgreSQL COPY commands for bulk data loading.
 *
 * @param <T> the entity type
 */
public class CopyExecutor<T> {

    private static final Logger log = LoggerFactory.getLogger(CopyExecutor.class);
    private static final int PIPE_BUFFER_SIZE = 1024 * 1024; // 1MB buffer

    private final Connection connection;
    private final TableMapping<T> mapping;
    private final BulkImportConfig config;
    private final TypeConverterRegistry converterRegistry;

    /**
     * Creates a new COPY executor.
     *
     * @param connection the database connection (must not be null)
     * @param mapping the table mapping (must not be null)
     * @param config the import configuration (must not be null)
     * @throws NullPointerException if any parameter is null
     */
    public CopyExecutor(Connection connection, TableMapping<T> mapping, BulkImportConfig config) {
        this(connection, mapping, config, TypeConverterRegistry.getDefault());
    }

    /**
     * Creates a new COPY executor with a custom converter registry.
     *
     * @param connection the database connection (must not be null)
     * @param mapping the table mapping (must not be null)
     * @param config the import configuration (must not be null)
     * @param converterRegistry the type converter registry (must not be null)
     * @throws NullPointerException if any parameter is null
     */
    public CopyExecutor(Connection connection, TableMapping<T> mapping,
                        BulkImportConfig config, TypeConverterRegistry converterRegistry) {
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");
        this.mapping = Objects.requireNonNull(mapping, "mapping cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.converterRegistry = Objects.requireNonNull(converterRegistry, "converterRegistry cannot be null");
    }

    /**
     * Executes COPY to insert entities from a list.
     *
     * @param entities the entities to insert
     * @return the number of rows inserted
     */
    public long copyIn(List<T> entities) {
        return entities.isEmpty() ? 0 : copyIn(entities.stream());
    }

    /**
     * Executes COPY to insert entities from a stream.
     *
     * @param entities the entities to insert
     * @return the number of rows inserted
     */
    public long copyIn(Stream<T> entities) {
        return executeCopy(getFullTableName(), entities);
    }

    /**
     * Executes COPY to the specified table (for staging tables).
     *
     * @param tableName the target table name
     * @param entities the entities to insert
     * @return the number of rows inserted
     */
    public long copyInTo(String tableName, List<T> entities) {
        return entities.isEmpty() ? 0 : copyInTo(tableName, entities.stream());
    }

    /**
     * Executes COPY to the specified table (for staging tables).
     *
     * @param tableName the target table name
     * @param entities the entities to insert
     * @return the number of rows inserted
     */
    public long copyInTo(String tableName, Stream<T> entities) {
        return executeCopy(tableName, entities);
    }

    private long executeCopy(String tableName, Stream<T> entities) {
        String copyCommand = buildCopyCommand(tableName);
        log.debug("Executing COPY command: {}", copyCommand);

        try {
            CopyManager copyManager = getCopyManager();
            CsvStreamWriter<T> csvWriter = new CsvStreamWriter<>(mapping, config, converterRegistry);

            // Use piped streams to connect CSV writer to COPY command
            try (PipedOutputStream pipedOut = new PipedOutputStream();
                 PipedInputStream pipedIn = new PipedInputStream(pipedOut, PIPE_BUFFER_SIZE)) {

                // Start CSV writing in a separate thread
                ExecutorService executor = Executors.newSingleThreadExecutor();
                CompletableFuture<Integer> writerFuture = CompletableFuture.supplyAsync(() -> {
                    try {
                        int count = csvWriter.write(entities, pipedOut);
                        pipedOut.close();
                        return count;
                    } catch (IOException e) {
                        throw ExecutionException.csvGenerationFailed(e);
                    }
                }, executor);

                // Execute COPY command (reads from piped input)
                long rowsCopied = copyManager.copyIn(copyCommand, pipedIn);

                // Wait for writer to complete and check for errors
                try {
                    writerFuture.join();
                } catch (CompletionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof ExecutionException) {
                        throw (ExecutionException) cause;
                    }
                    throw ExecutionException.csvGenerationFailed(
                            cause instanceof Exception ? (Exception) cause : e);
                } finally {
                    executor.shutdown();
                    try {
                        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                            executor.shutdownNow();
                            log.warn("CSV writer thread did not terminate within timeout");
                        }
                    } catch (InterruptedException ie) {
                        executor.shutdownNow();
                        Thread.currentThread().interrupt();
                    }
                }

                log.debug("COPY completed: {} rows", rowsCopied);
                return rowsCopied;
            }

        } catch (SQLException e) {
            throw ExecutionException.copyFailed(tableName, e);
        } catch (IOException e) {
            throw ExecutionException.csvGenerationFailed(e);
        }
    }

    private CopyManager getCopyManager() throws SQLException {
        PGConnection pgConnection = unwrapPGConnection();
        return pgConnection.getCopyAPI();
    }

    private PGConnection unwrapPGConnection() throws SQLException {
        if (connection.isWrapperFor(PGConnection.class)) {
            return connection.unwrap(PGConnection.class);
        }

        // Try direct cast for basic JDBC connections
        if (connection instanceof PGConnection) {
            return (PGConnection) connection;
        }

        throw ExecutionException.notPostgresConnection();
    }

    private String buildCopyCommand(String tableName) {
        List<String> columnNames = mapping.getColumnNames();

        // Build COPY command with CSV format - quote all identifiers
        StringBuilder sb = new StringBuilder();
        sb.append("COPY ").append(getQuotedTableName(tableName));
        sb.append(" (").append(SqlIdentifier.quoteAndJoin(columnNames)).append(")");
        sb.append(" FROM STDIN WITH (FORMAT csv");

        // Add NULL handling if not using empty string
        switch (config.getNullHandling()) {
            case LITERAL_NULL:
                sb.append(", NULL '\\N'");
                break;
            case NULL_STRING:
                sb.append(", NULL 'NULL'");
                break;
            case EMPTY_STRING:
                // EMPTY_STRING is the default, no need to specify
                break;
        }

        sb.append(")");

        return sb.toString();
    }

    private String getQuotedTableName(String tableName) {
        // If tableName is a staging table (no schema), just quote it
        if (tableName.startsWith(config.getStagingTablePrefix())) {
            return SqlIdentifier.quote(tableName);
        }
        // For target table, use schema-qualified quoting
        String schema = config.getSchemaName();
        if (schema != null && !schema.isEmpty()) {
            return SqlIdentifier.quoteQualified(schema, mapping.getTableName());
        }
        return SqlIdentifier.quoteQualified(mapping.getSchemaName(), mapping.getTableName());
    }

    private String getFullTableName() {
        // Return unquoted table name for internal use
        return mapping.getTableName();
    }
}
