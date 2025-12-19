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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
     */
    public CopyExecutor(Connection connection, TableMapping<T> mapping, BulkImportConfig config) {
        this(connection, mapping, config, TypeConverterRegistry.getDefault());
    }

    /**
     * Creates a new COPY executor with a custom converter registry.
     */
    public CopyExecutor(Connection connection, TableMapping<T> mapping,
                        BulkImportConfig config, TypeConverterRegistry converterRegistry) {
        this.connection = connection;
        this.mapping = mapping;
        this.config = config;
        this.converterRegistry = converterRegistry;
    }

    /**
     * Executes COPY to insert entities from a list.
     *
     * @param entities the entities to insert
     * @return the number of rows inserted
     */
    public long copyIn(List<T> entities) {
        if (entities.isEmpty()) {
            return 0;
        }

        return executeCopy(entities, null);
    }

    /**
     * Executes COPY to insert entities from a stream.
     *
     * @param entities the entities to insert
     * @return the number of rows inserted
     */
    public long copyIn(Stream<T> entities) {
        return executeCopy(null, entities);
    }

    /**
     * Executes COPY to the specified table (for staging tables).
     *
     * @param tableName the target table name
     * @param entities the entities to insert
     * @return the number of rows inserted
     */
    public long copyInTo(String tableName, List<T> entities) {
        if (entities.isEmpty()) {
            return 0;
        }

        return executeCopy(tableName, entities, null);
    }

    /**
     * Executes COPY to the specified table (for staging tables).
     *
     * @param tableName the target table name
     * @param entities the entities to insert
     * @return the number of rows inserted
     */
    public long copyInTo(String tableName, Stream<T> entities) {
        return executeCopy(tableName, null, entities);
    }

    private long executeCopy(List<T> list, Stream<T> stream) {
        String tableName = getFullTableName();
        return executeCopy(tableName, list, stream);
    }

    private long executeCopy(String tableName, List<T> list, Stream<T> stream) {
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
                        int count;
                        if (list != null) {
                            count = csvWriter.write(list, pipedOut);
                        } else {
                            count = csvWriter.write(stream, pipedOut);
                        }
                        pipedOut.close();
                        return count;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);

                // Execute COPY command (reads from piped input)
                long rowsCopied = copyManager.copyIn(copyCommand, pipedIn);

                // Wait for writer to complete and check for errors
                try {
                    writerFuture.join();
                } catch (Exception e) {
                    throw new RuntimeException("CSV writing failed", e);
                } finally {
                    executor.shutdown();
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
            case LITERAL_NULL -> sb.append(", NULL '\\N'");
            case NULL_STRING -> sb.append(", NULL 'NULL'");
            // EMPTY_STRING is the default, no need to specify
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
