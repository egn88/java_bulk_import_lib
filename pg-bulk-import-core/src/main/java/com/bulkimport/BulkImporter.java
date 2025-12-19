package com.bulkimport;

import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.converter.TypeConverter;
import com.bulkimport.converter.TypeConverterRegistry;
import com.bulkimport.exception.ExecutionException;
import com.bulkimport.executor.CopyExecutor;
import com.bulkimport.executor.StagingTableManager;
import com.bulkimport.executor.UpdateExecutor;
import com.bulkimport.mapping.EntityMapperResolver;
import com.bulkimport.mapping.TableMapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Main entry point for bulk import operations.
 * Provides a fluent API for inserting, updating, and upserting data
 * into PostgreSQL using the efficient COPY command.
 *
 * <h2>Usage Examples</h2>
 *
 * <h3>Basic Insert</h3>
 * <pre>{@code
 * BulkImporter importer = BulkImporter.create(dataSource);
 * int inserted = importer.insert(User.class, userList);
 * }</pre>
 *
 * <h3>Insert with Transaction</h3>
 * <pre>{@code
 * try (Connection conn = dataSource.getConnection()) {
 *     conn.setAutoCommit(false);
 *     BulkImporter importer = BulkImporter.create(conn);
 *     importer.insert(User.class, users);
 *     importer.insert(Order.class, orders);
 *     conn.commit();
 * }
 * }</pre>
 *
 * <h3>Upsert with Configuration</h3>
 * <pre>{@code
 * BulkImportConfig config = BulkImportConfig.builder()
 *     .conflictStrategy(ConflictStrategy.UPDATE_ALL)
 *     .conflictColumns("email")
 *     .build();
 *
 * BulkImporter importer = BulkImporter.create(dataSource)
 *     .withConfig(config);
 *
 * int affected = importer.upsert(User.class, userList);
 * }</pre>
 */
public class BulkImporter {

    private static final Logger log = LoggerFactory.getLogger(BulkImporter.class);

    private final ConnectionProvider connectionProvider;
    private final EntityMapperResolver mapperResolver;
    private final TypeConverterRegistry converterRegistry;
    private BulkImportConfig config;

    private BulkImporter(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
        this.mapperResolver = EntityMapperResolver.getInstance();
        this.converterRegistry = TypeConverterRegistry.createDefault();
        this.config = BulkImportConfig.defaults();
    }

    /**
     * Creates a BulkImporter using a Connection.
     * The connection will be used for all operations but NOT closed by the importer.
     *
     * @param connection the database connection
     * @return a new BulkImporter
     */
    public static BulkImporter create(Connection connection) {
        Objects.requireNonNull(connection, "connection cannot be null");
        return new BulkImporter(new SingleConnectionProvider(connection));
    }

    /**
     * Creates a BulkImporter using a DataSource.
     * A new connection will be obtained for each operation and closed after.
     *
     * @param dataSource the data source
     * @return a new BulkImporter
     */
    public static BulkImporter create(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource cannot be null");
        return new BulkImporter(new DataSourceConnectionProvider(dataSource));
    }

    /**
     * Configures the importer with the specified settings.
     *
     * @param config the configuration
     * @return this importer for chaining
     */
    public BulkImporter withConfig(BulkImportConfig config) {
        this.config = Objects.requireNonNull(config, "config cannot be null");
        return this;
    }

    /**
     * Registers a custom type converter.
     *
     * @param type the Java type
     * @param converter the converter
     * @return this importer for chaining
     */
    public <T> BulkImporter registerConverter(Class<T> type, TypeConverter<T> converter) {
        converterRegistry.register(type, converter);
        return this;
    }

    // ==================== INSERT Operations ====================

    /**
     * Bulk inserts entities using the entity class for mapping.
     *
     * @param entityClass the entity class
     * @param entities the entities to insert
     * @return the number of rows inserted
     */
    public <T> int insert(Class<T> entityClass, List<T> entities) {
        TableMapping<T> mapping = mapperResolver.resolve(entityClass);
        return insert(mapping, entities);
    }

    /**
     * Bulk inserts entities using the entity class for mapping.
     *
     * @param entityClass the entity class
     * @param entities the entities to insert
     * @return the number of rows inserted
     */
    public <T> int insert(Class<T> entityClass, Stream<T> entities) {
        TableMapping<T> mapping = mapperResolver.resolve(entityClass);
        return insert(mapping, entities);
    }

    /**
     * Bulk inserts entities using an explicit table mapping.
     *
     * @param mapping the table mapping
     * @param entities the entities to insert
     * @return the number of rows inserted
     */
    public <T> int insert(TableMapping<T> mapping, List<T> entities) {
        if (entities.isEmpty()) {
            log.debug("Empty list, skipping insert");
            return 0;
        }

        log.info("Starting bulk insert of {} entities to table '{}'",
                entities.size(), mapping.getTableName());

        return executeWithConnection(connection -> {
            CopyExecutor<T> executor = new CopyExecutor<>(connection, mapping, config, converterRegistry);
            long count = executor.copyIn(entities);
            return (int) count;
        });
    }

    /**
     * Bulk inserts entities using an explicit table mapping.
     *
     * @param mapping the table mapping
     * @param entities the entities to insert
     * @return the number of rows inserted
     */
    public <T> int insert(TableMapping<T> mapping, Stream<T> entities) {
        log.info("Starting bulk insert stream to table '{}'", mapping.getTableName());

        return executeWithConnection(connection -> {
            CopyExecutor<T> executor = new CopyExecutor<>(connection, mapping, config, converterRegistry);
            long count = executor.copyIn(entities);
            return (int) count;
        });
    }

    // ==================== UPDATE Operations ====================

    /**
     * Bulk updates entities using the entity class for mapping.
     * Requires entities to have ID columns defined.
     *
     * @param entityClass the entity class
     * @param entities the entities to update
     * @return the number of rows updated
     */
    public <T> int update(Class<T> entityClass, List<T> entities) {
        TableMapping<T> mapping = mapperResolver.resolve(entityClass);
        return update(mapping, entities);
    }

    /**
     * Bulk updates entities using the entity class for mapping.
     *
     * @param entityClass the entity class
     * @param entities the entities to update
     * @return the number of rows updated
     */
    public <T> int update(Class<T> entityClass, Stream<T> entities) {
        TableMapping<T> mapping = mapperResolver.resolve(entityClass);
        return update(mapping, entities);
    }

    /**
     * Bulk updates entities using an explicit table mapping.
     *
     * @param mapping the table mapping
     * @param entities the entities to update
     * @return the number of rows updated
     */
    public <T> int update(TableMapping<T> mapping, List<T> entities) {
        if (entities.isEmpty()) {
            log.debug("Empty list, skipping update");
            return 0;
        }

        log.info("Starting bulk update of {} entities to table '{}'",
                entities.size(), mapping.getTableName());

        return executeWithConnection(connection -> executeUpdate(connection, mapping, entities, null));
    }

    /**
     * Bulk updates entities using an explicit table mapping.
     *
     * @param mapping the table mapping
     * @param entities the entities to update
     * @return the number of rows updated
     */
    public <T> int update(TableMapping<T> mapping, Stream<T> entities) {
        log.info("Starting bulk update stream to table '{}'", mapping.getTableName());

        return executeWithConnection(connection -> executeUpdate(connection, mapping, null, entities));
    }

    private <T> int executeUpdate(Connection connection, TableMapping<T> mapping,
                                   List<T> list, Stream<T> stream) {
        StagingTableManager<T> stagingManager = new StagingTableManager<>(connection, mapping, config);

        try {
            // Create staging table
            String stagingTable = stagingManager.createStagingTable();

            // Copy data to staging table
            CopyExecutor<T> copyExecutor = new CopyExecutor<>(connection, mapping, config, converterRegistry);
            if (list != null) {
                copyExecutor.copyInTo(stagingTable, list);
            } else {
                copyExecutor.copyInTo(stagingTable, stream);
            }

            // Execute UPDATE from staging to target
            UpdateExecutor<T> updateExecutor = new UpdateExecutor<>(connection, mapping, config);
            return updateExecutor.executeUpdate(stagingTable);

        } finally {
            stagingManager.dropStagingTable();
        }
    }

    // ==================== UPSERT Operations ====================

    /**
     * Bulk upserts (insert or update) entities using the entity class for mapping.
     *
     * @param entityClass the entity class
     * @param entities the entities to upsert
     * @return the number of rows affected
     */
    public <T> int upsert(Class<T> entityClass, List<T> entities) {
        TableMapping<T> mapping = mapperResolver.resolve(entityClass);
        return upsert(mapping, entities);
    }

    /**
     * Bulk upserts entities using the entity class for mapping.
     *
     * @param entityClass the entity class
     * @param entities the entities to upsert
     * @return the number of rows affected
     */
    public <T> int upsert(Class<T> entityClass, Stream<T> entities) {
        TableMapping<T> mapping = mapperResolver.resolve(entityClass);
        return upsert(mapping, entities);
    }

    /**
     * Bulk upserts entities using an explicit table mapping.
     *
     * @param mapping the table mapping
     * @param entities the entities to upsert
     * @return the number of rows affected
     */
    public <T> int upsert(TableMapping<T> mapping, List<T> entities) {
        if (entities.isEmpty()) {
            log.debug("Empty list, skipping upsert");
            return 0;
        }

        log.info("Starting bulk upsert of {} entities to table '{}'",
                entities.size(), mapping.getTableName());

        return executeWithConnection(connection -> executeUpsert(connection, mapping, entities, null));
    }

    /**
     * Bulk upserts entities using an explicit table mapping.
     *
     * @param mapping the table mapping
     * @param entities the entities to upsert
     * @return the number of rows affected
     */
    public <T> int upsert(TableMapping<T> mapping, Stream<T> entities) {
        log.info("Starting bulk upsert stream to table '{}'", mapping.getTableName());

        return executeWithConnection(connection -> executeUpsert(connection, mapping, null, entities));
    }

    private <T> int executeUpsert(Connection connection, TableMapping<T> mapping,
                                   List<T> list, Stream<T> stream) {
        StagingTableManager<T> stagingManager = new StagingTableManager<>(connection, mapping, config);

        try {
            // Create staging table
            String stagingTable = stagingManager.createStagingTable();

            // Copy data to staging table
            CopyExecutor<T> copyExecutor = new CopyExecutor<>(connection, mapping, config, converterRegistry);
            if (list != null) {
                copyExecutor.copyInTo(stagingTable, list);
            } else {
                copyExecutor.copyInTo(stagingTable, stream);
            }

            // Execute UPSERT from staging to target
            UpdateExecutor<T> updateExecutor = new UpdateExecutor<>(connection, mapping, config);
            return updateExecutor.executeUpsert(stagingTable);

        } finally {
            stagingManager.dropStagingTable();
        }
    }

    // ==================== Helper Methods ====================

    private <T> T executeWithConnection(ConnectionFunction<T> function) {
        return connectionProvider.execute(function);
    }

    @FunctionalInterface
    interface ConnectionFunction<T> {
        T apply(Connection connection) throws SQLException;
    }

    interface ConnectionProvider {
        <T> T execute(ConnectionFunction<T> function);
    }

    private static class SingleConnectionProvider implements ConnectionProvider {
        private final Connection connection;

        SingleConnectionProvider(Connection connection) {
            this.connection = connection;
        }

        @Override
        public <T> T execute(ConnectionFunction<T> function) {
            try {
                return function.apply(connection);
            } catch (SQLException e) {
                throw ExecutionException.connectionError("operation", e);
            }
        }
    }

    private static class DataSourceConnectionProvider implements ConnectionProvider {
        private final DataSource dataSource;

        DataSourceConnectionProvider(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public <T> T execute(ConnectionFunction<T> function) {
            try (Connection connection = dataSource.getConnection()) {
                return function.apply(connection);
            } catch (SQLException e) {
                throw ExecutionException.connectionError("operation", e);
            }
        }
    }
}
