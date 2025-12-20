package com.bulkimport.mapping;

import com.bulkimport.exception.MappingException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents a complete mapping from an entity class to a database table.
 *
 * @param <T> the entity type
 */
public class TableMapping<T> {

    private final String tableName;
    private final String schemaName;
    private final Class<T> entityClass;
    private final Map<String, ColumnMapping<T, ?>> columns;
    private final List<ColumnMapping<T, ?>> idColumns;
    private final List<ColumnMapping<T, ?>> nonIdColumns;

    private TableMapping(Builder<T> builder) {
        this.tableName = builder.tableName;
        this.schemaName = builder.schemaName;
        this.entityClass = builder.entityClass;
        this.columns = Collections.unmodifiableMap(new LinkedHashMap<>(builder.columns));

        // Use Java 8 compatible unmodifiable list creation
        this.idColumns = Collections.unmodifiableList(
            columns.values().stream()
                .filter(ColumnMapping::isId)
                .collect(Collectors.toList()));

        this.nonIdColumns = Collections.unmodifiableList(
            columns.values().stream()
                .filter(col -> !col.isId())
                .collect(Collectors.toList()));
    }

    /**
     * Creates a new builder for a table mapping.
     *
     * @param tableName the database table name
     */
    public static <T> Builder<T> builder(String tableName) {
        return new Builder<>(tableName);
    }

    /**
     * Creates a new builder for a table mapping with entity class.
     *
     * @param tableName the database table name
     * @param entityClass the entity class
     */
    public static <T> Builder<T> builder(String tableName, Class<T> entityClass) {
        return new Builder<T>(tableName).entityClass(entityClass);
    }

    /**
     * Gets the database table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Gets the schema name, or null if using default schema.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Gets the fully qualified table name (schema.table or just table).
     */
    public String getFullTableName() {
        if (schemaName != null && !schemaName.isEmpty()) {
            return schemaName + "." + tableName;
        }
        return tableName;
    }

    /**
     * Gets the entity class, or null if not specified.
     */
    public Class<T> getEntityClass() {
        return entityClass;
    }

    /**
     * Gets all column mappings in order.
     * Returns an unmodifiable list.
     */
    public List<ColumnMapping<T, ?>> getColumns() {
        return Collections.unmodifiableList(new ArrayList<>(columns.values()));
    }

    /**
     * Gets a column mapping by name.
     */
    public ColumnMapping<T, ?> getColumn(String columnName) {
        return columns.get(columnName);
    }

    /**
     * Gets all ID column mappings.
     */
    public List<ColumnMapping<T, ?>> getIdColumns() {
        return idColumns;
    }

    /**
     * Gets all non-ID column mappings.
     */
    public List<ColumnMapping<T, ?>> getNonIdColumns() {
        return nonIdColumns;
    }

    /**
     * Gets the column names in order.
     * Returns an unmodifiable list.
     */
    public List<String> getColumnNames() {
        return Collections.unmodifiableList(new ArrayList<>(columns.keySet()));
    }

    /**
     * Gets the ID column names.
     * Returns an unmodifiable list.
     */
    public List<String> getIdColumnNames() {
        return Collections.unmodifiableList(
            idColumns.stream()
                .map(ColumnMapping::getColumnName)
                .collect(Collectors.toList()));
    }

    /**
     * Gets the non-ID column names.
     * Returns an unmodifiable list.
     */
    public List<String> getNonIdColumnNames() {
        return Collections.unmodifiableList(
            nonIdColumns.stream()
                .map(ColumnMapping::getColumnName)
                .collect(Collectors.toList()));
    }

    /**
     * Returns true if this mapping has ID columns defined.
     */
    public boolean hasIdColumns() {
        return !idColumns.isEmpty();
    }

    /**
     * Gets the number of columns.
     */
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public String toString() {
        return "TableMapping{" +
               "tableName='" + getFullTableName() + '\'' +
               ", columns=" + getColumnNames() +
               ", idColumns=" + getIdColumnNames() +
               '}';
    }

    /**
     * Builder for TableMapping with fluent API.
     */
    public static class Builder<T> {
        private final String tableName;
        private String schemaName;
        private Class<T> entityClass;
        private final Map<String, ColumnMapping<T, ?>> columns = new LinkedHashMap<>();

        private Builder(String tableName) {
            this.tableName = Objects.requireNonNull(tableName, "tableName cannot be null");
        }

        /**
         * Sets the schema name.
         */
        public Builder<T> schema(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        /**
         * Sets the entity class.
         */
        public Builder<T> entityClass(Class<T> entityClass) {
            this.entityClass = entityClass;
            return this;
        }

        /**
         * Adds an ID column with a value extractor.
         *
         * @param columnName the database column name
         * @param extractor function to extract the value from an entity
         */
        public <V> Builder<T> id(String columnName, Function<T, V> extractor) {
            return id(columnName, extractor, null);
        }

        /**
         * Adds an ID column with a value extractor and explicit type.
         *
         * @param columnName the database column name
         * @param extractor function to extract the value from an entity
         * @param valueType the Java type of the value
         */
        @SuppressWarnings("unchecked")
        public <V> Builder<T> id(String columnName, Function<T, V> extractor, Class<V> valueType) {
            Objects.requireNonNull(columnName, "columnName cannot be null");
            Objects.requireNonNull(extractor, "extractor cannot be null");

            if (columns.containsKey(columnName)) {
                throw MappingException.duplicateColumn(columnName);
            }

            Class<V> type = valueType != null ? valueType : (Class<V>) Object.class;
            ColumnMapping<T, V> mapping = ColumnMapping.<T, V>builder(columnName, type)
                .extractor(extractor)
                .id(true)
                .nullable(false)
                .build();

            columns.put(columnName, mapping);
            return this;
        }

        /**
         * Adds a regular column with a value extractor.
         *
         * @param columnName the database column name
         * @param extractor function to extract the value from an entity
         */
        public <V> Builder<T> column(String columnName, Function<T, V> extractor) {
            return column(columnName, extractor, null, true);
        }

        /**
         * Adds a regular column with a value extractor and nullable flag.
         *
         * @param columnName the database column name
         * @param extractor function to extract the value from an entity
         * @param nullable whether the column allows null values
         */
        public <V> Builder<T> column(String columnName, Function<T, V> extractor, boolean nullable) {
            return column(columnName, extractor, null, nullable);
        }

        /**
         * Adds a regular column with a value extractor and explicit type.
         *
         * @param columnName the database column name
         * @param extractor function to extract the value from an entity
         * @param valueType the Java type of the value
         * @param nullable whether the column allows null values
         */
        @SuppressWarnings("unchecked")
        public <V> Builder<T> column(String columnName, Function<T, V> extractor,
                                     Class<V> valueType, boolean nullable) {
            Objects.requireNonNull(columnName, "columnName cannot be null");
            Objects.requireNonNull(extractor, "extractor cannot be null");

            if (columns.containsKey(columnName)) {
                throw MappingException.duplicateColumn(columnName);
            }

            Class<V> type = valueType != null ? valueType : (Class<V>) Object.class;
            ColumnMapping<T, V> mapping = ColumnMapping.<T, V>builder(columnName, type)
                .extractor(extractor)
                .id(false)
                .nullable(nullable)
                .build();

            columns.put(columnName, mapping);
            return this;
        }

        /**
         * Adds a pre-built column mapping.
         */
        public Builder<T> column(ColumnMapping<T, ?> columnMapping) {
            Objects.requireNonNull(columnMapping, "columnMapping cannot be null");

            if (columns.containsKey(columnMapping.getColumnName())) {
                throw MappingException.duplicateColumn(columnMapping.getColumnName());
            }

            columns.put(columnMapping.getColumnName(), columnMapping);
            return this;
        }

        /**
         * Builds the table mapping.
         *
         * @throws MappingException if no columns are defined
         */
        public TableMapping<T> build() {
            if (columns.isEmpty()) {
                throw new MappingException("At least one column must be defined for table '" + tableName + "'");
            }
            return new TableMapping<>(this);
        }
    }
}
