package com.bulkimport.mapping;

import java.util.Objects;
import java.util.function.Function;

/**
 * Represents a mapping from an entity field to a database column.
 *
 * @param <T> the entity type
 * @param <V> the value type
 */
public class ColumnMapping<T, V> {

    private final String columnName;
    private final Class<V> valueType;
    private final Function<T, V> valueExtractor;
    private final boolean isId;
    private final boolean nullable;
    private final String fieldName;

    private ColumnMapping(Builder<T, V> builder) {
        this.columnName = builder.columnName;
        this.valueType = builder.valueType;
        this.valueExtractor = builder.valueExtractor;
        this.isId = builder.isId;
        this.nullable = builder.nullable;
        this.fieldName = builder.fieldName;
    }

    /**
     * Creates a new builder for a column mapping.
     *
     * @param columnName the database column name
     * @param valueType the Java type of the value
     */
    public static <T, V> Builder<T, V> builder(String columnName, Class<V> valueType) {
        return new Builder<>(columnName, valueType);
    }

    /**
     * Gets the database column name.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Gets the Java type of the column value.
     */
    public Class<V> getValueType() {
        return valueType;
    }

    /**
     * Extracts the value from an entity instance.
     *
     * @param entity the entity to extract the value from
     * @return the extracted value, may be null
     */
    public V extractValue(T entity) {
        return valueExtractor.apply(entity);
    }

    /**
     * Returns true if this column is part of the primary key.
     */
    public boolean isId() {
        return isId;
    }

    /**
     * Returns true if this column allows null values.
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * Gets the original field name in the entity class.
     */
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnMapping<?, ?> that = (ColumnMapping<?, ?>) o;
        return Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName);
    }

    @Override
    public String toString() {
        return "ColumnMapping{" +
               "columnName='" + columnName + '\'' +
               ", valueType=" + valueType.getSimpleName() +
               ", isId=" + isId +
               ", nullable=" + nullable +
               '}';
    }

    /**
     * Builder for ColumnMapping.
     */
    public static class Builder<T, V> {
        private final String columnName;
        private final Class<V> valueType;
        private Function<T, V> valueExtractor;
        private boolean isId = false;
        private boolean nullable = true;
        private String fieldName;

        private Builder(String columnName, Class<V> valueType) {
            this.columnName = Objects.requireNonNull(columnName, "columnName cannot be null");
            this.valueType = Objects.requireNonNull(valueType, "valueType cannot be null");
            this.fieldName = columnName;
        }

        /**
         * Sets the function to extract the value from an entity.
         */
        public Builder<T, V> extractor(Function<T, V> extractor) {
            this.valueExtractor = Objects.requireNonNull(extractor, "extractor cannot be null");
            return this;
        }

        /**
         * Marks this column as part of the primary key.
         */
        public Builder<T, V> id() {
            this.isId = true;
            return this;
        }

        /**
         * Marks this column as part of the primary key.
         */
        public Builder<T, V> id(boolean isId) {
            this.isId = isId;
            return this;
        }

        /**
         * Sets whether this column allows null values.
         */
        public Builder<T, V> nullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        /**
         * Sets the original field name.
         */
        public Builder<T, V> fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Builds the column mapping.
         */
        public ColumnMapping<T, V> build() {
            Objects.requireNonNull(valueExtractor, "valueExtractor must be set");
            return new ColumnMapping<>(this);
        }
    }
}
