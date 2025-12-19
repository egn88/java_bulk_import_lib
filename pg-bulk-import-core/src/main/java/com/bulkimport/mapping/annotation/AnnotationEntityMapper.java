package com.bulkimport.mapping.annotation;

import com.bulkimport.exception.MappingException;
import com.bulkimport.mapping.ColumnMapping;
import com.bulkimport.mapping.EntityMapper;
import com.bulkimport.mapping.TableMapping;
import com.bulkimport.util.SqlIdentifier;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * Entity mapper that uses custom bulk import annotations (@BulkTable, @BulkColumn, @BulkId).
 */
public class AnnotationEntityMapper<T> implements EntityMapper<T> {

    private static final AnnotationEntityMapper<?> INSTANCE = new AnnotationEntityMapper<>();

    /**
     * Gets the singleton instance.
     */
    @SuppressWarnings("unchecked")
    public static <T> AnnotationEntityMapper<T> getInstance() {
        return (AnnotationEntityMapper<T>) INSTANCE;
    }

    @Override
    public TableMapping<T> map(Class<T> entityClass) {
        if (!supports(entityClass)) {
            throw MappingException.noMappingFound(entityClass);
        }

        BulkTable tableAnnotation = entityClass.getAnnotation(BulkTable.class);
        String tableName = tableAnnotation.name();
        String schemaName = tableAnnotation.schema();

        if (tableName.isEmpty()) {
            throw MappingException.missingTableName(entityClass);
        }

        TableMapping.Builder<T> builder = TableMapping.<T>builder(tableName)
            .entityClass(entityClass);

        if (schemaName != null && !schemaName.isEmpty()) {
            builder.schema(schemaName);
        }

        List<Field> mappableFields = getMappableFields(entityClass);
        if (mappableFields.isEmpty()) {
            throw MappingException.noColumnsFound(entityClass);
        }

        for (Field field : mappableFields) {
            ColumnMapping<T, ?> columnMapping = createColumnMapping(field, entityClass);
            builder.column(columnMapping);
        }

        return builder.build();
    }

    @Override
    public boolean supports(Class<?> entityClass) {
        return entityClass.isAnnotationPresent(BulkTable.class);
    }

    private List<Field> getMappableFields(Class<?> entityClass) {
        List<Field> fields = new ArrayList<>();
        Class<?> current = entityClass;

        while (current != null && current != Object.class) {
            for (Field field : current.getDeclaredFields()) {
                if (isMappable(field)) {
                    fields.add(field);
                }
            }
            current = current.getSuperclass();
        }

        return fields;
    }

    private boolean isMappable(Field field) {
        // Skip static and transient fields
        int modifiers = field.getModifiers();
        if (Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers)) {
            return false;
        }

        // Skip fields marked with @BulkTransient
        if (field.isAnnotationPresent(BulkTransient.class)) {
            return false;
        }

        // Check if explicitly non-insertable
        BulkColumn column = field.getAnnotation(BulkColumn.class);
        if (column != null && !column.insertable()) {
            return false;
        }

        // Include fields with @BulkId, @BulkColumn, or all fields by default
        return true;
    }

    @SuppressWarnings("unchecked")
    private <V> ColumnMapping<T, V> createColumnMapping(Field field, Class<T> entityClass) {
        String columnName = resolveColumnName(field);
        boolean isId = field.isAnnotationPresent(BulkId.class);
        boolean nullable = resolveNullable(field, isId);
        Class<V> fieldType = (Class<V>) field.getType();

        field.setAccessible(true);

        return ColumnMapping.<T, V>builder(columnName, fieldType)
            .extractor(entity -> {
                try {
                    return (V) field.get(entity);
                } catch (IllegalAccessException e) {
                    throw MappingException.fieldAccessError(field.getName(), entityClass, e);
                }
            })
            .id(isId)
            .nullable(nullable)
            .fieldName(field.getName())
            .build();
    }

    private String resolveColumnName(Field field) {
        // Check @BulkId first
        BulkId idAnnotation = field.getAnnotation(BulkId.class);
        if (idAnnotation != null && !idAnnotation.name().isEmpty()) {
            return idAnnotation.name();
        }

        // Check @BulkColumn
        BulkColumn columnAnnotation = field.getAnnotation(BulkColumn.class);
        if (columnAnnotation != null && !columnAnnotation.name().isEmpty()) {
            return columnAnnotation.name();
        }

        // Default to field name converted to snake_case
        return SqlIdentifier.camelToSnake(field.getName());
    }

    private boolean resolveNullable(Field field, boolean isId) {
        if (isId) {
            return false;
        }

        BulkColumn column = field.getAnnotation(BulkColumn.class);
        if (column != null) {
            return column.nullable();
        }

        // Primitives are not nullable
        return !field.getType().isPrimitive();
    }
}
