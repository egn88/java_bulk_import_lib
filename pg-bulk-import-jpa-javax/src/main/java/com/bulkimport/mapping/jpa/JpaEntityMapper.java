package com.bulkimport.mapping.jpa;

import com.bulkimport.exception.MappingException;
import com.bulkimport.mapping.ColumnMapping;
import com.bulkimport.mapping.EntityMapper;
import com.bulkimport.mapping.EntityMapperResolver;
import com.bulkimport.mapping.TableMapping;
import com.bulkimport.util.SqlIdentifier;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * Entity mapper that uses JPA annotations (@Entity, @Table, @Column, @Id).
 *
 * <p>This version uses {@code javax.persistence} (JPA 2.x) for legacy Java 8+ projects.</p>
 *
 * @param <T> the entity type
 */
public class JpaEntityMapper<T> implements EntityMapper<T> {

    private static final JpaEntityMapper<?> INSTANCE = new JpaEntityMapper<>();

    // Static initializer to auto-register with EntityMapperResolver
    static {
        EntityMapperResolver.getInstance().registerMapper(getInstance());
    }

    /**
     * Ensures this mapper is registered with the EntityMapperResolver.
     * Call this method from your application startup if static initialization
     * doesn't work in your environment.
     */
    public static void register() {
        // Static initializer already registered, this is just a trigger method
    }

    /**
     * Gets the singleton instance.
     */
    @SuppressWarnings("unchecked")
    public static <T> JpaEntityMapper<T> getInstance() {
        return (JpaEntityMapper<T>) INSTANCE;
    }

    @Override
    public TableMapping<T> map(Class<T> entityClass) {
        if (!supports(entityClass)) {
            throw MappingException.noMappingFound(entityClass);
        }

        String tableName = resolveTableName(entityClass);
        String schemaName = resolveSchemaName(entityClass);

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
        return entityClass.isAnnotationPresent(Entity.class) ||
               entityClass.isAnnotationPresent(Table.class);
    }

    private String resolveTableName(Class<?> entityClass) {
        Table tableAnnotation = entityClass.getAnnotation(Table.class);
        if (tableAnnotation != null && !tableAnnotation.name().isEmpty()) {
            return tableAnnotation.name();
        }

        // Fall back to simple class name in lowercase
        return SqlIdentifier.camelToSnake(entityClass.getSimpleName());
    }

    private String resolveSchemaName(Class<?> entityClass) {
        Table tableAnnotation = entityClass.getAnnotation(Table.class);
        if (tableAnnotation != null && !tableAnnotation.schema().isEmpty()) {
            return tableAnnotation.schema();
        }
        return null;
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
        // Skip static, transient, and @Transient fields
        int modifiers = field.getModifiers();
        if (Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers)) {
            return false;
        }

        if (field.isAnnotationPresent(Transient.class)) {
            return false;
        }

        // Check if explicitly non-insertable
        Column column = field.getAnnotation(Column.class);
        if (column != null && !column.insertable()) {
            return false;
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    private <V> ColumnMapping<T, V> createColumnMapping(Field field, Class<T> entityClass) {
        String columnName = resolveColumnName(field);
        boolean isId = field.isAnnotationPresent(Id.class);
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
        Column column = field.getAnnotation(Column.class);
        if (column != null && !column.name().isEmpty()) {
            return column.name();
        }
        return SqlIdentifier.camelToSnake(field.getName());
    }

    private boolean resolveNullable(Field field, boolean isId) {
        if (isId) {
            return false;
        }

        Column column = field.getAnnotation(Column.class);
        if (column != null) {
            return column.nullable();
        }

        // Primitives are not nullable
        return !field.getType().isPrimitive();
    }
}
