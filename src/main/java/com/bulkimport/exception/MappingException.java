package com.bulkimport.exception;

/**
 * Exception thrown when entity mapping fails or is invalid.
 */
public class MappingException extends BulkImportException {

    public MappingException(String message) {
        super(message);
    }

    public MappingException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates an exception when no mapping is found for a class.
     */
    public static MappingException noMappingFound(Class<?> entityClass) {
        return new MappingException(
            String.format(
                "No mapping found for class '%s'. " +
                "Ensure the class has @Table/@Entity (JPA), @BulkTable (custom), " +
                "or provide a TableMapping explicitly.",
                entityClass.getName()
            )
        );
    }

    /**
     * Creates an exception when the table name is missing.
     */
    public static MappingException missingTableName(Class<?> entityClass) {
        return new MappingException(
            String.format(
                "Table name is not specified for class '%s'. " +
                "Use @Table(name=\"...\") or @BulkTable(name=\"...\") to specify the table name.",
                entityClass.getName()
            )
        );
    }

    /**
     * Creates an exception when no ID column is found.
     */
    public static MappingException missingIdColumn(Class<?> entityClass) {
        return new MappingException(
            String.format(
                "No ID column found for class '%s'. " +
                "At least one field must be annotated with @Id or @BulkId for update/upsert operations.",
                entityClass.getName()
            )
        );
    }

    /**
     * Creates an exception when no columns are found for mapping.
     */
    public static MappingException noColumnsFound(Class<?> entityClass) {
        return new MappingException(
            String.format(
                "No columns found for class '%s'. " +
                "Ensure fields are accessible and properly annotated.",
                entityClass.getName()
            )
        );
    }

    /**
     * Creates an exception for invalid field access.
     */
    public static MappingException fieldAccessError(String fieldName, Class<?> entityClass, Throwable cause) {
        return new MappingException(
            String.format(
                "Failed to access field '%s' on class '%s'",
                fieldName,
                entityClass.getName()
            ),
            cause
        );
    }

    /**
     * Creates an exception for duplicate column mapping.
     */
    public static MappingException duplicateColumn(String columnName) {
        return new MappingException(
            String.format("Duplicate column mapping for column '%s'", columnName)
        );
    }
}
