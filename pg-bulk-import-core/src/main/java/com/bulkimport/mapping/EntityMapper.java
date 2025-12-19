package com.bulkimport.mapping;

/**
 * Interface for mapping entity classes to table mappings.
 * Implementations can extract mapping information from different sources
 * (JPA annotations, custom annotations, etc.).
 *
 * @param <T> the entity type
 */
public interface EntityMapper<T> {

    /**
     * Creates a table mapping for the given entity class.
     *
     * @param entityClass the entity class to map
     * @return the table mapping
     * @throws com.bulkimport.exception.MappingException if mapping fails
     */
    TableMapping<T> map(Class<T> entityClass);

    /**
     * Checks if this mapper supports the given entity class.
     *
     * @param entityClass the entity class to check
     * @return true if this mapper can handle the class
     */
    boolean supports(Class<?> entityClass);
}
