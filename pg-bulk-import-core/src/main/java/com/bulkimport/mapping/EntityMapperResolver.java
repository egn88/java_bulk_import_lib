package com.bulkimport.mapping;

import com.bulkimport.exception.MappingException;
import com.bulkimport.mapping.annotation.AnnotationEntityMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves the appropriate entity mapper for a given class.
 * Maintains a cache of resolved mappings for performance.
 *
 * <p>By default, only the {@link AnnotationEntityMapper} is registered.
 * JPA support can be added by including the pg-bulk-import-jpa-javax or
 * pg-bulk-import-jpa-jakarta module, which will register the JPA mapper.</p>
 */
public class EntityMapperResolver {

    private static final EntityMapperResolver INSTANCE = new EntityMapperResolver();

    private final List<EntityMapper<?>> mappers;
    private final Map<Class<?>, TableMapping<?>> mappingCache;

    private EntityMapperResolver() {
        this.mappers = new ArrayList<>();
        this.mappingCache = new ConcurrentHashMap<>();

        // Register only the annotation mapper in core
        // JPA modules will register their mappers via registerMapper()
        registerMapper(AnnotationEntityMapper.getInstance());
    }

    /**
     * Gets the singleton instance.
     */
    public static EntityMapperResolver getInstance() {
        return INSTANCE;
    }

    /**
     * Registers a custom entity mapper.
     * Custom mappers are checked after built-in mappers.
     */
    public void registerMapper(EntityMapper<?> mapper) {
        mappers.add(mapper);
    }

    /**
     * Resolves the table mapping for an entity class.
     *
     * @param entityClass the entity class
     * @return the table mapping
     * @throws MappingException if no suitable mapper is found
     */
    @SuppressWarnings("unchecked")
    public <T> TableMapping<T> resolve(Class<T> entityClass) {
        // Check cache first
        TableMapping<?> cached = mappingCache.get(entityClass);
        if (cached != null) {
            return (TableMapping<T>) cached;
        }

        // Find a suitable mapper
        for (EntityMapper<?> mapper : mappers) {
            if (mapper.supports(entityClass)) {
                EntityMapper<T> typedMapper = (EntityMapper<T>) mapper;
                TableMapping<T> mapping = typedMapper.map(entityClass);
                mappingCache.put(entityClass, mapping);
                return mapping;
            }
        }

        throw MappingException.noMappingFound(entityClass);
    }

    /**
     * Clears the mapping cache.
     */
    public void clearCache() {
        mappingCache.clear();
    }

    /**
     * Removes a specific class from the cache.
     */
    public void evict(Class<?> entityClass) {
        mappingCache.remove(entityClass);
    }

    /**
     * Checks if any registered mapper supports the given class.
     */
    public boolean supports(Class<?> entityClass) {
        return mappers.stream().anyMatch(m -> m.supports(entityClass));
    }
}
