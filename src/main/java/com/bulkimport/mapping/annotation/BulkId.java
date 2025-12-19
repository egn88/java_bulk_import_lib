package com.bulkimport.mapping.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field as part of the primary key for bulk import operations.
 * This is the custom annotation alternative to JPA's @Id.
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface BulkId {

    /**
     * The name of the database column.
     * If empty, the field name is used.
     */
    String name() default "";
}
