package com.bulkimport.mapping.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field as a bulk import column and specifies column properties.
 * This is the custom annotation alternative to JPA's @Column.
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface BulkColumn {

    /**
     * The name of the database column.
     * If empty, the field name is used.
     */
    String name() default "";

    /**
     * Whether the column allows null values.
     */
    boolean nullable() default true;

    /**
     * Whether this field should be included in bulk operations.
     * Set to false to exclude a field.
     */
    boolean insertable() default true;

    /**
     * Whether this field should be included in update operations.
     */
    boolean updatable() default true;
}
