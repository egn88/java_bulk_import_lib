package com.bulkimport.mapping.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a bulk import entity and specifies the target table.
 * This is the custom annotation alternative to JPA's @Table.
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface BulkTable {

    /**
     * The name of the database table.
     */
    String name();

    /**
     * The schema name. If empty, the default schema is used.
     */
    String schema() default "";
}
