package com.bulkimport.converter.builtin;

import com.bulkimport.config.NullHandling;
import com.bulkimport.converter.TypeConverter;

/**
 * Converter for Boolean values.
 * Uses PostgreSQL's boolean literal format (true/false).
 */
public class BooleanConverter implements TypeConverter<Boolean> {

    @Override
    public String toCsvValue(Boolean value, NullHandling nullHandling) {
        if (value == null) {
            return handleNull(nullHandling);
        }
        // PostgreSQL accepts 't', 'true', 'y', 'yes', '1' for true
        // and 'f', 'false', 'n', 'no', '0' for false
        return value ? "true" : "false";
    }

    @Override
    public Class<Boolean> supportedType() {
        return Boolean.class;
    }
}
