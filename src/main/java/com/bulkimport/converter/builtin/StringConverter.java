package com.bulkimport.converter.builtin;

import com.bulkimport.config.NullHandling;
import com.bulkimport.converter.TypeConverter;

/**
 * Converter for String values.
 */
public class StringConverter implements TypeConverter<String> {

    @Override
    public String toCsvValue(String value, NullHandling nullHandling) {
        if (value == null) {
            return handleNull(nullHandling);
        }
        // Return raw value - FastCSV handles CSV escaping
        return value;
    }

    @Override
    public Class<String> supportedType() {
        return String.class;
    }
}
