package com.bulkimport.converter.builtin;

import com.bulkimport.config.NullHandling;
import com.bulkimport.converter.TypeConverter;

/**
 * Converter for enum values.
 * Converts enums to their name() representation.
 */
public class EnumConverter implements TypeConverter<Enum<?>> {

    @Override
    public String toCsvValue(Enum<?> value, NullHandling nullHandling) {
        if (value == null) {
            return handleNull(nullHandling);
        }
        return value.name();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<Enum<?>> supportedType() {
        return (Class<Enum<?>>) (Class<?>) Enum.class;
    }
}
