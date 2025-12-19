package com.bulkimport.converter.builtin;

import com.bulkimport.config.NullHandling;
import com.bulkimport.converter.TypeConverter;

import java.util.UUID;

/**
 * Converter for UUID values.
 */
public class UUIDConverter implements TypeConverter<UUID> {

    @Override
    public String toCsvValue(UUID value, NullHandling nullHandling) {
        if (value == null) {
            return handleNull(nullHandling);
        }
        return value.toString();
    }

    @Override
    public Class<UUID> supportedType() {
        return UUID.class;
    }
}
