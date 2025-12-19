package com.bulkimport.converter.builtin;

import com.bulkimport.config.NullHandling;
import com.bulkimport.converter.TypeConverter;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Converter for Jackson JsonNode (PostgreSQL JSON/JSONB types).
 */
public class JsonConverter implements TypeConverter<JsonNode> {

    @Override
    public String toCsvValue(JsonNode value, NullHandling nullHandling) {
        if (value == null || value.isNull()) {
            return handleNull(nullHandling);
        }

        // Return raw JSON string - FastCSV handles CSV escaping
        return value.toString();
    }

    @Override
    public Class<JsonNode> supportedType() {
        return JsonNode.class;
    }
}
