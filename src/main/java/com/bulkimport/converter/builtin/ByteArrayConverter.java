package com.bulkimport.converter.builtin;

import com.bulkimport.config.NullHandling;
import com.bulkimport.converter.TypeConverter;

/**
 * Converter for byte arrays (PostgreSQL bytea type).
 * Uses PostgreSQL's hex format: \x followed by hexadecimal digits.
 */
public class ByteArrayConverter implements TypeConverter<byte[]> {

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    @Override
    public String toCsvValue(byte[] value, NullHandling nullHandling) {
        if (value == null) {
            return handleNull(nullHandling);
        }

        if (value.length == 0) {
            return "\\\\x";
        }

        // PostgreSQL hex format: \x followed by hex digits
        // In CSV, we need to escape the backslash
        StringBuilder sb = new StringBuilder(value.length * 2 + 3);
        sb.append("\\\\x");

        for (byte b : value) {
            sb.append(HEX_CHARS[(b >> 4) & 0x0F]);
            sb.append(HEX_CHARS[b & 0x0F]);
        }

        return sb.toString();
    }

    @Override
    public Class<byte[]> supportedType() {
        return byte[].class;
    }
}
