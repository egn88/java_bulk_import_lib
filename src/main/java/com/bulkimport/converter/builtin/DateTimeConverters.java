package com.bulkimport.converter.builtin;

import com.bulkimport.config.NullHandling;
import com.bulkimport.converter.TypeConverter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Converters for date and time types.
 */
public final class DateTimeConverters {

    private DateTimeConverters() {
    }

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_TIME;
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    private static final DateTimeFormatter OFFSET_DATETIME_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public static class LocalDateConverter implements TypeConverter<LocalDate> {
        @Override
        public String toCsvValue(LocalDate value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.format(DATE_FORMATTER);
        }

        @Override
        public Class<LocalDate> supportedType() {
            return LocalDate.class;
        }
    }

    public static class LocalDateTimeConverter implements TypeConverter<LocalDateTime> {
        @Override
        public String toCsvValue(LocalDateTime value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.format(DATETIME_FORMATTER);
        }

        @Override
        public Class<LocalDateTime> supportedType() {
            return LocalDateTime.class;
        }
    }

    public static class LocalTimeConverter implements TypeConverter<LocalTime> {
        @Override
        public String toCsvValue(LocalTime value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.format(TIME_FORMATTER);
        }

        @Override
        public Class<LocalTime> supportedType() {
            return LocalTime.class;
        }
    }

    public static class InstantConverter implements TypeConverter<Instant> {
        @Override
        public String toCsvValue(Instant value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            // Convert to UTC timestamp with timezone
            return value.atOffset(ZoneOffset.UTC).format(OFFSET_DATETIME_FORMATTER);
        }

        @Override
        public Class<Instant> supportedType() {
            return Instant.class;
        }
    }

    public static class ZonedDateTimeConverter implements TypeConverter<ZonedDateTime> {
        @Override
        public String toCsvValue(ZonedDateTime value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.format(OFFSET_DATETIME_FORMATTER);
        }

        @Override
        public Class<ZonedDateTime> supportedType() {
            return ZonedDateTime.class;
        }
    }

    public static class OffsetDateTimeConverter implements TypeConverter<OffsetDateTime> {
        @Override
        public String toCsvValue(OffsetDateTime value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.format(OFFSET_DATETIME_FORMATTER);
        }

        @Override
        public Class<OffsetDateTime> supportedType() {
            return OffsetDateTime.class;
        }
    }

    public static class SqlDateConverter implements TypeConverter<java.sql.Date> {
        @Override
        public String toCsvValue(java.sql.Date value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.toLocalDate().format(DATE_FORMATTER);
        }

        @Override
        public Class<java.sql.Date> supportedType() {
            return java.sql.Date.class;
        }
    }

    public static class SqlTimestampConverter implements TypeConverter<java.sql.Timestamp> {
        @Override
        public String toCsvValue(java.sql.Timestamp value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.toLocalDateTime().format(DATETIME_FORMATTER);
        }

        @Override
        public Class<java.sql.Timestamp> supportedType() {
            return java.sql.Timestamp.class;
        }
    }

    public static class UtilDateConverter implements TypeConverter<java.util.Date> {
        @Override
        public String toCsvValue(java.util.Date value, NullHandling nullHandling) {
            if (value == null) {
                return handleNull(nullHandling);
            }
            return value.toInstant().atOffset(ZoneOffset.UTC).format(OFFSET_DATETIME_FORMATTER);
        }

        @Override
        public Class<java.util.Date> supportedType() {
            return java.util.Date.class;
        }
    }
}
