package com.bulkimport;

import com.bulkimport.util.SqlIdentifier;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlIdentifierTest {

    @Nested
    class Quote {

        @Test
        void shouldQuoteSimpleIdentifier() {
            assertThat(SqlIdentifier.quote("users")).isEqualTo("\"users\"");
        }

        @Test
        void shouldQuoteIdentifierWithUnderscore() {
            assertThat(SqlIdentifier.quote("user_accounts")).isEqualTo("\"user_accounts\"");
        }

        @Test
        void shouldQuoteIdentifierWithNumbers() {
            assertThat(SqlIdentifier.quote("table123")).isEqualTo("\"table123\"");
        }

        @Test
        void shouldQuoteIdentifierStartingWithUnderscore() {
            assertThat(SqlIdentifier.quote("_private")).isEqualTo("\"_private\"");
        }

        @ParameterizedTest
        @NullAndEmptySource
        void shouldRejectNullOrEmptyIdentifier(String identifier) {
            assertThatThrownBy(() -> SqlIdentifier.quote(identifier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be null or empty");
        }

        @ParameterizedTest
        @ValueSource(strings = {"123table", "1_column", "9test"})
        void shouldRejectIdentifierStartingWithNumber(String identifier) {
            assertThatThrownBy(() -> SqlIdentifier.quote(identifier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid SQL identifier");
        }

        @ParameterizedTest
        @ValueSource(strings = {"user-name", "table.name", "col@mn", "user name", "table;drop"})
        void shouldRejectIdentifierWithSpecialCharacters(String identifier) {
            assertThatThrownBy(() -> SqlIdentifier.quote(identifier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid SQL identifier");
        }

        @Test
        void shouldRejectIdentifierExceedingMaxLength() {
            String longIdentifier = "a".repeat(64);
            assertThatThrownBy(() -> SqlIdentifier.quote(longIdentifier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum length");
        }

        @Test
        void shouldAcceptIdentifierAtMaxLength() {
            String maxLengthIdentifier = "a".repeat(63);
            assertThat(SqlIdentifier.quote(maxLengthIdentifier))
                .isEqualTo("\"" + maxLengthIdentifier + "\"");
        }
    }

    @Nested
    class QuoteQualified {

        @Test
        void shouldQuoteSchemaAndTable() {
            assertThat(SqlIdentifier.quoteQualified("public", "users"))
                .isEqualTo("\"public\".\"users\"");
        }

        @Test
        void shouldQuoteTableOnlyWhenSchemaIsNull() {
            assertThat(SqlIdentifier.quoteQualified(null, "users"))
                .isEqualTo("\"users\"");
        }

        @Test
        void shouldQuoteTableOnlyWhenSchemaIsEmpty() {
            assertThat(SqlIdentifier.quoteQualified("", "users"))
                .isEqualTo("\"users\"");
        }

        @Test
        void shouldValidateBothSchemaAndTable() {
            assertThatThrownBy(() -> SqlIdentifier.quoteQualified("invalid-schema", "users"))
                .isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(() -> SqlIdentifier.quoteQualified("public", "invalid-table"))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class QuoteAndJoin {

        @Test
        void shouldQuoteAndJoinMultipleColumns() {
            List<String> columns = List.of("id", "name", "email");
            assertThat(SqlIdentifier.quoteAndJoin(columns))
                .isEqualTo("\"id\", \"name\", \"email\"");
        }

        @Test
        void shouldHandleSingleColumn() {
            List<String> columns = List.of("id");
            assertThat(SqlIdentifier.quoteAndJoin(columns))
                .isEqualTo("\"id\"");
        }

        @Test
        void shouldHandleEmptyList() {
            List<String> columns = List.of();
            assertThat(SqlIdentifier.quoteAndJoin(columns))
                .isEqualTo("");
        }

        @Test
        void shouldValidateEachColumn() {
            List<String> columns = List.of("valid_column", "invalid-column");
            assertThatThrownBy(() -> SqlIdentifier.quoteAndJoin(columns))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid SQL identifier");
        }
    }

    @Nested
    class Validate {

        @Test
        void shouldAcceptValidIdentifiers() {
            // Should not throw
            SqlIdentifier.validate("users");
            SqlIdentifier.validate("user_accounts");
            SqlIdentifier.validate("_private");
            SqlIdentifier.validate("Table123");
            SqlIdentifier.validate("UPPERCASE");
            SqlIdentifier.validate("mixedCase");
        }

        @ParameterizedTest
        @NullAndEmptySource
        void shouldRejectNullOrEmpty(String identifier) {
            assertThatThrownBy(() -> SqlIdentifier.validate(identifier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be null or empty");
        }
    }

    @Nested
    class IsValid {

        @ParameterizedTest
        @ValueSource(strings = {"users", "user_accounts", "_private", "Table123", "a"})
        void shouldReturnTrueForValidIdentifiers(String identifier) {
            assertThat(SqlIdentifier.isValid(identifier)).isTrue();
        }

        @ParameterizedTest
        @ValueSource(strings = {"123table", "user-name", "table.name", "col@mn"})
        void shouldReturnFalseForInvalidIdentifiers(String identifier) {
            assertThat(SqlIdentifier.isValid(identifier)).isFalse();
        }

        @Test
        void shouldReturnFalseForNull() {
            assertThat(SqlIdentifier.isValid(null)).isFalse();
        }

        @Test
        void shouldReturnFalseForEmpty() {
            assertThat(SqlIdentifier.isValid("")).isFalse();
        }

        @Test
        void shouldReturnFalseForTooLongIdentifier() {
            String longIdentifier = "a".repeat(64);
            assertThat(SqlIdentifier.isValid(longIdentifier)).isFalse();
        }
    }

    @Nested
    class CamelToSnake {

        @ParameterizedTest
        @CsvSource({
            "userName, user_name",
            "firstName, first_name",
            "lastName, last_name",
            "createdAt, created_at",
            "updatedAt, updated_at",
            "isActive, is_active",
            "hasPermission, has_permission"
        })
        void shouldConvertCamelCaseToSnakeCase(String input, String expected) {
            assertThat(SqlIdentifier.camelToSnake(input)).isEqualTo(expected);
        }

        @Test
        void shouldHandleSingleWord() {
            assertThat(SqlIdentifier.camelToSnake("name")).isEqualTo("name");
        }

        @Test
        void shouldHandleAlreadyLowercase() {
            assertThat(SqlIdentifier.camelToSnake("alreadylowercase")).isEqualTo("alreadylowercase");
        }

        @Test
        void shouldHandleLeadingUppercase() {
            assertThat(SqlIdentifier.camelToSnake("UserName")).isEqualTo("user_name");
        }

        @Test
        void shouldHandleConsecutiveUppercase() {
            assertThat(SqlIdentifier.camelToSnake("parseHTML")).isEqualTo("parse_h_t_m_l");
            assertThat(SqlIdentifier.camelToSnake("XMLParser")).isEqualTo("x_m_l_parser");
        }

        @Test
        void shouldHandleAllUppercase() {
            assertThat(SqlIdentifier.camelToSnake("ID")).isEqualTo("i_d");
            assertThat(SqlIdentifier.camelToSnake("URL")).isEqualTo("u_r_l");
        }

        @Test
        void shouldHandleSingleCharacter() {
            assertThat(SqlIdentifier.camelToSnake("a")).isEqualTo("a");
            assertThat(SqlIdentifier.camelToSnake("A")).isEqualTo("a");
        }

        @Test
        void shouldHandleNull() {
            assertThat(SqlIdentifier.camelToSnake(null)).isNull();
        }

        @Test
        void shouldHandleEmptyString() {
            assertThat(SqlIdentifier.camelToSnake("")).isEqualTo("");
        }

        @Test
        void shouldPreserveUnderscores() {
            assertThat(SqlIdentifier.camelToSnake("user_name")).isEqualTo("user_name");
            assertThat(SqlIdentifier.camelToSnake("_private")).isEqualTo("_private");
        }

        @Test
        void shouldPreserveNumbers() {
            assertThat(SqlIdentifier.camelToSnake("user123")).isEqualTo("user123");
            assertThat(SqlIdentifier.camelToSnake("user123Name")).isEqualTo("user123_name");
        }
    }

    @Nested
    class SqlInjectionPrevention {

        @ParameterizedTest
        @ValueSource(strings = {
            "users; DROP TABLE users;--",
            "users' OR '1'='1",
            "users\" OR \"1\"=\"1",
            "users\"; DROP TABLE users;--",
            "users\\",
            "users\nDROP TABLE users",
            "users\tDROP TABLE users"
        })
        void shouldRejectPotentialSqlInjectionAttempts(String maliciousInput) {
            assertThat(SqlIdentifier.isValid(maliciousInput)).isFalse();
            assertThatThrownBy(() -> SqlIdentifier.quote(maliciousInput))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
