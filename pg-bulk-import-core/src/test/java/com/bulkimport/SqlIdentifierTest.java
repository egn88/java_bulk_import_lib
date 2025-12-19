package com.bulkimport;

import com.bulkimport.util.SqlIdentifier;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for SqlIdentifier utility class.
 * Java 8 compatible.
 */
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
        void shouldRejectNullIdentifier() {
            assertThatThrownBy(() -> SqlIdentifier.quote(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be null or empty");
        }

        @Test
        void shouldRejectEmptyIdentifier() {
            assertThatThrownBy(() -> SqlIdentifier.quote(""))
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
            String longIdentifier = String.join("", Collections.nCopies(64, "a"));
            assertThatThrownBy(() -> SqlIdentifier.quote(longIdentifier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum length");
        }

        @Test
        void shouldAcceptIdentifierAtMaxLength() {
            String maxLengthIdentifier = String.join("", Collections.nCopies(63, "a"));
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
            assertThat(SqlIdentifier.quoteAndJoin(Arrays.asList("id", "name", "email")))
                .isEqualTo("\"id\", \"name\", \"email\"");
        }

        @Test
        void shouldHandleSingleColumn() {
            assertThat(SqlIdentifier.quoteAndJoin(Collections.singletonList("id")))
                .isEqualTo("\"id\"");
        }

        @Test
        void shouldHandleEmptyList() {
            assertThat(SqlIdentifier.quoteAndJoin(Collections.emptyList()))
                .isEqualTo("");
        }

        @Test
        void shouldValidateEachColumn() {
            assertThatThrownBy(() -> SqlIdentifier.quoteAndJoin(Arrays.asList("valid_column", "invalid-column")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid SQL identifier");
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
    }

    @Nested
    class CamelToSnake {

        @Test
        void shouldConvertCamelCaseToSnakeCase() {
            assertThat(SqlIdentifier.camelToSnake("userName")).isEqualTo("user_name");
            assertThat(SqlIdentifier.camelToSnake("firstName")).isEqualTo("first_name");
            assertThat(SqlIdentifier.camelToSnake("createdAt")).isEqualTo("created_at");
            assertThat(SqlIdentifier.camelToSnake("isActive")).isEqualTo("is_active");
        }

        @Test
        void shouldHandleSingleWord() {
            assertThat(SqlIdentifier.camelToSnake("name")).isEqualTo("name");
        }

        @Test
        void shouldHandleLeadingUppercase() {
            assertThat(SqlIdentifier.camelToSnake("UserName")).isEqualTo("user_name");
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
            "users\" OR \"1\"=\"1"
        })
        void shouldRejectPotentialSqlInjectionAttempts(String maliciousInput) {
            assertThat(SqlIdentifier.isValid(maliciousInput)).isFalse();
            assertThatThrownBy(() -> SqlIdentifier.quote(maliciousInput))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
