package com.bulkimport.spring;

import com.bulkimport.config.ConflictStrategy;
import com.bulkimport.config.NullHandling;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BulkImportProperties}.
 */
class BulkImportPropertiesTest {

    @Test
    void shouldHaveDefaultValues() {
        BulkImportProperties properties = new BulkImportProperties();

        assertThat(properties.getConflictStrategy()).isEqualTo(ConflictStrategy.FAIL);
        assertThat(properties.getNullHandling()).isEqualTo(NullHandling.EMPTY_STRING);
        assertThat(properties.getStagingTablePrefix()).isEqualTo("bulk_staging_");
        assertThat(properties.isAutoCleanupStaging()).isTrue();
        assertThat(properties.isEnabled()).isTrue();
        assertThat(properties.getConflictColumns()).isNull();
        assertThat(properties.getUpdateColumns()).isNull();
        assertThat(properties.getMatchColumns()).isNull();
        assertThat(properties.getSchemaName()).isNull();
    }

    @Test
    void shouldAllowSettingConflictStrategy() {
        BulkImportProperties properties = new BulkImportProperties();

        properties.setConflictStrategy(ConflictStrategy.UPDATE_ALL);

        assertThat(properties.getConflictStrategy()).isEqualTo(ConflictStrategy.UPDATE_ALL);
    }

    @Test
    void shouldAllowSettingNullHandling() {
        BulkImportProperties properties = new BulkImportProperties();

        properties.setNullHandling(NullHandling.LITERAL_NULL);

        assertThat(properties.getNullHandling()).isEqualTo(NullHandling.LITERAL_NULL);
    }

    @Test
    void shouldAllowSettingConflictColumns() {
        BulkImportProperties properties = new BulkImportProperties();

        properties.setConflictColumns(Arrays.asList("id", "email"));

        assertThat(properties.getConflictColumns()).containsExactly("id", "email");
    }

    @Test
    void shouldAllowSettingUpdateColumns() {
        BulkImportProperties properties = new BulkImportProperties();

        properties.setUpdateColumns(Arrays.asList("name", "updated_at"));

        assertThat(properties.getUpdateColumns()).containsExactly("name", "updated_at");
    }

    @Test
    void shouldAllowSettingMatchColumns() {
        BulkImportProperties properties = new BulkImportProperties();

        properties.setMatchColumns(Arrays.asList("external_id"));

        assertThat(properties.getMatchColumns()).containsExactly("external_id");
    }

    @Test
    void shouldAllowSettingStagingTablePrefix() {
        BulkImportProperties properties = new BulkImportProperties();

        properties.setStagingTablePrefix("my_staging_");

        assertThat(properties.getStagingTablePrefix()).isEqualTo("my_staging_");
    }

    @Test
    void shouldAllowSettingAutoCleanupStaging() {
        BulkImportProperties properties = new BulkImportProperties();

        properties.setAutoCleanupStaging(false);

        assertThat(properties.isAutoCleanupStaging()).isFalse();
    }

    @Test
    void shouldAllowSettingSchemaName() {
        BulkImportProperties properties = new BulkImportProperties();

        properties.setSchemaName("custom_schema");

        assertThat(properties.getSchemaName()).isEqualTo("custom_schema");
    }

    @Test
    void shouldAllowDisabling() {
        BulkImportProperties properties = new BulkImportProperties();

        properties.setEnabled(false);

        assertThat(properties.isEnabled()).isFalse();
    }
}
