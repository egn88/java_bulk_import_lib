package com.bulkimport.spring;

import com.bulkimport.BulkImporter;
import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.config.ConflictStrategy;
import com.bulkimport.config.NullHandling;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link BulkImportAutoConfiguration}.
 */
class BulkImportAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(BulkImportAutoConfiguration.class));

    @Test
    void shouldCreateBulkImportConfigBeanWithDefaults() {
        this.contextRunner
            .withUserConfiguration(DataSourceConfiguration.class)
            .run(context -> {
                assertThat(context).hasSingleBean(BulkImportConfig.class);

                BulkImportConfig config = context.getBean(BulkImportConfig.class);
                assertThat(config.getConflictStrategy()).isEqualTo(ConflictStrategy.FAIL);
                assertThat(config.getNullHandling()).isEqualTo(NullHandling.EMPTY_STRING);
                assertThat(config.getStagingTablePrefix()).isEqualTo("bulk_staging_");
                assertThat(config.isAutoCleanupStaging()).isTrue();
            });
    }

    @Test
    void shouldCreateBulkImporterBean() {
        this.contextRunner
            .withUserConfiguration(DataSourceConfiguration.class)
            .run(context -> {
                assertThat(context).hasSingleBean(BulkImporter.class);
            });
    }

    @Test
    void shouldNotCreateBeansWhenDisabled() {
        this.contextRunner
            .withUserConfiguration(DataSourceConfiguration.class)
            .withPropertyValues("bulkimport.enabled=false")
            .run(context -> {
                assertThat(context).doesNotHaveBean(BulkImportConfig.class);
                assertThat(context).doesNotHaveBean(BulkImporter.class);
            });
    }

    @Test
    void shouldApplyCustomProperties() {
        this.contextRunner
            .withUserConfiguration(DataSourceConfiguration.class)
            .withPropertyValues(
                "bulkimport.conflict-strategy=UPDATE_ALL",
                "bulkimport.conflict-columns=id",
                "bulkimport.null-handling=LITERAL_NULL",
                "bulkimport.staging-table-prefix=custom_staging_",
                "bulkimport.auto-cleanup-staging=false"
            )
            .run(context -> {
                assertThat(context).hasSingleBean(BulkImportConfig.class);

                BulkImportConfig config = context.getBean(BulkImportConfig.class);
                assertThat(config.getConflictStrategy()).isEqualTo(ConflictStrategy.UPDATE_ALL);
                assertThat(config.getConflictColumns()).containsExactly("id");
                assertThat(config.getNullHandling()).isEqualTo(NullHandling.LITERAL_NULL);
                assertThat(config.getStagingTablePrefix()).isEqualTo("custom_staging_");
                assertThat(config.isAutoCleanupStaging()).isFalse();
            });
    }

    @Test
    void shouldApplyUpdateColumnsProperty() {
        this.contextRunner
            .withUserConfiguration(DataSourceConfiguration.class)
            .withPropertyValues(
                "bulkimport.conflict-strategy=UPDATE_SPECIFIED",
                "bulkimport.conflict-columns=id",
                "bulkimport.update-columns=name,email"
            )
            .run(context -> {
                BulkImportConfig config = context.getBean(BulkImportConfig.class);
                assertThat(config.getUpdateColumns()).containsExactly("name", "email");
            });
    }

    @Test
    void shouldApplySchemaNameProperty() {
        this.contextRunner
            .withUserConfiguration(DataSourceConfiguration.class)
            .withPropertyValues("bulkimport.schema-name=custom_schema")
            .run(context -> {
                BulkImportConfig config = context.getBean(BulkImportConfig.class);
                assertThat(config.getSchemaName()).isEqualTo("custom_schema");
            });
    }

    @Test
    void shouldNotOverrideExistingBulkImportConfigBean() {
        this.contextRunner
            .withUserConfiguration(DataSourceConfiguration.class, CustomConfigConfiguration.class)
            .run(context -> {
                assertThat(context).hasSingleBean(BulkImportConfig.class);

                BulkImportConfig config = context.getBean(BulkImportConfig.class);
                // Custom config should be used
                assertThat(config.getStagingTablePrefix()).isEqualTo("my_custom_prefix_");
            });
    }

    @Test
    void shouldNotOverrideExistingBulkImporterBean() {
        this.contextRunner
            .withUserConfiguration(DataSourceConfiguration.class, CustomImporterConfiguration.class)
            .run(context -> {
                assertThat(context).hasSingleBean(BulkImporter.class);
                // Should be the custom instance, not auto-configured
            });
    }

    @Test
    void shouldFailToStartWithoutDataSource() {
        this.contextRunner
            .run(context -> {
                // Without DataSource, context should fail to start
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure())
                    .hasRootCauseInstanceOf(org.springframework.beans.factory.NoSuchBeanDefinitionException.class);
            });
    }

    @Configuration
    static class DataSourceConfiguration {
        @Bean
        DataSource dataSource() {
            return mock(DataSource.class);
        }
    }

    @Configuration
    static class CustomConfigConfiguration {
        @Bean
        BulkImportConfig bulkImportConfig() {
            return BulkImportConfig.builder()
                .stagingTablePrefix("my_custom_prefix_")
                .build();
        }
    }

    @Configuration
    static class CustomImporterConfiguration {
        @Bean
        BulkImporter bulkImporter(DataSource dataSource, BulkImportConfig config) {
            return BulkImporter.create(dataSource).withConfig(config);
        }
    }
}
