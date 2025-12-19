package com.bulkimport.spring;

import com.bulkimport.BulkImporter;
import com.bulkimport.config.BulkImportConfig;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

/**
 * Spring Boot auto-configuration for the bulk import library.
 *
 * <p>This configuration is enabled when:
 * <ul>
 *   <li>A DataSource bean is available</li>
 *   <li>The property bulkimport.enabled is true (default)</li>
 * </ul>
 *
 * <p>Configuration can be customized via properties:
 * <pre>
 * bulkimport.batch-size=5000
 * bulkimport.conflict-strategy=UPDATE_ALL
 * bulkimport.use-unlogged-tables=true
 * </pre>
 */
@AutoConfiguration
@ConditionalOnClass({DataSource.class, BulkImporter.class})
@ConditionalOnProperty(prefix = "bulkimport", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(BulkImportProperties.class)
public class BulkImportAutoConfiguration {

    private final BulkImportProperties properties;

    public BulkImportAutoConfiguration(BulkImportProperties properties) {
        this.properties = properties;
    }

    /**
     * Creates the default BulkImportConfig bean from properties.
     */
    @Bean
    @ConditionalOnMissingBean
    public BulkImportConfig bulkImportConfig() {
        BulkImportConfig.Builder builder = BulkImportConfig.builder()
            .batchSize(properties.getBatchSize())
            .conflictStrategy(properties.getConflictStrategy())
            .useUnloggedTables(properties.isUseUnloggedTables())
            .stagingTablePrefix(properties.getStagingTablePrefix())
            .autoCleanupStaging(properties.isAutoCleanupStaging())
            .nullHandling(properties.getNullHandling());

        if (properties.getConflictColumns() != null) {
            builder.conflictColumns(properties.getConflictColumns());
        }

        if (properties.getUpdateColumns() != null) {
            builder.updateColumns(properties.getUpdateColumns());
        }

        if (properties.getMatchColumns() != null) {
            builder.matchColumns(properties.getMatchColumns());
        }

        if (properties.getSchemaName() != null) {
            builder.schemaName(properties.getSchemaName());
        }

        return builder.build();
    }

    /**
     * Creates the BulkImporter bean using the DataSource and configuration.
     */
    @Bean
    @ConditionalOnMissingBean
    public BulkImporter bulkImporter(DataSource dataSource, BulkImportConfig config) {
        return BulkImporter.create(dataSource)
            .withConfig(config);
    }
}
