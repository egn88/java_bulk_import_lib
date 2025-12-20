package com.bulkimport;

import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.config.ConflictStrategy;
import com.bulkimport.testutil.DatabaseIntegrationTest;
import com.bulkimport.testutil.PostgresTestContainer;
import com.bulkimport.testutil.StressTestProduct;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stress tests for bulk import operations with large datasets.
 * These tests verify performance with 1M+ records and validate
 * that the staging table index optimization works correctly.
 *
 * Run with: mvn test -Dgroups=stress
 */
@Tag("stress")
class BulkImportStressTest extends DatabaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(BulkImportStressTest.class);

    private static final int RECORD_COUNT_1M = 1_000_000;
    private static final int RECORD_COUNT_500K = 500_000;
    private static final int RECORD_COUNT_250K = 250_000;

    private static final String[] CATEGORIES = {"Electronics", "Clothing", "Food", "Books", "Home", "Sports"};
    private static final String[] SUBCATEGORIES = {"Premium", "Standard", "Budget", "Luxury", "Basic"};
    private static final String[] BRANDS = {"BrandA", "BrandB", "BrandC", "BrandD", "BrandE"};
    private static final String[] STATUSES = {"active", "inactive", "pending", "discontinued"};

    private static final String TABLE_NAME = "stress_test_products";

    @BeforeAll
    static void setUpDatabase() throws SQLException {
        createStressTestTable();
    }

    @BeforeEach
    void setUp() throws SQLException {
        truncateTable(TABLE_NAME);
    }

    @Test
    void shouldInsert1MRecordsEfficiently() throws SQLException {
        // Given
        log.info("========================================");
        log.info("TEST: Insert 1M Records");
        log.info("========================================");

        Stream<StressTestProduct> products = generateProducts(0, RECORD_COUNT_1M);

        // When
        long startTime = System.currentTimeMillis();
        int inserted = importer.insert(StressTestProduct.class, products);
        long duration = System.currentTimeMillis() - startTime;

        // Then
        assertThat(inserted).isEqualTo(RECORD_COUNT_1M);
        assertThat(countRows(TABLE_NAME)).isEqualTo(RECORD_COUNT_1M);

        double recordsPerSecond = (double) inserted / duration * 1000;
        log.info("INSERT RESULTS:");
        log.info("  Records inserted: {}", String.format("%,d", inserted));
        log.info("  Duration: {} ms", String.format("%,d", duration));
        log.info("  Throughput: {} records/sec", String.format("%,.2f", recordsPerSecond));
        log.info("========================================");

        // Reasonable performance expectation: at least 10,000 records/sec
        assertThat(recordsPerSecond).isGreaterThan(10_000);
    }

    @Test
    void shouldUpdate500KRecordsEfficiently() throws SQLException {
        // Given - First insert 500K records
        log.info("========================================");
        log.info("TEST: Update 500K Records");
        log.info("========================================");

        log.info("Phase 1: Inserting initial 500K records...");
        Stream<StressTestProduct> initialProducts = generateProducts(0, RECORD_COUNT_500K);
        int inserted = importer.insert(StressTestProduct.class, initialProducts);
        assertThat(inserted).isEqualTo(RECORD_COUNT_500K);
        log.info("Initial insert complete: {} records", String.format("%,d", inserted));

        // When - Update all 500K records with modified data
        log.info("Phase 2: Updating 500K records...");
        Stream<StressTestProduct> updatedProducts = generateProducts(0, RECORD_COUNT_500K)
            .map(p -> {
                p.setName(p.getName() + " UPDATED");
                p.setPrice(p.getPrice().add(BigDecimal.TEN));
                p.setUpdatedAt(LocalDateTime.now());
                return p;
            });

        BulkImportConfig config = BulkImportConfig.builder()
            .matchColumns("code")
            .build();

        BulkImporter configuredImporter = BulkImporter.create(connection).withConfig(config);

        long startTime = System.currentTimeMillis();
        int updated = configuredImporter.update(StressTestProduct.class, updatedProducts);
        long duration = System.currentTimeMillis() - startTime;

        // Then
        assertThat(updated).isEqualTo(RECORD_COUNT_500K);
        assertThat(countRows(TABLE_NAME)).isEqualTo(RECORD_COUNT_500K); // No new rows added

        // Verify some records were actually updated
        String sampleName = getProductName("PROD-000001");
        assertThat(sampleName).endsWith("UPDATED");

        double recordsPerSecond = (double) updated / duration * 1000;
        log.info("UPDATE RESULTS:");
        log.info("  Records updated: {}", String.format("%,d", updated));
        log.info("  Duration: {} ms", String.format("%,d", duration));
        log.info("  Throughput: {} records/sec", String.format("%,.2f", recordsPerSecond));
        log.info("========================================");

        // With staging table index, updates should be reasonably fast
        // At least 5,000 records/sec expected
        assertThat(recordsPerSecond).isGreaterThan(5_000);
    }

    @Test
    void shouldUpsert500KRecordsEfficiently() throws SQLException {
        // Given - First insert 500K records
        log.info("========================================");
        log.info("TEST: Upsert 500K Records (250K updates + 250K inserts)");
        log.info("========================================");

        log.info("Phase 1: Inserting initial 500K records...");
        Stream<StressTestProduct> initialProducts = generateProducts(0, RECORD_COUNT_500K);
        int inserted = importer.insert(StressTestProduct.class, initialProducts);
        assertThat(inserted).isEqualTo(RECORD_COUNT_500K);
        log.info("Initial insert complete: {} records", String.format("%,d", inserted));

        // When - Upsert 500K records: 250K existing (update) + 250K new (insert)
        log.info("Phase 2: Upserting 500K records (250K updates + 250K new inserts)...");

        // Generate 500K products: 250K overlapping with existing (will update), 250K new (will insert)
        Stream<StressTestProduct> upsertProducts = generateProducts(RECORD_COUNT_250K, RECORD_COUNT_500K)
            .map(p -> {
                p.setName(p.getName() + " UPSERTED");
                p.setPrice(p.getPrice().multiply(BigDecimal.valueOf(1.5)));
                p.setUpdatedAt(LocalDateTime.now());
                return p;
            });

        BulkImportConfig config = BulkImportConfig.builder()
            .conflictStrategy(ConflictStrategy.UPDATE_ALL)
            .conflictColumns("code")
            .build();

        BulkImporter configuredImporter = BulkImporter.create(connection).withConfig(config);

        long startTime = System.currentTimeMillis();
        int affected = configuredImporter.upsert(StressTestProduct.class, upsertProducts);
        long duration = System.currentTimeMillis() - startTime;

        // Then
        assertThat(affected).isEqualTo(RECORD_COUNT_500K); // 250K updates + 250K inserts
        assertThat(countRows(TABLE_NAME)).isEqualTo(RECORD_COUNT_500K + RECORD_COUNT_250K); // 750K total

        // Verify an updated record
        String updatedName = getProductName("PROD-250001");
        assertThat(updatedName).endsWith("UPSERTED");

        // Verify a newly inserted record
        String newName = getProductName("PROD-500001");
        assertThat(newName).endsWith("UPSERTED");

        // Verify an untouched record (from first 250K that were not in upsert set)
        String untouchedName = getProductName("PROD-000001");
        assertThat(untouchedName).doesNotEndWith("UPSERTED");

        double recordsPerSecond = (double) affected / duration * 1000;
        log.info("UPSERT RESULTS:");
        log.info("  Records affected: {}", String.format("%,d", affected));
        log.info("  Duration: {} ms", String.format("%,d", duration));
        log.info("  Throughput: {} records/sec", String.format("%,.2f", recordsPerSecond));
        log.info("  Total rows in DB: {}", String.format("%,d", countRows(TABLE_NAME)));
        log.info("========================================");

        // Upsert should have reasonable performance
        // At least 5,000 records/sec expected
        assertThat(recordsPerSecond).isGreaterThan(5_000);
    }

    @Test
    void shouldHandleMixedInsertAndUpdateScenario() throws SQLException {
        // Given - Insert initial 500K records
        log.info("========================================");
        log.info("TEST: Mixed 500K Updates + 500K Inserts");
        log.info("========================================");

        log.info("Phase 1: Inserting initial 500K records...");
        Stream<StressTestProduct> initialProducts = generateProducts(0, RECORD_COUNT_500K);
        int inserted = importer.insert(StressTestProduct.class, initialProducts);
        assertThat(inserted).isEqualTo(RECORD_COUNT_500K);
        log.info("Initial insert complete: {} records", String.format("%,d", inserted));

        // When - Update first 500K and insert another 500K
        log.info("Phase 2: Updating 500K + Inserting 500K new records...");

        // Updates for existing records (0 to 500K-1)
        Stream<StressTestProduct> updates = generateProducts(0, RECORD_COUNT_500K)
            .map(p -> {
                p.setName(p.getName() + " v2");
                p.setPrice(p.getPrice().multiply(BigDecimal.valueOf(1.1)));
                return p;
            });

        BulkImportConfig updateConfig = BulkImportConfig.builder()
            .matchColumns("code")
            .build();

        long updateStart = System.currentTimeMillis();
        int updated = BulkImporter.create(connection)
            .withConfig(updateConfig)
            .update(StressTestProduct.class, updates);
        long updateDuration = System.currentTimeMillis() - updateStart;

        // New inserts (500K to 1M-1)
        Stream<StressTestProduct> newProducts = generateProducts(RECORD_COUNT_500K, RECORD_COUNT_500K);

        long insertStart = System.currentTimeMillis();
        int newInserted = BulkImporter.create(connection).insert(StressTestProduct.class, newProducts);
        long insertDuration = System.currentTimeMillis() - insertStart;

        // Then
        assertThat(updated).isEqualTo(RECORD_COUNT_500K);
        assertThat(newInserted).isEqualTo(RECORD_COUNT_500K);
        assertThat(countRows(TABLE_NAME)).isEqualTo(RECORD_COUNT_1M);

        log.info("MIXED OPERATION RESULTS:");
        log.info("  Updates: {} records in {} ms ({} rec/sec)",
            String.format("%,d", updated), String.format("%,d", updateDuration),
            String.format("%,.2f", (double) updated / updateDuration * 1000));
        log.info("  Inserts: {} records in {} ms ({} rec/sec)",
            String.format("%,d", newInserted), String.format("%,d", insertDuration),
            String.format("%,.2f", (double) newInserted / insertDuration * 1000));
        log.info("  Total records in DB: {}", String.format("%,d", countRows(TABLE_NAME)));
        log.info("========================================");
    }

    /**
     * Generates a stream of test products.
     *
     * @param startIndex starting index for product codes
     * @param count number of products to generate
     * @return stream of products
     */
    private Stream<StressTestProduct> generateProducts(int startIndex, int count) {
        return IntStream.range(startIndex, startIndex + count)
            .mapToObj(this::createProduct);
    }

    private StressTestProduct createProduct(int index) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        StressTestProduct product = new StressTestProduct();
        product.setCode(String.format("PROD-%06d", index));
        product.setName("Product " + index);
        product.setSku("SKU-" + index);
        product.setCategory(CATEGORIES[random.nextInt(CATEGORIES.length)]);
        product.setSubcategory(SUBCATEGORIES[random.nextInt(SUBCATEGORIES.length)]);
        product.setBrand(BRANDS[random.nextInt(BRANDS.length)]);
        product.setSupplier("Supplier " + (index % 100));
        product.setStatus(STATUSES[random.nextInt(STATUSES.length)]);
        product.setDescription("Description for product " + index + ". This is a sample product description.");
        product.setNotes("Notes for product " + index);
        product.setSpecifications("{\"weight\": \"1kg\", \"dimensions\": \"10x10x10\"}");
        product.setPrice(BigDecimal.valueOf(random.nextDouble(1, 1000)).setScale(2, RoundingMode.HALF_UP));
        product.setQuantity(random.nextInt(0, 1000));
        product.setWeight(BigDecimal.valueOf(random.nextDouble(0.1, 50)).setScale(3, RoundingMode.HALF_UP));
        product.setRating(BigDecimal.valueOf(random.nextDouble(1, 5)).setScale(2, RoundingMode.HALF_UP));
        product.setViewCount((long) random.nextInt(0, 100000));
        product.setCreatedAt(LocalDateTime.now().minusDays(random.nextInt(1, 365)));
        product.setUpdatedAt(LocalDateTime.now());
        product.setLastOrderDate(LocalDate.now().minusDays(random.nextInt(1, 30)));
        product.setExpirationDate(LocalDate.now().plusDays(random.nextInt(30, 365)));
        product.setActive(random.nextBoolean());
        product.setFeatured(random.nextInt(10) == 0); // 10% featured
        product.setInStock(random.nextInt(10) > 1); // 90% in stock
        product.setTaxable(random.nextInt(10) > 0); // 90% taxable

        return product;
    }

    private static void createStressTestTable() throws SQLException {
        PostgresTestContainer.executeSql(
            "DROP TABLE IF EXISTS stress_test_products CASCADE",
            """
            CREATE TABLE stress_test_products (
                code VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                sku VARCHAR(100),
                category VARCHAR(100),
                subcategory VARCHAR(100),
                brand VARCHAR(100),
                supplier VARCHAR(150),
                status VARCHAR(50),
                description TEXT,
                notes TEXT,
                specifications TEXT,
                price DECIMAL(12, 2),
                quantity INTEGER,
                weight DECIMAL(10, 3),
                rating DECIMAL(3, 2),
                view_count BIGINT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                last_order_date DATE,
                expiration_date DATE,
                active BOOLEAN,
                featured BOOLEAN,
                in_stock BOOLEAN,
                taxable BOOLEAN
            )
            """,
            "CREATE INDEX idx_stress_code ON stress_test_products(code)",
            "CREATE INDEX idx_stress_category ON stress_test_products(category)"
        );
    }

    private String getProductName(String code) throws SQLException {
        return getString(TABLE_NAME, "name", "code", code);
    }
}
