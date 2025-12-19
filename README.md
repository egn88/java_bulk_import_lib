# PostgreSQL Bulk Import Library

A high-performance Java library for bulk INSERT, UPDATE, and UPSERT operations on PostgreSQL using the efficient COPY command. Designed with flexible entity mapping strategies and support for both legacy Java 8 and modern Java 17+ projects.

## Features

- **High Performance**: Uses PostgreSQL's COPY command for maximum throughput
- **Three Mapping Strategies**: JPA annotations, custom annotations, or fluent programmatic API
- **Full Operation Support**: INSERT, UPDATE, and UPSERT (INSERT ... ON CONFLICT)
- **Streaming Support**: Process large datasets with `List<T>` or `Stream<T>`
- **Extensible Type System**: Built-in converters for common types plus custom converter support
- **Transaction Control**: Attach to existing transactions for ACID compliance
- **Spring Boot Integration**: Auto-configuration with externalized properties
- **Java 8+ Compatible**: Multi-module design supports both legacy and modern Java versions

## Quick Start Decision Guide

Choose the right module for your project:

```
┌─────────────────────────────────────────────────────────────────┐
│                     What Java version?                          │
├─────────────────────┬───────────────────────────────────────────┤
│      Java 8-16      │              Java 17+                     │
├─────────────────────┴───────────────────────────────────────────┤
│                                                                 │
│  Java 8-16:                    Java 17+:                        │
│  ┌─────────────────────┐       ┌─────────────────────────────┐  │
│  │ Using JPA entities? │       │ Using Spring Boot 3.x?      │  │
│  └─────────┬───────────┘       └─────────────┬───────────────┘  │
│      Yes   │   No                   Yes      │   No             │
│      ▼     ▼                        ▼        ▼                  │
│  ┌──────┐ ┌──────┐              ┌──────┐  ┌─────────────────┐   │
│  │jpa-  │ │core  │              │spring│  │ Using JPA?      │   │
│  │javax │ │      │              │-boot │  └────────┬────────┘   │
│  └──────┘ └──────┘              └──────┘     Yes   │  No        │
│                                              ▼     ▼            │
│                                          ┌──────┐ ┌──────┐      │
│                                          │jpa-  │ │core  │      │
│                                          │jakart│ │      │      │
│                                          └──────┘ └──────┘      │
└─────────────────────────────────────────────────────────────────┘
```

**Quick Reference:**

| Your Project | Module to Use |
|--------------|---------------|
| Spring Boot 3.x with JPA | `pg-bulk-import-spring-boot` |
| Java 17+ with JPA (no Spring) | `pg-bulk-import-jpa-jakarta` |
| Java 8-16 with JPA | `pg-bulk-import-jpa-javax` |
| Any Java version, no JPA | `pg-bulk-import-core` |

## When to Use This Library

### ✅ USE when:
- Inserting/updating **1,000+ rows** at once
- Batch importing from files (CSV, JSON, etc.)
- ETL/data migration jobs
- Syncing data from external systems
- Performance is critical for bulk operations

### ❌ DON'T USE when:
- Inserting single rows or small batches (<100 rows)
- You need database-generated values (sequences, defaults) returned
- Complex entity relationships with cascading
- You need JPA lifecycle events (@PrePersist, @PostUpdate)
- Real-time CRUD operations (use standard JPA/JDBC instead)

### Performance Comparison

| Records | Standard JPA | This Library | Speedup |
|---------|--------------|--------------|---------|
| 1,000 | ~500ms | ~50ms | 10x |
| 10,000 | ~5s | ~150ms | 33x |
| 100,000 | ~50s | ~1.5s | 33x |
| 1,000,000 | ~8min | ~15s | 32x |

*Benchmarks on PostgreSQL 16, Java 21, local Docker container*

## Module Structure

The library is organized as a multi-module Maven project:

| Module | Java Version | Description |
|--------|--------------|-------------|
| `pg-bulk-import-core` | 8+ | Core library with custom annotations, fluent API |
| `pg-bulk-import-jpa-javax` | 8+ | JPA support using `javax.persistence` (JPA 2.x) |
| `pg-bulk-import-jpa-jakarta` | 17+ | JPA support using `jakarta.persistence` (JPA 3.x) |
| `pg-bulk-import-spring-boot` | 17+ | Spring Boot 3.x auto-configuration |

## Requirements

- PostgreSQL 12+
- Maven 3.6+ (for building)
- Java version depends on module (see table above)

## Adding to Your Project

Choose the appropriate dependency based on your project's Java version and requirements:

### Java 8 Legacy Projects (No JPA)

Use the core module with custom `@BulkTable` annotations or fluent builder API:

```xml
<dependency>
    <groupId>io.github.egn88</groupId>
    <artifactId>pg-bulk-import-core</artifactId>
    <version>2.0.1</version>
</dependency>
```

```java
// Option 1: Custom annotations
@BulkTable(name = "users")
public class User {
    @BulkId
    private Long id;
    @BulkColumn(name = "user_name")
    private String userName;
}

// Option 2: Fluent builder
TableMapping<User> mapping = TableMapping.<User>builder("users")
    .id("id", User::getId)
    .column("user_name", User::getUserName)
    .build();

BulkImporter importer = BulkImporter.create(dataSource);
importer.insert(mapping, users);
```

### Java 8 Legacy Projects (With JPA Entities)

Use the javax.persistence module to work with existing JPA entities:

```xml
<dependency>
    <groupId>io.github.egn88</groupId>
    <artifactId>pg-bulk-import-jpa-javax</artifactId>
    <version>2.0.1</version>
</dependency>
```

```java
import javax.persistence.*;

@Entity
@Table(name = "users")
public class User {
    @Id
    private Long id;
    @Column(name = "user_name")
    private String userName;
}

// Ensure JPA mapper is registered (call once at startup)
com.bulkimport.mapping.jpa.JpaEntityMapper.register();

BulkImporter importer = BulkImporter.create(dataSource);
importer.insert(User.class, users);
```

### Java 17+ Projects (With JPA Entities)

Use the jakarta.persistence module for modern JPA:

```xml
<dependency>
    <groupId>io.github.egn88</groupId>
    <artifactId>pg-bulk-import-jpa-jakarta</artifactId>
    <version>2.0.1</version>
</dependency>
```

```java
import jakarta.persistence.*;

@Entity
@Table(name = "users")
public class User {
    @Id
    private Long id;
    @Column(name = "user_name")
    private String userName;
}

// Ensure JPA mapper is registered (call once at startup)
com.bulkimport.mapping.jpa.JpaEntityMapper.register();

BulkImporter importer = BulkImporter.create(dataSource);
importer.insert(User.class, users);
```

### Spring Boot 3.x Projects

Use the Spring Boot starter for auto-configuration:

```xml
<dependency>
    <groupId>io.github.egn88</groupId>
    <artifactId>pg-bulk-import-spring-boot</artifactId>
    <version>2.0.1</version>
</dependency>
```

```java
@Service
public class UserService {
    private final BulkImporter bulkImporter;

    public UserService(BulkImporter bulkImporter) {
        this.bulkImporter = bulkImporter;
    }

    public void importUsers(List<User> users) {
        bulkImporter.insert(User.class, users);
    }
}
```

### Gradle

```groovy
// Java 8 (core only)
implementation 'io.github.egn88:pg-bulk-import-core:2.0.1'

// Java 8 with JPA (javax)
implementation 'io.github.egn88:pg-bulk-import-jpa-javax:2.0.1'

// Java 17+ with JPA (jakarta)
implementation 'io.github.egn88:pg-bulk-import-jpa-jakarta:2.0.1'

// Spring Boot 3.x
implementation 'io.github.egn88:pg-bulk-import-spring-boot:2.0.1'
```

## Building the Library

```bash
# Clone the repository
git clone <repository-url>
cd bulk_import_lib

# Build and install all modules to local Maven repository
mvn clean install

# Run tests (requires Docker for Testcontainers)
mvn test
```

## Quick Start

### Basic Insert

```java
import com.bulkimport.BulkImporter;

// Create importer with DataSource
BulkImporter importer = BulkImporter.create(dataSource);

// Insert entities using fluent mapping
TableMapping<User> mapping = TableMapping.<User>builder("users")
    .id("id", User::getId)
    .column("name", User::getName)
    .column("email", User::getEmail)
    .build();

List<User> users = Arrays.asList(
    new User(1L, "Alice", "alice@example.com"),
    new User(2L, "Bob", "bob@example.com")
);

int inserted = importer.insert(mapping, users);
System.out.println("Inserted " + inserted + " rows");
```

## Entity Mapping

The library supports three ways to define how your Java objects map to database tables:

### Option 1: JPA Annotations

Use standard JPA annotations - perfect for existing JPA entities.

**For Java 8 (javax.persistence):**
```java
import javax.persistence.*;

@Entity
@Table(name = "users")
public class User {
    @Id
    private Long id;

    @Column(name = "user_name", nullable = false)
    private String userName;

    @Column(name = "email")
    private String email;
}
```

**For Java 17+ (jakarta.persistence):**
```java
import jakarta.persistence.*;

@Entity
@Table(name = "users")
public class User {
    @Id
    private Long id;

    @Column(name = "user_name", nullable = false)
    private String userName;

    @Column(name = "email")
    private String email;
}
```

### Option 2: Custom Annotations (Java 8+)

Use library-specific annotations for DTOs or non-JPA classes:

```java
import com.bulkimport.mapping.annotation.*;

@BulkTable(name = "users")
public class UserDto {
    @BulkId
    private Long id;

    @BulkColumn(name = "user_name", nullable = false)
    private String userName;

    @BulkColumn
    private String email;

    @BulkTransient  // Excluded from bulk operations
    private String temporaryField;
}
```

### Option 3: Fluent Builder API (Java 8+)

Define mappings programmatically - no annotations required:

```java
import com.bulkimport.mapping.TableMapping;

TableMapping<User> mapping = TableMapping.<User>builder("users")
    .id("id", User::getId)
    .column("user_name", User::getUserName)
    .column("email", User::getEmail)
    .column("created_at", User::getCreatedAt)
    .build();

// Use the mapping
importer.insert(mapping, users);
```

## Operations

### INSERT

Direct COPY to target table - fastest option for new data:

```java
// With fluent mapping
int inserted = importer.insert(mapping, userList);

// With annotated class (JPA or @BulkTable)
int inserted = importer.insert(User.class, userList);

// From a Stream (for large datasets)
Stream<User> userStream = generateUsers();
int inserted = importer.insert(mapping, userStream);
```

### UPDATE

COPY to staging table, then UPDATE via JOIN:

```java
// Update existing rows (matches on @Id/@BulkId columns by default)
List<User> updatedUsers = Arrays.asList(
    new User(1L, "Alice Updated", "alice.new@example.com"),
    new User(2L, "Bob Updated", "bob.new@example.com")
);

int updated = importer.update(mapping, updatedUsers);
```

### UPSERT (INSERT or UPDATE)

COPY to staging table, then INSERT ... ON CONFLICT:

```java
import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.config.ConflictStrategy;

BulkImportConfig config = BulkImportConfig.builder()
    .conflictStrategy(ConflictStrategy.UPDATE_ALL)
    .conflictColumns("id")  // Column(s) to detect conflicts
    .build();

BulkImporter importer = BulkImporter.create(dataSource)
    .withConfig(config);

int affected = importer.upsert(mapping, users);
```

## Configuration

### BulkImportConfig Options

```java
BulkImportConfig config = BulkImportConfig.builder()
    // Conflict strategy for upserts
    .conflictStrategy(ConflictStrategy.UPDATE_ALL)

    // Columns for ON CONFLICT clause
    .conflictColumns("email")

    // Columns to update on conflict (for UPDATE_SPECIFIED)
    .updateColumns("name", "updated_at")

    // Columns for matching rows in UPDATE operations
    // (defaults to @Id columns if not specified)
    .matchColumns("external_id")

    // Staging table name prefix (default: "bulk_staging_")
    .stagingTablePrefix("tmp_")

    // Auto-cleanup staging tables (default: true)
    .autoCleanupStaging(true)

    // Null value handling in CSV
    .nullHandling(NullHandling.EMPTY_STRING)

    // Schema name (optional)
    .schemaName("public")

    .build();
```

### Conflict Strategies

| Strategy | Description |
|----------|-------------|
| `FAIL` | Throw exception on conflict (default) |
| `DO_NOTHING` | Skip conflicting rows (`ON CONFLICT DO NOTHING`) |
| `UPDATE_ALL` | Update all non-ID columns on conflict |
| `UPDATE_SPECIFIED` | Update only specified columns on conflict |

### Null Handling

| Option | CSV Representation |
|--------|-------------------|
| `EMPTY_STRING` | Empty string (default) |
| `LITERAL_NULL` | `\N` |
| `NULL_STRING` | `NULL` |

## Transaction Support

Attach to an existing transaction for ACID compliance:

```java
Connection conn = dataSource.getConnection();
conn.setAutoCommit(false);

try {
    BulkImporter importer = BulkImporter.create(conn);

    importer.insert(userMapping, users);
    importer.insert(orderMapping, orders);
    conn.commit();
} catch (Exception e) {
    conn.rollback();
    throw e;
} finally {
    conn.close();
}
```

## Custom Type Converters

Register custom converters for domain-specific types:

```java
import com.bulkimport.converter.TypeConverter;
import com.bulkimport.config.NullHandling;

public class MoneyConverter implements TypeConverter<Money> {
    @Override
    public String toCsvValue(Money value, NullHandling nullHandling) {
        if (value == null) {
            return handleNull(nullHandling);
        }
        return value.getAmount().toPlainString();
    }

    @Override
    public Class<Money> supportedType() {
        return Money.class;
    }
}

// Register the converter
BulkImporter importer = BulkImporter.create(dataSource)
    .registerConverter(Money.class, new MoneyConverter());
```

### Built-in Type Support

| Category | Types |
|----------|-------|
| **String** | `String` |
| **Numeric** | `Integer`, `Long`, `Double`, `Float`, `BigDecimal`, `BigInteger`, `Short`, `Byte` |
| **Boolean** | `Boolean` |
| **Date/Time** | `LocalDate`, `LocalDateTime`, `LocalTime`, `Instant`, `ZonedDateTime`, `OffsetDateTime`, `java.sql.Date`, `java.sql.Timestamp`, `java.util.Date` |
| **UUID** | `UUID` |
| **Binary** | `byte[]` (PostgreSQL `bytea`) |
| **Arrays** | `Object[]`, `List<?>` (PostgreSQL array format) |
| **JSON** | `JsonNode` (requires Jackson) |
| **Enum** | Any enum (converted to name) |

## Spring Boot Integration

### Auto-Configuration (Spring Boot 3.x)

Add the `pg-bulk-import-spring-boot` dependency and the library auto-configures when a `DataSource` is available:

```java
@Service
public class UserService {

    private final BulkImporter bulkImporter;

    public UserService(BulkImporter bulkImporter) {
        this.bulkImporter = bulkImporter;
    }

    public void importUsers(List<User> users) {
        bulkImporter.insert(User.class, users);
    }
}
```

### Configuration Properties

Configure via `application.properties` or `application.yml`:

```properties
# application.properties
bulkimport.enabled=true
bulkimport.conflict-strategy=UPDATE_ALL
bulkimport.staging-table-prefix=bulk_staging_
bulkimport.auto-cleanup-staging=true
bulkimport.null-handling=EMPTY_STRING
bulkimport.schema-name=public
```

```yaml
# application.yml
bulkimport:
  enabled: true
  conflict-strategy: UPDATE_ALL
  conflict-columns:
    - id
  update-columns:
    - name
    - email
    - updated_at
```

### Custom Bean Configuration

Override the auto-configured beans:

```java
@Configuration
public class BulkImportConfiguration {

    @Bean
    public BulkImportConfig bulkImportConfig() {
        return BulkImportConfig.builder()
            .conflictStrategy(ConflictStrategy.UPDATE_ALL)
            .conflictColumns("id")
            .build();
    }

    @Bean
    public BulkImporter bulkImporter(DataSource dataSource, BulkImportConfig config) {
        return BulkImporter.create(dataSource)
            .withConfig(config)
            .registerConverter(Money.class, new MoneyConverter());
    }
}
```

## Exception Handling

The library throws descriptive exceptions:

```java
try {
    importer.insert(mapping, users);
} catch (MappingException e) {
    // Entity mapping issues (missing annotations, no ID column, etc.)
} catch (ConfigurationException e) {
    // Invalid configuration (bad batch size, missing conflict columns, etc.)
} catch (ExecutionException e) {
    // Runtime errors (COPY failed, connection issues, etc.)
} catch (BulkImportException e) {
    // Base exception for all library errors
}
```

## Performance Tips

1. **Use Streams for Large Datasets**: Avoid loading millions of records into memory
   ```java
   Stream<User> users = readUsersFromFile();
   importer.insert(mapping, users);
   ```

2. **Disable Auto-Commit**: For multiple operations, use explicit transactions

3. **Index Conflict Columns**: Ensure columns used in `ON CONFLICT` have indexes

4. **No Size Limits**: The library streams data directly to PostgreSQL, so there's no practical limit on dataset size - 1 million or 10 million rows work equally well

## How It Works

### INSERT Flow
1. Resolve entity → table mapping
2. Stream entities through FastCSV
3. Execute `COPY table(columns) FROM STDIN WITH (FORMAT csv)`
4. Return affected row count

### UPDATE Flow
1. Create temporary staging table (session-scoped, no WAL overhead)
2. COPY data to staging table
3. Execute `UPDATE target SET ... FROM staging WHERE ...`
4. Drop staging table (or auto-cleanup on session end)
5. Return affected row count

### UPSERT Flow
1. Create temporary staging table (session-scoped, no WAL overhead)
2. COPY data to staging table
3. Execute `INSERT INTO target SELECT ... FROM staging ON CONFLICT (...) DO UPDATE SET ...`
4. Drop staging table (or auto-cleanup on session end)
5. Return affected row count

## Migration Guide

### From 1.x to 2.x

The 2.x version introduces a multi-module structure. Update your dependencies:

**Java 8 projects:**
```xml
<!-- Old -->
<artifactId>pg-bulk-import</artifactId>
<version>1.x</version>

<!-- New (pick one) -->
<artifactId>pg-bulk-import-core</artifactId>      <!-- No JPA -->
<artifactId>pg-bulk-import-jpa-javax</artifactId> <!-- With JPA -->
<version>2.0.0</version>
```

**Java 17+ projects:**
```xml
<!-- Old -->
<artifactId>pg-bulk-import</artifactId>
<version>1.x</version>

<!-- New (pick one) -->
<artifactId>pg-bulk-import-jpa-jakarta</artifactId> <!-- With JPA -->
<artifactId>pg-bulk-import-spring-boot</artifactId> <!-- Spring Boot 3.x -->
<version>2.0.0</version>
```

## Troubleshooting

### Common Issues

#### "COPY command is only supported in an open transaction"
**Cause**: Connection pool returned a connection with auto-commit enabled.
**Solution**: Wrap the operation in a transaction:
```java
Connection conn = dataSource.getConnection();
conn.setAutoCommit(false);
try {
    BulkImporter.create(conn).insert(mapping, data);
    conn.commit();
} finally {
    conn.close();
}
```

#### "MappingException: No mapping found for class X"
**Cause**: Entity class doesn't have required annotations.
**Solution**: Ensure your class has either:
- JPA annotations (`@Entity`, `@Table`, `@Id`)
- Custom annotations (`@BulkTable`, `@BulkId`)
- Or use the fluent builder API

#### "Relation 'table_name' does not exist"
**Cause**: Table name doesn't match the database.
**Solution**: Check `@Table(name = "exact_table_name")` matches your database. PostgreSQL table names are case-sensitive when quoted.

#### Slow performance with small batches
**Cause**: COPY has overhead that outweighs benefits for small datasets.
**Solution**: For <100 rows, use standard JPA batch inserts. This library shines with 1,000+ rows.

#### "Connection refused" in tests with Testcontainers
**Cause**: Multiple test classes creating separate containers, causing connection pool to hold stale connections.
**Solution**: Use a singleton container pattern:
```java
public abstract class BaseIntegrationTest {
    static final PostgreSQLContainer<?> postgres;

    static {
        postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("testdb");
        postgres.start();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
    }
}
```

#### "ERROR: extra data after last expected column"
**Cause**: CSV data has more columns than the table mapping expects.
**Solution**: Ensure your mapping includes all columns being written. Check for commas in string data (library handles escaping, but verify).

### Getting Help

If you encounter issues not covered here:
1. Check the [GitHub Issues](https://github.com/elgina88/pg-bulk-import/issues) for similar problems
2. Open a new issue with:
   - Java version and module used
   - PostgreSQL version
   - Minimal code example reproducing the issue
   - Full stack trace

## License

MIT
