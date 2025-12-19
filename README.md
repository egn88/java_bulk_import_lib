# Bulk Import Library

A high-performance Java library for bulk INSERT, UPDATE, and UPSERT operations on PostgreSQL using the efficient COPY command. Designed for Spring Boot and JPA compatibility with flexible entity mapping strategies.

## Features

- **High Performance**: Uses PostgreSQL's COPY command for maximum throughput
- **Three Mapping Strategies**: JPA annotations, custom annotations, or fluent programmatic API
- **Full Operation Support**: INSERT, UPDATE, and UPSERT (INSERT ... ON CONFLICT)
- **Streaming Support**: Process large datasets with `List<T>` or `Stream<T>`
- **Extensible Type System**: Built-in converters for common types plus custom converter support
- **Transaction Control**: Attach to existing transactions for ACID compliance
- **Spring Boot Integration**: Auto-configuration with externalized properties
- **Configurable**: Batch size, conflict strategies, null handling, and more

## Requirements

- Java 17+
- PostgreSQL 12+
- Maven 3.6+ (for building)

## Building the Library

```bash
# Clone the repository
git clone <repository-url>
cd bulk_import_lib

# Build and install to local Maven repository
mvn clean install

# Run tests (requires Docker for Testcontainers)
mvn test
```

## Adding to Your Project

### Maven

```xml
<dependency>
    <groupId>com.bulkimport</groupId>
    <artifactId>bulk-import-lib</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Gradle

```groovy
implementation 'com.bulkimport:bulk-import-lib:1.0.0-SNAPSHOT'
```

## Quick Start

### Basic Insert

```java
import com.bulkimport.BulkImporter;

// Create importer with DataSource
BulkImporter importer = BulkImporter.create(dataSource);

// Insert entities
List<User> users = List.of(
    new User(1L, "Alice", "alice@example.com"),
    new User(2L, "Bob", "bob@example.com")
);

int inserted = importer.insert(User.class, users);
System.out.println("Inserted " + inserted + " rows");
```

## Entity Mapping

The library supports three ways to define how your Java objects map to database tables:

### Option 1: JPA Annotations

Use standard JPA annotations - perfect for existing JPA entities:

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

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    // Getters and setters...
}
```

### Option 2: Custom Annotations

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

    // Getters and setters...
}
```

### Option 3: Fluent Builder API

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
// From a List
int inserted = importer.insert(User.class, userList);

// From a Stream (for large datasets)
Stream<User> userStream = generateUsers();
int inserted = importer.insert(User.class, userStream);

// With explicit mapping
int inserted = importer.insert(mapping, userList);
```

### UPDATE

COPY to staging table, then UPDATE via JOIN:

```java
// Update existing rows (matches on @Id columns by default)
List<User> updatedUsers = List.of(
    new User(1L, "Alice Updated", "alice.new@example.com"),
    new User(2L, "Bob Updated", "bob.new@example.com")
);

int updated = importer.update(User.class, updatedUsers);
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

int affected = importer.upsert(User.class, users);
```

## Configuration

### BulkImportConfig Options

```java
BulkImportConfig config = BulkImportConfig.builder()
    // Batch size for streaming (default: 10,000)
    .batchSize(5000)

    // Conflict strategy for upserts
    .conflictStrategy(ConflictStrategy.UPDATE_ALL)

    // Columns for ON CONFLICT clause
    .conflictColumns("email")

    // Columns to update on conflict (for UPDATE_SPECIFIED)
    .updateColumns("name", "updated_at")

    // Columns for matching rows in UPDATE operations
    // (defaults to @Id columns if not specified)
    .matchColumns("external_id")

    // Use UNLOGGED tables for staging (faster, default: true)
    .useUnloggedTables(true)

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
try (Connection conn = dataSource.getConnection()) {
    conn.setAutoCommit(false);

    BulkImporter importer = BulkImporter.create(conn);

    try {
        importer.insert(User.class, users);
        importer.insert(Order.class, orders);
        conn.commit();
    } catch (Exception e) {
        conn.rollback();
        throw e;
    }
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

### Auto-Configuration

The library auto-configures when Spring Boot and a `DataSource` are available:

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
bulkimport.batch-size=5000
bulkimport.conflict-strategy=UPDATE_ALL
bulkimport.use-unlogged-tables=true
bulkimport.staging-table-prefix=bulk_staging_
bulkimport.auto-cleanup-staging=true
bulkimport.null-handling=EMPTY_STRING
bulkimport.schema-name=public
```

```yaml
# application.yml
bulkimport:
  enabled: true
  batch-size: 5000
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
            .batchSize(10000)
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
    importer.insert(User.class, users);
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
   importer.insert(User.class, users);
   ```

2. **Tune Batch Size**: Default is 10,000; adjust based on your data size and memory
   ```java
   .batchSize(50000)
   ```

3. **Use UNLOGGED Tables**: Enabled by default for staging tables (faster but not crash-safe)

4. **Disable Auto-Commit**: For multiple operations, use explicit transactions

5. **Index Conflict Columns**: Ensure columns used in `ON CONFLICT` have indexes

## How It Works

### INSERT Flow
1. Resolve entity → table mapping
2. Stream entities through FastCSV
3. Execute `COPY table(columns) FROM STDIN WITH (FORMAT csv)`
4. Return affected row count

### UPDATE Flow
1. Create UNLOGGED temporary staging table
2. COPY data to staging table
3. Execute `UPDATE target SET ... FROM staging WHERE ...`
4. Drop staging table
5. Return affected row count

### UPSERT Flow
1. Create UNLOGGED temporary staging table
2. COPY data to staging table
3. Execute `INSERT INTO target SELECT ... FROM staging ON CONFLICT (...) DO UPDATE SET ...`
4. Drop staging table
5. Return affected row count

## License

MIT

