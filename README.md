# PostgreSQL Bulk Import Library

A high-performance Java library for bulk INSERT, UPDATE, and UPSERT operations on PostgreSQL using the COPY command. Supports JPA entities, custom annotations, or fluent API mapping.

## Features

- **High Performance**: Uses PostgreSQL's COPY command for maximum throughput
- **Multiple Mapping Options**: JPA annotations, custom annotations, or fluent builder API
- **Full Operation Support**: INSERT, UPDATE, and UPSERT (INSERT ... ON CONFLICT)
- **Streaming Support**: Process large datasets with `Stream<T>` without memory limits
- **Transaction Control**: Attach to existing transactions for ACID compliance
- **Spring Boot Integration**: Auto-configuration with externalized properties
- **Java 8+ Compatible**: Multi-module design supports both legacy and modern projects

## Performance

**INSERT** (1M records, 24 columns):

| Records | Standard JPA | This Library | Speedup | Throughput |
|---------|--------------|--------------|---------|------------|
| 100,000 | ~60s | ~1.3s | **46x** | 74,000 rec/sec |
| 500,000 | ~5min | ~6.7s | **45x** | 74,000 rec/sec |
| 1,000,000 | ~11min | ~13s | **50x** | 74,000 rec/sec |

**UPDATE** (500K records):

| Records | Standard JPA | This Library | Speedup | Throughput |
|---------|--------------|--------------|---------|------------|
| 500,000 | ~8min | ~15s | **32x** | 32,800 rec/sec |

**UPSERT** (500K records - 250K updates + 250K inserts):

| Records | Standard JPA | This Library | Speedup | Throughput |
|---------|--------------|--------------|---------|------------|
| 500,000 | ~10min | ~19s | **31x** | 26,600 rec/sec |

*Benchmarks on PostgreSQL 16, Java 21, Docker container, 24-column entity*

## Installation

**Step 1:** Import the BOM:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.github.egn88</groupId>
            <artifactId>pg-bulk-import-bom</artifactId>
            <version>2.0.3</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

**Step 2:** Add the module you need:

| Your Project | Module |
|--------------|--------|
| Spring Boot 3.x | `pg-bulk-import-spring-boot` |
| Java 17+ with JPA | `pg-bulk-import-jpa-jakarta` |
| Java 8-16 with JPA | `pg-bulk-import-jpa-javax` |
| No JPA (any Java) | `pg-bulk-import-core` |

```xml
<dependency>
    <groupId>io.github.egn88</groupId>
    <artifactId>pg-bulk-import-jpa-jakarta</artifactId> <!-- or your chosen module -->
</dependency>
```

<details>
<summary><b>Gradle</b></summary>

```groovy
dependencies {
    implementation platform('io.github.egn88:pg-bulk-import-bom:2.0.3')
    implementation 'io.github.egn88:pg-bulk-import-jpa-jakarta'
}
```
</details>

## Quick Start

```java
// With JPA entities - just use your existing @Entity classes
BulkImporter importer = BulkImporter.create(dataSource);
importer.insert(User.class, users);        // INSERT
importer.update(User.class, users);        // UPDATE
importer.upsert(User.class, users);        // UPSERT (INSERT ... ON CONFLICT)
```

## Entity Mapping

Three options to map your objects to database tables:

### JPA Annotations (recommended for JPA projects)

```java
@Entity
@Table(name = "users")
public class User {
    @Id
    private Long id;

    @Column(name = "user_name")
    private String userName;
}

// Register JPA mapper once at startup
JpaEntityMapper.register();

importer.insert(User.class, users);
```

### Custom Annotations (for DTOs or non-JPA classes)

```java
@BulkTable(name = "users")
public class UserDto {
    @BulkId
    private Long id;

    @BulkColumn(name = "user_name")
    private String userName;

    @BulkTransient  // excluded
    private String temp;
}

importer.insert(UserDto.class, users);
```

### Fluent Builder (no annotations required)

```java
TableMapping<User> mapping = TableMapping.<User>builder("users")
    .id("id", User::getId)
    .column("user_name", User::getUserName)
    .column("email", User::getEmail)
    .build();

importer.insert(mapping, users);
```

## Operations

### INSERT
```java
importer.insert(User.class, userList);
importer.insert(User.class, userStream);  // streaming for large datasets
```

### UPDATE
```java
// Matches on @Id columns by default
importer.update(User.class, usersToUpdate);
```

### UPSERT
```java
BulkImportConfig config = BulkImportConfig.builder()
    .conflictStrategy(ConflictStrategy.UPDATE_ALL)
    .conflictColumns("id")
    .build();

BulkImporter importer = BulkImporter.create(dataSource).withConfig(config);
importer.upsert(User.class, users);
```

**Conflict Strategies:**

| Strategy | Behavior |
|----------|----------|
| `FAIL` | Throw exception (default) |
| `DO_NOTHING` | Skip conflicting rows |
| `UPDATE_ALL` | Update all non-ID columns |
| `UPDATE_SPECIFIED` | Update only specified columns |

## Configuration

```java
BulkImportConfig config = BulkImportConfig.builder()
    // --- UPSERT options ---
    .conflictStrategy(ConflictStrategy.UPDATE_ALL) // optional, default: FAIL
    .conflictColumns("email")                      // optional, default: @Id columns (must have unique constraint)
    .updateColumns("name", "updated_at")           // optional, only for UPDATE_SPECIFIED strategy

    // --- UPDATE options ---
    .matchColumns("external_id")                   // optional, default: @Id columns (no unique constraint required)

    // --- General options ---
    .schemaName("public")                          // optional, default: from @Table annotation or DB default
    .stagingTablePrefix("tmp_")                    // optional, default: "bulk_staging_"
    .autoCleanupStaging(true)                      // optional, default: true
    .nullHandling(NullHandling.EMPTY_STRING)       // optional, default: EMPTY_STRING
    .build();
```

## Transaction Support

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

## Spring Boot

Add `pg-bulk-import-spring-boot` and inject `BulkImporter`:

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

Configure via `application.yml`:
```yaml
bulkimport:
  conflict-strategy: UPDATE_ALL
  conflict-columns: [id]
```

## Custom Type Converters

```java
public class MoneyConverter implements TypeConverter<Money> {
    @Override
    public String toCsvValue(Money value, NullHandling nullHandling) {
        return value == null ? handleNull(nullHandling) : value.getAmount().toPlainString();
    }
}

BulkImporter importer = BulkImporter.create(dataSource)
    .registerConverter(Money.class, new MoneyConverter());
```

**Built-in types:** String, Integer, Long, Double, BigDecimal, Boolean, LocalDate, LocalDateTime, Instant, UUID, byte[], arrays, JsonNode, enums.

## When to Use

✅ **Use for:** 1,000+ rows, batch imports, ETL jobs, data sync

❌ **Don't use for:** Single rows, need returned generated IDs, JPA lifecycle events

## Troubleshooting

<details>
<summary><b>"No mapping found for class X"</b></summary>

Ensure your class has `@Entity`/`@Table`/`@Id` (JPA) or `@BulkTable`/`@BulkId` (custom), or use the fluent builder.
</details>

<details>
<summary><b>"Relation 'table_name' does not exist"</b></summary>

Check that `@Table(name = "...")` matches your database exactly. PostgreSQL is case-sensitive for quoted identifiers.
</details>

<details>
<summary><b>Slow with small batches</b></summary>

COPY has overhead. For <100 rows, use standard JPA. This library shines at 1,000+ rows.
</details>

## License

MIT
