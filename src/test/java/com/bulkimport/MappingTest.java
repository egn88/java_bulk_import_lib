package com.bulkimport;

import com.bulkimport.exception.MappingException;
import com.bulkimport.mapping.ColumnMapping;
import com.bulkimport.mapping.EntityMapperResolver;
import com.bulkimport.mapping.TableMapping;
import com.bulkimport.mapping.annotation.AnnotationEntityMapper;
import com.bulkimport.mapping.jpa.JpaEntityMapper;
import com.bulkimport.testutil.TestEntities.BulkUser;
import com.bulkimport.testutil.TestEntities.JpaUser;
import com.bulkimport.testutil.TestEntities.SimpleUser;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MappingTest {

    @Test
    void shouldMapJpaEntity() {
        // When
        TableMapping<JpaUser> mapping = JpaEntityMapper.<JpaUser>getInstance()
            .map(JpaUser.class);

        // Then
        assertThat(mapping.getTableName()).isEqualTo("users");
        assertThat(mapping.getColumnNames()).contains("id", "name", "email", "age", "active");
        assertThat(mapping.getIdColumnNames()).containsExactly("id");
    }

    @Test
    void shouldMapBulkAnnotatedEntity() {
        // When
        TableMapping<BulkUser> mapping = AnnotationEntityMapper.<BulkUser>getInstance()
            .map(BulkUser.class);

        // Then
        assertThat(mapping.getTableName()).isEqualTo("users");
        assertThat(mapping.getColumnNames()).contains("id", "name", "email", "age", "active");
        assertThat(mapping.getIdColumnNames()).containsExactly("id");
    }

    @Test
    void shouldBuildFluentMapping() {
        // When
        TableMapping<SimpleUser> mapping = TableMapping.<SimpleUser>builder("users")
            .id("id", SimpleUser::getId)
            .column("name", SimpleUser::getName)
            .column("email", SimpleUser::getEmail)
            .build();

        // Then
        assertThat(mapping.getTableName()).isEqualTo("users");
        assertThat(mapping.getColumnNames()).containsExactly("id", "name", "email");
        assertThat(mapping.getIdColumnNames()).containsExactly("id");
    }

    @Test
    void shouldResolveMapperAutomatically() {
        // Given
        EntityMapperResolver resolver = EntityMapperResolver.getInstance();

        // When
        TableMapping<JpaUser> jpaMapping = resolver.resolve(JpaUser.class);
        TableMapping<BulkUser> bulkMapping = resolver.resolve(BulkUser.class);

        // Then
        assertThat(jpaMapping.getTableName()).isEqualTo("users");
        assertThat(bulkMapping.getTableName()).isEqualTo("users");
    }

    @Test
    void shouldThrowForUnmappedClass() {
        // Given
        EntityMapperResolver resolver = EntityMapperResolver.getInstance();

        // When/Then
        assertThatThrownBy(() -> resolver.resolve(SimpleUser.class))
            .isInstanceOf(MappingException.class)
            .hasMessageContaining("No mapping found");
    }

    @Test
    void shouldExtractValuesCorrectly() {
        // Given
        TableMapping<SimpleUser> mapping = TableMapping.<SimpleUser>builder("users")
            .id("id", SimpleUser::getId)
            .column("name", SimpleUser::getName)
            .column("email", SimpleUser::getEmail)
            .build();

        SimpleUser user = new SimpleUser(42L, "Test", "test@example.com");

        // When
        ColumnMapping<SimpleUser, ?> idColumn = mapping.getColumn("id");
        ColumnMapping<SimpleUser, ?> nameColumn = mapping.getColumn("name");

        // Then
        assertThat(idColumn.extractValue(user)).isEqualTo(42L);
        assertThat(nameColumn.extractValue(user)).isEqualTo("Test");
    }

    @Test
    void shouldPreventDuplicateColumns() {
        // When/Then
        assertThatThrownBy(() ->
            TableMapping.<SimpleUser>builder("users")
                .id("id", SimpleUser::getId)
                .column("id", SimpleUser::getName) // Duplicate!
                .build()
        ).isInstanceOf(MappingException.class)
         .hasMessageContaining("Duplicate column");
    }

    @Test
    void shouldRequireAtLeastOneColumn() {
        // When/Then
        assertThatThrownBy(() ->
            TableMapping.<SimpleUser>builder("users")
                .build()
        ).isInstanceOf(MappingException.class)
         .hasMessageContaining("At least one column");
    }

    @Test
    void shouldConvertCamelCaseToSnakeCase() {
        // Given
        JpaEntityMapper<JpaUser> mapper = JpaEntityMapper.getInstance();

        // When
        TableMapping<JpaUser> mapping = mapper.map(JpaUser.class);

        // Then - createdAt field should become created_at column
        assertThat(mapping.getColumnNames()).contains("created_at");
    }

    @Test
    void shouldSupportSchemaName() {
        // When
        TableMapping<SimpleUser> mapping = TableMapping.<SimpleUser>builder("users")
            .schema("public")
            .id("id", SimpleUser::getId)
            .build();

        // Then
        assertThat(mapping.getSchemaName()).isEqualTo("public");
        assertThat(mapping.getFullTableName()).isEqualTo("public.users");
    }
}
