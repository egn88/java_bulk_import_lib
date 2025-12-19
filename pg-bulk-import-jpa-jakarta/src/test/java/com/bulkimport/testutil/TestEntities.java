package com.bulkimport.testutil;

import com.bulkimport.mapping.annotation.BulkColumn;
import com.bulkimport.mapping.annotation.BulkId;
import com.bulkimport.mapping.annotation.BulkTable;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Test entities for integration tests.
 */
public class TestEntities {

    /**
     * JPA-annotated User entity.
     */
    @Entity
    @Table(name = "users")
    public static class JpaUser {
        @Id
        private Long id;

        @Column(name = "name", nullable = false)
        private String name;

        @Column(name = "email")
        private String email;

        @Column(name = "age")
        private Integer age;

        @Column(name = "active")
        private Boolean active;

        @Column(name = "created_at")
        private LocalDateTime createdAt;

        public JpaUser() {
        }

        public JpaUser(Long id, String name, String email, Integer age, Boolean active) {
            this.id = id;
            this.name = name;
            this.email = email;
            this.age = age;
            this.active = active;
            this.createdAt = LocalDateTime.now();
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public Boolean getActive() {
            return active;
        }

        public void setActive(Boolean active) {
            this.active = active;
        }

        public LocalDateTime getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(LocalDateTime createdAt) {
            this.createdAt = createdAt;
        }
    }

    /**
     * Custom annotation-based User entity.
     */
    @BulkTable(name = "users")
    public static class BulkUser {
        @BulkId
        private Long id;

        @BulkColumn(nullable = false)
        private String name;

        @BulkColumn
        private String email;

        @BulkColumn
        private Integer age;

        @BulkColumn
        private Boolean active;

        @BulkColumn(name = "created_at")
        private LocalDateTime createdAt;

        public BulkUser() {
        }

        public BulkUser(Long id, String name, String email, Integer age, Boolean active) {
            this.id = id;
            this.name = name;
            this.email = email;
            this.age = age;
            this.active = active;
            this.createdAt = LocalDateTime.now();
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public Boolean getActive() {
            return active;
        }

        public void setActive(Boolean active) {
            this.active = active;
        }

        public LocalDateTime getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(LocalDateTime createdAt) {
            this.createdAt = createdAt;
        }
    }

    /**
     * Product entity with UUID, arrays, and binary data.
     */
    @BulkTable(name = "products")
    public static class Product {
        @BulkId
        private UUID id;

        @BulkColumn(nullable = false)
        private String name;

        @BulkColumn
        private BigDecimal price;

        @BulkColumn
        private List<String> tags;

        @BulkColumn
        private byte[] data;

        public Product() {
        }

        public Product(UUID id, String name, BigDecimal price, List<String> tags, byte[] data) {
            this.id = id;
            this.name = name;
            this.price = price;
            this.tags = tags;
            this.data = data;
        }

        public UUID getId() {
            return id;
        }

        public void setId(UUID id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public BigDecimal getPrice() {
            return price;
        }

        public void setPrice(BigDecimal price) {
            this.price = price;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }
    }

    /**
     * Simple POJO without annotations for fluent mapping tests.
     */
    public static class SimpleUser {
        private Long id;
        private String name;
        private String email;

        public SimpleUser(Long id, String name, String email) {
            this.id = id;
            this.name = name;
            this.email = email;
        }

        public Long getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getEmail() {
            return email;
        }
    }
}
