package com.bulkimport.testutil;

import com.bulkimport.mapping.annotation.BulkColumn;
import com.bulkimport.mapping.annotation.BulkId;
import com.bulkimport.mapping.annotation.BulkTable;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Test entity for stress testing with 24 columns.
 */
@BulkTable(name = "stress_test_products")
public class StressTestProduct {

    @BulkId
    private String code;

    @BulkColumn
    private String name;

    @BulkColumn
    private String sku;

    @BulkColumn
    private String category;

    @BulkColumn
    private String subcategory;

    @BulkColumn
    private String brand;

    @BulkColumn
    private String supplier;

    @BulkColumn
    private String status;

    @BulkColumn
    private String description;

    @BulkColumn
    private String notes;

    @BulkColumn
    private String specifications;

    @BulkColumn
    private BigDecimal price;

    @BulkColumn
    private Integer quantity;

    @BulkColumn
    private BigDecimal weight;

    @BulkColumn
    private BigDecimal rating;

    @BulkColumn(name = "view_count")
    private Long viewCount;

    @BulkColumn(name = "created_at")
    private LocalDateTime createdAt;

    @BulkColumn(name = "updated_at")
    private LocalDateTime updatedAt;

    @BulkColumn(name = "last_order_date")
    private LocalDate lastOrderDate;

    @BulkColumn(name = "expiration_date")
    private LocalDate expirationDate;

    @BulkColumn
    private Boolean active;

    @BulkColumn
    private Boolean featured;

    @BulkColumn(name = "in_stock")
    private Boolean inStock;

    @BulkColumn
    private Boolean taxable;

    // Getters and Setters
    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getSku() { return sku; }
    public void setSku(String sku) { this.sku = sku; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getSubcategory() { return subcategory; }
    public void setSubcategory(String subcategory) { this.subcategory = subcategory; }

    public String getBrand() { return brand; }
    public void setBrand(String brand) { this.brand = brand; }

    public String getSupplier() { return supplier; }
    public void setSupplier(String supplier) { this.supplier = supplier; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getNotes() { return notes; }
    public void setNotes(String notes) { this.notes = notes; }

    public String getSpecifications() { return specifications; }
    public void setSpecifications(String specifications) { this.specifications = specifications; }

    public BigDecimal getPrice() { return price; }
    public void setPrice(BigDecimal price) { this.price = price; }

    public Integer getQuantity() { return quantity; }
    public void setQuantity(Integer quantity) { this.quantity = quantity; }

    public BigDecimal getWeight() { return weight; }
    public void setWeight(BigDecimal weight) { this.weight = weight; }

    public BigDecimal getRating() { return rating; }
    public void setRating(BigDecimal rating) { this.rating = rating; }

    public Long getViewCount() { return viewCount; }
    public void setViewCount(Long viewCount) { this.viewCount = viewCount; }

    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }

    public LocalDate getLastOrderDate() { return lastOrderDate; }
    public void setLastOrderDate(LocalDate lastOrderDate) { this.lastOrderDate = lastOrderDate; }

    public LocalDate getExpirationDate() { return expirationDate; }
    public void setExpirationDate(LocalDate expirationDate) { this.expirationDate = expirationDate; }

    public Boolean getActive() { return active; }
    public void setActive(Boolean active) { this.active = active; }

    public Boolean getFeatured() { return featured; }
    public void setFeatured(Boolean featured) { this.featured = featured; }

    public Boolean getInStock() { return inStock; }
    public void setInStock(Boolean inStock) { this.inStock = inStock; }

    public Boolean getTaxable() { return taxable; }
    public void setTaxable(Boolean taxable) { this.taxable = taxable; }
}
