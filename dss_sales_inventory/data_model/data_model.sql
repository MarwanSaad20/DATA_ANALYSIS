-- data_model/data_model.sql
-- Version: 1.2 (Final)
-- Description: Star Schema DDL for DSS Week 10
-- Standards: Strict Star Schema, User-Managed Keys, Grain Protection
-- Dialect: SQLite

-- Enable Foreign Keys (Critical for SQLite)
PRAGMA foreign_keys = ON;

-- Clean up existing objects in correct order (Fact first, then Dims)
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_region;

-- 1. Dimension: Region
-- Logic: Static dimension for this project scope.
CREATE TABLE dim_region (
    region_id INTEGER PRIMARY KEY,  -- Managed by Python (No AUTOINCREMENT)
    region_name TEXT NOT NULL
);

-- 2. Dimension: Date
-- Logic: Time dimension with surrogate key.
CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY,    -- Managed by Python (No AUTOINCREMENT)
    full_date TEXT NOT NULL UNIQUE, -- Format: YYYY-MM-DD
    day INTEGER,
    month INTEGER,
    quarter INTEGER,
    year INTEGER
);

-- 3. Dimension: Product
-- Logic: Type 1 SCD. product_id serves as the SK for this implementation.
CREATE TABLE dim_product (
    product_id INTEGER PRIMARY KEY, -- Managed by Python (Business Key used as SK)
    unit_price REAL,
    unit_cost REAL
);

-- 4. Fact: Sales
-- Analytical Grain: One row per Product per Date per Region
CREATE TABLE fact_sales (
    -- Technical Surrogate Key (Row Identifier Only)
    -- NOT part of the analytical grain.
    sales_id INTEGER PRIMARY KEY,   
    
    -- Analytical Grain Components
    product_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    region_id INTEGER NOT NULL,
    
    -- Measures
    quantity INTEGER,
    revenue REAL,
    cost REAL,
    
    -- Explicit Referential Integrity
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (region_id) REFERENCES dim_region(region_id),
    
    -- GRAIN PROTECTION: Ensure uniqueness at the analytical level
    UNIQUE(product_id, date_id, region_id)
);

-- Performance Indexes (Foreign Keys)
CREATE INDEX idx_fact_product ON fact_sales(product_id);
CREATE INDEX idx_fact_date ON fact_sales(date_id);
CREATE INDEX idx_fact_region ON fact_sales(region_id);