-- ============================================================
-- FILE: 01_schema_setup.sql
-- PROJECT: E-Commerce Sales Analytics
-- AUTHOR: Subha Ragupathi
-- DESCRIPTION: Create database schema and load raw data
-- ============================================================

-- Create database (run once)
CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE ecommerce_db;

-- ============================================================
-- DROP EXISTING TABLES (for re-runs)
-- ============================================================
DROP TABLE IF EXISTS fact_orders;
DROP TABLE IF EXISTS dim_customers;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_date;

-- ============================================================
-- DIMENSION TABLE: dim_date
-- ============================================================
CREATE TABLE dim_date (
    date_key        DATE         PRIMARY KEY,
    year            SMALLINT     NOT NULL,
    quarter         TINYINT      NOT NULL,
    month           TINYINT      NOT NULL,
    month_name      VARCHAR(10)  NOT NULL,
    week_number     TINYINT      NOT NULL,
    day_of_week     VARCHAR(10)  NOT NULL,
    is_weekend      BOOLEAN      NOT NULL,
    is_q4           BOOLEAN      NOT NULL  -- Flag for holiday season
);

-- ============================================================
-- DIMENSION TABLE: dim_customers
-- ============================================================
CREATE TABLE dim_customers (
    customer_id         VARCHAR(10)  PRIMARY KEY,
    customer_segment    VARCHAR(20)  NOT NULL,
    region              VARCHAR(20)  NOT NULL,
    CONSTRAINT chk_segment CHECK (customer_segment IN ('New','Returning','Premium','VIP'))
);

-- ============================================================
-- DIMENSION TABLE: dim_products
-- ============================================================
CREATE TABLE dim_products (
    product_key     INT          AUTO_INCREMENT PRIMARY KEY,
    category        VARCHAR(50)  NOT NULL,
    product_name    VARCHAR(100) NOT NULL,
    UNIQUE KEY uk_product (category, product_name)
);

-- ============================================================
-- FACT TABLE: fact_orders
-- ============================================================
CREATE TABLE fact_orders (
    order_id        VARCHAR(10)     PRIMARY KEY,
    order_date      DATE            NOT NULL,
    customer_id     VARCHAR(10)     NOT NULL,
    product_key     INT             NOT NULL,
    sales_channel   VARCHAR(20)     NOT NULL,
    payment_method  VARCHAR(20)     NOT NULL,
    order_status    VARCHAR(20)     NOT NULL,
    unit_price      DECIMAL(10,2)   NOT NULL,
    quantity        TINYINT         NOT NULL,
    discount_pct    TINYINT         NOT NULL DEFAULT 0,
    discount_amount DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    revenue         DECIMAL(10,2)   NOT NULL,
    shipping_cost   DECIMAL(8,2)    NOT NULL DEFAULT 0.00,
    total_amount    DECIMAL(10,2)   NOT NULL,
    customer_rating DECIMAL(3,1),
    FOREIGN KEY (customer_id)  REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_key)  REFERENCES dim_products(product_key),
    FOREIGN KEY (order_date)   REFERENCES dim_date(date_key),
    CONSTRAINT chk_status      CHECK (order_status IN ('Delivered','Returned','Cancelled','Pending')),
    CONSTRAINT chk_rating      CHECK (customer_rating BETWEEN 1.0 AND 5.0),
    CONSTRAINT chk_quantity    CHECK (quantity > 0),
    CONSTRAINT chk_discount    CHECK (discount_pct BETWEEN 0 AND 100)
);

-- ============================================================
-- INDEXES for query performance
-- ============================================================
CREATE INDEX idx_order_date       ON fact_orders (order_date);
CREATE INDEX idx_order_status     ON fact_orders (order_status);
CREATE INDEX idx_sales_channel    ON fact_orders (sales_channel);
CREATE INDEX idx_customer_segment ON dim_customers (customer_segment, region);

-- ============================================================
-- LOAD DATA FROM CSV (adjust path as needed)
-- ============================================================
-- NOTE: Run from MySQL with LOCAL INFILE enabled
-- Or use Python/ETL pipeline (see azure/data_pipeline.py)

-- LOAD DATA LOCAL INFILE '../data/processed/ecommerce_sales_cleaned.csv'
-- INTO TABLE staging_orders
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS;

SELECT 'Schema created successfully ✅' AS status;
