-- ====================================================================
-- PostgreSQL Production DDL for E-Commerce Data Warehouse (Star Schema)
-- ====================================================================

-- 1. Create Schemas
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS analytics;

-- 2. Dimension Tables
CREATE TABLE IF NOT EXISTS dwh.dim_brand (
    brand_id VARCHAR(100) PRIMARY KEY,
    brand_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_seller (
    seller_id VARCHAR(100) PRIMARY KEY,
    seller_name VARCHAR(255) NOT NULL,
    is_official_seller BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dwh.dim_subcategory (
    subcategory_id VARCHAR(100) PRIMARY KEY,
    subcategory_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_sk VARCHAR(100) PRIMARY KEY,
    product_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    brand_id VARCHAR(100) REFERENCES dwh.dim_brand(brand_id),
    seller_id VARCHAR(100) REFERENCES dwh.dim_seller(seller_id),
    subcategory_id VARCHAR(100) REFERENCES dwh.dim_subcategory(subcategory_id),
    original_price NUMERIC(15, 2) DEFAULT 0,
    origin VARCHAR(100),
    warranty_type VARCHAR(100),
    power_wattage VARCHAR(100),
    capacity VARCHAR(100),
    valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
    valid_to TIMESTAMP WITH TIME ZONE NOT NULL,
    is_current BOOLEAN DEFAULT TRUE
);

-- 3. Fact Tables
CREATE TABLE IF NOT EXISTS dwh.fact_product_snapshot (
    snapshot_id VARCHAR(100) PRIMARY KEY,
    date_key INT REFERENCES dwh.dim_date(date_key),
    product_sk VARCHAR(100) REFERENCES dwh.dim_product(product_sk),
    product_id BIGINT NOT NULL,
    brand_id VARCHAR(100) REFERENCES dwh.dim_brand(brand_id),
    seller_id VARCHAR(100) REFERENCES dwh.dim_seller(seller_id),
    subcategory_id VARCHAR(100) REFERENCES dwh.dim_subcategory(subcategory_id),
    price NUMERIC(15, 2) NOT NULL,
    original_price NUMERIC(15, 2) NOT NULL,
    discount_rate_calc NUMERIC(5, 2) DEFAULT 0,
    quantity_sold INT DEFAULT 0,
    rating_average NUMERIC(3, 2) DEFAULT 0,
    review_count INT DEFAULT 0,
    price_segment VARCHAR(50),
    is_discount_discrepancy BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dwh.fact_reviews (
    review_id BIGINT PRIMARY KEY,
    product_sk VARCHAR(100) REFERENCES dwh.dim_product(product_sk),
    product_id BIGINT NOT NULL,
    date_key INT REFERENCES dwh.dim_date(date_key),
    customer_hash VARCHAR(64) NOT NULL,
    rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    sentiment_label VARCHAR(20) NOT NULL,
    is_after_sales_complaint BOOLEAN DEFAULT FALSE,
    is_defect_complaint BOOLEAN DEFAULT FALSE,
    thank_count INT DEFAULT 0,
    tokenized_text TEXT
);

-- 4. Streaming Alerts Table
CREATE TABLE IF NOT EXISTS dwh.alerts (
    alert_id VARCHAR(100) PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,
    product_id BIGINT NOT NULL,
    product_name TEXT NOT NULL,
    brand_name VARCHAR(255),
    seller_name VARCHAR(255),
    old_price NUMERIC(15, 2) NOT NULL,
    new_price NUMERIC(15, 2) NOT NULL,
    price_drop_pct NUMERIC(5, 2) NOT NULL,
    savings_amount NUMERIC(15, 2) NOT NULL,
    triggered_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- 5. Indexes for fast Power BI DirectQuery
CREATE INDEX IF NOT EXISTS idx_fact_prod_date ON dwh.fact_product_snapshot(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_prod_brand ON dwh.fact_product_snapshot(brand_id);
CREATE INDEX IF NOT EXISTS idx_fact_prod_seller ON dwh.fact_product_snapshot(seller_id);
CREATE INDEX IF NOT EXISTS idx_fact_rev_prod ON dwh.fact_reviews(product_sk);
CREATE INDEX IF NOT EXISTS idx_fact_rev_sentiment ON dwh.fact_reviews(sentiment_label);
