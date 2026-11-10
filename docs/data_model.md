# 📊 Data Model & Schema Specification

## 1. Dimensional Model (Star Schema) - Gold Layer

```mermaid
erDiagram
    dim_product ||--o{ fact_product_snapshot : "tracks"
    dim_brand ||--o{ fact_product_snapshot : "categorizes"
    dim_seller ||--o{ fact_product_snapshot : "sold_by"
    dim_subcategory ||--o{ fact_product_snapshot : "belongs_to"
    dim_date ||--o{ fact_product_snapshot : "snapshot_date"

    dim_product ||--o{ fact_reviews : "reviewed"
    dim_date ||--o{ fact_reviews : "review_date"

    dim_product {
        bigint product_sk PK
        bigint product_id
        string product_name
        string brand_id FK
        string seller_id FK
        string subcategory_id FK
        decimal original_price
        timestamp valid_from
        timestamp valid_to
        boolean is_current
    }

    dim_brand {
        string brand_id PK
        string brand_name
        string country_origin
    }

    dim_seller {
        string seller_id PK
        string seller_name
        boolean is_official_store
        decimal seller_rating
    }

    dim_subcategory {
        string subcategory_id PK
        string subcategory_name
        string parent_category
    }

    dim_date {
        int date_key PK
        date full_date
        int day_of_week
        int month
        int quarter
        int year
        boolean is_weekend
    }

    fact_product_snapshot {
        bigint snapshot_id PK
        int date_key FK
        bigint product_sk FK
        decimal current_price
        decimal discount_rate
        int all_time_quantity_sold
        decimal current_rating
        int review_count
        boolean in_stock
    }

    fact_reviews {
        bigint review_id PK
        bigint product_sk FK
        int date_key FK
        int rating
        string comment_text_cleaned
        string sentiment_label
        boolean contains_defect_keywords
    }
```

---

## 2. Medallion Layer Schema Progression

### 🥉 Bronze Layer (Raw Streaming/Batch Dump)
- Format: Delta Lake / Raw JSON strings
- Partition: `event_date` (`YYYY-MM-DD`)
- Attributes: Unmodified payload from Tiki API + ingestion metadata (`_ingested_at`, `_source_topic`, `_offset`).

### 🥈 Silver Layer (Cleaned & Conformed)
- Format: Delta Lake
- Transformations:
  - Deduplication via `(product_id, crawl_date)`
  - Data hygiene: trim strings, normalize brand names, remove HTML tags from reviews
  - Anonymization: SHA256 hashed customer identifiers
  - Calculated columns: `discount_rate_calc = (original_price - price) / original_price`
  - Validation: flag anomalies (`is_price_anomaly`, `is_rating_invalid`)

### 🥇 Gold Layer (Star Schema & Business Aggregations)
- Storage: Delta Lake + Synced to PostgreSQL `dwh` & `analytics` schemas
- Optimized for BI reporting and sub-second Power BI querying.
