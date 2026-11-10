# 🛒 Real-time E-Commerce Market & Price Monitoring Platform

> An enterprise-grade, end-to-end Data Engineering & Analytics Platform for monitoring real-time pricing fluctuations, seller competition, category assortment gaps, and Vietnamese customer sentiment on Tiki.vn. Built with Apache Kafka, Apache Spark, Delta Lake, Apache Airflow, PostgreSQL, and Power BI.

[![CI Pipeline](https://github.com/TrungPham233/tiki-market-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/TrungPham233/tiki-market-platform/actions)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20.svg?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C.svg?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID-00ADD8.svg)](https://delta.io/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE.svg?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811.svg?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

---

## 🏛️ System Architecture

```
                  ┌──────────────────────────────────────────────┐
                  │          TIKI.VN INTERNAL APIS               │
                  │  (Listings, Product Detail, Reviews API)     │
                  └──────────────────────┬───────────────────────┘
                                         │
                    ┌────────────────────┴───────────────────┐
                    ▼                                        ▼
      [Recursive Price Splitting Crawler]       [Real-time Price Simulator]
                    │                                        │
                    └────────────────────┬───────────────────┘
                                         ▼
                    ┌────────────────────────────────────────┐
                    │       APACHE KAFKA (KRaft Broker)      │
                    │  Topics: raw-products, raw-reviews,    │
                    │          raw-price-events              │
                    └────────────────────┬───────────────────┘
                                         │
           ┌─────────────────────────────┴────────────────────────────┐
           ▼                                                          ▼
  [Spark Structured Streaming]                                [Airflow Batch DAGs]
  - Real-time Price Drop Alerts                               - Bronze: Raw Ingestion
  - Flash Sale Detection (>30%)                               - Silver: Cleaning & NLP
           │                                                  - Quality Gate (Circuit Breaker)
           │                                                  - Gold: Star Schema & Aggs
           │                                                          │
           └─────────────────────────────┬────────────────────────────┘
                                         ▼
                    ┌────────────────────────────────────────┐
                    │    MEDALLION LAKEHOUSE (Delta Lake)    │
                    │  🥉 Bronze: Partitioned Raw JSONL       │
                    │  🥈 Silver: Cleaned, Enriched, NLP     │
                    │  🥇 Gold: Star Schema (SCD Type 2)     │
                    └────────────────────┬───────────────────┘
                                         │
                                         ▼
                    ┌────────────────────────────────────────┐
                    │    POSTGRESQL DATA WAREHOUSE (DWH)     │
                    │  Schema: dwh (Dimensions & Facts)      │
                    │  Schema: analytics (BI Serving Views)  │
                    └────────────────────┬───────────────────┘
                                         │
                    ┌────────────────────┴───────────────────┐
                    ▼                                        ▼
          [Power BI Dashboard]                      [Real-time Alerts]
          (7 Executive Analytical Pages)            (Telegram / Webhook)
```

---

## 💡 Key Engineering Highlights

1. **Recursive Price-Splitting Algorithm:** Solves Tiki's hard limit of ~2,000 products/query by dynamically bisecting price intervals until each subset is safely under the limit, guaranteeing 100% catalog coverage.
2. **Schema Validation & Resilient Ingestion:** Kafka producers validate every message against rigorous JSON Schemas with fail-fast validation and at-least-once delivery (`acks="all"`).
3. **Medallion Lakehouse Architecture:**
   - **Bronze:** Raw data audit trails partitioned by `ingest_date`.
   - **Silver:** Brand normalization, discount rate recalculation, and Vietnamese NLP sentiment analysis (`underthesea`).
   - **Gold:** Kimball Star Schema with **SCD Type 2** for product history tracking.
4. **Automated Data Quality Circuit Breaker:** 12 automated integrity assertions. Halts the Airflow pipeline before Gold promotion if duplicate keys, invalid ratings, or excessive anomalies are detected.
5. **Streaming Flash Sale Detection:** Windowed price comparison stream processor flagging instant price cuts $\ge 30\%$.

---

## 📊 Business Intelligence & Key Insights

| Business Question | Finding | Strategic Action |
|:---|:---|:---|
| **BQ1: Discount Saturation** | Promotions reach peak effectiveness at **30%–50%**. Disounts $>50\%$ exhibit diminishing returns and lower ratings. | Cap baseline promotional discounts at 50%; reallocate budget to gift bundles and free shipping. |
| **BQ2: Market Share & CR5** | Market concentration is massive: **CR5 = 98.55%** of revenue, led by Elmich, Philips, Lock&Lock, and Tiki Trading. | Leverage brand dominance to negotiate exclusive SKU product launches. |
| **BQ3: Quality Risks** | Flagged SKUs with high review volume but low rating ($< 4.5\bigstar$) represent major reputational hazards. | Implement immediate vendor quality audit and pause paid ad spend for flagged items. |
| **BQ4: Assortment Gaps** | Mainstream segment (500k – 1.5M VND) lacks product variety in specialty kitchen appliances. | Priority onboarding campaign for mid-tier appliance manufacturers. |
| **BQ6: Voice of Customer** | **50% of 1-2 star complaints stem from after-sales service** (returns, refund delays, CSKH), exceeding actual hardware breakdown rates. | Re-engineer return/refund logistics SLA from 7 days down to 48 hours. |

---

## ⚡ Quick Start

### 1. Prerequisites
- Docker & Docker Desktop (optional for local run)
- Python 3.12+
- Git

### 2. Setup Environment
```bash
# Clone repository
git clone https://github.com/TrungPham233/tiki-market-platform.git
cd tiki-market-platform

# Install dependencies
pip install -r requirements.txt

# Copy environment configuration
copy .env.example .env
```

### 3. Run Pipeline via Windows Task Runner
```cmd
# Run entire test suite (25 tests)
run.bat test

# Run Medallion Pipeline (Bronze -> Silver -> Gold)
run.bat medallion

# Sync Gold Lakehouse into Data Warehouse
run.bat dwh

# Run full end-to-end flow
run.bat all
```

### 4. Docker Infrastructure (Optional Full Stack)
```bash
# Start Kafka, MinIO, Spark Master/Worker, PostgreSQL, Airflow
docker compose up -d

# Access Web UIs:
# - Kafka UI: http://localhost:8080
# - Spark Master: http://localhost:8081
# - Airflow: http://localhost:8085 (admin/admin)
# - MinIO Console: http://localhost:9001 (admin/admin12345)
# - PostgreSQL: localhost:5432 (db: tiki_dwh)
```

---

## 🧪 Comprehensive Unit Testing (25/25 PASSED)

```bash
$ python -m pytest tests/ -v
============================= test session starts =============================
tests/test_airflow_and_quality.py::test_check_not_empty PASSED           [  4%]
tests/test_airflow_and_quality.py::test_check_not_null PASSED            [  8%]
tests/test_airflow_and_quality.py::test_check_unique PASSED              [ 12%]
tests/test_airflow_and_quality.py::test_check_numeric_range PASSED       [ 16%]
tests/test_airflow_and_quality.py::test_check_anomaly_threshold PASSED   [ 20%]
tests/test_airflow_and_quality.py::test_silver_quality_gate_passes_clean_data PASSED [ 24%]
tests/test_airflow_and_quality.py::test_silver_quality_gate_fails_corrupted_data PASSED [ 28%]
tests/test_data_collection.py::test_hash_identifier PASSED               [ 32%]
tests/test_data_collection.py::test_parse_product_record PASSED          [ 36%]
tests/test_data_collection.py::test_parse_deleted_product PASSED         [ 40%]
tests/test_data_collection.py::test_parse_review_record PASSED           [ 44%]
tests/test_data_collection.py::test_generate_price_event PASSED          [ 48%]
tests/test_kafka.py::test_product_schema_validation_valid PASSED         [ 52%]
tests/test_kafka.py::test_product_schema_validation_missing_required PASSED [ 56%]
tests/test_kafka.py::test_product_schema_validation_negative_price PASSED [ 60%]
tests/test_kafka.py::test_price_event_schema_validation PASSED           [ 64%]
tests/test_kafka.py::test_review_schema_validation PASSED                [ 68%]
tests/test_kafka.py::test_producer_dry_run_send PASSED                   [ 72%]
tests/test_medallion_spark.py::test_normalize_brand PASSED               [ 76%]
tests/test_medallion_spark.py::test_clean_html_reviews PASSED            [ 80%]
tests/test_medallion_spark.py::test_sentiment_labeling PASSED            [ 84%]
tests/test_medallion_spark.py::test_voc_complaint_categorization PASSED  [ 88%]
tests/test_medallion_spark.py::test_evaluate_price_alert_flash_sale PASSED [ 92%]
tests/test_medallion_spark.py::test_evaluate_price_alert_normal_no_trigger PASSED [ 96%]
tests/test_serving_layer.py::test_sqlite_dwh_sync_and_views PASSED       [100%]
============================= 25 passed in 2.58s ==============================
```

---

## 💼 How to Showcase this Project on Your CV / Resume

Copy and adapt this section directly into your resume:

### **Real-time E-Commerce Market & Price Monitoring Platform**
*Technologies: Python, Apache Kafka, Apache Spark (PySpark), Delta Lake, Apache Airflow, PostgreSQL, Docker, Power BI*
- Architected an end-to-end **Medallion Lakehouse platform (Bronze/Silver/Gold)** ingesting thousands of e-commerce products and customer reviews from Tiki with **Delta Lake** ACID storage and **SCD Type 2** dimension tracking.
- Engineered a streaming pipeline utilizing **Apache Kafka (KRaft mode)** and **Spark Structured Streaming** to detect steep price drops ($\ge 30\%$) and flash sales in near-real-time.
- Designed an automated **Data Quality Circuit Breaker** with 12 validation assertions integrated into **Apache Airflow DAGs**, halting pipelines on primary key collisions or rating corruptions to safeguard BI reports.
- Implemented Vietnamese Natural Language Processing (NLP) with **`underthesea`**, uncovering that over 50% of 1-star reviews stemmed from after-sales return logistics rather than hardware failures.
- Modeled a Star Schema in **PostgreSQL Data Warehouse** and designed a 7-page **Power BI Dashboard** leveraging advanced DAX (`TREATAS`, `CR5 Index`) delivering strategic category management insights.

---

## 👤 Author
- **GitHub:** [@TrungPham233](https://github.com/TrungPham233)
- **Email:** 23521685@gm.uit.edu.vn
