# 🏗️ System Design Document (SDD)

## 1. Executive Summary
- **Project Name:** Real-time E-Commerce Market & Price Monitoring Platform
- **Domain:** Kitchen Appliances on Tiki.vn (expandable to all categories)
- **Primary Users:** Category Manager, Pricing Strategy Analyst, Brand Relationship Lead
- **Architecture Style:** Kappa + Lambda hybrid (Event-driven streaming via Kafka + Medallion Lakehouse via Spark & Delta Lake + DWH serving for BI)

---

## 2. Business Objectives & SLA
1. **Flash Sale & Price Drop Alert:** Detect price drops > 30% within 10 seconds of occurrence.
2. **Daily Category Market Pulse:** Refresh market share, CR5 index, and assortment gap metrics by 06:00 AM daily.
3. **Voice of Customer (VoC) Tracking:** Automatically detect surge in negative review keywords (`trả hàng`, `hàng giả`, `lỗi`).
4. **Data Integrity:** Zero silent schema violations; fail-fast validation before promoting data to Gold layer.

---

## 3. Architecture Overview

```
 [Tiki APIs / Simulator]
          │
          ▼
   [Apache Kafka] (Topics: raw-products, raw-price-events, raw-reviews)
     │          │
     │ (Stream) │ (Batch Dump)
     ▼          ▼
   [PySpark Streaming & Batch] ──► [MinIO / Delta Lake]
                                     ├─ Bronze (Raw event logs)
                                     ├─ Silver (Cleaned, deduplicated, enriched)
                                     └─ Gold (Star Schema dimensional model)
                                              │
                                              ▼
                                     [PostgreSQL DWH]
                                              │
                                      ┌───────┴───────┐
                                      ▼               ▼
                                [Power BI]      [Telegram Bot]
```

---

## 4. Key Design Decisions & Trade-Offs

| Decision | Alternative Considered | Justification |
|:---|:---|:---|
| **Kafka (KRaft mode)** | Kafka + Zookeeper / RabbitMQ | KRaft eliminates Zookeeper overhead, saving ~500MB RAM locally. Kafka provides replayability and offset management critical for data recovery. |
| **Delta Lake over Parquet** | Raw Parquet / CSV | Delta Lake enables ACID transactions, `MERGE INTO` (Upsert for SCD Type 2), and Time Travel audit. |
| **LocalExecutor for Airflow** | CeleryExecutor / DockerOperator | Lightweight for local single-node development, zero Redis dependency required. |
| **MinIO** | AWS S3 directly | 100% free, runs offline, zero AWS billing risk, S3-API compatible. |
| **PostgreSQL for Serving** | Snowflake / ClickHouse | Lightweight, native integration with Power BI via DirectQuery / Import mode. |

---

## 5. Port Allocation Reference

- **Kafka UI:** `http://localhost:8080`
- **Spark Master UI:** `http://localhost:8081`
- **Airflow Web UI:** `http://localhost:8085` (admin/admin)
- **MinIO Console:** `http://localhost:9001` (admin/admin12345)
- **MinIO S3 API:** `http://localhost:9000`
- **PostgreSQL DWH:** `localhost:5432` (db: `tiki_dwh`, user: `postgres`, pw: `postgres`)

