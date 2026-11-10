# 🛒 Real-time E-Commerce Market & Price Monitoring Platform

> An end-to-end Data Engineering & Analytics Platform for monitoring pricing, seller competition, category assortments, and customer sentiment on Tiki.vn. Built with Kafka, Apache Spark, Delta Lake, Apache Airflow, PostgreSQL, and Power BI.

[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-black.svg?logo=apachekafka)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C.svg?logo=apachespark)](https://spark.apache.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE.svg?logo=apacheairflow)](https://airflow.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID-00ADD8.svg)](https://delta.io/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811.svg?logo=powerbi)](https://powerbi.microsoft.com/)

---

## 🏛️ Architecture Overview

```
                      ┌──────────────────────┐
                      │   Tiki API Crawler   │
                      └──────────┬───────────┘
                                 │
                                 ▼
                     ┌───────────────────────┐
                     │     Apache Kafka      │
                     │  (KRaft Event Broker) │
                     └──────────┬────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
         [PySpark Streaming]       [Airflow Batch DAGs]
                    │                       │
                    └───────────┬───────────┘
                                ▼
                     ┌─────────────────────┐
                     │  MinIO / Delta Lake │
                     │  (Bronze/Silver/Gold)│
                     └──────────┬──────────┘
                                │
                                ▼
                     ┌─────────────────────┐
                     │   PostgreSQL DWH    │
                     └──────────┬──────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
            [Power BI Dashboard]     [Telegram Alert Bot]
```

---

## ⚡ Quick Start

### 1. Prerequisites
- Docker & Docker Desktop (enabled WSL2 backend on Windows)
- Python 3.11+ (Python 3.12 recommended)

### 2. Environment Setup
```bash
# Clone and navigate
cd E:\baitap\Project1\tiki-market-platform

# Create virtual environment
python -m venv .venv
.venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt

# Copy environment config
cp .env.example .env
```

### 3. Launch Docker Infrastructure
```bash
docker compose up -d
```

### 4. Access UIs
| Service | URL | Default Credentials |
|:---|:---|:---|
| **Kafka UI** | `http://localhost:8080` | None |
| **Spark Master UI** | `http://localhost:8081` | None |
| **Airflow Web UI** | `http://localhost:8085` | `admin` / `admin` |
| **MinIO Console** | `http://localhost:9001` | `admin` / `admin12345` |
| **PostgreSQL DWH** | `localhost:5432` | `postgres` / `postgres` (db: `tiki_dwh`) |

---

## 📁 Repository Structure

```
├── .env.example
├── .gitignore
├── docker-compose.yml
├── requirements.txt
├── README.md
├── docs/
│   ├── system_design.md
│   └── data_model.md
├── data_collection/
│   ├── crawl_product_ids.py
│   ├── crawl_product_detail.py
│   ├── crawl_reviews.py
│   └── simulate_price_events.py
├── kafka/
│   ├── producers/
│   ├── consumers/
│   └── schemas/
├── spark/
│   ├── streaming/
│   └── batch/
├── dags/
│   ├── dag_daily_crawl.py
│   ├── dag_etl_pipeline.py
│   └── dag_data_quality.py
├── data_warehouse/
│   └── sql/
│       └── init.sql
└── tests/
```
