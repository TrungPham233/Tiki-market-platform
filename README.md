# 🛒 Tiki Market & Price Monitoring Platform

> End-to-end data engineering project on real Tiki.vn data: a recursive crawler, a **PySpark + Delta Lake** Medallion lakehouse with a data-quality circuit breaker and an **SCD Type 2** dimension, a **Spark Structured Streaming** price-alert job, a PostgreSQL/SQLite serving layer and a Power BI report.

[![CI Pipeline](https://github.com/TrungPham233/tiki-market-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/TrungPham233/tiki-market-platform/actions)
[![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C.svg?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2-00ADD8.svg)](https://delta.io/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20.svg?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE.svg?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-PBIP-F2C811.svg?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

---

## 📏 Latest run (measured, not estimated)

| Metric | Value | Where it comes from |
|:---|---:|:---|
| Products crawled (Kitchen appliances, category 1882) | **2,000** | `data_collection/crawl_*.py` → `data/raw/` |
| Reviews crawled | **7,772** | up to 20 per product, 885 products with reviews |
| Brands / sellers / subcategories | **281 / 206 / 53** | Gold dimensions |
| Data quality gate | **12 / 12 passed** | `data/lakehouse/silver_quality_report.json` |
| Full pipeline runtime (local, `local[*]`) | **93 s** | `data/lakehouse/pipeline_run_summary.json` |
| Re-run on unchanged data | **0** new Bronze rows, **0** SCD2 changes | MERGE-based, idempotent |
| Price-alert stream latency (event → alert written) | **p50 2.3 s / p95 8.8 s** (78/78 alerts) | `python -m spark.streaming.measure_latency` (file source, 2 s micro-batches) |
| Automated tests | **36 passed** | `pytest tests/` (unit + PySpark/Delta integration) |

---

## 🏛️ Architecture

```
 Tiki public APIs (listings / product / reviews)
        │  recursive price-bisection crawler (data_collection/)
        ▼
 data/raw/*.jsonl ───────────────► BRONZE  (Delta, partitioned by ingest_date)
                                     raw_json + _record_hash + audit columns
                                     insert-only MERGE on _record_hash  → idempotent
                                        │  explicit schemas, native Spark SQL transforms
                                        ▼
                                   SILVER  (Delta)
                                     products: brand normalization, discount recompute,
                                               anomaly flags, latest crawl per product (MERGE)
                                     reviews : HTML cleanup, sentiment, complaint tags,
                                               Vietnamese tokenization (underthesea)
                                        │
                                        ▼
                             ┌── DATA QUALITY GATE (12 checks) ──┐  fail → DataQualityError,
                             └───────────────┬───────────────────┘  Gold is not touched
                                             ▼
                                   GOLD  (Delta, Kimball star schema)
                                     dim_product  : SCD Type 2 via one atomic MERGE
                                     dim_brand / dim_seller / dim_subcategory / dim_date
                                     fact_product_snapshot : product x day (daily price history)
                                     fact_reviews : point-in-time join to dim_product versions
                                     agg_* : business aggregates (BQ1–BQ6)
                                             │
 price events ─► Kafka topic / file dir ─► Spark Structured Streaming ─► gold/price_alerts
 (simulated from real products)             foreachBatch MERGE + checkpoint, latency_sec per alert
                                             │
                                             ▼
                                   SERVING  (Parquet export) ─► PostgreSQL / SQLite DWH ─► Power BI (PBIP)
```

Orchestration: `dags/dag_medallion_etl.py` (Airflow) or `spark/run_medallion_pipeline.py` (same tasks, one local SparkSession).

---

## 💡 Engineering highlights

1. **Recursive price-bisection crawler** – Tiki's listing API stops paginating at ~2,000 items per query. The crawler counts items per price range and splits the range in half until each range is under the limit, so a whole category can be collected (`crawl_product_ids.py`).
2. **Idempotent loads everywhere** – Bronze uses an insert-only MERGE on a SHA-256 of the raw payload; Silver upserts only when a newer crawl arrives; facts MERGE on their grain; the stream sink MERGEs on `alert_id`. Re-running any task does not duplicate data.
3. **SCD Type 2 with Delta MERGE** (`silver_to_gold_dimensions.py`) – a hash of the tracked attributes (name, list price, brand, seller, subcategory) is compared with the current row; changed products get their current version expired and a new version inserted in the same MERGE ("staged updates" pattern). Selling price, which changes daily, is kept in `fact_product_snapshot` instead of exploding the dimension.
4. **Point-in-time joins** – facts link to the `dim_product` version that was valid at snapshot / review time.
5. **Data quality circuit breaker** – 12 checks (not-empty, not-null, unique keys, value ranges, anomaly rate) on the Silver Delta tables; any critical failure raises and stops the DAG before Gold.
6. **Structured Streaming with measured latency** – Kafka or file source, `foreachBatch` MERGE into Delta with a checkpoint (restart-safe), and every alert stores `latency_sec`.
7. **No Python UDFs** – all transforms are native Spark SQL expressions; only underthesea tokenization runs on the driver over distinct review texts.

---

## 📊 Findings from the latest crawl

Numbers below come from the Gold aggregates of the run above. They describe **2,000 products (priced up to 1.3M VND, see limitations) listed in one Tiki category on one day**; "revenue" is `price × quantity_sold` as displayed by Tiki, so treat them as indicative.

| Question | Result (latest crawl) | Read it as |
|:---|:---|:---|
| **BQ2 Brand concentration** | Top 5 brands = **74.1%** of estimated revenue across **281** brands (Lock&Lock 27.8%, Sunhouse 20.9%, Philips 16.5%, Damas 5.9%, Kemei 3.1%) | A few brands dominate; the long tail is very long |
| **1P vs 3P** | **Tiki Trading** = **69.7%** of estimated revenue (206 sellers in total) | First-party selling dominates the category |
| **BQ1 Discount vs sales** | Median units sold: **2** at 0–10% discount (1,600 SKUs) vs **17** at 30–50% (222 SKUs) and **17** above 50% (60 SKUs). Average rating does not drop with deeper discounts (4.43 → 4.79) | Deep discounts go together with higher sales; no saturation above 50% is visible in this data (correlation, not causation) |
| **BQ3 Quality watchlist** | **67** SKUs with ≥ 5 reviews and rating < 4.5 | Candidates for a vendor quality check |
| **BQ4 Assortment gaps** | **59 of 159** subcategory × price-segment cells have no product | Where the category has no offer at a price point |
| **BQ6 Negative reviews** | Of **693** 1–2★ reviews, keyword rules tag **10.5%** as after-sales (returns/refunds/CS) and **13.9%** as product defects | Most negative reviews match neither keyword list, so the rules need a better classifier before drawing conclusions |

---

## ⚡ Quick start

```bash
pip install -r requirements.txt          # needs Java 17 for PySpark
run.bat crawl                            # crawl products + reviews, simulate price events
run.bat medallion                        # Bronze → Silver → Quality Gate → Gold → Stream → Serving
run.bat dwh                              # load serving layer into PostgreSQL (SQLite fallback)
run.bat dashboard                        # refresh Power BI sources
run.bat test                             # unit + Spark/Delta integration tests
python -m spark.streaming.measure_latency   # reproduce the streaming latency benchmark
```

Streaming from Kafka instead of files:

```bash
docker compose up -d kafka
python -m kafka_pipeline.producers.price_event_producer --count 200 --rate 5
python -m spark.streaming.price_alert_stream --source kafka --trigger continuous --interval 2
```

---

## 🧪 Tests

`tests/test_spark_delta.py` runs the real PySpark/Delta code on a local SparkSession:

| Test | What it proves |
|:---|:---|
| `test_bronze_ingestion_is_idempotent` | ingesting the same file twice adds 0 rows |
| `test_silver_products_keeps_latest_and_normalizes` | newest crawl wins, brand aliases, discount recompute, anomaly flag; Spark rule = Python reference |
| `test_scd2_creates_new_version_only_on_change` | changed list price → v1 expired, v2 current, `valid_to(v1) = valid_from(v2)`; re-run is a no-op |
| `test_structured_streaming_price_alerts` | correct alerts from a file stream; restart from checkpoint reprocesses nothing |
| `test_quality_gate_reads_delta_and_blocks_bad_data` | duplicate keys in Silver Delta stop the pipeline |

The other test files cover crawler parsing, Kafka JSON-schema validation, quality checks, the DWH loader/views and the Power BI project files. CI (GitHub Actions) runs everything with Java 17 + Python 3.11.

---

## ⚠️ Known limitations

- **Sample, not the full category:** the last crawl used `--max-products 2000`. Tiki reports at most 2,000 items per query, so the bisection split the category into 10 price intervals, and these are crawled from low to high price; the cap was reached at 1.3M VND. The findings therefore describe the lower-priced part of the category. Remove the cap to collect everything.
- Runs Spark in `local[*]` mode on one machine; the docker-compose Spark cluster is not used by the jobs yet.
- Price-change events are **simulated** from real crawled products (Tiki has no public price-change feed). The latency benchmark uses the file source; the Kafka source is implemented but was not part of the benchmark.
- One crawl so far, so every product is at SCD2 version 1; versions accumulate with daily crawls (logic covered by tests).
- MinIO/S3A settings exist but the `hadoop-aws` jar is not wired in; storage is the local filesystem.
- The stock `apache/airflow` image has no Java/PySpark, so DAG tasks need a custom image; `dags/local_dag_runner.py` runs the same tasks locally.
- Spark 3.5 Python workers do not run on Python 3.12 on Windows; the pipeline avoids Python UDFs, so it works there anyway.

---

## 👤 Author
- **GitHub:** [@TrungPham233](https://github.com/TrungPham233)
