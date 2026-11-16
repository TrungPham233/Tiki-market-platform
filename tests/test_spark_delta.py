"""
Integration tests for the PySpark + Delta Lake pipeline (run on a local SparkSession).

Covers: idempotent Bronze ingestion, Silver dedup/cleaning rules, SCD Type 2 MERGE,
Structured Streaming alerts, and the quality gate over Delta tables.
Skipped automatically when Java/PySpark is not available.
"""
import json
import shutil
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None and not __import__("os").environ.get("JAVA_HOME"):
    pytest.skip("Java is required for Spark tests", allow_module_level=True)

import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

# Test DataFrames are built from pandas (Arrow) rather than Python lists: the pipeline
# itself uses no Python UDFs/workers, so the tests avoid them too (PySpark 3.5 workers
# do not run on Python 3.12 on Windows).


def sdf(spark, rows, schema=None):
    pdf = pd.DataFrame(rows, columns=schema)
    return spark.createDataFrame(pdf)


@pytest.fixture(scope="module")
def spark():
    from spark.spark_session import get_spark_session
    session = get_spark_session("pytest_spark_delta")
    yield session
    session.stop()


def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8")


def product(pid, price, original, brand="Philips", seller="1", crawled="2026-11-01T00:00:00+00:00", **kw):
    row = {
        "product_id": pid, "name": f"Product {pid}", "price": price, "original_price": original,
        "discount_rate": 0, "rating_average": 4.5, "review_count": 10, "quantity_sold": 100,
        "brand_id": "b1", "brand_name": brand, "seller_id": seller, "seller_name": "Tiki Trading",
        "is_official_seller": True, "subcategory_id": "s1", "subcategory_name": "Nồi chiên",
        "crawled_at": crawled,
    }
    row.update(kw)
    return row


def test_bronze_ingestion_is_idempotent(spark, tmp_path):
    from spark.jobs.ingest_to_bronze import ingest_jsonl_to_bronze

    raw = tmp_path / "products_raw.jsonl"
    write_jsonl(raw, [product(1, 100_000, 120_000), product(2, 200_000, 200_000)])
    bronze = (tmp_path / "bronze").as_posix()

    assert ingest_jsonl_to_bronze(spark, raw, "products", bronze) == 2
    assert ingest_jsonl_to_bronze(spark, raw, "products", bronze) == 0  # same file again
    assert spark.read.format("delta").load(f"{bronze}/products").count() == 2


def test_silver_products_keeps_latest_and_normalizes(spark, tmp_path):
    from spark.jobs.bronze_to_silver_products import (
        normalize_brand, normalize_brand_col, process_bronze_to_silver_products,
    )
    from spark.jobs.ingest_to_bronze import ingest_jsonl_to_bronze

    raw = tmp_path / "products_raw.jsonl"
    write_jsonl(raw, [
        product(1, 100_000, 200_000, brand=" locknlock ", crawled="2026-11-01T00:00:00+00:00"),
        product(1, 90_000, 200_000, brand="LocknLock", crawled="2026-11-02T00:00:00+00:00"),
        product(2, 300_000, 250_000, brand="", crawled="2026-11-02T00:00:00+00:00"),
    ])
    bronze, silver = (tmp_path / "bronze").as_posix(), (tmp_path / "silver").as_posix()
    ingest_jsonl_to_bronze(spark, raw, "products", bronze)
    assert process_bronze_to_silver_products(spark, bronze, silver) == 2

    rows = {r["product_id"]: r for r in spark.read.format("delta").load(f"{silver}/products").collect()}
    assert rows[1]["price"] == 90_000            # newest crawl wins
    assert rows[1]["brand_name"] == "Lock&Lock"
    assert rows[1]["discount_rate_calc"] == 55.0
    assert rows[2]["brand_name"] == "Unknown"
    assert rows[2]["is_price_anomaly"] is True   # original < price

    # Spark expression and Python reference agree
    samples = ["PHILIPS", "lock and lock", "  Tefal ", "", None, "NewBrand"]
    df = sdf(spark, {"b": pd.Series(samples, dtype="object")})
    got = [r[0] for r in df.select(normalize_brand_col(F.col("b"))).collect()]
    assert got == [normalize_brand(s) for s in samples]


def test_scd2_creates_new_version_only_on_change(spark, tmp_path):
    from spark.jobs.silver_to_gold_dimensions import apply_scd2, prepare_product_changes

    target = (tmp_path / "dim_product").as_posix()

    def silver(original_price, crawled):
        return sdf(spark, [product(1, 100_000, original_price, crawled=crawled),
                           product(2, 50_000, 60_000, crawled=crawled)]) \
            .withColumn("crawled_at", F.to_timestamp("crawled_at")) \
            .withColumn("original_price", F.col("original_price").cast("double")) \
            .withColumn("origin", F.lit("VN")).withColumn("warranty_type", F.lit("x")) \
            .withColumn("power_wattage", F.lit("")).withColumn("capacity", F.lit(""))

    s1 = apply_scd2(spark, prepare_product_changes(silver(120_000.0, "2026-11-01T00:00:00+00:00")), target)
    assert s1["inserted_versions"] == 2

    # product 1 list price changes, product 2 unchanged
    s2 = apply_scd2(spark, prepare_product_changes(silver(150_000.0, "2026-11-05T00:00:00+00:00")), target)
    assert s2["inserted_versions"] == 1 and s2["expired_versions"] == 1

    dim = spark.read.format("delta").load(target)
    p1 = {r["version"]: r for r in dim.filter("product_id = 1").collect()}
    assert set(p1) == {1, 2}
    assert p1[1]["is_current"] is False and p1[2]["is_current"] is True
    assert p1[1]["valid_to"] == p1[2]["valid_from"]
    assert p1[2]["product_sk"] == "1_v2" and p1[2]["original_price"] == 150_000.0
    assert dim.filter("product_id = 2").count() == 1

    # re-running the same load is a no-op
    s3 = apply_scd2(spark, prepare_product_changes(silver(150_000.0, "2026-11-05T00:00:00+00:00")), target)
    assert s3["inserted_versions"] == 0 and s3["expired_versions"] == 0
    assert dim.count() == 3


def test_structured_streaming_price_alerts(spark, tmp_path):
    from spark.streaming.price_alert_stream import run_price_alert_stream

    events_dir = tmp_path / "events"
    base = {"product_name": "X", "brand_name": "B", "seller_name": "S", "original_price": 1.0,
            "event_timestamp": "2026-11-16T10:00:00+00:00"}
    write_jsonl(events_dir / "e1.jsonl", [
        {**base, "event_id": "a", "product_id": 1, "old_price": 100.0, "new_price": 50.0,
         "price_change_pct": -50.0, "is_flash_sale": False},
        {**base, "event_id": "b", "product_id": 2, "old_price": 100.0, "new_price": 95.0,
         "price_change_pct": -5.0, "is_flash_sale": False},
        {**base, "event_id": "c", "product_id": 3, "old_price": 100.0, "new_price": 90.0,
         "price_change_pct": -10.0, "is_flash_sale": True},
    ])
    gold, ckpt = (tmp_path / "gold").as_posix(), tmp_path / "ckpt"

    stats = run_price_alert_stream(spark, "files", "available-now", events_dir=events_dir,
                                   gold_path=gold, checkpoint_dir=ckpt)
    assert stats["alerts"] == 2
    alerts = {r["event_id"]: r for r in spark.read.format("delta").load(f"{gold}/price_alerts").collect()}
    assert alerts["a"]["alert_type"] == "STEEP_PRICE_DROP"
    assert alerts["c"]["alert_type"] == "FLASH_SALE_DETECTED"
    assert alerts["a"]["latency_sec"] > 0

    # restart with the same checkpoint: nothing is reprocessed
    stats2 = run_price_alert_stream(spark, "files", "available-now", events_dir=events_dir,
                                    gold_path=gold, checkpoint_dir=ckpt)
    assert stats2["alerts"] == 2


def test_quality_gate_reads_delta_and_blocks_bad_data(spark, tmp_path):
    from dags.data_quality import DataQualityError, run_silver_quality_gate_delta

    silver = (tmp_path / "silver").as_posix()
    sdf(spark, [(1, "A", 10.0, 4.0, False), (1, "B", 20.0, 9.0, False)],
        ["product_id", "name", "price", "rating_average", "is_price_anomaly"]) \
        .write.format("delta").save(f"{silver}/products")
    sdf(spark, [(10, 1, 5)], ["review_id", "product_id", "rating"]) \
        .write.format("delta").save(f"{silver}/reviews")

    with pytest.raises(DataQualityError, match="UNIQUE_product_id"):
        run_silver_quality_gate_delta(spark, silver)
