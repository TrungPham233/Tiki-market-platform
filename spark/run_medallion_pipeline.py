"""
Master Pipeline Runner for the Medallion Lakehouse (PySpark + Delta Lake).

Runs every stage in ONE SparkSession (local mode):
  Bronze ingest -> Silver products/reviews -> Data Quality Gate (circuit breaker)
  -> Gold dimensions (SCD2) -> Gold facts -> Gold aggregates
  -> Price-alert stream (Structured Streaming, available-now) -> Serving export

The Airflow DAG runs the same functions as separate tasks.
A run summary (row counts, SCD2 stats, quality gate result, timings) is written to
data/lakehouse/pipeline_run_summary.json.
"""
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from dags.data_quality import run_silver_quality_gate_delta
from data_collection.config import RAW_DATA_DIR
from spark.config import LAKEHOUSE_DIR, SILVER_PATH
from spark.jobs.bronze_to_silver_products import process_bronze_to_silver_products
from spark.jobs.bronze_to_silver_reviews import process_bronze_to_silver_reviews
from spark.jobs.export_serving import export_serving_layer
from spark.jobs.ingest_to_bronze import ingest_jsonl_to_bronze
from spark.jobs.silver_to_gold_aggregations import compute_business_aggregates
from spark.jobs.silver_to_gold_dimensions import process_dimensions
from spark.jobs.silver_to_gold_facts import process_facts
from spark.spark_session import get_spark_session
from spark.streaming.price_alert_stream import run_price_alert_stream

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("medallion_pipeline")


def run_pipeline(skip_stream: bool = False) -> dict:
    summary = {"started_at": datetime.now(timezone.utc).isoformat(), "stages": {}}
    spark = get_spark_session("tiki_medallion_pipeline")
    t0 = time.time()

    def stage(name, fn):
        t = time.time()
        logger.info("--- %s ---", name)
        result = fn()
        summary["stages"][name] = {"result": result, "seconds": round(time.time() - t, 2)}
        return result

    try:
        stage("bronze_products", lambda: ingest_jsonl_to_bronze(spark, RAW_DATA_DIR / "products_raw.jsonl", "products"))
        stage("bronze_reviews", lambda: ingest_jsonl_to_bronze(spark, RAW_DATA_DIR / "reviews_raw.jsonl", "reviews"))
        stage("silver_products", lambda: process_bronze_to_silver_products(spark))
        stage("silver_reviews", lambda: process_bronze_to_silver_reviews(spark))
        gate = stage("quality_gate", lambda: run_silver_quality_gate_delta(
            spark, SILVER_PATH, LAKEHOUSE_DIR / "silver_quality_report.json"))
        summary["stages"]["quality_gate"]["result"] = {
            k: gate[k] for k in ("total_checks", "passed", "warnings", "failures", "gate_status")
        }
        stage("gold_dimensions", lambda: process_dimensions(spark))
        stage("gold_facts", lambda: process_facts(spark))
        stage("gold_aggregates", lambda: compute_business_aggregates(spark))
        if not skip_stream:
            stage("price_alert_stream", lambda: run_price_alert_stream(spark, source="files"))
        stage("serving_export", lambda: export_serving_layer(spark))
    finally:
        summary["total_seconds"] = round(time.time() - t0, 2)
        (LAKEHOUSE_DIR / "pipeline_run_summary.json").write_text(
            json.dumps(summary, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        spark.stop()

    logger.info("PIPELINE COMPLETED in %.1fs", summary["total_seconds"])
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the full Medallion pipeline")
    parser.add_argument("--skip-stream", action="store_true")
    run_pipeline(parser.parse_args().skip_stream)
