"""
Local Airflow DAG Task Runner & Simulation Engine.

Allows local testing and continuous integration of DAG tasks
without requiring Docker containers to be running.
"""
import argparse
import logging
import sys
import time
from pathlib import Path

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from dags.data_quality import run_silver_quality_gate
from spark.config import SILVER_DIR
from spark.jobs.bronze_to_silver_products import process_bronze_to_silver_products
from spark.jobs.bronze_to_silver_reviews import process_bronze_to_silver_reviews
from spark.jobs.ingest_to_bronze import ingest_jsonl_to_bronze
from spark.jobs.silver_to_gold_aggregations import compute_business_aggregates
from spark.jobs.silver_to_gold_dimensions import process_dimensions
from spark.jobs.silver_to_gold_facts import process_facts
from spark.streaming.price_alert_stream import process_price_events_stream
from data_collection.config import RAW_DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [AirflowRunner] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("airflow_runner")


def run_simulated_dag() -> bool:
    """Run all tasks in tiki_medallion_etl_pipeline sequentially."""
    logger.info("================================================================")
    logger.info("▶ EXECUTING AIRFLOW DAG: 'tiki_medallion_etl_pipeline'")
    logger.info("================================================================")
    t0 = time.time()

    # Task 1
    logger.info("\n[Task: bronze_ingestion]")
    ingest_jsonl_to_bronze(RAW_DATA_DIR / "products_raw.jsonl", "products")
    ingest_jsonl_to_bronze(RAW_DATA_DIR / "reviews_raw.jsonl", "reviews")

    # Task 2 (Parallel branches simulated)
    logger.info("\n[Task: silver_transform_products]")
    process_bronze_to_silver_products()

    logger.info("\n[Task: silver_transform_reviews_nlp]")
    process_bronze_to_silver_reviews()

    # Task 3: Quality Gate
    logger.info("\n[Task: data_quality_gate]")
    silver_prod = SILVER_DIR / "products" / "clean_products.parquet"
    silver_rev = SILVER_DIR / "reviews" / "clean_reviews.parquet"
    report_file = SILVER_DIR / "data_quality_report.json"

    try:
        summary = run_silver_quality_gate(silver_prod, silver_rev, report_file)
        logger.info("✅ Quality Gate PASSED: %d/%d checks passed",
                    summary["passed"], summary["total_checks"])
    except Exception as err:
        logger.error("❌ Quality Gate FAILED: %s", err)
        return False

    # Task 4
    logger.info("\n[Task: gold_build_dimensions_scd2]")
    process_dimensions()

    logger.info("\n[Task: gold_build_facts]")
    process_facts()

    # Task 5
    logger.info("\n[Task: gold_compute_business_aggregates]")
    compute_business_aggregates()

    # Task 6
    logger.info("\n[Task: streaming_price_alerts_detection]")
    process_price_events_stream()

    total_time = time.time() - t0
    logger.info("\n================================================================")
    logger.info("🏁 AIRFLOW DAG RUN SUCCESSFUL in %.2f seconds", total_time)
    logger.info("================================================================")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local Airflow DAG Runner")
    parser.parse_args()
    success = run_simulated_dag()
    sys.exit(0 if success else 1)
