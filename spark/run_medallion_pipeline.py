"""
Master Pipeline Runner for Medallion Lakehouse Architecture.

Executes Bronze -> Silver -> Gold data progression in sequence.
"""
import logging
import sys
import time
from pathlib import Path

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from spark.config import BRONZE_DIR, GOLD_DIR, SILVER_DIR
from spark.jobs.ingest_to_bronze import ingest_jsonl_to_bronze
from spark.jobs.bronze_to_silver_products import process_bronze_to_silver_products
from spark.jobs.bronze_to_silver_reviews import process_bronze_to_silver_reviews
from spark.jobs.silver_to_gold_dimensions import process_dimensions
from spark.jobs.silver_to_gold_facts import process_facts
from spark.jobs.silver_to_gold_aggregations import compute_business_aggregates
from spark.streaming.price_alert_stream import process_price_events_stream
from data_collection.config import RAW_DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("medallion_pipeline")


def run_pipeline() -> None:
    """Execute end-to-end Medallion pipeline."""
    start_time = time.time()
    logger.info("=====================================================")
    logger.info("🚀 STARTING MEDALLION LAKEHOUSE ETL PIPELINE")
    logger.info("=====================================================")

    # Step 1: Ingest to Bronze
    logger.info("\n--- STEP 1: BRONZE INGESTION ---")
    p_bronze = ingest_jsonl_to_bronze(RAW_DATA_DIR / "products_raw.jsonl", "products")
    r_bronze = ingest_jsonl_to_bronze(RAW_DATA_DIR / "reviews_raw.jsonl", "reviews")

    # Step 2: Bronze to Silver
    logger.info("\n--- STEP 2: SILVER CLEANING & ENRICHMENT ---")
    p_silver = process_bronze_to_silver_products()
    r_silver = process_bronze_to_silver_reviews()

    # Step 3: Silver to Gold Dimensions
    logger.info("\n--- STEP 3: GOLD DIMENSIONS (SCD TYPE 2) ---")
    process_dimensions()

    # Step 4: Silver to Gold Facts
    logger.info("\n--- STEP 4: GOLD FACTS (STAR SCHEMA) ---")
    process_facts()

    # Step 5: Gold Business Aggregations (BQ1 to BQ6)
    logger.info("\n--- STEP 5: GOLD BUSINESS AGGREGATIONS ---")
    compute_business_aggregates()

    # Step 6: Real-time Price Alerts Stream
    logger.info("\n--- STEP 6: STREAMING PRICE ALERTS ---")
    alerts = process_price_events_stream()

    elapsed = time.time() - start_time
    logger.info("\n=====================================================")
    logger.info("🎉 MEDALLION PIPELINE COMPLETED IN %.2f SECONDS", elapsed)
    logger.info("   - Bronze Products: %d | Reviews: %d", p_bronze, r_bronze)
    logger.info("   - Silver Products: %d | Reviews: %d", p_silver, r_silver)
    logger.info("   - Triggered Price Alerts: %d", len(alerts))
    logger.info("   - Gold Tables created at: %s", GOLD_DIR)
    logger.info("=====================================================")


if __name__ == "__main__":
    run_pipeline()

