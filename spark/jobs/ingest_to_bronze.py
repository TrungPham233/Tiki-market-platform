"""
Bronze Layer Ingestion Job.

Ingests raw JSON/JSONL records into the Bronze layer of the Medallion architecture.
Enriches raw payloads with ingestion audit metadata (_ingested_at, _source_file, ingest_date).
"""
import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from data_collection.config import RAW_DATA_DIR
from spark.config import BRONZE_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ingest_to_bronze")


def ingest_jsonl_to_bronze(
    input_file: Path,
    dataset_name: str,
    output_base_dir: Path = BRONZE_DIR,
) -> int:
    """Read raw JSONL file, append metadata, and write partitioned Parquet/Delta to Bronze layer."""
    if not input_file.exists():
        logger.error("Input file %s does not exist!", input_file)
        return 0

    records = []
    batch_id = str(uuid.uuid4())[:8]
    now_utc = datetime.now(timezone.utc)
    ingest_date = now_utc.strftime("%Y-%m-%d")
    ingested_at = now_utc.isoformat()

    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    data = json.loads(line)
                    data["_ingested_at"] = ingested_at
                    data["_batch_id"] = batch_id
                    data["_source_file"] = input_file.name
                    data["ingest_date"] = ingest_date
                    records.append(data)
                except Exception as err:
                    logger.warning("Failed parsing line in %s: %s", input_file, err)

    if not records:
        logger.warning("No valid records found in %s", input_file)
        return 0

    df = pd.DataFrame(records)

    # Destination directory: bronze/{dataset_name}/ingest_date=YYYY-MM-DD
    dest_dir = output_base_dir / dataset_name / f"ingest_date={ingest_date}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file = dest_dir / f"part_{batch_id}.parquet"

    df.to_parquet(dest_file, index=False, engine="pyarrow")
    logger.info("✅ [BRONZE] Ingested %d records to %s", len(records), dest_file)
    return len(records)


def main():
    parser = argparse.ArgumentParser(description="Ingest Raw Data to Bronze Layer")
    parser.add_argument("--products", default=str(RAW_DATA_DIR / "products_raw.jsonl"))
    parser.add_argument("--reviews", default=str(RAW_DATA_DIR / "reviews_raw.jsonl"))
    args = parser.parse_args()

    prod_count = ingest_jsonl_to_bronze(Path(args.products), "products")
    rev_count = ingest_jsonl_to_bronze(Path(args.reviews), "reviews")

    logger.info("Bronze Layer Ingestion Complete: %d products, %d reviews", prod_count, rev_count)


if __name__ == "__main__":
    main()

