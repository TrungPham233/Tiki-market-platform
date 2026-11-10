"""
Product Event Producer.

Reads raw product records from JSONL, validates against product_schema.json,
and publishes to the Kafka topic 'raw-products'.
"""
import argparse
import json
import logging
import sys
import time
from pathlib import Path

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from data_collection.config import RAW_DATA_DIR
from kafka_pipeline.producers.base_producer import BaseProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("product_producer")

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "product_schema.json"


def publish_products(
    file_path: Path,
    bootstrap_servers: str = None,
    dry_run: bool = False,
    delay: float = 0.05,
) -> int:
    """Stream products from file into Kafka."""
    if not file_path.exists():
        logger.error("File %s does not exist!", file_path)
        return 0

    producer = BaseProducer(
        topic="raw-products",
        schema_path=SCHEMA_PATH,
        bootstrap_servers=bootstrap_servers,
        dry_run=dry_run,
    )

    count = 0
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                pid = record.get("product_id")
                if not pid:
                    continue

                success = producer.send(key=pid, value=record)
                if success:
                    count += 1
                    if delay > 0:
                        time.sleep(delay)
            except Exception as err:
                logger.error("Failed to parse/publish product: %s", err)

    producer.flush()
    producer.close()
    logger.info("✅ Published %d products to 'raw-products'", count)
    return count


def main():
    parser = argparse.ArgumentParser(description="Publish Products to Kafka")
    parser.add_argument("--input", default=str(RAW_DATA_DIR / "products_raw.jsonl"))
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--delay", type=float, default=0.01)
    args = parser.parse_args()

    publish_products(
        file_path=Path(args.input),
        bootstrap_servers=args.bootstrap,
        dry_run=args.dry_run,
        delay=args.delay,
    )


if __name__ == "__main__":
    main()
