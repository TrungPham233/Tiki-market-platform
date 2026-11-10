"""
Real-time Price Event Producer.

Continuously generates or streams price changes / flash sales directly into Kafka.
Crucial for demonstrating Spark Structured Streaming & alert detection.
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
from data_collection.simulate_price_events import load_sample_products, stream_events
from kafka_pipeline.producers.base_producer import BaseProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("price_event_producer")

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "price_event_schema.json"


def stream_price_events_to_kafka(
    products_path: Path,
    bootstrap_servers: str = None,
    dry_run: bool = False,
    rate_per_sec: float = 2.0,
    max_events: int = 100,
) -> int:
    """Stream simulated events into Kafka topic 'raw-price-events'."""
    producer = BaseProducer(
        topic="raw-price-events",
        schema_path=SCHEMA_PATH,
        bootstrap_servers=bootstrap_servers,
        dry_run=dry_run,
    )

    products = load_sample_products(products_path)
    logger.info("Starting live price event stream into 'raw-price-events' (rate: %.1f ev/s)...", rate_per_sec)

    count = 0
    flash_sales = 0
    for event in stream_events(products, rate_per_sec=rate_per_sec, max_events=max_events):
        success = producer.send(key=event["product_id"], value=event)
        if success:
            count += 1
            if event["is_flash_sale"]:
                flash_sales += 1
                logger.info("🔥 [FLASH SALE EVENT] %s: %s -> %s VND (-%.1f%%)",
                            event["product_name"][:30], f"{event['old_price']:,}",
                            f"{event['new_price']:,}", abs(event["price_change_pct"]))
            elif count % 20 == 0:
                logger.info("Produced %d events so far...", count)

    producer.flush()
    producer.close()
    logger.info("✅ Finished streaming %d price events (Flash sales: %d)", count, flash_sales)
    return count


def main():
    parser = argparse.ArgumentParser(description="Stream Price Events to Kafka")
    parser.add_argument("--input", default=str(RAW_DATA_DIR / "products_raw.jsonl"))
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--rate", type=float, default=2.0, help="Events per second")
    parser.add_argument("--count", type=int, default=100, help="Total events to produce")
    args = parser.parse_args()

    stream_price_events_to_kafka(
        products_path=Path(args.input),
        bootstrap_servers=args.bootstrap,
        dry_run=args.dry_run,
        rate_per_sec=args.rate,
        max_events=args.count,
    )


if __name__ == "__main__":
    main()
