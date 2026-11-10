"""
Price Event Simulator for Streaming Ingestion & Flash Sale Detection.

Generates realistic price change and flash sale events based on the crawled product catalog.
Can output to stdout, a JSON file, or directly yield events for Kafka producers in Phase 2.
"""
import argparse
import json
import logging
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, List

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from data_collection.config import RAW_DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("simulate_price_events")


def load_sample_products(input_path: Path, max_samples: int = 500) -> List[dict]:
    """Load products from raw crawled JSONL file."""
    products = []
    if not input_path.exists():
        logger.warning("Products file %s not found. Using fallback mock products.", input_path)
        # Fallback mock items if crawled data is not yet available
        return [
            {"product_id": 10001, "name": "Nồi chiên không dầu Lock&Lock 5.2L", "price": 1890000, "original_price": 2500000},
            {"product_id": 10002, "name": "Máy xay sinh tố Philips HR2223", "price": 1250000, "original_price": 1590000},
            {"product_id": 10003, "name": "Nồi cơm điện tử Tefal 1.8L", "price": 1190000, "original_price": 1790000},
            {"product_id": 10004, "name": "Bếp từ đơn Sunhouse SHD6145", "price": 499000, "original_price": 850000},
            {"product_id": 10005, "name": "Lò vi sóng có nướng Sharp 20L", "price": 1690000, "original_price": 2100000},
        ]

    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    p = json.loads(line)
                    if p.get("product_id") and p.get("price", 0) > 0:
                        products.append(p)
                        if len(products) >= max_samples:
                            break
                except Exception:
                    continue
    return products


def generate_price_event(product: dict, force_flash_sale: bool = False) -> dict:
    """Generate a single price change event."""
    pid = product["product_id"]
    current_price = product.get("price", 1000000)
    original_price = product.get("original_price", current_price)

    # 15% probability of a steep flash sale (>30% price cut), or forced
    if force_flash_sale or random.random() < 0.15:
        drop_pct = random.uniform(0.30, 0.55)
        new_price = int(current_price * (1 - drop_pct))
        is_flash_sale = True
    else:
        # Normal routine price adjustment (-10% to +10%)
        fluctuation = random.uniform(-0.10, 0.10)
        new_price = int(current_price * (1 + fluctuation))
        is_flash_sale = False

    # Round price to nearest 1,000 VND
    new_price = max(10000, (new_price // 1000) * 1000)
    price_change_pct = round(((new_price - current_price) / current_price) * 100, 2)

    return {
        "event_id": f"pevt_{uuid.uuid4().hex[:12]}",
        "product_id": pid,
        "product_name": product.get("name", ""),
        "brand_name": product.get("brand_name", "Unknown"),
        "seller_name": product.get("seller_name", "Unknown"),
        "old_price": current_price,
        "new_price": new_price,
        "original_price": original_price,
        "price_change_pct": price_change_pct,
        "is_flash_sale": is_flash_sale,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def stream_events(
    products: List[dict],
    rate_per_sec: float = 2.0,
    max_events: int = None,
) -> Generator[dict, None, None]:
    """Yield simulated price events at a designated frequency."""
    count = 0
    delay = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.5

    while True:
        prod = random.choice(products)
        event = generate_price_event(prod)
        yield event
        count += 1
        if max_events and count >= max_events:
            break
        time.sleep(delay)


def main():
    parser = argparse.ArgumentParser(description="Simulate Real-time Price Events")
    parser.add_argument("--input", type=str, default=str(RAW_DATA_DIR / "products_raw.jsonl"), help="Products input file")
    parser.add_argument("--output", type=str, default=str(RAW_DATA_DIR / "simulated_price_events.jsonl"), help="Output file")
    parser.add_argument("--count", type=int, default=100, help="Total number of events to generate")
    parser.add_argument("--rate", type=float, default=10.0, help="Events per second (0 for instant dump)")
    args = parser.parse_args()

    products = load_sample_products(Path(args.input))
    logger.info("Loaded %d products to generate price events from", len(products))

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Generating %d simulated price events...", args.count)
    flash_sales_count = 0

    with open(output_path, "w", encoding="utf-8") as out_f:
        for idx, event in enumerate(stream_events(products, rate_per_sec=args.rate, max_events=args.count), 1):
            out_f.write(json.dumps(event, ensure_ascii=False) + "\n")
            if event["is_flash_sale"]:
                flash_sales_count += 1
            if idx % 25 == 0 or idx == args.count:
                logger.info("Generated %d/%d events (Flash Sales detected: %d)",
                            idx, args.count, flash_sales_count)

    logger.info("✅ Done! Saved %d events (including %d flash sales) to %s",
                args.count, flash_sales_count, output_path)


if __name__ == "__main__":
    main()
