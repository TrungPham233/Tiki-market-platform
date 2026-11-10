"""
Real-time Price Alert & Flash Sale Stream Processor.

Consumes price change events from Kafka 'raw-price-events' (or file stream),
evaluates price drop thresholds (> 30% drop or marked flash sale),
and persists alert logs to the Gold lakehouse layer.
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
from spark.config import GOLD_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("price_alert_stream")


def evaluate_price_event(event: Dict) -> Dict:
    """Check if price event qualifies for a real-time discount alert."""
    pid = event.get("product_id")
    old_p = event.get("old_price", 0)
    new_p = event.get("new_price", 0)
    change_pct = event.get("price_change_pct", 0.0)
    is_flash = event.get("is_flash_sale", False)

    # Threshold: Flash sale flag or price drop >= 30%
    if is_flash or change_pct <= -30.0:
        return {
            "alert_id": f"alt_{uuid.uuid4().hex[:10]}",
            "alert_type": "FLASH_SALE_DETECTED" if is_flash else "STEEP_PRICE_DROP",
            "product_id": pid,
            "product_name": event.get("product_name", ""),
            "brand_name": event.get("brand_name", "Unknown"),
            "seller_name": event.get("seller_name", "Unknown"),
            "old_price": old_p,
            "new_price": new_p,
            "price_drop_pct": abs(change_pct),
            "savings_amount": old_p - new_p,
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }
    return {}


def process_price_events_stream(
    events_file: Path = RAW_DATA_DIR / "simulated_price_events.jsonl",
    output_dir: Path = GOLD_DIR / "alerts",
) -> List[Dict]:
    """Process event stream and output generated alerts."""
    if not events_file.exists():
        logger.warning("Events file %s does not exist!", events_file)
        return []

    alerts = []
    with open(events_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    event = json.loads(line)
                    alert = evaluate_price_event(event)
                    if alert:
                        alerts.append(alert)
                        logger.info("🚨 [ALERT TRIGGERED] %s (-%.1f%%) | Savings: %s VND | %s",
                                    alert["product_name"][:30], alert["price_drop_pct"],
                                    f"{alert['savings_amount']:,}", alert["brand_name"])
                except Exception as err:
                    logger.error("Error evaluating event: %s", err)

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "price_alerts.parquet"

    if alerts:
        df_alerts = pd.DataFrame(alerts)
        df_alerts.to_parquet(out_file, index=False, engine="pyarrow")
        logger.info("✅ Saved %d triggered price alerts to %s", len(alerts), out_file)
    else:
        logger.info("No price alerts exceeded threshold in this stream.")

    return alerts


def main():
    parser = argparse.ArgumentParser(description="Process Price Alerts Stream")
    parser.add_argument("--events", default=str(RAW_DATA_DIR / "simulated_price_events.jsonl"))
    parser.add_argument("--output-dir", default=str(GOLD_DIR / "alerts"))
    args = parser.parse_args()

    process_price_events_stream(Path(args.events), Path(args.output_dir))


if __name__ == "__main__":
    main()
