"""
Tiki Product Details Crawler.

Fetches in-depth product attributes (pricing, seller, brand, specs, sales)
for each product ID and appends them to a JSON Lines file.
Supports resuming from checkpoint.
"""
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Set
import urllib.request
import urllib.error

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from data_collection.config import (
    DEFAULT_HEADERS,
    RAW_DATA_DIR,
    REQUEST_DELAY,
    TIMEOUT_SECONDS,
    TIKI_PRODUCT_API,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("crawl_product_detail")


def fetch_product_detail(product_id: int) -> dict:
    """Fetch raw product detail JSON from Tiki API."""
    url = f"{TIKI_PRODUCT_API}/{product_id}"
    req = urllib.request.Request(url, headers=DEFAULT_HEADERS)
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
                if resp.status == 200:
                    return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            if err.code == 404:
                # Product removed / discontinued
                return {"id": product_id, "is_deleted": True}
            time.sleep(attempt * 1.5)
        except Exception as err:
            time.sleep(attempt * 1.5)
    return {}


def parse_product_record(raw: dict) -> dict:
    """Extract and normalize key fields into a structured dictionary."""
    if not raw or raw.get("is_deleted"):
        return {"id": raw.get("id"), "is_active": False}

    # Brand
    brand_obj = raw.get("brand") or {}
    brand_id = str(brand_obj.get("id", "")) if brand_obj else "unknown"
    brand_name = brand_obj.get("name", "Unknown") if brand_obj else "Unknown"

    # Current Seller
    seller_obj = raw.get("current_seller") or {}
    seller_id = str(seller_obj.get("id", "")) if seller_obj else "unknown"
    seller_name = seller_obj.get("name", "Unknown") if seller_obj else "Unknown"
    is_official_seller = bool(seller_obj.get("is_best_store", False))

    # Categories
    categories = raw.get("categories", {})
    subcat_id = ""
    subcat_name = ""
    if isinstance(categories, dict):
        subcat_id = str(categories.get("id", ""))
        subcat_name = categories.get("name", "")
    elif isinstance(categories, list) and categories:
        subcat_id = str(categories[-1].get("id", ""))
        subcat_name = categories[-1].get("name", "")

    # Specifications
    specs = {}
    for spec_group in raw.get("specifications", []):
        for attr in spec_group.get("attributes", []):
            code = attr.get("code")
            val = attr.get("value")
            if code and val:
                specs[code] = str(val).strip()

    # Quantity sold
    qty_sold = 0
    raw_qty = raw.get("all_time_quantity_sold") or raw.get("quantity_sold", {}).get("value")
    if raw_qty is not None:
        try:
            qty_sold = int(raw_qty)
        except (ValueError, TypeError):
            qty_sold = 0

    return {
        "product_id": raw.get("id"),
        "name": raw.get("name", "").strip(),
        "url_key": raw.get("url_key", ""),
        "price": raw.get("price", 0),
        "original_price": raw.get("original_price", 0),
        "discount": raw.get("discount", 0),
        "discount_rate": raw.get("discount_rate", 0),
        "rating_average": raw.get("rating_average", 0.0),
        "review_count": raw.get("review_count", 0),
        "quantity_sold": qty_sold,
        "brand_id": brand_id,
        "brand_name": brand_name,
        "seller_id": seller_id,
        "seller_name": seller_name,
        "is_official_seller": is_official_seller,
        "subcategory_id": subcat_id,
        "subcategory_name": subcat_name,
        "origin": specs.get("origin", specs.get("xuat_xu", "Unknown")),
        "warranty_type": specs.get("warranty_type", "Unknown"),
        "power_wattage": specs.get("cong_suat", ""),
        "capacity": specs.get("dung_tich", ""),
        "is_active": True,
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }


def get_already_scraped_ids(output_path: Path) -> Set[int]:
    """Inspect JSONL file and return set of already processed product IDs."""
    done_ids = set()
    if not output_path.exists():
        return done_ids
    with open(output_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    data = json.loads(line)
                    pid = data.get("product_id") or data.get("id")
                    if pid:
                        done_ids.add(int(pid))
                except Exception:
                    continue
    return done_ids


def main():
    parser = argparse.ArgumentParser(description="Crawl Tiki Product Details")
    parser.add_argument("--input", type=str, default=str(RAW_DATA_DIR / "product_ids.json"), help="Path to product_ids.json")
    parser.add_argument("--output", type=str, default=str(RAW_DATA_DIR / "products_raw.jsonl"), help="Output JSONL file")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of items to crawl")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        logger.error("Input file %s does not exist. Run crawl_product_ids.py first!", input_path)
        sys.exit(1)

    with open(input_path, "r", encoding="utf-8") as f:
        id_data = json.load(f)
        all_ids = id_data.get("product_ids", [])

    logger.info("Total product IDs in input: %d", len(all_ids))
    scraped_ids = get_already_scraped_ids(output_path)
    logger.info("Already scraped IDs: %d", len(scraped_ids))

    remaining_ids = [pid for pid in all_ids if pid not in scraped_ids]
    if args.limit:
        remaining_ids = remaining_ids[:args.limit]

    logger.info("Products to crawl in this run: %d", len(remaining_ids))

    count_success = 0
    with open(output_path, "a", encoding="utf-8") as out_f:
        for idx, pid in enumerate(remaining_ids, 1):
            time.sleep(REQUEST_DELAY)
            raw = fetch_product_detail(pid)
            if raw:
                record = parse_product_record(raw)
                out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                out_f.flush()
                count_success += 1

            if idx % 20 == 0 or idx == len(remaining_ids):
                logger.info("Progress: %d/%d (%.1f%%) | Saved: %d",
                            idx, len(remaining_ids), (idx / len(remaining_ids)) * 100, count_success)

    logger.info("✅ Finished crawling details! Newly added: %d products", count_success)
    logger.info("Data stored in: %s", output_path)


if __name__ == "__main__":
    main()

