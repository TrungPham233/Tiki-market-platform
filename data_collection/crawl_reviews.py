"""
Tiki Customer Reviews Crawler.

Scrapes customer reviews, star ratings, and feedback comments for each product.
Anonymizes sensitive customer information.
Outputs line-by-line JSON Lines to data/raw/reviews_raw.jsonl.
"""
import argparse
import hashlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set
import urllib.request
import urllib.error

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from data_collection.config import (
    DEFAULT_HEADERS,
    RAW_DATA_DIR,
    REQUEST_DELAY,
    TIMEOUT_SECONDS,
    TIKI_REVIEW_API,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("crawl_reviews")


def hash_identifier(val: str) -> str:
    """Anonymize customer ID / name using SHA-256."""
    if not val:
        return "anonymous"
    return hashlib.sha256(str(val).encode("utf-8")).hexdigest()[:16]


def fetch_product_reviews(product_id: int, page: int = 1, limit: int = 20) -> dict:
    """Fetch reviews page for a product from Tiki API."""
    url = f"{TIKI_REVIEW_API}?product_id={product_id}&sort=score%7Cdesc&page={page}&limit={limit}&include=comments"
    req = urllib.request.Request(url, headers=DEFAULT_HEADERS)
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
                if resp.status == 200:
                    return json.loads(resp.read().decode("utf-8"))
        except Exception:
            time.sleep(attempt * 1.0)
    return {}


def parse_review_record(raw_rev: dict, product_id: int) -> dict:
    """Extract and anonymize relevant review fields."""
    cust = raw_rev.get("created_by") or {}
    cust_id = cust.get("id") or cust.get("full_name", "")
    anonymized_customer = hash_identifier(cust_id)

    # Created date
    created_ts = raw_rev.get("created_at")
    created_iso = ""
    if created_ts:
        try:
            created_iso = datetime.fromtimestamp(created_ts, timezone.utc).isoformat()
        except Exception:
            created_iso = datetime.now(timezone.utc).isoformat()

    return {
        "review_id": raw_rev.get("id"),
        "product_id": product_id,
        "customer_hash": anonymized_customer,
        "rating": raw_rev.get("rating", 0),
        "title": raw_rev.get("title", "").strip(),
        "content": raw_rev.get("content", "").strip(),
        "thank_count": raw_rev.get("thank_count", 0),
        "is_buyer": bool(raw_rev.get("bought", False)),
        "created_at": created_iso,
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }


def main():
    parser = argparse.ArgumentParser(description="Crawl Tiki Customer Reviews")
    parser.add_argument("--input", type=str, default=str(RAW_DATA_DIR / "products_raw.jsonl"), help="Path to products_raw.jsonl")
    parser.add_argument("--output", type=str, default=str(RAW_DATA_DIR / "reviews_raw.jsonl"), help="Output JSONL file")
    parser.add_argument("--max-reviews-per-product", type=int, default=50, help="Max reviews per product")
    parser.add_argument("--limit-products", type=int, default=None, help="Limit number of products to crawl reviews for")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        logger.error("Input file %s does not exist. Run crawl_product_detail.py first!", input_path)
        sys.exit(1)

    # Read product IDs
    products = []
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    p = json.loads(line)
                    if p.get("review_count", 0) > 0 and p.get("product_id"):
                        products.append((p["product_id"], p.get("review_count", 0)))
                except Exception:
                    continue

    logger.info("Found %d products with reviews", len(products))
    if args.limit_products:
        products = products[:args.limit_products]

    # Track existing review IDs to avoid duplicates
    existing_rev_ids: Set[int] = set()
    if output_path.exists():
        with open(output_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        r = json.loads(line)
                        if r.get("review_id"):
                            existing_rev_ids.add(r["review_id"])
                    except Exception:
                        continue
        logger.info("Loaded %d existing review records", len(existing_rev_ids))

    total_new_reviews = 0
    with open(output_path, "a", encoding="utf-8") as out_f:
        for idx, (pid, rev_count) in enumerate(products, 1):
            pages_needed = min(5, (args.max_reviews_per_product + 19) // 20)
            product_reviews_count = 0

            for page in range(1, pages_needed + 1):
                time.sleep(REQUEST_DELAY)
                data = fetch_product_reviews(pid, page=page, limit=20)
                reviews = data.get("data", [])
                if not reviews:
                    break

                for rev in reviews:
                    rev_id = rev.get("id")
                    if rev_id and rev_id not in existing_rev_ids:
                        record = parse_review_record(rev, pid)
                        out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                        existing_rev_ids.add(rev_id)
                        total_new_reviews += 1
                        product_reviews_count += 1

                if product_reviews_count >= args.max_reviews_per_product:
                    break

            out_f.flush()
            if idx % 10 == 0 or idx == len(products):
                logger.info("Processed %d/%d products | Total reviews collected: %d",
                            idx, len(products), total_new_reviews)

    logger.info("✅ Finished crawling reviews! Total new reviews saved: %d", total_new_reviews)
    logger.info("Data saved to: %s", output_path)


if __name__ == "__main__":
    main()

