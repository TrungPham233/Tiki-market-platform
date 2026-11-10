"""
Tiki Product IDs Crawler with Recursive Price-Splitting Algorithm.

Overcomes Tiki's hard limit of ~2,000 products per query by dynamically
bisecting price intervals until each interval has <= SAFE_MAX_PRODUCTS.
"""
import argparse
import json
import logging
import math
import sys
import time
from pathlib import Path
from typing import List, Set, Tuple
import urllib.request
import urllib.error

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from data_collection.config import (
    DEFAULT_CATEGORY_ID,
    DEFAULT_HEADERS,
    PAGE_SIZE,
    RAW_DATA_DIR,
    REQUEST_DELAY,
    SAFE_MAX_PRODUCTS_PER_QUERY,
    TIMEOUT_SECONDS,
    TIKI_LISTING_API,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("crawl_product_ids")


def fetch_listing(
    category_id: int,
    page: int = 1,
    price_range: Tuple[int, int] = None,
    limit: int = PAGE_SIZE,
) -> dict:
    """Fetch a single page from Tiki listing API with optional price filtering."""
    url = f"{TIKI_LISTING_API}?limit={limit}&category={category_id}&page={page}"
    if price_range:
        min_p, max_p = price_range
        url += f"&price={min_p},{max_p}"

    req = urllib.request.Request(url, headers=DEFAULT_HEADERS)
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
                if resp.status == 200:
                    return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as err:
            logger.warning("Attempt %d failed for URL %s: %s", attempt, url, err)
            time.sleep(attempt * 1.5)

    return {}


def get_total_in_range(category_id: int, min_price: int, max_price: int) -> int:
    """Check how many products exist in this price range."""
    time.sleep(REQUEST_DELAY)
    data = fetch_listing(category_id, page=1, price_range=(min_price, max_price), limit=1)
    return data.get("paging", {}).get("total", 0)


def split_price_ranges(
    category_id: int, min_price: int, max_price: int
) -> List[Tuple[int, int]]:
    """
    Recursively split price ranges until each range contains
    fewer than SAFE_MAX_PRODUCTS_PER_QUERY items.
    """
    total = get_total_in_range(category_id, min_price, max_price)
    logger.info("Range [%d, %d] has %d products", min_price, max_price, total)

    if total == 0:
        return []

    # If count is safe or range is too small to split further, return range
    if total <= SAFE_MAX_PRODUCTS_PER_QUERY or (max_price - min_price) <= 10000:
        return [(min_price, max_price)]

    # Bisect price interval
    mid_price = (min_price + max_price) // 2
    logger.info("Splitting [%d, %d] -> [%d, %d] and [%d, %d]",
                min_price, max_price, min_price, mid_price, mid_price + 1, max_price)

    left_ranges = split_price_ranges(category_id, min_price, mid_price)
    right_ranges = split_price_ranges(category_id, mid_price + 1, max_price)
    return left_ranges + right_ranges


def crawl_ids_in_range(
    category_id: int,
    price_range: Tuple[int, int],
    collected_ids: Set[int],
    max_items: int = None,
) -> None:
    """Crawl all product IDs across all pages in a given price range."""
    min_p, max_p = price_range
    logger.info("--- Crawling range [%d - %d VND] ---", min_p, max_p)

    # First request to get total pages
    first_page = fetch_listing(category_id, page=1, price_range=price_range)
    total_items = first_page.get("paging", {}).get("total", 0)
    total_pages = first_page.get("paging", {}).get("last_page", 1)

    logger.info("Range [%d, %d]: %d total items, %d pages", min_p, max_p, total_items, total_pages)

    for item in first_page.get("data", []):
        if "id" in item:
            collected_ids.add(item["id"])
            if max_items and len(collected_ids) >= max_items:
                return

    # Crawl remaining pages
    for page in range(2, total_pages + 1):
        if max_items and len(collected_ids) >= max_items:
            break
        time.sleep(REQUEST_DELAY)
        data = fetch_listing(category_id, page=page, price_range=price_range)
        items = data.get("data", [])
        if not items:
            break
        for item in items:
            if "id" in item:
                collected_ids.add(item["id"])
                if max_items and len(collected_ids) >= max_items:
                    return

        if page % 5 == 0 or page == total_pages:
            logger.info("Range [%d, %d] - Page %d/%d (Total Unique IDs: %d)",
                        min_p, max_p, page, total_pages, len(collected_ids))


def main():
    parser = argparse.ArgumentParser(description="Crawl Tiki Product IDs")
    parser.add_argument("--category-id", type=int, default=DEFAULT_CATEGORY_ID, help="Tiki category ID")
    parser.add_argument("--min-price", type=int, default=10000, help="Minimum price in VND")
    parser.add_argument("--max-price", type=int, default=50000000, help="Maximum price in VND")
    parser.add_argument("--max-products", type=int, default=None, help="Cap total products (useful for testing)")
    parser.add_argument("--output", type=str, default=str(RAW_DATA_DIR / "product_ids.json"), help="Output JSON file")
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    collected_ids: Set[int] = set()

    # Load existing IDs if file exists (checkpointing)
    if output_path.exists():
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
                collected_ids = set(existing.get("product_ids", []))
                logger.info("Loaded %d existing product IDs from %s", len(collected_ids), output_path)
        except Exception as err:
            logger.warning("Could not read existing file: %s", err)

    logger.info("Determining optimal price ranges for category %d...", args.category_id)
    price_ranges = split_price_ranges(args.category_id, args.min_price, args.max_price)
    logger.info("Found %d discrete price intervals", len(price_ranges))

    for idx, p_range in enumerate(price_ranges, 1):
        logger.info("Processing interval %d/%d: %s", idx, len(price_ranges), p_range)
        crawl_ids_in_range(args.category_id, p_range, collected_ids, max_items=args.max_products)

        # Save checkpoint periodically
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({
                "category_id": args.category_id,
                "total_count": len(collected_ids),
                "product_ids": sorted(list(collected_ids)),
            }, f, indent=2)

        if args.max_products and len(collected_ids) >= args.max_products:
            logger.info("Reached target product limit of %d items", args.max_products)
            break

    logger.info("✅ Finished crawling product IDs! Total unique IDs: %d", len(collected_ids))
    logger.info("Saved to: %s", output_path)


if __name__ == "__main__":
    main()
