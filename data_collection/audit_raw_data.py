"""
Data Audit & Quality Inspection Tool for Raw Collected Data.

Reads raw crawled JSONL files and generates an audit summary report in Markdown.
"""
import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from data_collection.config import BASE_DIR, RAW_DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("audit_raw_data")


def audit_products(path: Path) -> dict:
    """Audit products_raw.jsonl."""
    if not path.exists():
        return {"exists": False}

    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    records.append(json.loads(line))
                except Exception:
                    continue

    total = len(records)
    if total == 0:
        return {"exists": True, "count": 0}

    prices = [r.get("price", 0) for r in records if r.get("price", 0) > 0]
    ratings = [r.get("rating_average", 0) for r in records if r.get("rating_average", 0) > 0]
    brands = Counter(r.get("brand_name", "Unknown") for r in records)
    sellers = Counter(r.get("seller_name", "Unknown") for r in records)
    subcategories = Counter(r.get("subcategory_name", "Unknown") for r in records)

    # Missing field counts
    missing_brand = sum(1 for r in records if not r.get("brand_name") or r.get("brand_name") == "Unknown")
    missing_price = sum(1 for r in records if r.get("price", 0) <= 0)

    prices_sorted = sorted(prices)
    min_price = prices_sorted[0] if prices_sorted else 0
    max_price = prices_sorted[-1] if prices_sorted else 0
    median_price = prices_sorted[len(prices_sorted) // 2] if prices_sorted else 0
    avg_price = sum(prices) / len(prices) if prices else 0

    return {
        "exists": True,
        "count": total,
        "min_price": min_price,
        "max_price": max_price,
        "median_price": median_price,
        "avg_price": round(avg_price, 0),
        "avg_rating": round(sum(ratings) / len(ratings), 2) if ratings else 0,
        "top_brands": brands.most_common(5),
        "top_sellers": sellers.most_common(5),
        "top_subcategories": subcategories.most_common(5),
        "missing_brand_pct": round((missing_brand / total) * 100, 2),
        "missing_price_pct": round((missing_price / total) * 100, 2),
    }


def audit_reviews(path: Path) -> dict:
    """Audit reviews_raw.jsonl."""
    if not path.exists():
        return {"exists": False}

    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    records.append(json.loads(line))
                except Exception:
                    continue

    total = len(records)
    if total == 0:
        return {"exists": True, "count": 0}

    ratings = Counter(r.get("rating", 0) for r in records)
    empty_content = sum(1 for r in records if not r.get("content"))

    return {
        "exists": True,
        "count": total,
        "rating_breakdown": dict(sorted(ratings.items())),
        "empty_content_pct": round((empty_content / total) * 100, 2),
    }


def generate_report(prod_stats: dict, rev_stats: dict, output_path: Path):
    """Write markdown summary audit report."""
    md = []
    md.append("# 📋 Data Audit & Ingestion Quality Report\n")
    md.append("This report summarizes the health, distribution, and completeness of raw data collected from Tiki API.\n")

    md.append("## 1. Products Dataset (`products_raw.jsonl`)\n")
    if not prod_stats.get("exists") or prod_stats.get("count", 0) == 0:
        md.append("_No products collected yet._\n")
    else:
        md.append(f"- **Total Products Collected:** {prod_stats['count']:,}")
        md.append(f"- **Price Range:** {prod_stats['min_price']:,} VND → {prod_stats['max_price']:,} VND")
        md.append(f"- **Median Price:** {prod_stats['median_price']:,} VND | **Average Price:** {prod_stats['avg_price']:,} VND")
        md.append(f"- **Average Rating:** {prod_stats['avg_rating']} ⭐")
        md.append(f"- **Missing/Unknown Brand:** {prod_stats['missing_brand_pct']}%")
        md.append(f"- **Missing Price:** {prod_stats['missing_price_pct']}%\n")

        md.append("### Top 5 Brands by Product Count")
        md.append("| Brand | Product Count |")
        md.append("|---|---:|")
        for brand, count in prod_stats["top_brands"]:
            md.append(f"| {brand} | {count:,} |")
        md.append("")

        md.append("### Top 5 Sellers by Product Count")
        md.append("| Seller | Product Count |")
        md.append("|---|---:|")
        for seller, count in prod_stats["top_sellers"]:
            md.append(f"| {seller} | {count:,} |")
        md.append("")

    md.append("## 2. Customer Reviews Dataset (`reviews_raw.jsonl`)\n")
    if not rev_stats.get("exists") or rev_stats.get("count", 0) == 0:
        md.append("_No customer reviews collected yet._\n")
    else:
        md.append(f"- **Total Reviews Collected:** {rev_stats['count']:,}")
        md.append(f"- **Reviews with empty content:** {rev_stats['empty_content_pct']}%\n")

        md.append("### Rating Distribution")
        md.append("| Stars | Count |")
        md.append("|---|---:|")
        for stars, count in rev_stats["rating_breakdown"].items():
            md.append(f"| {stars} ⭐ | {count:,} |")
        md.append("")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    logger.info("Report written to %s", output_path)


def main():
    parser = argparse.ArgumentParser(description="Audit Raw Crawled Data")
    parser.add_argument("--products", type=str, default=str(RAW_DATA_DIR / "products_raw.jsonl"))
    parser.add_argument("--reviews", type=str, default=str(RAW_DATA_DIR / "reviews_raw.jsonl"))
    parser.add_argument("--output", type=str, default=str(BASE_DIR / "docs" / "data_collection_report.md"))
    args = parser.parse_args()

    prod_stats = audit_products(Path(args.products))
    rev_stats = audit_reviews(Path(args.reviews))
    generate_report(prod_stats, rev_stats, Path(args.output))


if __name__ == "__main__":
    main()
