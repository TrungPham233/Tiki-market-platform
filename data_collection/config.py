"""
Configuration module for Tiki Data Collection Pipeline.
"""
import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
SAMPLE_DATA_DIR = DATA_DIR / "sample"

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
SAMPLE_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Default Category: Thiết Bị Nhà Bếp (Kitchen Appliances)
DEFAULT_CATEGORY_ID = int(os.getenv("TIKI_CATEGORY_ID", "1882"))

# API Endpoints
TIKI_LISTING_API = "https://tiki.vn/api/personalish/v1/blocks/listings"
TIKI_PRODUCT_API = "https://tiki.vn/api/v2/products"
TIKI_REVIEW_API = "https://tiki.vn/api/v2/reviews"

# Crawler Behavior
REQUEST_DELAY = float(os.getenv("TIKI_REQUEST_DELAY", "0.3"))  # seconds between requests
MAX_RETRIES = 3
TIMEOUT_SECONDS = 15
PAGE_SIZE = 40
SAFE_MAX_PRODUCTS_PER_QUERY = 1600  # Tiki caps pagination around 2000 items

# Realistic User-Agent Headers to avoid rate limiting
DEFAULT_HEADERS = {
    "User-Agent": os.getenv(
        "TIKI_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://tiki.vn/",
    "Origin": "https://tiki.vn",
    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

