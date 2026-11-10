"""
Unit Tests for Phase 1: Data Collection & Parsing Modules.
"""
import pytest
from data_collection.crawl_product_detail import parse_product_record
from data_collection.crawl_reviews import parse_review_record, hash_identifier
from data_collection.simulate_price_events import generate_price_event


def test_hash_identifier():
    h1 = hash_identifier("123456")
    h2 = hash_identifier("123456")
    h3 = hash_identifier("654321")
    assert h1 == h2
    assert h1 != h3
    assert len(h1) == 16
    assert hash_identifier("") == "anonymous"


def test_parse_product_record():
    mock_raw = {
        "id": 12345,
        "name": "Nồi chiên không dầu Lock&Lock 5.2L",
        "url_key": "noi-chien-lock-and-lock",
        "price": 1890000,
        "original_price": 2500000,
        "discount": 610000,
        "discount_rate": 24,
        "rating_average": 4.8,
        "review_count": 150,
        "all_time_quantity_sold": 500,
        "brand": {"id": 101, "name": "Lock&Lock"},
        "current_seller": {"id": 1, "name": "Tiki Trading", "is_best_store": True},
        "categories": {"id": 1882, "name": "Thiết Bị Nhà Bếp"},
        "specifications": [
            {
                "attributes": [
                    {"code": "origin", "value": "Hàn Quốc"},
                    {"code": "cong_suat", "value": "1800W"},
                    {"code": "dung_tich", "value": "5.2L"},
                ]
            }
        ],
    }

    parsed = parse_product_record(mock_raw)
    assert parsed["product_id"] == 12345
    assert parsed["name"] == "Nồi chiên không dầu Lock&Lock 5.2L"
    assert parsed["price"] == 1890000
    assert parsed["brand_name"] == "Lock&Lock"
    assert parsed["seller_name"] == "Tiki Trading"
    assert parsed["is_official_seller"] is True
    assert parsed["origin"] == "Hàn Quốc"
    assert parsed["power_wattage"] == "1800W"
    assert parsed["capacity"] == "5.2L"
    assert parsed["is_active"] is True


def test_parse_deleted_product():
    deleted_raw = {"id": 99999, "is_deleted": True}
    parsed = parse_product_record(deleted_raw)
    assert parsed["id"] == 99999
    assert parsed["is_active"] is False


def test_parse_review_record():
    mock_review = {
        "id": 98765,
        "rating": 5,
        "title": "Tuyệt vời",
        "content": "Sản phẩm dùng rất êm, đóng gói cẩn thận.",
        "thank_count": 12,
        "bought": True,
        "created_at": 1700000000,
        "created_by": {"id": 555, "full_name": "Nguyen Van A"},
    }

    parsed = parse_review_record(mock_review, product_id=12345)
    assert parsed["review_id"] == 98765
    assert parsed["product_id"] == 12345
    assert parsed["rating"] == 5
    assert parsed["customer_hash"] != "Nguyen Van A"  # Must be hashed/anonymized!
    assert parsed["is_buyer"] is True
    assert "2023" in parsed["created_at"]


def test_generate_price_event():
    mock_product = {
        "product_id": 1001,
        "name": "Nồi cơm điện",
        "price": 1000000,
        "original_price": 1500000,
        "brand_name": "Tefal",
        "seller_name": "Tiki Trading",
    }

    event = generate_price_event(mock_product, force_flash_sale=True)
    assert event["product_id"] == 1001
    assert event["is_flash_sale"] is True
    assert event["new_price"] < event["old_price"]
    assert event["price_change_pct"] <= -30.0

