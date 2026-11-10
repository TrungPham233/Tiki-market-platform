"""
Unit Tests for Phase 2: Kafka Schemas, Serialization, and Producers.
"""
from pathlib import Path
import pytest
from kafka_pipeline.producers.base_producer import BaseProducer

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "kafka_pipeline" / "schemas"
PRODUCT_SCHEMA = SCHEMA_DIR / "product_schema.json"
PRICE_SCHEMA = SCHEMA_DIR / "price_event_schema.json"
REVIEW_SCHEMA = SCHEMA_DIR / "review_schema.json"


def test_product_schema_validation_valid():
    producer = BaseProducer(topic="raw-products", schema_path=PRODUCT_SCHEMA, dry_run=True)
    valid_record = {
        "product_id": 1001,
        "name": "Nồi cơm điện tử Tefal 1.8L",
        "price": 1200000,
        "original_price": 1800000,
        "crawled_at": "2026-11-10T12:00:00Z",
        "brand_name": "Tefal",
        "rating_average": 4.5,
    }
    assert producer.validate_message(valid_record) is True


def test_product_schema_validation_missing_required():
    producer = BaseProducer(topic="raw-products", schema_path=PRODUCT_SCHEMA, dry_run=True)
    # Missing required 'name' and 'crawled_at'
    invalid_record = {
        "product_id": 1001,
        "price": 1200000,
    }
    assert producer.validate_message(invalid_record) is False


def test_product_schema_validation_negative_price():
    producer = BaseProducer(topic="raw-products", schema_path=PRODUCT_SCHEMA, dry_run=True)
    invalid_record = {
        "product_id": 1001,
        "name": "Lò vi sóng",
        "price": -50000,  # Negative price!
        "original_price": 100000,
        "crawled_at": "2026-11-10T12:00:00Z",
    }
    assert producer.validate_message(invalid_record) is False


def test_price_event_schema_validation():
    producer = BaseProducer(topic="raw-price-events", schema_path=PRICE_SCHEMA, dry_run=True)
    valid_event = {
        "event_id": "evt_12345",
        "product_id": 1002,
        "old_price": 1000000,
        "new_price": 650000,
        "price_change_pct": -35.0,
        "is_flash_sale": True,
        "event_timestamp": "2026-11-10T12:30:00Z",
    }
    assert producer.validate_message(valid_event) is True


def test_review_schema_validation():
    producer = BaseProducer(topic="raw-reviews", schema_path=REVIEW_SCHEMA, dry_run=True)
    valid_rev = {
        "review_id": 9999,
        "product_id": 1002,
        "customer_hash": "a1b2c3d4e5f60718",
        "rating": 5,
        "created_at": "2026-11-10T12:30:00Z",
    }
    assert producer.validate_message(valid_rev) is True

    # Invalid rating > 5
    invalid_rev = {
        "review_id": 9999,
        "product_id": 1002,
        "customer_hash": "a1b2c3d4e5f60718",
        "rating": 10,  # Max allowed is 5!
        "created_at": "2026-11-10T12:30:00Z",
    }
    assert producer.validate_message(invalid_rev) is False


def test_producer_dry_run_send():
    producer = BaseProducer(topic="raw-products", schema_path=PRODUCT_SCHEMA, dry_run=True)
    record = {
        "product_id": 1001,
        "name": "Bếp từ",
        "price": 500000,
        "original_price": 700000,
        "crawled_at": "2026-11-10T12:00:00Z",
    }
    success = producer.send(key=1001, value=record)
    assert success is True
