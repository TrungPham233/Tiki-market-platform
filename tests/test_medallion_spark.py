"""
Unit Tests for Phase 3: Medallion Architecture, Transformations & Business Logic.
"""
from spark.jobs.bronze_to_silver_products import normalize_brand
from spark.jobs.bronze_to_silver_reviews import assign_sentiment, clean_html, contains_any, AFTER_SALES_KEYWORDS, DEFECT_KEYWORDS
from spark.streaming.price_alert_stream import evaluate_price_event


def test_normalize_brand():
    assert normalize_brand("locknlock") == "Lock&Lock"
    assert normalize_brand("Lock and Lock") == "Lock&Lock"
    assert normalize_brand("PHILIPS") == "Philips"
    assert normalize_brand("Sunhouse") == "Sunhouse"
    assert normalize_brand("") == "Unknown"
    assert normalize_brand(None) == "Unknown"


def test_clean_html_reviews():
    raw_html = "<p>Sản phẩm rất tốt!<br>Đóng gói cẩn thận.</p>"
    cleaned = clean_html(raw_html)
    assert "<p>" not in cleaned
    assert "<br>" not in cleaned
    assert "Sản phẩm rất tốt! Đóng gói cẩn thận." in cleaned


def test_sentiment_labeling():
    assert assign_sentiment(5) == "positive"
    assert assign_sentiment(4) == "positive"
    assert assign_sentiment(3) == "neutral"
    assert assign_sentiment(2) == "negative"
    assert assign_sentiment(1) == "negative"


def test_voc_complaint_categorization():
    text_after_sales = "Tôi muốn đổi trả hàng và hoàn tiền vì chờ cskh giải quyết quá lâu!"
    assert contains_any(text_after_sales, AFTER_SALES_KEYWORDS) is True
    assert contains_any(text_after_sales, DEFECT_KEYWORDS) is False

    text_defect = "Máy vừa cắm điện đã bốc khói, cháy và không lên nguồn."
    assert contains_any(text_defect, DEFECT_KEYWORDS) is True


def test_evaluate_price_alert_flash_sale():
    event_flash = {
        "product_id": 999,
        "product_name": "Nồi chiên không dầu",
        "old_price": 2000000,
        "new_price": 1100000,
        "price_change_pct": -45.0,
        "is_flash_sale": True,
        "brand_name": "Philips",
        "seller_name": "Tiki Trading",
    }
    alert = evaluate_price_event(event_flash)
    assert alert != {}
    assert alert["alert_type"] == "FLASH_SALE_DETECTED"
    assert alert["price_drop_pct"] == 45.0
    assert alert["savings_amount"] == 900000


def test_evaluate_price_alert_normal_no_trigger():
    event_normal = {
        "product_id": 999,
        "old_price": 1000000,
        "new_price": 950000,
        "price_change_pct": -5.0,
        "is_flash_sale": False,
    }
    alert = evaluate_price_event(event_normal)
    assert alert == {}
