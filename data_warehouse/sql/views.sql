-- ====================================================================
-- PostgreSQL Analytics Views for Power BI DirectQuery / Import
-- ====================================================================

-- 1. Executive Market Overview (KPI Cards)
CREATE OR REPLACE VIEW analytics.vw_market_overview AS
SELECT
    COUNT(DISTINCT f.product_id) AS total_skus,
    COALESCE(SUM(f.quantity_sold), 0) AS total_units_sold,
    COALESCE(SUM(f.price * f.quantity_sold), 0) AS total_estimated_revenue,
    ROUND(AVG(f.rating_average), 2) AS category_avg_rating,
    COUNT(DISTINCT f.brand_id) AS active_brands_count,
    COUNT(DISTINCT f.seller_id) AS active_sellers_count,
    COUNT(CASE WHEN f.is_discount_discrepancy THEN 1 END) AS discount_discrepancy_count
FROM dwh.fact_product_snapshot f;

-- 2. BQ2: Brand Market Share & Volume Performance
CREATE OR REPLACE VIEW analytics.vw_brand_market_share AS
WITH brand_totals AS (
    SELECT
        b.brand_name,
        COUNT(f.product_id) AS sku_count,
        COALESCE(SUM(f.quantity_sold), 0) AS total_units_sold,
        COALESCE(SUM(f.price * f.quantity_sold), 0) AS estimated_revenue,
        ROUND(AVG(f.rating_average), 2) AS avg_rating
    FROM dwh.fact_product_snapshot f
    LEFT JOIN dwh.dim_brand b ON f.brand_id = b.brand_id
    GROUP BY b.brand_name
),
market_sum AS (
    SELECT
        SUM(estimated_revenue) AS grand_revenue,
        SUM(total_units_sold) AS grand_volume
    FROM brand_totals
)
SELECT
    bt.brand_name,
    bt.sku_count,
    bt.total_units_sold,
    bt.estimated_revenue,
    bt.avg_rating,
    ROUND((bt.estimated_revenue / NULLIF(m.grand_revenue, 0)) * 100, 2) AS revenue_share_pct,
    ROUND((bt.total_units_sold / NULLIF(m.grand_volume, 0)) * 100, 2) AS volume_share_pct
FROM brand_totals bt
CROSS JOIN market_sum m
ORDER BY bt.estimated_revenue DESC;

-- 3. BQ1: Pricing & Discount Saturation Analysis
CREATE OR REPLACE VIEW analytics.vw_discount_saturation AS
SELECT
    CASE
        WHEN f.discount_rate_calc < 10 THEN '0 - 10%'
        WHEN f.discount_rate_calc < 20 THEN '10 - 20%'
        WHEN f.discount_rate_calc < 30 THEN '20 - 30%'
        WHEN f.discount_rate_calc <= 50 THEN '30 - 50%'
        ELSE 'Trên 50%'
    END AS discount_bracket,
    COUNT(f.product_id) AS sku_count,
    ROUND(AVG(f.quantity_sold), 1) AS avg_units_sold_per_sku,
    ROUND(AVG(f.rating_average), 2) AS avg_rating,
    COALESCE(SUM(f.price * f.quantity_sold), 0) AS bracket_revenue
FROM dwh.fact_product_snapshot f
GROUP BY 1
ORDER BY MIN(f.discount_rate_calc);

-- 4. BQ3: Quality Risk Watchlist (High Review Count + Low Rating)
CREATE OR REPLACE VIEW analytics.vw_quality_risk_watchlist AS
SELECT
    p.product_id,
    p.name AS product_name,
    b.brand_name,
    s.seller_name,
    f.price,
    f.rating_average,
    f.review_count,
    f.quantity_sold
FROM dwh.fact_product_snapshot f
JOIN dwh.dim_product p ON f.product_sk = p.product_sk
LEFT JOIN dwh.dim_brand b ON f.brand_id = b.brand_id
LEFT JOIN dwh.dim_seller s ON f.seller_id = s.seller_id
WHERE f.review_count >= 5 AND f.rating_average < 4.5
ORDER BY f.rating_average ASC, f.review_count DESC;

-- 5. BQ4: Assortment Matrix (Price Segment x Subcategory)
CREATE OR REPLACE VIEW analytics.vw_assortment_matrix AS
SELECT
    COALESCE(sub.subcategory_name, 'Chưa phân loại') AS subcategory_name,
    f.price_segment,
    COUNT(f.product_id) AS sku_count,
    COALESCE(SUM(f.quantity_sold), 0) AS units_sold,
    ROUND(AVG(f.price), 0) AS avg_segment_price
FROM dwh.fact_product_snapshot f
LEFT JOIN dwh.dim_subcategory sub ON f.subcategory_id = sub.subcategory_id
GROUP BY 1, 2
ORDER BY 1, 2;

-- 6. BQ6: Voice of Customer (VoC) Complaint Root Causes
CREATE OR REPLACE VIEW analytics.vw_voice_of_customer AS
SELECT
    COUNT(*) AS total_negative_reviews,
    SUM(CASE WHEN is_after_sales_complaint THEN 1 ELSE 0 END) AS after_sales_complaints,
    SUM(CASE WHEN is_defect_complaint THEN 1 ELSE 0 END) AS hardware_defect_complaints,
    ROUND((SUM(CASE WHEN is_after_sales_complaint THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0)) * 100, 2) AS after_sales_pct,
    ROUND((SUM(CASE WHEN is_defect_complaint THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0)) * 100, 2) AS hardware_defect_pct
FROM dwh.fact_reviews
WHERE sentiment_label = 'negative';

-- 7. Live Flash Sale & Price Drop Alerts
CREATE OR REPLACE VIEW analytics.vw_live_price_alerts AS
SELECT
    alert_id,
    alert_type,
    product_name,
    brand_name,
    seller_name,
    old_price,
    new_price,
    price_drop_pct,
    savings_amount,
    triggered_at
FROM dwh.alerts
ORDER BY triggered_at DESC;

