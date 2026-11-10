# 📊 Power BI Dashboard Architecture & DAX Specification

## 1. Connection & Data Model (Star Schema)

### Data Source
- **Connector:** PostgreSQL database (`localhost:5432`, db: `tiki_dwh`, schema: `dwh` / `analytics`)
- **Alternative (Local file):** SQLite via ODBC / Parquet direct import from `data/lakehouse/gold/`
- **Storage Mode:** Import (recommended for fast DAX calculation and offline demo) or DirectQuery.

### Relationship Diagram
```
  dwh.dim_brand (1)       ──────< (*) dwh.fact_product_snapshot
  dwh.dim_seller (1)      ──────< (*) dwh.fact_product_snapshot
  dwh.dim_subcategory (1) ──────< (*) dwh.fact_product_snapshot
  dwh.dim_date (1)        ──────< (*) dwh.fact_product_snapshot
  dwh.dim_product (1)     ──────< (*) dwh.fact_product_snapshot
  dwh.dim_product (1)     ──────< (*) dwh.fact_reviews
  dwh.dim_date (1)        ──────< (*) dwh.fact_reviews
```

> [!NOTE]
> `dim_seller` does not link directly to `fact_reviews` in the source data. When slicing reviews by seller, use the DAX `TREATAS` measure provided below to avoid ambiguous snowflake joins.

---

## 2. Seven Core Dashboard Pages

### 📄 Page 1: Executive Market Overview
- **Header:** Category KPI Cards:
  - `[Total Estimated Revenue]`
  - `[Total Quantity Sold]`
  - `[Total Active SKUs]`
  - `[Category Average Rating]`
  - `[CR5 Index %]`
- **Visuals:**
  - Revenue & Units sold by Subcategory (Clustered Bar Chart)
  - Rating distribution across Top 10 Brands (Gauge / Bullet Chart)
  - Live Alert Banner: Most recent steep price drops from `dwh.alerts`

### 📄 Page 2: Pricing & Discount Saturation (BQ1)
- **Business Question:** At what discount level does additional discounting stop driving sales?
- **Visuals:**
  - Average Units Sold per SKU by `discount_bracket` (0-10%, 10-20%, 20-30%, 30-50%, >50%) (Column Chart)
  - Scatter Plot: `discount_rate_calc` (X-axis) vs `quantity_sold` (Y-axis), bubble size = `revenue`
  - Slicer: Price Segments (`Dưới 200k`, `200k - 500k`, `500k - 1.5M`, `1.5M - 3M`, `Trên 3M`)

### 📄 Page 3: Brand & Seller Market Share (BQ2)
- **Business Question:** Who controls the category? Is the market overly concentrated?
- **Visuals:**
  - Market Share by Estimated Revenue (Treemap)
  - Top 5 Brands Concentration Ratio (CR5 KPI Card: ~98.55%)
  - Seller Breakdown: Official Store (Tiki Trading) vs 3rd Party Sellers (Donut Chart)

### 📄 Page 4: Customer Perception & Quality Risk Watchlist (BQ3)
- **Business Question:** Which products pose severe reputational risks?
- **Visuals:**
  - Scatter Matrix: Review Count (X-axis) vs Rating Average (Y-axis)
  - Table: **Quality Risk Watchlist** (Filtered where `review_count >= 5` AND `rating_average < 4.5`)
    - Columns: Product Name, Brand, Price, Rating, Total Reviews, Complaints Flagged

### 📄 Page 5: Assortment & Merchandising Gap Analysis (BQ4)
- **Business Question:** Which subcategories lack mainstream price offerings?
- **Visuals:**
  - Heatmap Matrix: Rows = `subcategory_name`, Columns = `price_segment`, Values = `Count of SKUs`
  - Conditional formatting: Red = 0 or low SKU count (White-space opportunity for Category Manager to onboard new sellers)

### 📄 Page 6: Marketing Priority Matrix
- **Business Question:** Where should promotional and marketing budgets be allocated?
- **Visuals:**
  - 4-Quadrant Bubble Chart:
    - **Quadrant I (Stars):** High Sales Volume + High Rating $\to$ Prioritize for Homepage Banners & Mega Sale Campaigns.
    - **Quadrant II (Opportunities):** Low Volume + High Rating $\to$ Boost ad spend / SEO indexing.
    - **Quadrant III (Risks):** High Volume + Low Rating $\to$ Urgent supplier audit / return policy check.
    - **Quadrant IV (Low Priority):** Low Volume + Low Rating $\to$ Phase out / delist.

### 📄 Page 7: Voice of Customer (VoC) Complaint Root-Causes (BQ6)
- **Business Question:** What drives customer negative reviews (1-2 stars)?
- **Visuals:**
  - Breakdown: **After-sales / CSKH complaints** vs **Hardware technical defects** (Donut Chart)
  - Insight Callout: Demonstrates that customer dissatisfaction on Tiki is predominantly driven by returns, delays, and dispute resolution rather than actual hardware breakdowns!
  - Word cloud / Top n-grams frequency table

---

## 3. Production DAX Formula Catalog

```dax
// 1. Total Estimated Revenue
Total Estimated Revenue = 
SUMX(
    fact_product_snapshot,
    fact_product_snapshot[price] * fact_product_snapshot[quantity_sold]
)

// 2. Total Volume Sold
Total Volume Sold = 
SUM(fact_product_snapshot[quantity_sold])

// 3. Category Average Rating
Average Rating = 
AVERAGE(fact_product_snapshot[rating_average])

// 4. CR5 Concentration Ratio (Top 5 Brands Revenue Share %)
CR5 Index % = 
VAR Top5Revenue = 
    CALCULATE(
        [Total Estimated Revenue],
        TOPN(5, ALL(dim_brand[brand_name]), [Total Estimated Revenue], DESC)
    )
VAR TotalRevenueAll = 
    CALCULATE([Total Estimated Revenue], ALL(dim_brand))
RETURN
    DIVIDE(Top5Revenue, TotalRevenueAll, 0)

// 5. Positive Sentiment Ratio %
Positive Review Ratio % = 
VAR PositiveCount = 
    CALCULATE(COUNTROWS(fact_reviews), fact_reviews[sentiment_label] = "positive")
VAR TotalReviews = 
    COUNTROWS(fact_reviews)
RETURN
    DIVIDE(PositiveCount, TotalReviews, 0)

// 6. After-Sales Complaint Rate in Negative Reviews %
AfterSales Complaint % = 
VAR AfterSalesCount = 
    CALCULATE(
        COUNTROWS(fact_reviews),
        fact_reviews[is_after_sales_complaint] = TRUE(),
        fact_reviews[sentiment_label] = "negative"
    )
VAR TotalNegative = 
    CALCULATE(COUNTROWS(fact_reviews), fact_reviews[sentiment_label] = "negative")
RETURN
    DIVIDE(AfterSalesCount, TotalNegative, 0)

// 7. Fact-to-Fact Virtual Join via TREATAS (Seller -> Reviews)
Reviews By Seller = 
CALCULATE(
    COUNTROWS(fact_reviews),
    TREATAS(
        VALUES(fact_product_snapshot[product_sk]),
        fact_reviews[product_sk]
    )
)
```

