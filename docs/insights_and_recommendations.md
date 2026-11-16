# 📈 Market Insights & Recommendations

> **Audience:** Category Manager (Thiết bị nhà bếp), Commercial / Marketplace Operations
> **Data:** 2,000 products and 7,772 reviews crawled from Tiki.vn (category 1882), one crawl day.
> The crawl was capped at 2,000 products and covers prices up to 1.3M VND (lower end of the category).
> All numbers come from the Gold aggregates (`data/lakehouse/serving/agg_*.parquet`).
> "Revenue" = displayed price × `quantity_sold` shown by Tiki, so it is an estimate.

---

## BQ1 – Discount vs sales

| Discount bracket | SKUs | Median units sold | Avg units sold | Avg rating (rated SKUs) |
|:---|---:|---:|---:|---:|
| 0 – 10% | 1,600 | 2 | 37.5 | 4.43 (663) |
| 10 – 20% | 65 | 1 | 7.1 | 4.51 (18) |
| 20 – 30% | 53 | 3 | 125.1 | 4.59 (28) |
| 30 – 50% | 222 | 17 | 520.4 | 4.66 (139) |
| > 50% | 60 | 17 | 417.4 | 4.79 (37) |

- Products discounted 30%+ sell far more (median 17 vs 2 units). Above 50% the median stays at 17, while the average dips: there is no clear extra gain from going deeper than 50%.
- Ratings do **not** fall with deeper discounts.
- **Caveat:** this is a cross-section; best-sellers may simply be the ones that get promoted. A causal claim needs price history over time (being collected through daily snapshots).
- **Recommendation:** test a 30–50% cap against >50% on comparable SKUs before setting a policy.

## BQ2 – Brand concentration & 1P share

- **CR5 = 74.1%** of estimated revenue across 281 brands: Lock&Lock 27.8%, Sunhouse 20.9%, Philips 16.5%, Damas 5.9%, Kemei 3.1%.
- **Tiki Trading (1P) = 69.7%** of estimated revenue; 205 other sellers share the rest.
- **Recommendation:** negotiate exclusive launches with the top 3 brands; grow authorized brand stores to reduce dependence on 1P stock.

## BQ3 – Quality watchlist

- **67 SKUs** have ≥ 5 reviews and an average rating below 4.5 (`agg_quality_risk_watchlist`).
- **Recommendation:** review these with vendors and pause paid promotion for the lowest-rated ones.

## BQ4 – Assortment gaps

- **59 of 159** subcategory × price-segment combinations have no product (`agg_assortment_gaps`).
- **Recommendation:** check which empty cells have search demand, then prioritize seller onboarding there.

## BQ6 – Negative reviews (Voice of Customer)

- 693 reviews are 1–2★ (8.9% of all reviews).
- Keyword rules tag **10.5%** of them as after-sales issues (returns, refunds, customer service, slow delivery) and **13.9%** as product defects.
- **Most negative reviews match neither list.** The keyword approach has low coverage, so these shares are a lower bound, not a breakdown.
- **Recommendation:** label a sample of negative reviews and train a proper Vietnamese text classifier before using this for SLA decisions.
