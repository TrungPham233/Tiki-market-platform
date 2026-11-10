# 📋 Data Audit & Ingestion Quality Report

This report summarizes the health, distribution, and completeness of raw data collected from Tiki API.

## 1. Products Dataset (`products_raw.jsonl`)

- **Total Products Collected:** 15
- **Price Range:** 38,000 VND → 396,000 VND
- **Median Price:** 330,000 VND | **Average Price:** 309,405.0 VND
- **Average Rating:** 4.69 ⭐
- **Missing/Unknown Brand:** 0.0%
- **Missing Price:** 0.0%

### Top 5 Brands by Product Count
| Brand | Product Count |
|---|---:|
| Elmich | 3 |
| Philips | 2 |
| LocknLock | 2 |
| Daewoo | 2 |
| Sunhouse | 1 |

### Top 5 Sellers by Product Count
| Seller | Product Count |
|---|---:|
| Tiki Trading | 15 |

## 2. Customer Reviews Dataset (`reviews_raw.jsonl`)

- **Total Reviews Collected:** 165
- **Reviews with empty content:** 40.61%

### Rating Distribution
| Stars | Count |
|---|---:|
| 1 ⭐ | 6 |
| 2 ⭐ | 1 |
| 3 ⭐ | 1 |
| 4 ⭐ | 21 |
| 5 ⭐ | 136 |
