# 📋 Data Audit & Ingestion Quality Report

This report summarizes the health, distribution, and completeness of raw data collected from Tiki API.

## 1. Products Dataset (`products_raw.jsonl`)

- **Total Products Collected:** 2,000
- **Price Range:** 8,000 VND → 1,300,000 VND
- **Median Price:** 379,000 VND | **Average Price:** 385,362.0 VND
- **Average Rating:** 4.49 ⭐
- **Missing/Unknown Brand:** 0.0%
- **Missing Price:** 0.0%

### Top 5 Brands by Product Count
| Brand | Product Count |
|---|---:|
| SMY | 134 |
| MINIIN | 93 |
| Sunhouse | 80 |
| NaMilux | 70 |
| Tiross | 57 |

### Top 5 Sellers by Product Count
| Seller | Product Count |
|---|---:|
| Tiki Trading | 200 |
| SMY (Tp.HCM) | 150 |
| Home Star Store | 120 |
| GIA DỤNG VIỆT | 85 |
| Smarthome29 | 78 |

## 2. Customer Reviews Dataset (`reviews_raw.jsonl`)

- **Total Reviews Collected:** 7,772
- **Reviews with empty content:** 43.43%

### Rating Distribution
| Stars | Count |
|---|---:|
| 1 ⭐ | 530 |
| 2 ⭐ | 163 |
| 3 ⭐ | 294 |
| 4 ⭐ | 1,191 |
| 5 ⭐ | 5,594 |
