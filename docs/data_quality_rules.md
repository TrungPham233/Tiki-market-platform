# 🛡️ Data Quality & Validation Gate Specification

## 1. Overview
In a production Modern Data Platform, corrupt or invalid data must never reach the Gold layer or Power BI dashboards.
The **Data Quality Gate** operates as a circuit-breaker (Fail-Fast barrier) between the **Silver** and **Gold** layers.

```
 [Bronze Layer] ──► [Silver Layer] ──► [QUALITY GATE] ──► [Gold Star Schema]
                                              │
                                              ▼ (If assertions fail)
                                    [CIRCUIT BREAKER: HALT]
                                              │
                                              ▼
                                    [Alert Engineer / On-Call]
```

---

## 2. Quality Assertion Matrix

| Layer | Check Name | Assertion Rule | Severity | Action on Failure |
|:---|:---|:---|:---:|:---|
| **Silver Products** | `TABLE_NOT_EMPTY` | Row count $\ge 1$ | 🔴 Critical | Halt DAG; trigger on-call alert |
| **Silver Products** | `NOT_NULL_product_id` | Null count $== 0$ | 🔴 Critical | Halt DAG; quarantine batch |
| **Silver Products** | `NOT_NULL_name` | Null count $== 0$ | 🔴 Critical | Halt DAG |
| **Silver Products** | `NOT_NULL_price` | Null count $== 0$ | 🔴 Critical | Halt DAG |
| **Silver Products** | `UNIQUE_product_id` | Duplicate count $== 0$ | 🔴 Critical | Halt DAG; prevent skew |
| **Silver Products** | `RANGE_rating_average` | $0.0 \le \text{rating} \le 5.0$ | 🔴 Critical | Halt DAG |
| **Silver Products** | `ANOMALY_is_price_anomaly` | Anomaly rate $\le 5.0\%$ | 🟡 Warning | Log alert; promote valid rows |
| **Silver Reviews** | `TABLE_NOT_EMPTY` | Row count $\ge 1$ | 🔴 Critical | Halt DAG |
| **Silver Reviews** | `NOT_NULL_review_id` | Null count $== 0$ | 🔴 Critical | Halt DAG |
| **Silver Reviews** | `UNIQUE_review_id` | Duplicate count $== 0$ | 🔴 Critical | Halt DAG |
| **Silver Reviews** | `RANGE_rating` | $1 \le \text{rating} \le 5$ | 🔴 Critical | Halt DAG |

---

## 3. SLA & Anomaly Handling
- **Tiki Display Discrepancy:** When Tiki reports `discount_rate = 0%` but actual calculation is $15\%$, the record is flagged with `is_discount_discrepancy = True` for audit without blocking business dashboards.
- **Circuit Breaker:** If critical checks fail, the DAG raises `DataQualityError` and stops before triggering Gold dimension/fact builds.
- **Audit Artifact:** Results are persisted to `data/lakehouse/silver/data_quality_report.json` on every run for governance and compliance.
