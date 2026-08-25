# Midterm Data Pipeline — Execution Report
**Run ID:** `prod_30m_20260825_063326`  
**Generated At:** `2026-08-25T11:09:25.085132+00:00`  
**Status:** **SUCCESS**

---

## 1. Input File & Engine Selection
| Property | Value |
|---|---|
| **Input File** | `orders_30m.csv` |
| **File Size** | `12650.32 MB` |
| **Threshold** | `200.0 MB` |
| **Engine Used** | `pyspark` |
| **Partitions / Batch Size** | `99` |

---

## 2. Ingestion & ELT Processing Metrics
| Metric | Count / Value |
|---|---|
| **Total Read Rows** | `30,000,000` |
| **Raw Loaded (orders_raw)** | `30,000,000` |
| **Valid Records** | `23,767,068` |
| **Corrected Records** | `4,553,839` |
| **Quarantined Records** | `1,679,093` |
| **Consistency Equation** | **`PASS`** (`Raw == Valid + Corrected + Quarantined`) |

---

## 3. Idempotent Upsert Breakdown (orders_validated)
| Upsert Result | Count |
|---|---|
| **Inserted (New Business Records)** | `28,121,300` |
| **Updated (Modified Existing)** | `199,607` |
| **Unchanged (Exact Match)** | `0` |

---

## 4. Performance & Throughput
- **Total Pipeline Time:** `16558.249 seconds`
- **Overall Throughput:** `1,811.79 records/second`

---

## 5. Rules & Quarantine Error Distribution
### Cleaning Rules Triggered:
```json
{
  "RULE_05": 670062,
  "RULE_09": 500976,
  "RULE_04": 333569,
  "RULE_03": 333494,
  "RULE_01": 1003269,
  "RULE_10": 209114,
  "RULE_07": 668151,
  "RULE_06": 667720,
  "RULE_02": 333888
}
```

### Quarantine Reasons:
```json
{
  "MISSING_CUSTOMER_ID": 419474,
  "CORRUPTED_ITEMS_JSON": 629924,
  "UNSAFE_EMAIL": 418709,
  "MULTIPLE_CONFLICTING_ERRORS": 209432,
  "INVALID_IMPOSSIBLE_DATE": 210524,
  "MISSING_ORDER_ID": 209392,
  "EMPTY_ITEMS": 209934
}
```
