# Midterm Data Pipeline — Execution Report
**Run ID:** `viva_verification_run_01`  
**Generated At:** `2026-08-24T03:31:05.035812+00:00`  
**Status:** **SUCCESS**

---

## 1. Input File & Engine Selection
| Property | Value |
|---|---|
| **Input File** | `orders_sample_100k.csv` |
| **File Size** | `41.77 MB` |
| **Threshold** | `200.0 MB` |
| **Engine Used** | `python_batch` |
| **Partitions / Batch Size** | `1` |

---

## 2. Ingestion & ELT Processing Metrics
| Metric | Count / Value |
|---|---|
| **Total Read Rows** | `100,000` |
| **Raw Loaded (orders_raw)** | `100,000` |
| **Valid Records** | `79,835` |
| **Corrected Records** | `14,618` |
| **Quarantined Records** | `5,547` |
| **Consistency Equation** | **`PASS`** (`Raw == Valid + Corrected + Quarantined`) |

---

## 3. Idempotent Upsert Breakdown (orders_validated)
| Upsert Result | Count |
|---|---|
| **Inserted (New Business Records)** | `93,806` |
| **Updated (Modified Existing)** | `647` |
| **Unchanged (Exact Match)** | `0` |

---

## 4. Performance & Throughput
- **Total Pipeline Time:** `53.295 seconds`
- **Overall Throughput:** `1,876.34 records/second`

---

## 5. Rules & Quarantine Error Distribution
### Cleaning Rules Triggered:
```json
{
  "RULE_01": 3347,
  "RULE_03": 1129,
  "RULE_06": 2217,
  "RULE_07": 2277,
  "RULE_02": 1146,
  "RULE_04": 1080,
  "RULE_05": 2324,
  "RULE_09": 1641
}
```

### Quarantine Reasons:
```json
{
  "CORRUPTED_ITEMS_JSON": 2021,
  "MULTIPLE_CONFLICTING_ERRORS": 674,
  "UNSAFE_EMAIL": 1343,
  "MISSING_CUSTOMER_ID": 1411,
  "MISSING_ORDER_ID": 721,
  "EMPTY_ITEMS": 677,
  "INVALID_IMPOSSIBLE_DATE": 722
}
```
