# Midterm Data Pipeline — Execution Report
**Run ID:** `run_f62e363b859e`  
**Generated At:** `2026-08-24T08:59:21.816617+00:00`  
**Status:** **SUCCESS**

---

## 1. Input File & Engine Selection
| Property | Value |
|---|---|
| **Input File** | `orders_huge_mixed_quality.csv` |
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
| **Valid Records** | `23,976,182` |
| **Corrected Records** | `4,344,725` |
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
- **Total Pipeline Time:** `15493.115 seconds`
- **Overall Throughput:** `1,936.34 records/second`

---

## 5. Rules & Quarantine Error Distribution
### Cleaning Rules Triggered:
```json
{
  "RULE_09": 500976,
  "RULE_01": 1003269,
  "RULE_05": 670062,
  "RULE_03": 333494,
  "RULE_07": 668151,
  "RULE_06": 667720,
  "RULE_02": 333888,
  "RULE_04": 333569
}
```

### Quarantine Reasons:
```json
{
  "INVALID_IMPOSSIBLE_DATE": 210524,
  "UNSAFE_EMAIL": 418709,
  "EMPTY_ITEMS": 209934,
  "MISSING_CUSTOMER_ID": 419474,
  "CORRUPTED_ITEMS_JSON": 629924,
  "MULTIPLE_CONFLICTING_ERRORS": 209432,
  "MISSING_ORDER_ID": 209392
}
```
