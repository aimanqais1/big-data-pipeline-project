# Empirical Validation Summary (100K & 30M Runs)

This document provides a concise, verified summary of both the **100,000-row sample validation** and the **30,000,000-row full-scale production run**.

> **Note on Evidence Artifacts:** The full raw evidence packages (raw logs, per-minute monitor logs, JSON dumps, execution traces) are preserved in local directories outside Git (project_evidence/100k_analysis/ and project_evidence/30m_analysis/final_production_run/) to keep the repository lightweight and GitHub-safe.

---

## 1. Executive Comparison: 100K vs. 30M

| Metric / Dimension | 100K Validation Run | 30M Production Run | Status |
|---|---|---|---|
| **Input File** | data/orders_sample_100k.csv | data/orders_30m.csv | Verified |
| **Input Size** | 41.77 MB | 12,650.32 MB (~12.35 GB) | Verified |
| **Engine Selected** | Python Streaming Batch Loader | PySpark Distributed DataFrame | Verified (File Router >200MB threshold) |
| **Partitions / Batches** | 10,000 batch size ((1)$ RAM) | 99 Spark partitions, 10K ELT batch | Verified |
| **Total Rows Read** | **100,000** | **30,000,000** | 100% Ingested |
| **Valid Records** | 79,158 (79.16%) | 23,767,068 (79.22%) | Consistent (~79.2%) |
| **Corrected Records** | 15,295 (15.30%) | 4,553,839 (15.18%) | Consistent (~15.2%) |
| **Quarantined Records** | 5,547 (5.55%) | 1,679,093 (5.60%) | Consistent (~5.6%) |
| **Accounting Equation** | ,000 = 79,158 + 15,295 + 5,547$ | ,000,000 = 23,767,068 + 4,553,839 + 1,679,093$ | **PASS** (Zero loss) |
| **orders_raw** | 100,000 | 30,000,000 | Exact Match |
| **orders_validated** | 93,806 (upserted unique entities) | 28,121,300 (upserted unique entities) | Exact Match |
| **orders_quarantine** | 5,547 | 1,679,093 | Exact Match |
| **Validated Inserted** | 93,806 | 28,121,300 | Exact Match |
| **Validated Updated** | 647 (duplicate orders merged) | 199,607 (duplicate orders merged) | Idempotent |
| **Duplicate id_order in Validated** | **0** | **0** | Enforced by Unique Index |
| **Negative Financials in Validated** | **0** | **0** | Enforced by Rule 10 & Schema |
| **MongoDB $jsonSchema Failures** | **0** | **0** | 100% Strict Conformance |
| **Automated Tests (PyTest)** | 19 / 19 PASSED | 19 / 19 PASSED | 100% Green |

---

## 2. 100K Sample Validation Details

- **Run Purpose:** Comprehensive end-to-end logic, schema, rule correctness, and idempotency proof before production scale.
- **Database:** midterm_ecommerce
- **Execution Summary:**
  - orders_raw: 100,000 rows
  - orders_validated: 93,806 documents (79,158 clean valid + 15,295 corrected, minus 647 superseded duplicates)
  - orders_quarantine: 5,547 documents
  - Consistency Check: 100,000 = 79,158 + 15,295 + 5,547 -> **PASS**
- **RULE_10 Reconciliation (Negative Payments):**
  - Exactly 677 raw negative-payment records were present in the 100K dataset.
  - 677/677 satisfied the deterministic matching condition ($|payment| = total$).
  - 674 corrected records were persisted into orders_validated.
  - 3 records were subsequently updated/superseded by later positive duplicate occurrences of the same id_order.
  - Complete 677-row reconciliation: **PASS**.
- **PyTest Suite:** 19/19 passing tests covering all rules (RULE_01 to RULE_10), schema validation, and CSV parity.

---

## 3. 30M Full-Scale Production Run Details

- **Run ID:** prod_30m_20260825_063326
- **Database:** midterm_ecommerce_30m_production
- **Engine:** PySpark (file size 12.65 GB exceeds 200 MB threshold)
- **Partitions:** 99 input partitions
- **Timing & Performance:**
  - **Spark Raw Ingestion:** ~13 minutes (30M CSV rows -> MongoDB orders_raw)
  - **Streaming ELT & Classification:** ~263 minutes (4h 23m)
  - **Total Elapsed:** 16,558.25s (~4h 35m 58s / 275.97 min)
  - **Overall Throughput:** **1,811.79 rows/second**
  - **ELT Streaming Throughput:** **1,894.43 rows/second**
- **Resource Footprint & Safety:**
  - **Peak RAM:** 21.85 GB (68.0% utilization on 32 GB system; no OOM risk)
  - **RAM at Completion:** 18.43 GB (57.8%)
  - **Peak CPU:** 100% during parallel Spark ingestion; average ~46% during streaming ELT
  - **Disk Space:** 23.2 GB free remaining on Drive D at completion
  - **Resource Monitor:** 271 continuous 60-second health snapshots logged
- **Source Row Number Architecture:**
  - Replaced non-conforming monotonically_increasing_id() (which generates 64-bit sparse values up to 51B) with a native Spark cumulative partition-offset algorithm.
  - Generates exact deterministic sequential row numbers 1, 2, ..., 30,000,000 cast explicitly to IntegerType / BSON Int32.
  - Memory Safety: Only a tiny metadata collection of 99 partition counts is performed on the Driver (<2 KB); no full dataset is collected.
  - Validated range: Min = 1, Max = 30,000,000 (0 gaps, 0 duplicates).
- **Rule Applications in 30M:**
  - RULE_01 (Arabic digits): 1,003,269
  - RULE_02 (Currency text): 333,888
  - RULE_03 (Thousands separators): 333,494
  - RULE_04 (Known number words): 333,569
  - RULE_05 (Phone normalization): 670,062
  - RULE_06 (Email cleanup): 667,720
  - RULE_07 (Date normalization): 668,151
  - RULE_09 (Total recalculation): 500,976
  - RULE_10 (Negative payment matching): 209,114
  - Total rule repairs applied: ~4,720,243
- **Quarantine Breakdown in 30M:**
  - CORRUPTED_ITEMS_JSON: 629,924
  - MISSING_CUSTOMER_ID: 419,474
  - UNSAFE_EMAIL: 418,709
  - MULTIPLE_CONFLICTING_ERRORS: 209,432
  - INVALID_IMPOSSIBLE_DATE: 210,524
  - EMPTY_ITEMS: 209,934
  - MISSING_ORDER_ID: 209,392
  - Total Quarantined: **1,679,093** (5.60%)

---

## 4. Reference Databases Integrity

The production pipeline ran against midterm_ecommerce_30m_production in complete isolation:
- midterm_ecommerce (100K reference): Untouched and preserved.
- midterm_ecommerce_final (historical 30M reference): Untouched and preserved.
