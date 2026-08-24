# Final Verification & Compliance Checklist
**Project:** Midterm Hybrid Data Pipeline (Big Data Practical)  
**Author:** Individual Student Project Submission  
**Verification Date:** 2026-08-24  
**Test Dataset:** `data/orders_sample_100k.csv` (100,000 rows, 41.77 MB) & `orders_huge_mixed_quality.csv` (30,000,000 rows, 12.65 GB)

---

## 1. Requirement Compliance & Verification Matrix

| # | Specification / Requirement | Verification Status | Test Command | Actual Empirical Result & Evidence |
|---|---|---|---|---|
| 1 | **Single Unified Entry Point** | **VERIFIED** | `python -m src.main --input orders_huge_mixed_quality.csv --db-name midterm_ecommerce_final` | Executes full lifecycle (Routing -> Load -> ELT -> Metrics) via `src/main.py`. |
| 2 | **File Router (200MB Threshold)** | **VERIFIED** | `python -m src.file_router --file data/orders_sample_100k.csv`<br>`python -m src.file_router --file orders_huge_mixed_quality.csv` | Correctly selected `python_batch` for 41.77 MB file and `pyspark` for 12,650.32 MB file. |
| 3 | **Reproducible Small Sample Generator** | **VERIFIED** | `python -m src.create_small_sample --input orders_huge_mixed_quality.csv --output data/orders_sample_100k.csv --rows 100000` | Streamed exactly 100,000 rows to 41.77 MB CSV using RFC-4180 streaming with $O(1)$ RAM. |
| 4 | **Python Streaming Batch Loader** | **VERIFIED** | `python -m src.batch_loader data/orders_sample_100k.csv 10000` | Loaded 100K rows into `orders_raw` in 10 batches in 5.658s (~17,674 rows/s) with 0 errors. |
| 5 | **PySpark Distributed Loader** | **VERIFIED** | `python -m src.spark_loader orders_huge_mixed_quality.csv` | Distributed 30,000,000 rows across 99 parallel partitions in 626.106s (47,915 rows/s). |
| 6 | **MongoDB Spark Connector Integration** | **VERIFIED** | PySpark DataFrame write test & verification via PyMongo | Parallel DataFrame write of 30M records verified with `mongo-spark-connector_2.13:10.4.0`. |
| 7 | **ELT Paradigm (Raw First)** | **VERIFIED** | DB inspection of `orders_raw` before ELT step | 30,000,000 raw documents persisted with exact original string values before any transformation. |
| 8 | **9 Deterministic Quality Rules** | **VERIFIED** | `python -m pytest tests/test_cleaning_rules.py -v` | 11 unit tests passed in 0.08s covering all 9 rules, audit trail, and quarantine conditions. |
| 9 | **Audit Trail Tracking** | **VERIFIED** | Inspection of `orders_validated.corrections` | Corrected documents capture `rule_code`, `field`, `original_value`, `corrected_value`, `reason`. |
| 10 | **3-Way Record Classification** | **VERIFIED** | `python -m src.elt_pipeline run_f62e363b859e --db-name midterm_ecommerce_final` | Exactly 23,976,182 Valid, 4,344,725 Corrected, and 1,679,093 Quarantined records classified. |
| 11 | **Quarantine Error Codes & Storage** | **VERIFIED** | Inspection of `orders_quarantine` | 1,679,093 bad records isolated with codes (`MISSING_CUSTOMER_ID`, `CORRUPTED_ITEMS_JSON`, etc.). |
| 12 | **Stable Business Key (`id_order`)** | **VERIFIED** | Inspection of MongoDB indexes via `src/mongo_setup.py` | Unique Index `uniq_id_order` created and enforced on `orders_validated`. |
| 13 | **Idempotent Upsert Implementation** | **VERIFIED** | Full-scale streaming bulk upsert | 28,121,300 unique entities inserted; 199,607 duplicate occurrences updated in-place without duplicate keys. |
| 14 | **Record Update Scenario** | **VERIFIED** | `python -m tests.run_verification` | Modified raw record updated in `orders_validated` while total collection count remained unchanged. |
| 15 | **Strict Consistency Equation** | **VERIFIED** | Automated assertion in `src/elt_pipeline.py` | $\text{Raw (30,000,000)} == \text{Valid (23,976,182)} + \text{Corrected (4,344,725)} + \text{Quarantine (1,679,093)}$. |
| 16 | **Metrics & Results JSON** | **VERIFIED** | Inspection of `reports/results.json` | Fully populated JSON matching exact database counts, throughput, and error distributions. |
| 17 | **Clean Git Repository** | **VERIFIED** | `git status` & `git log` | Clean working tree; heavy CSVs, JARs, and caches excluded via `.gitignore`. |

---

## 2. Quantitative Verification Summary (Full-Scale 12.65 GB / 30,000,000 Rows)

### 2.1 Classification Breakdown (Row Level):
- **Total Ingested Raw Records:** `30,000,000`
- **Valid Records (Clean):** `23,976,182` (79.92%)
- **Corrected Records (Audited):** `4,344,725` (14.48%)
- **Quarantined Records (Isolated):** `1,679,093` (5.60%)
- **Consistency Verification:** **`PASS`** ($30,000,000 = 23,976,182 + 4,344,725 + 1,679,093$)

### 2.2 Upsert & Entity Resolution Breakdown (Collection Level):
> [!NOTE]
> Classification counts (30M rows) and Upsert counts (28.12M entities) represent different dimensions: multiple raw rows sharing the same business key are merged into a single business document in `orders_validated`.

- **Validated Inserted (New Entities):** `28,121,300`
- **Validated Updated (Merged Duplicates):** `199,607`
- **Quarantine Inserted:** `1,679,093`
- **Total Processed Business Records:** `28,121,300 + 199,607 = 28,320,907` (Exactly equals `Valid + Corrected`).

### 2.3 Performance & Throughput:
- **PySpark Stage 0 Load Time:** `626.106 seconds` (~10.43 mins) at `47,915.21 rows/s` across 99 partitions.
- **Streaming ELT Stage 1 Time:** `14,866.692 seconds` (~4.13 hours) at `2,017.93 rows/s`.
- **Overall Pipeline Execution Time:** `15,493.115 seconds` (~4.30 hours).
- **Overall Pipeline Throughput:** `1,936.34 records/second`.

---

## 3. Architectural Boundary Statements

1. **Nature of `source_row_number` in PySpark:**
   - In Python Batch, `source_row_number` represents the exact physical line number in the CSV.
   - In PySpark, `source_row_number` is a globally unique 64-bit distributed identifier generated via `monotonically_increasing_id()`. It guarantees record uniqueness across partitions without triggering an expensive global distributed sort.

2. **MongoDB Storage Engine Placement:**
   - WiredTiger storage and database journal resided on drive `D:\MongoDB\data` ensuring disk safety headroom (>15 GB free space maintained throughout the entire run).

---

## 4. Final Verdict

**OVERALL VERDICT:**  
**ALL BIG DATA PRACTICAL PIPELINE REQUIREMENTS FULLY VERIFIED & EMPIRICALLY PROVEN ON THE COMPLETE 12.65 GB (30,000,000 ROWS) DATASET.**
