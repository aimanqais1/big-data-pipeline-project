# Final Verification & Compliance Checklist
**Project:** Midterm Hybrid Data Pipeline (Big Data Practical)  
**Author:** Individual Student Project Submission  
**Verification Date:** 2026-08-24  
**Test Dataset:** `data/orders_sample_100k.csv` (100,000 rows, 41.77 MB) & `orders_huge_mixed_quality.csv` (12.65 GB)

---

## 1. Requirement Compliance & Verification Matrix

| # | Specification / Requirement | Verification Status | Test Command | Actual Empirical Result & Evidence |
|---|---|---|---|---|
| 1 | **Single Unified Entry Point** | **VERIFIED** | `python -m src.main --input data/orders_sample_100k.csv --reset-db` | Executes full lifecycle (Routing -> Load -> ELT -> Metrics) via `src/main.py`. |
| 2 | **File Router (200MB Threshold)** | **VERIFIED** | `python -m src.file_router --file data/orders_sample_100k.csv`<br>`python -m src.file_router --file orders_huge_mixed_quality.csv` | Correctly selected `python_batch` for 41.77 MB file and `pyspark` for 12,650.32 MB file. |
| 3 | **Reproducible Small Sample Generator** | **VERIFIED** | `python -m src.create_small_sample --input orders_huge_mixed_quality.csv --output data/orders_sample_100k.csv --rows 100000` | Streamed exactly 100,000 rows to 41.77 MB CSV using RFC-4180 streaming with $O(1)$ RAM. |
| 4 | **Python Streaming Batch Loader** | **VERIFIED** | `python -m src.batch_loader data/orders_sample_100k.csv 10000` | Loaded 100K rows into `orders_raw` in 10 batches in 5.658s (~17,674 rows/s) with 0 errors. |
| 5 | **PySpark Distributed Loader** | **VERIFIED** | `python -m src.spark_loader data/orders_sample_100k.csv` | Read with Explicit Schema (`StringType`) and wrote parallel to MongoDB across 8 partitions. |
| 6 | **MongoDB Spark Connector Integration** | **VERIFIED** | PySpark DataFrame write test & verification via PyMongo | Parallel DataFrame write to MongoDB verified with `mongo-spark-connector_2.13:10.4.0`. |
| 7 | **ELT Paradigm (Raw First)** | **VERIFIED** | DB inspection of `orders_raw` before ELT step | 100,000 raw documents persisted with exact original string values before any transformation. |
| 8 | **9 Deterministic Quality Rules** | **VERIFIED** | `python -m pytest tests/test_cleaning_rules.py -v` | 11 unit tests passed in 0.08s covering all 9 rules, audit trail, and quarantine conditions. |
| 9 | **Audit Trail Tracking** | **VERIFIED** | Inspection of `orders_validated.corrections` | Corrected documents capture `rule_code`, `field`, `original_value`, `corrected_value`, `reason`. |
| 10 | **3-Way Record Classification** | **VERIFIED** | `python -m src.elt_pipeline viva_verification_run_01` | Exactly 79,835 Valid, 14,618 Corrected, and 5,547 Quarantined records classified. |
| 11 | **Quarantine Error Codes & Storage** | **VERIFIED** | Inspection of `orders_quarantine` | 5,547 bad records isolated with codes (`MISSING_CUSTOMER_ID`, `CORRUPTED_ITEMS_JSON`, etc.). |
| 12 | **Stable Business Key (`id_order`)** | **VERIFIED** | Inspection of MongoDB indexes via `src/mongo_setup.py` | Unique Index `uniq_id_order` created and enforced on `orders_validated`. |
| 13 | **Idempotent Upsert Implementation** | **VERIFIED** | `python -m tests.run_verification` | Run 1 inserted 93,806 entities; Run 2 on same data resulted in **0 new inserts** and 0 duplicate documents. |
| 14 | **Record Update Scenario** | **VERIFIED** | `python -m tests.run_verification` | Modified raw record updated in `orders_validated` while total collection count remained unchanged. |
| 15 | **Strict Consistency Equation** | **VERIFIED** | Automated assertion in `src/elt_pipeline.py` | $\text{Raw (100,000)} == \text{Valid (79,835)} + \text{Corrected (14,618)} + \text{Quarantine (5,547)}$. |
| 16 | **Metrics & Results JSON** | **VERIFIED** | Inspection of `reports/results.json` | Fully populated JSON matching exact database counts, throughput, and error distributions. |
| 17 | **Clean Git Repository** | **VERIFIED** | `git status` & `git log` | Clean working tree; heavy CSVs, JARs, and caches excluded via `.gitignore`. |

---

## 2. Quantitative Verification Summary (Run 1 vs Run 2 vs Update)

### 2.1 Classification Breakdown (Row Level):
- **Total Ingested Raw Records:** `100,000`
- **Valid Records (Clean):** `79,835` (79.84%)
- **Corrected Records (Audited):** `14,618` (14.62%)
- **Quarantined Records (Isolated):** `5,547` (5.55%)
- **Consistency Verification:** **`PASS`** ($100,000 = 79,835 + 14,618 + 5,547$)

### 2.2 Upsert & Entity Resolution Breakdown (Collection Level):
> [!NOTE]
> Classification counts (100K rows) and Upsert counts (93.8K entities) represent different dimensions: multiple raw rows sharing the same business key are merged into a single business document in `orders_validated`.

- **Run 1 Validated Inserted (New Entities):** `93,806`
- **Run 1 Validated Updated (Merged Duplicates):** `647`
- **Run 1 Quarantine Inserted:** `5,547`
- **Run 2 Validated Inserted:** **`0`** (No duplicates created)
- **Run 2 Quarantine Inserted:** **`0`** (No duplicates created)
- **Run 2 Difference in DB Counts:** **`0`**

### 2.3 Update Scenario Verification:
- **Target Entity:** `id_order = "طلب-100000"`
- **Original Name:** `محمد علي`
- **Modified Raw Name:** `محمد علي المحدث للتحقق`
- **Result in `orders_validated`:** Document updated successfully to `محمد علي المحدث للتحقق`.
- **Collection Count Impact:** Exactly `93,806` before and after (Diff: 0).

---

## 3. Known Limitations & Explicit Boundary Statements

1. **Full 12.65 GB End-to-End Ingestion:**
   - **Status:** **NOT VERIFIED FOR FULL 12.6GB DATASET — VERIFIED ON 100K SAMPLE & ROUTING VERIFIED**.
   - **Details:** The File Router was verified on the 12.65 GB file (correctly routing to PySpark), and PySpark's DataFrame API and MongoDB Connector were verified end-to-end on the 100K dataset. Full processing of the entire 12.65 GB file is available via `python -m src.main --input orders_huge_mixed_quality.csv` and was omitted from automated test loops to prevent disk/time constraints during rapid evaluation.

2. **Nature of `source_row_number` in PySpark:**
   - In Python Batch, `source_row_number` represents the exact physical line number in the CSV.
   - In PySpark, `source_row_number` is a globally unique 64-bit distributed identifier generated via `monotonically_increasing_id()`. It guarantees record uniqueness across partitions without triggering an expensive global distributed sort.

---

## 4. Final Verdict

**OVERALL VERDICT:**  
**CORE PIPELINE REQUIREMENTS VERIFIED ON THE 100K TEST DATASET.**  
**FULL-SCALE 12.65 GB EXECUTION REMAINS PENDING.**

