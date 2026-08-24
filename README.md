# Hybrid Data Pipeline for E-Commerce Orders (Midterm Project)
**Course:** Big Data – Practical | **Institution:** Razee University  
**Project Type:** Individual Student Data Engineering Pipeline  
**Core Technologies:** Python 3.12, Apache Spark 4.2.0 (PySpark), MongoDB 7.0, OpenJDK 17 Temurin LTS

---

## 1. Executive Summary & Project Overview

This project implements a production-grade, reproducible **Hybrid Data Pipeline** engineered to ingest, clean, audit, classify, and idempotently store e-commerce order records from large-scale, mixed-quality CSV datasets.

The system features an automated **File Router** that dynamically selects the optimal processing engine based on input file size:
- **Small Files ($\le$ 200 MB):** Processed via a **Python Streaming Batch Loader** using low-overhead, constant-memory ($O(1)$) generator streams.
- **Large Files (> 200 MB):** Processed via a **PySpark Distributed DataFrame Loader** with an explicit schema and parallel multi-partition ingestion.

All data strictly follows the **ELT paradigm (Extract $\to$ Load $\to$ Transform)**, loading raw records into MongoDB (`orders_raw`) with complete fidelity before applying **9 deterministic data quality rules**, recording an end-to-end **Audit Trail**, performing a **3-way classification** (`Valid`, `Corrected`, `Quarantined`), and persisting business entities via **Idempotent Upsert** on a **Stable Business Key** (`id_order`).

---

## 2. End-to-End Architecture & Data Flow

```
+-------------------------------------------------------------------------------+
|                       Incoming CSV Dataset (Mixed Quality)                   |
+-------------------------------------------------------------------------------+
                                       |
                                       v
+-------------------------------------------------------------------------------+
|             Unified Entrypoint: src/main.py -> src/file_router.py            |
|       Computes File Size in MB | Generates Unique id_run | Evaluates 200MB    |
+-------------------------------------------------------------------------------+
                    /                                     \
    (File Size <= 200 MB)                             (File Size > 200 MB)
                  /                                         \
                 v                                           v
+------------------------------------+      +-----------------------------------+
|     src/batch_loader.py            |      |     src/spark_loader.py           |
| Python Streaming CSV Generator     |      | PySpark DataFrame API (Spark 4.2) |
| Batch Size: 10,000 | Memory: O(1)  |      | Explicit Schema | StringType Raw  |
| pymongo insert_many (ordered=False)|      | MongoDB Spark Connector 10.4.0    |
+------------------------------------+      +-----------------------------------+
                  \                                         /
                   \-------------------   -----------------/
                                       \ /
                                        v
+-------------------------------------------------------------------------------+
|                     RAW ELT LAYER: MongoDB `orders_raw`                       |
|   Untransformed CSV Strings Preserved + Metadata (id_run, row_number, time)  |
+-------------------------------------------------------------------------------+
                                       |
                                       v
+-------------------------------------------------------------------------------+
|       ELT TRANSFORMATION & QUALITY RULES: src/elt_pipeline.py                 |
|                   Engineered via src/quality_rules.py                         |
|   - 9 Deterministic Rules (Arabic digits, Currencies, Dates, Emails, Totals)  |
|   - Fine-grained Audit Trail: [{rule_code, field, original, corrected}]      |
+-------------------------------------------------------------------------------+
                                       |
                                       v
+-------------------------------------------------------------------------------+
|                     3-WAY RECORD CLASSIFICATION                               |
+-------------------------------------------------------------------------------+
       /                               |                               \
      v                                v                                v
[ Valid Record ]              [ Corrected Record ]            [ Quarantined Record ]
Passed 100% clean             Applied safe rules              Irrecoverable errors
No corrections needed         Has Audit Trail                 (Corrupted JSON, Missing ID)
      \                               /                                 |
       \                             /                                  v
        v                           v                 +-----------------------------------+
+--------------------------------------------------+  | MongoDB `orders_quarantine`       |
| Streaming Batch Upsert (UpdateOne, upsert=True)  |  | Compound Key (id_run, row_number) |
| Target Collection: MongoDB `orders_validated`    |  | Error Codes & Original raw_record |
| Unique Index on Stable Business Key (`id_order`) |  +-----------------------------------+
+--------------------------------------------------+
                                       |
                                       v
+-------------------------------------------------------------------------------+
|                     STRICT CONSISTENCY EQUATION CHECK                         |
|           Raw Ingested == Valid + Corrected + Quarantined (100% Match)        |
+-------------------------------------------------------------------------------+
                                       |
                                       v
+-------------------------------------------------------------------------------+
|           METRICS & AUDIT REPORTING: src/metrics.py -> reports/results.json   |
+-------------------------------------------------------------------------------+
```

---

## 3. Verified Environment & Compatibility Matrix

| Component | Verified Version | Configuration / Location | Purpose |
|---|---|---|---|
| **Python** | `3.12.4` | `C:\Users\MC\AppData\Local\Programs\Python\Python312` | Runtime engine & batch scripts |
| **Java JDK** | `OpenJDK 17.0.20 Temurin LTS` | `C:\Users\MC\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.20.8-hotspot` | Standard LTS runtime for Spark 4.2 |
| **Apache Spark** | `4.2.0` | PySpark 4.2.0 with Scala 2.13.18 | Distributed DataFrame processing |
| **MongoDB** | `7.0.39` | `mongodb://localhost:27017/` | Persistent document database |
| **Spark Connector**| `mongo-spark-connector_2.13:10.4.0` | Local Classpath `jars/*.jar` | Parallel DataFrame write to MongoDB |
| **PyTest** | `9.1.1` | `pytest tests/` | Automated unit testing framework |

---

## 4. Project Directory Structure

```
midterm-data-pipeline/
├── README.md                          # Comprehensive project documentation
├── requirements.txt                   # Production Python dependencies
├── .gitignore                         # Strict exclusion for heavy datasets, jars, envs
│
├── config/
│   ├── __init__.py
│   └── settings.py                    # Single Source of Truth for all configurations
│
├── data/
│   ├── .gitkeep                       # Preserves directory in git
│   └── orders_sample_100k.csv         # Generated 100K test sample (41.77 MB)
│
├── src/
│   ├── __init__.py
│   ├── main.py                        # Single unified CLI entry point
│   ├── file_router.py                 # File size inspection & engine routing
│   ├── create_small_sample.py         # RFC-4180 streaming sample generator
│   ├── batch_loader.py                # Python streaming batch loader (O(1) RAM)
│   ├── spark_loader.py                # PySpark DataFrame loader (Explicit schema)
│   ├── quality_rules.py               # 9 deterministic quality rules & audit trail
│   ├── elt_pipeline.py                # Streaming ELT, classification & idempotent upsert
│   ├── mongo_setup.py                 # MongoDB collections & unique indexes initialization
│   ├── incremental_loader.py          # Documented extension module (Optional Path B)
│   └── metrics.py                     # Results metrics exporter (JSON & MD)
│
├── tests/
│   ├── __init__.py
│   ├── test_cleaning_rules.py         # PyTest test cases for all 9 rules & quarantine
│   └── run_verification.py            # Automated end-to-end verification runner
│
├── reports/
│   ├── results.json                   # Machine-readable execution metrics
│   ├── results.md                     # Human-readable execution summary
│   └── screenshots/                   # Demonstration visual evidence directory
│
└── docs/
    ├── architecture.md                # In-depth architectural & design decisions
    ├── viva_questions.md              # 25+ detailed Viva examination questions & answers
    └── FINAL_VERIFICATION.md          # Formal verification checklist with empirical results
```

---

## 5. Installation & Setup Guide

### 5.1 Clone Repository
```bash
git clone <repository-url>
cd midterm-data-pipeline
```

### 5.2 Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 5.3 Configure Java Environment
Ensure `JAVA_HOME` points to OpenJDK 17 (auto-configured in `config/settings.py`):
```powershell
$env:JAVA_HOME = "C:\Users\MC\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.20.8-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

### 5.4 Verify MongoDB Connection
Ensure MongoDB 7.0 is running on `localhost:27017`:
```bash
python -m src.mongo_setup --reset
```

---

## 6. Dataset Placement & Sample Generation

### 6.1 Original Dataset
Place the original dataset in the project root:
```text
orders_huge_mixed_quality.csv  (12.65 GB)
```

### 6.2 Generating the Small Sample (100,000 Rows)
To extract a reproducible 100K sample without loading the 12.6 GB dataset into RAM:
```bash
python -m src.create_small_sample --input orders_huge_mixed_quality.csv --output data/orders_sample_100k.csv --rows 100000
```
- **Output:** `data/orders_sample_100k.csv` (Size: 41.77 MB).

---

## 7. Execution Commands

### 7.1 Running the Unified Pipeline (Recommended)
```bash
# Process small sample (Triggers Python Streaming Batch Loader)
python -m src.main --input data/orders_sample_100k.csv --reset-db

# Process large file (Triggers PySpark Distributed Loader)
python -m src.main --input orders_huge_mixed_quality.csv
```

### 7.2 Running Individual Modules
```bash
# 1. File Router Inspection
python -m src.file_router --file data/orders_sample_100k.csv
python -m src.file_router --file orders_huge_mixed_quality.csv

# 2. Python Streaming Batch Loader
python -m src.batch_loader data/orders_sample_100k.csv 10000

# 3. PySpark Distributed Loader
python -m src.spark_loader data/orders_sample_100k.csv

# 4. ELT Transformation & Classification
python -m src.elt_pipeline <id_run>
```

### 7.3 Running Automated Unit Tests & Verification
```bash
# Run PyTest Unit Tests
python -m pytest tests/test_cleaning_rules.py -v

# Run End-to-End Idempotency & Update Verification
python -m tests.run_verification
```

---

## 8. Data Quality Rules & Audit Trail

The pipeline implements **9 strictly deterministic rules**:

| Code | Rule Name | Problem Solved | Corrective Action |
|---|---|---|---|
| **RULE_01** | Arabic Digits | Eastern Arabic numerals (`٧٠٦٠٠٠٫٠`) | Converted to Latin digits (`706000.0`) |
| **RULE_02** | Currency Normalization | Currency text in amount (`5000 ريال يمني`) | Extracted numeric value; normalized to `YER` |
| **RULE_03** | Thousands Separators | Commas inside numbers (`125,000.50`) | Stripped commas (`125000.50`) |
| **RULE_04** | Known Number Words | Written Arabic words (`ألفان`) | Mapped to numeric values (`2000.0`) |
| **RULE_05** | Phone Normalization | Non-standard Yemeni phone formats | Normalized to 9-digit standard (`771234567`) |
| **RULE_06** | Email Repeated Symbols | Repeated symbols (`user@@mail..com`) | Fixed to `user@mail.com`; broken emails quarantined |
| **RULE_07** | Date Normalization | Non-ISO dates (`17-01-2025 04:50:00`) | Standardized to ISO 8601; impossible dates quarantined |
| **RULE_08** | Trim & Synonyms | Extra spaces & synonym variants | Trimmed; mapped to canonical city/status dictionaries |
| **RULE_09** | Total Recalculation | Mismatched total or unparseable `???` | Recalculated from item components + delivery cost |

### Audit Trail Structure:
Every corrected field contains a structured entry inside the `corrections` array:
```json
{
  "rule_code": "RULE_01",
  "field": "total_amount",
  "original_value": "٧٠٦٠٠٠٫٠",
  "corrected_value": "706000.0",
  "reason": "Eastern/Arabic digits converted to Latin"
}
```

---

## 9. Quarantine Error Codes

Records that cannot be deterministically and safely repaired are routed to `orders_quarantine` with clear error codes:
- `MISSING_ORDER_ID`: Order ID is null, empty, or whitespace.
- `MISSING_CUSTOMER_ID`: Customer ID is missing.
- `INVALID_IMPOSSIBLE_DATE`: Impossible date (e.g. 31-Feb, year outside 2000-2030).
- `CORRUPTED_ITEMS_JSON`: Corrupted non-JSON string in items.
- `EMPTY_ITEMS`: Item array is empty.
- `UNKNOWN_PRICE`: Price is unparseable (`???`) and items cannot resolve it.
- `AMBIGUOUS_NEGATIVE_VALUE`: Negative item total / quantity that cannot be resolved.
- `UNSAFE_EMAIL`: Malformed email with missing `@` or missing domain.
- `MULTIPLE_CONFLICTING_ERRORS`: Record triggered two or more distinct errors.

---

## 10. Empirical Demonstration & Verification Evidence

All metrics below are **empirically measured and verified** on the 100,000-row dataset:

### 10.1 Ingestion & Classification Counts (Run 1):
- **Raw Records Ingested (`orders_raw`):** `100,000`
- **Valid Records (Clean):** `79,835` (79.84%)
- **Corrected Records (Audited):** `14,618` (14.62%)
- **Quarantined Records (Isolated):** `5,547` (5.55%)
- **Consistency Equation:** **`PASS`** ($100,000 = 79,835 + 14,618 + 5,547$)

### 10.2 Idempotent Upsert Counts (Run 1 vs Run 2):
- **Run 1 Inserted (`orders_validated`):** `93,806` unique business entities
- **Run 1 Updated (Duplicates merged):** `647`
- **Run 2 Inserted (`orders_validated`):** **`0`** (Zero duplicate documents created)
- **Run 2 Inserted (`orders_quarantine`):** **`0`** (Zero duplicate quarantine entries)
- **Run 2 Difference in DB Documents:** **`0`**

### 10.3 Update Scenario:
- Modified `customer_name` for `طلب-100000` in `orders_raw`.
- Re-ran ELT pipeline: `customer_name` successfully updated in `orders_validated` while total document count remained exactly `93,806` (Diff: 0).

---

## 11. Key Technical Decisions & Justifications

1. **Why `utf-8-sig` Encoding?**  
   The CSV dataset starts with a UTF-8 Byte Order Mark (`\xef\xbb\xbf`). Using `utf-8-sig` strips the BOM automatically, preventing column corruption (e.g. `\ufefforder_id`).

2. **Why Explicit Schema with `StringType` in Raw?**  
   Using `inferSchema` across mixed-quality datasets causes Spark to crash or silently coerce dirty strings (`"???"`, Arabic digits) to `null`. Defining all 17 columns as `StringType` guarantees 100% data fidelity in the Raw Layer.

3. **Why Python Streaming Batch for Small Files ($\le$ 200 MB)?**  
   JVM and Spark context initialization incur 15–20s overhead. A Python generator stream ingests 100K rows in ~5.6s with negligible memory ($O(1)$) and zero cluster overhead.

4. **Why PySpark for Large Files (> 200 MB)?**  
   PySpark breaks large out-of-core files across multiple partitions and worker cores, preventing single-node memory exhaustion and enabling horizontal scale.

5. **Why `id_order` as Stable Business Key?**  
   Orders represent immutable business transactions. Enforcing a Unique Index on `id_order` in `orders_validated` guarantees entity uniqueness and prevents data duplication.

6. **Why `UpdateOne(..., upsert=True)`?**  
   Direct inserts fail on re-runs when unique indexes exist. Upsert enables complete idempotency: new records are inserted, modified records are updated, and unchanged records are preserved.

7. **Understanding `source_row_number`:**  
   In Python Batch, `source_row_number` is the physical sequential CSV line number. In PySpark, `source_row_number` is a globally unique 64-bit distributed identifier generated via `monotonically_increasing_id()`, which guarantees partition-safe uniqueness without costly global sorting.

---

## 12. Viva Examination Reference

Refer to [docs/viva_questions.md](file:///d:/Big%20Data/docs/viva_questions.md) for 25+ detailed examination questions and technical justifications tailored for the oral defense.
