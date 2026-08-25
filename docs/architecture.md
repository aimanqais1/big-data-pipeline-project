# Data Pipeline Architecture & Engineering Design Document
**Project:** Hybrid Data Pipeline (Midterm)  
**Author:** ENG\AIMAN QAIS  
**Target Platform:** Python 3.12, PySpark 4.2.0, MongoDB 7.0, Java 17 Temurin LTS

---

## 1. Architectural Philosophy: The ELT Paradigm

Traditional data architectures often follow **ETL (Extract -> Transform -> Load)**, where data is cleaned and transformed in memory *before* being loaded into the destination database. In Big Data and production data engineering, this introduces two fatal flaws:
1. **Data Loss:** If a transformation rule is buggy or drops records prematurely, raw source information is lost permanently.
2. **Lack of Auditability:** Data engineers cannot inspect the original incoming values to debug data quality anomalies.

This pipeline strictly adopts the **ELT (Extract -> Load -> Transform)** philosophy:
1. **Load Raw First:** Untransformed strings are ingested into `orders_raw` with 100% fidelity.
2. **Transform Later:** Cleaning, auditing, and classification occur downstream as an isolated, repeatable step.

```
+---------------+     +--------------------+     +------------------------+
|  Incoming CSV | --> | MongoDB orders_raw | --> | Cleaning & Audit Trail |
+---------------+     +--------------------+     +------------------------+
                                                              |
                                             +----------------+----------------+
                                             |                                 |
                                             v                                 v
                                 +------------------------+        +---------------------------+
                                 | MongoDB orders_validated|        | MongoDB orders_quarantine |
                                 +------------------------+        +---------------------------+
```

---

## 2. Component-by-Component Walkthrough

### 2.1 Unified Entrypoint (`src/main.py`)
- Single executable interface orchestrating file routing, raw loading, ELT transformations, idempotency checks, and metrics collection.
- Accepts arguments: `--input`, `--batch-size`, `--reset-db`, `--run-id`.

### 2.2 File Router (`src/file_router.py`)
- Evaluates exact file size in MB using OS filesystem metadata.
- Reads `SMALL_FILE_THRESHOLD_MB = 200.0` from `config/settings.py` (Single Source of Truth).
- Generates a cryptographically strong UUID4 `id_run`.
- Deterministic routing decision:
  - **$\le 200.0$ MB:** `python_batch`
  - **$> 200.0$ MB:** `pyspark`

### 2.3 Python Streaming Batch Loader (`src/batch_loader.py`)
- Implements Python generator streams (`csv.DictReader`) reading line-by-line.
- **Strictly Prohibits:** `list(reader)` or reading whole files into RAM.
- Flushes batches of 10,000 documents via `insert_many(ordered=False)`.
- Tracks batch-level throughput, elapsed time, and error events.

### 2.4 PySpark Distributed Loader (`src/spark_loader.py`)
- Initializes `SparkSession` configured with OpenJDK 17 and local MongoDB Connector classpaths.
- Enforces an **Explicit Schema** (`StructType` with 17 `StringType` fields).
- Constructs distributed DataFrame with ELT metadata:
  - `id_run`: constant string
  - `source_file`: base filename
  - `source_row_number`: 64-bit partition-distributed ID via `monotonically_increasing_id() + 1`
  - `ingested_at`: UTC timestamp string
  - `engine_used`: `"pyspark"`
  - `raw_record`: struct of all 17 CSV fields
- Writes parallel partitions to MongoDB `orders_raw` via `mongo-spark-connector_2.13:10.4.0`.

### 2.5 Quality Rules & Audit Trail (`src/quality_rules.py`)
- Encapsulates 9 deterministic quality rules.
- Tracks granular transformations in the `corrections` list.
- Implements safe numeric parsing (`parse_item_number`) to prevent silent masking of corrupted strings as `0.0`.

### 2.6 ELT Streaming Pipeline & Idempotent Upsert (`src/elt_pipeline.py`)
- Consumes `orders_raw` via MongoDB cursor in memory-safe streaming buffers (`batch_size = 5,000 / 10,000`).
- Performs bulk upserts (`UpdateOne(..., upsert=True)`) directly to `orders_validated` and `orders_quarantine`.
- Flushes buffers to database and clears RAM immediately ($O(1)$ memory footprint).
- Enforces the **Strict Consistency Equation**:
  $$\text{Raw Ingested} = \text{Valid} + \text{Corrected} + \text{Quarantined}$$

### 2.7 Metrics Engine (`src/metrics.py`)
- Collects execution statistics, throughput, rule distributions, error distributions, and upsert counts.
- Emits structured reports to `reports/results.json` and `reports/results.md`.

---

## 3. In-Depth Technical Decisions

### 3.1 Encoding: `utf-8-sig`
- **Context:** The original CSV dataset begins with a 3-byte UTF-8 Byte Order Mark (`\xef\xbb\xbf`).
- **Standard `utf-8`:** Reads `\ufefforder_id`, corrupting the first column key.
- **`utf-8-sig`:** Automatically strips the BOM during streaming, preserving clean column names.

### 3.2 Explicit Schema vs `inferSchema`
- **Risk with `inferSchema`:** Spark must make two full passes over the dataset to guess column types. On mixed-quality data containing strings like `"???"` or `"٥٠٠٠"` inside numeric columns, `inferSchema` either fails or converts dirty values to `null`, corrupting raw data fidelity.
- **Explicit Schema:** We define all 17 columns explicitly as `StringType`, ensuring every dirty character arrives intact in `orders_raw`.

### 3.3 The Stable Business Key (`id_order`) & Unique Index
- **Business Reality:** In e-commerce, each order represents an immutable transaction entity identified by `id_order`.
- **Database Enforcement:** A Unique Index (`uniq_id_order`) on `id_order` in `orders_validated` prevents multiple documents representing the same order.
- **Duplicate Handling:** Duplicates within an ingestion run or across re-runs update the existing document rather than creating redundant entries.

### 3.4 Idempotency via `UpdateOne(..., upsert=True)`
- **Why Not `insert_many`?** Re-running the pipeline would violate the Unique Index and crash.
- **Why Not `delete_many` + `insert_many`?** Unsafe in production (wipes data during downtime, loses uncommitted updates).
- **Upsert Mechanics:**
  - If `id_order` does not exist $\to$ **INSERT** (`upserted_count`).
  - If `id_order` exists with changes $\to$ **UPDATE** (`modified_count`).
  - If `id_order` exists unchanged $\to$ **NO-OP** (`matched_count - modified_count`).

### 3.5 Quarantine Idempotency Key
- To prevent duplicate quarantine entries upon re-running the same `id_run`, `orders_quarantine` enforces a **Unique Compound Index** on `(id_run, source_row_number)`.

### 3.6 Engineering Analysis: `source_row_number`
- **Python Batch Loader:** Uses sequential line counter $1, 2, 3, \dots, N$.
- **PySpark Loader:** Uses `monotonically_increasing_id()`, where the top 33 bits encode the partition ID and the lower 31 bits encode the record number within that partition. This produces globally unique 64-bit integers without requiring an expensive global shuffle. It is documented as a distributed record identifier.

---

## 4. Memory Profiling & Constant-Space Guarantee

```
Total Dataset Size: 12.65 GB (Millions of records)
Available RAM: 8-16 GB

Memory Profile:
- Python Reader Buffer: 10,000 records (~4 MB RAM)
- Processing Step: Record-by-record generator stream
- MongoDB Upsert Buffer: 10,000 records (~8 MB RAM)
- Peak Pipeline RAM: < 50 MB (Constant O(1) space)
```
