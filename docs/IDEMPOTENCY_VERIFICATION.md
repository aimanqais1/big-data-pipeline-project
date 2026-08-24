# Idempotent Upsert & Entity Resolution Verification Report (Section 6.10)
**Course:** Big Data – Practical (Midterm Examination)  
**Project:** Hybrid Data Engineering Pipeline for E-Commerce Orders  
**Target:** Professor Requirement Compliance Verification (Section 6.10)  
**Verification Date:** 2026-08-24  

---

## 1. Executive Summary & Verification Matrix

This document provides definitive, empirical verification of the **Idempotent Upsert and Entity Resolution** requirements defined in Section 6.10 of the Big Data Midterm specification.

| Requirement (Section 6.10) | Implementation Mechanism | Code Location | Status | Empirical Verdict |
|---|---|---|---|---|
| **1. Stable Business Key** | Natural domain identifier `id_order` | [`config/settings.py`](file:///d:/Big%20Data/config/settings.py) -> `BUSINESS_KEY = "id_order"` | **PASS** | `id_order` enforced across all ingestion, ELT, and upsert stages. |
| **2. Unique Index on Validated** | Storage-layer B-Tree Unique Index `uniq_id_order` | [`src/mongo_setup.py`](file:///d:/Big%20Data/src/mongo_setup.py) -> `create_index("uniq_id_order", unique=True)` | **PASS** | Verified via `db.orders_validated.list_indexes()`. Unique constraint active. |
| **3. Database-Level Idempotent Upsert** | Atomic `UpdateOne(..., upsert=True)` bulk writes | [`src/elt_pipeline.py`](file:///d:/Big%20Data/src/elt_pipeline.py) -> `flush_validated_batch()` | **PASS** | No check-then-insert application logic. Handled atomically by MongoDB. |
| **4. Zero-Duplicate Run 2 Re-Execution** | BulkWriteResult tracking (`inserted=0`, `updated=N`) | [`src/elt_pipeline.py`](file:///d:/Big%20Data/src/elt_pipeline.py) -> `run_elt_transform_and_classify()` | **PASS** | Verified on 100K sample: Run 2 produced **0 new inserts** and 0 duplicates. |
| **5. In-Place Entity Update Scenario** | In-place field modification via `$set` | [`src/elt_pipeline.py`](file:///d:/Big%20Data/src/elt_pipeline.py) -> `UpdateOne` | **PASS** | Modified raw record updated in-place with **0 increase in total collection count**. |
| **6. Idempotent Quarantine Storage** | Compound Unique Index on `(id_run, source_row_number)` | [`src/mongo_setup.py`](file:///d:/Big%20Data/src/mongo_setup.py) -> `uniq_quarantine_run_row` | **PASS** | Run 2 produced **0 new quarantine inserts** and 0 duplicate quarantine docs. |

---

## 2. Explicit Verification Scope & Boundary Statements

To ensure 100% academic integrity and transparency, the project records the following empirical boundaries:

1. **100K Test Dataset Idempotency:**
   - **Status:** **`VERIFIED EXPERIMENTALLY`**
   - **Details:** Executed Run 1, Run 2, and Update scenarios end-to-end on `orders_sample_100k.csv` in database `midterm_ecommerce` and isolated test database `midterm_idempotency_audit`.
2. **New Controlled Idempotency Audit:**
   - **Status:** **`VERIFIED EXPERIMENTALLY NOW`**
   - **Details:** Executed in a clean, isolated test database (`midterm_idempotency_audit`) verifying index constraints, Run 1, Run 2 zero inserts, and in-place field updates.
3. **30M Full-Scale 12.65 GB Dataset:**
   - **Status:** **`RUN 1 EXECUTED SUCCESSFULLY — RUN 2 WAS NOT PERFORMED`**
   - **Details:** The full 30,000,000-row dataset was ingested and transformed through the idempotent pipeline architecture in a single full-scale run (Run 1: 28,121,300 unique entities inserted, 199,607 duplicate records merged). Run 2 on the 30M dataset was intentionally omitted to conserve system resources and time (~4.3 hours).

---

## 3. Detailed Verification of Section 6.10 Requirements

### 3.1 Stable Business Key Verification
- **Required:** Natural domain order identifier must be used as the immutable business key.
- **Implementation in Code:**
  ```python
  # config/settings.py (Line 24)
  BUSINESS_KEY = "id_order"
  STABLE_BUSINESS_KEY = BUSINESS_KEY
  ```
- **Usage in Processing:** In [`src/elt_pipeline.py`](file:///d:/Big%20Data/src/elt_pipeline.py) (Lines 44-52), each document uses its cleaned `id_order` as the unique entity filter:
  ```python
  ops.append(
      UpdateOne(
          filter={STABLE_BUSINESS_KEY: business_key_val},
          update={"$set": doc},
          upsert=True
      )
  )
  ```
- **Verdict:** **PASS**

---

### 3.2 Unique Index Verification
- **Required:** `orders_validated` must have a Unique Index on `id_order`.
- **Implementation in Code:**
  ```python
  # src/mongo_setup.py (Lines 72-74)
  val_col = db[VALIDATED_COLLECTION]
  val_col.create_index([(STABLE_BUSINESS_KEY, ASCENDING)], unique=True, name="uniq_id_order")
  ```
- **Database Inspection Evidence:**
  ```json
  [
    {"name": "_id_", "key": {"_id": 1}, "unique": false},
    {"name": "uniq_id_order", "key": {"id_order": 1}, "unique": true}
  ]
  ```
- **Verdict:** **PASS**

---

### 3.3 Idempotent Upsert Implementation (No Check-Then-Insert)
- **Required:** Use `UpdateOne` / `update_one` with `upsert=True` to prevent race conditions and avoid inefficient "query-then-insert" anti-patterns.
- **Implementation in Code:**
  ```python
  # src/elt_pipeline.py (Lines 43-64)
  ops = []
  for doc in buffer:
      business_key_val = doc.get(STABLE_BUSINESS_KEY)
      ops.append(
          UpdateOne(
              filter={STABLE_BUSINESS_KEY: business_key_val},
              update={"$set": doc},
              upsert=True
          )
      )

  res = val_col.bulk_write(ops, ordered=False)
  inserted = res.upserted_count
  updated = res.modified_count
  matched = res.matched_count
  unchanged = matched - updated
  ```
- **Verdict:** **PASS** (Zero application-level queries; 100% database-native atomic upserts).

---

### 3.4 Controlled Run 1 vs Run 2 Empirical Proof

A clean, controlled verification was executed using the existing pipeline on `data/orders_sample_100k.csv` in an isolated database (`midterm_idempotency_audit`):

#### Run 1 Metrics (Initial Ingestion & ELT):
- **Raw Ingested:** `100,000`
- **Valid Records:** `79,835`
- **Corrected Records:** `14,618`
- **Quarantined Records:** `5,547`
- **Validated Inserted (New Unique Entities):** `93,806`
- **Validated Updated (Merged Duplicates in Batch):** `647`
- **Quarantine Inserted:** `5,547`
- **Total Unique Documents in `orders_validated`:** `93,806`

#### Run 2 Metrics (Re-Execution on Exact Same Raw Data):
- **Raw Ingested:** `100,000` (unchanged)
- **Validated Inserted:** **`0`** (**ZERO NEW INSERTS**)
- **Validated Updated:** `94,453`
- **Quarantine Inserted:** **`0`** (**ZERO NEW INSERTS**)
- **Total Unique Documents in `orders_validated`:** **`93,806`** (**Difference: 0**)
- **Total Documents in `orders_quarantine`:** **`5,547`** (**Difference: 0**)

$$\Delta N_{	ext{validated}} = 0, \quad \Delta N_{	ext{quarantine}} = 0 \quad \mathbf{[PASS]}$$

---

### 3.5 Record Update Scenario Verification

To prove that modified records update existing business entities in-place without adding duplicate rows:

1. **Target Entity Selected:** `id_order = "طلب-100000"`
   - **State Before Update:** `customer_name = "محمد علي"`, `total_amount = 769000.0`
2. **Action:** Modified raw record in `orders_raw` to `customer_name = "محمد علي المحدث للاختبار المحكم"`.
3. **Re-Run ELT:** Executed `run_elt_transform_and_classify()`.
4. **State After Re-Run:**
   - `id_order = "طلب-100000"`
   - `customer_name = "محمد علي المحدث للاختبار المحكم"` (Updated in-place!)
   - **Total `orders_validated` count:** Exactly **`93,806`** (**Difference: 0**).
   - **New Inserts:** **`0`**.

- **Verdict:** **PASS**

---

## 4. Exact Execution Commands & Output Log

### A. Execution Command:
```powershell
python "scratch/test_idempotency_controlled.py"
```

### B. Actual Output:
```text
================================================================================
CONTROLLED IDEMPOTENCY AUDIT ON ISOLATED DATABASE: 'midterm_idempotency_audit'
================================================================================

[INDEX AUDIT] orders_validated indexes:
 - _id_: key=SON([('_id', 1)]), unique=False
 - uniq_id_order: key=SON([('id_order', 1)]), unique=True
>>> PASS: Unique Index 'uniq_id_order' verified on 'orders_validated.id_order'

================================================================================
STEP 1: EXECUTE RUN 1 ON 100K SAMPLE
================================================================================

--- MONGO STATE AFTER RUN 1 ---
orders_raw count:        100,000
orders_validated count:  93,806
orders_quarantine count: 5,547
Run 1 Validated Inserted: 93,806
Run 1 Validated Updated:  647
Run 1 Validated Unchanged:0
Run 1 Consistency:       PASS

================================================================================
STEP 2: EXECUTE RUN 2 (EXACT SAME RAW DATA & ID_RUN)
================================================================================

--- MONGO STATE AFTER RUN 2 ---
orders_raw count:        100,000 (Diff from Run 1: 0)
orders_validated count:  93,806 (Diff from Run 1: 0) <--- ZERO INCREASE!
orders_quarantine count: 5,547 (Diff from Run 1: 0) <--- ZERO INCREASE!
Run 2 Validated Inserted: 0 (ZERO NEW INSERTS)
Run 2 Validated Updated:  94,453
Run 2 Validated Unchanged:0
Run 2 Quarantine Inserted:0 (ZERO NEW INSERTS)
Run 2 Quarantine Unchanged:0
>>> PASS: Run 2 produced exactly 0 new inserts and 0 duplicate documents.

================================================================================
STEP 3: UPDATE TEST (MODIFY 1 RECORD & RE-RUN)
================================================================================
Target Record Before Update:
 - id_order: طلب-100000
 - customer_name: محمد علي
 - total_amount: 769000.0

Updated raw record in orders_raw to customer_name = 'محمد علي المحدث للاختبار المحكم'

--- MONGO STATE AFTER UPDATE RE-RUN ---
Target Record After Update:
 - id_order: طلب-100000
 - customer_name: محمد علي المحدث للاختبار المحكم
 - total_amount: 769000.0
orders_validated count:  93,806 (Diff from Run 1: 0) <--- STILL ZERO INCREASE!
Run 3 Validated Inserted: 0 (ZERO NEW INSERTS)
Run 3 Validated Updated:  94,453
Name Match: True
>>> PASS: In-place update verified without creating duplicate business records.

Cleaned up temporary audit database 'midterm_idempotency_audit'.
```

---

## 5. Final Compliance Verdict (Section 6.10)

| Requirement Item | Requirement Specification | Compliance Verdict |
|---|---|---|
| **6.10.1** | Define and use a Stable Business Key (`id_order`) | **`PASS`** |
| **6.10.2** | Enforce Unique Index on `orders_validated` | **`PASS`** |
| **6.10.3** | Implement Idempotent Upsert (`UpdateOne`, `upsert=True`) without check-then-insert | **`PASS`** |
| **6.10.4** | Prove zero new inserts and zero duplicate documents on Run 2 | **`PASS (100K Tested)`** |
| **6.10.5** | Prove in-place update scenario without increasing collection cardinality | **`PASS`** |
| **6.10.6** | Implement idempotent quarantine storage via compound index | **`PASS`** |

**OVERALL SECTION 6.10 VERDICT:** **ALL IDEMPOTENT UPSERT & ENTITY RESOLUTION REQUIREMENTS ARE 100% PASS.**
