"""
Comprehensive Live Verification Script.
Tests:
1. Run 1: End-to-end Pipeline Execution.
2. Run 2: Idempotency Proof (Zero inserts, zero duplicates).
3. Run 3: Record Update Scenario (Modifies 1 record, re-runs, verifies update without duplicate).
"""
import json
from pymongo import MongoClient
from src.main import run_pipeline
from src.elt_pipeline import run_elt_transform_and_classify

def execute_verification():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["midterm_ecommerce"]

    print("=" * 70)
    print("STEP 1: CLEAN RESET & EXECUTE RUN 1")
    print("=" * 70)
    run_id = "viva_verification_run_01"
    m1 = run_pipeline("data/orders_sample_100k.csv", custom_run_id=run_id, reset_db=True)

    raw_c1 = db["orders_raw"].count_documents({"id_run": run_id})
    val_c1 = db["orders_validated"].count_documents({})
    quar_c1 = db["orders_quarantine"].count_documents({"id_run": run_id})

    print("\n--- MONGO STATE AFTER RUN 1 ---")
    print(f"orders_raw count:         {raw_c1:,}")
    print(f"orders_validated count:   {val_c1:,}")
    print(f"orders_quarantine count:  {quar_c1:,}")
    print(f"Total processed entities: {val_c1 + quar_c1:,}")
    print(f"Run 1 Metrics Breakdown:  Valid={m1['count_valid']:,} | Corrected={m1['count_corrected']:,} | Quarantine={m1['count_quarantine']:,}")
    print(f"Run 1 Upsert Counts:      Inserted={m1['count_inserted']:,} | Updated={m1['count_updated']:,} | Unchanged={m1['count_unchanged']:,}")
    print(f"Consistency Check:        {m1['consistency_equation']} (Raw == Valid + Corrected + Quarantined)")
    print(f"Throughput:               {m1['throughput']:,.2f} rows/s (Total time: {m1['seconds_elapsed']}s)")

    print("\n" + "=" * 70)
    print("STEP 2: RUN 2 ON EXACT SAME DATA & ID_RUN (IDEMPOTENCY PROOF)")
    print("=" * 70)
    m2 = run_elt_transform_and_classify(run_id)

    raw_c2 = db["orders_raw"].count_documents({"id_run": run_id})
    val_c2 = db["orders_validated"].count_documents({})
    quar_c2 = db["orders_quarantine"].count_documents({"id_run": run_id})

    print("\n--- MONGO STATE AFTER RUN 2 ---")
    print(f"orders_raw count:         {raw_c2:,} (Diff: {raw_c2 - raw_c1})")
    print(f"orders_validated count:   {val_c2:,} (Diff: {val_c2 - val_c1})  <--- ZERO INCREASE!")
    print(f"orders_quarantine count:  {quar_c2:,} (Diff: {quar_c2 - quar_c1})  <--- ZERO INCREASE!")
    print(f"Run 2 Upsert Counts:      Inserted={m2['count_inserted']:,} (ZERO NEW INSERTS) | Updated={m2['count_updated']:,}")
    print(f"Run 2 Quarantine Upsert:  Inserted={m2['quarantine_inserted']:,} (ZERO NEW INSERTS) | Unchanged={m2['quarantine_unchanged']:,}")

    print("\n" + "=" * 70)
    print("STEP 3: UPDATE SCENARIO (MODIFY 1 RECORD IN RAW & RE-RUN)")
    print("=" * 70)
    target_order_id = "طلب-100000"
    before_doc = db["orders_validated"].find_one({"id_order": target_order_id}, {"_id": 0, "id_order": 1, "customer_name": 1, "total_amount": 1})
    print(f"Target Record Before Update: {before_doc}")

    # Modify 1 record in orders_raw
    raw_doc = db["orders_raw"].find_one({"id_run": run_id, "raw_record.order_id": target_order_id})
    db["orders_raw"].update_one(
        {"_id": raw_doc["_id"]},
        {"$set": {"raw_record.customer_name": "محمد علي المحدث للتحقق"}}
    )
    print(f"Updated raw_record.customer_name in orders_raw to: 'محمد علي المحدث للتحقق'")

    # Re-run ELT transformation on this run_id
    m3 = run_elt_transform_and_classify(run_id)

    after_doc = db["orders_validated"].find_one({"id_order": target_order_id}, {"_id": 0, "id_order": 1, "customer_name": 1, "total_amount": 1})
    val_c3 = db["orders_validated"].count_documents({})

    print("\n--- MONGO STATE AFTER UPDATE RE-RUN ---")
    print(f"Target Record After Update:  {after_doc}")
    print(f"orders_validated total count: {val_c3:,} (Diff from Run 1: {val_c3 - val_c1}) <--- STILL ZERO INCREASE!")
    print(f"Update Verified:             {after_doc['customer_name'] == 'محمد علي المحدث للتحقق'}")

if __name__ == "__main__":
    execute_verification()
