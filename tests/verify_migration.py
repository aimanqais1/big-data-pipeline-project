"""
Post-Migration Verification Script for MongoDB on D: Drive.
"""
import time
import psutil
from pymongo import MongoClient

def run_verification():
    print("=" * 60)
    print("MONGODB STORAGE MIGRATION VERIFICATION AUDIT")
    print("=" * 60)

    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    server_info = client.server_info()
    print(f"1. MongoDB Ping:      SUCCESS (ALIVE)")
    print(f"2. MongoDB Version:   {server_info.get('version')}")

    # Verify dbPath from server parameters
    cmd_line_opts = client.admin.command("getCmdLineOpts")
    active_dbpath = cmd_line_opts.get("parsed", {}).get("storage", {}).get("dbPath")
    active_logpath = cmd_line_opts.get("parsed", {}).get("systemLog", {}).get("path")
    print(f"3. Active dbPath:     {active_dbpath}")
    print(f"4. Active logPath:    {active_logpath}")

    # Verify midterm_ecommerce (Test DB)
    print("\n--- VERIFYING TEST DATABASE (midterm_ecommerce) ---")
    db_test = client["midterm_ecommerce"]
    raw_test = db_test["orders_raw"].count_documents({})
    val_test = db_test["orders_validated"].count_documents({})
    quar_test = db_test["orders_quarantine"].count_documents({})
    print(f"  orders_raw:        {raw_test:,} (Expected: 100,000) -> MATCH: {raw_test == 100000}")
    print(f"  orders_validated:  {val_test:,} (Expected: 93,806)  -> MATCH: {val_test == 93806}")
    print(f"  orders_quarantine: {quar_test:,} (Expected: 5,547)   -> MATCH: {quar_test == 5547}")

    # Verify midterm_ecommerce_final
    print("\n--- VERIFYING FINAL DATABASE (midterm_ecommerce_final) ---")
    db_final = client["midterm_ecommerce_final"]
    raw_final = db_final["orders_raw"].count_documents({})
    val_final = db_final["orders_validated"].count_documents({})
    quar_final = db_final["orders_quarantine"].count_documents({})
    print(f"  orders_raw (partially loaded): {raw_final:,}")
    print(f"  orders_validated:              {val_final:,}")
    print(f"  orders_quarantine:             {quar_final:,}")

    # Perform Test Write
    print("\n--- PERFORMING TEST WRITE ON FINAL DATABASE ---")
    test_col = db_final["test_storage_migration"]
    ins_res = test_col.insert_one({"test_key": "migration_check", "status": "OK", "timestamp": time.time()})
    print(f"  Inserted Test Doc ID:  {ins_res.inserted_id}")

    read_doc = test_col.find_one({"_id": ins_res.inserted_id})
    print(f"  Read Back Test Doc:    {read_doc}")

    del_res = test_col.delete_one({"_id": ins_res.inserted_id})
    print(f"  Deleted Test Doc:      {del_res.deleted_count == 1}")
    test_col.drop()
    print(f"  Test Collection Drop:  CLEAN")

    # Disk & Storage Space
    disk_c = psutil.disk_usage("C:\\")
    disk_d = psutil.disk_usage("D:\\")
    mem = psutil.virtual_memory()

    print("\n--- DISK & CAPACITY SUMMARY ---")
    print(f"  Disk C Free Space: {disk_c.free / (1024**3):.2f} GB (Total: {disk_c.total / (1024**3):.2f} GB)")
    print(f"  Disk D Free Space: {disk_d.free / (1024**3):.2f} GB (Total: {disk_d.total / (1024**3):.2f} GB)")
    print(f"  Available RAM:     {mem.available / (1024**3):.2f} GB ({mem.percent}% used)")

if __name__ == "__main__":
    run_verification()
