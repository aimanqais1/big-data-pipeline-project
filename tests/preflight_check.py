"""
System Resource Preflight Check for Full-Scale Pipeline Run.
"""
import os
import sys
import psutil
from pymongo import MongoClient

def check_system():
    print("=" * 60)
    print("SYSTEM RESOURCE PREFLIGHT AUDIT")
    print("=" * 60)

    # 1. RAM Audit
    mem = psutil.virtual_memory()
    total_ram = mem.total / (1024**3)
    avail_ram = mem.available / (1024**3)
    print(f"Total RAM:       {total_ram:.2f} GB")
    print(f"Available RAM:   {avail_ram:.2f} GB ({mem.percent}% used)")

    # 2. Disk Space Audit
    disk_d = psutil.disk_usage("D:\\")
    disk_c = psutil.disk_usage("C:\\")
    print(f"Disk D (Project): Free: {disk_d.free / (1024**3):.2f} GB / Total: {disk_d.total / (1024**3):.2f} GB")
    print(f"Disk C (System):  Free: {disk_c.free / (1024**3):.2f} GB / Total: {disk_c.total / (1024**3):.2f} GB")

    # 3. MongoDB Storage & Service Status
    print("\n--- MongoDB Service & Storage ---")
    try:
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        server_info = client.server_info()
        print(f"MongoDB Service: ACTIVE (Version: {server_info.get('version')})")

        db = client["midterm_ecommerce"]
        stats = db.command("dbStats")
        storage_mb = stats.get("storageSize", 0) / (1024**2)
        data_mb = stats.get("dataSize", 0) / (1024**2)
        index_mb = stats.get("indexSize", 0) / (1024**2)
        objects_cnt = stats.get("objects", 0)

        print(f"Database Name:   midterm_ecommerce")
        print(f"Storage Size:    {storage_mb:.2f} MB")
        print(f"Data Size:       {data_mb:.2f} MB")
        print(f"Index Size:      {index_mb:.2f} MB")
        print(f"Total Documents: {objects_cnt:,}")
    except Exception as e:
        print(f"MongoDB Service Status: ERROR ({e})")

    # 4. Stale Processes Check
    print("\n--- Active Java / Spark / Mongo Processes ---")
    stale_java = []
    for p in psutil.process_iter(['pid', 'name']):
        try:
            p_name = p.info['name'].lower() if p.info['name'] else ""
            if "java" in p_name:
                stale_java.append(f"PID={p.info['pid']} ({p.info['name']})")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    if stale_java:
        print("Active Java Processes:", ", ".join(stale_java))
    else:
        print("Active Java Processes: None (Clean environment)")

    # 5. Dataset Audit
    csv_path = "orders_huge_mixed_quality.csv"
    if os.path.exists(csv_path):
        size_gb = os.path.getsize(csv_path) / (1024**3)
        print(f"\nTarget Large CSV: {csv_path} ({size_gb:.2f} GB / {size_gb*1024:.2f} MB) -> EXISTS")
    else:
        print(f"\nTarget Large CSV: {csv_path} -> NOT FOUND")

if __name__ == "__main__":
    check_system()
