"""
Unified Single Entry Point for Midterm Hybrid Data Pipeline.
Orchestrates the entire end-to-end data lifecycle:
1. File Discovery & Metadata.
2. File Router (Python Streaming Batch vs PySpark Distributed DataFrame).
3. Raw ELT Ingestion -> MongoDB orders_raw.
4. Streaming Cleaning, 9 Quality Rules & Audit Trail Tracking.
5. Record Classification: Valid / Corrected / Quarantined.
6. Idempotent Upsert into orders_validated (Business Key: id_order) & orders_quarantine.
7. Strict Consistency Equation Verification.
8. Performance & Audit Metrics Generation -> reports/results.json.
"""
import sys
import time
import argparse
import logging
from pathlib import Path
from typing import Dict, Any

from config.settings import DEFAULT_BATCH_SIZE, MONGO_DB_NAME
from src.mongo_setup import setup_mongodb_collections
from src.file_router import inspect_and_route, ENGINE_PYTHON_BATCH, ENGINE_PYSPARK
from src.batch_loader import load_csv_to_raw_batch
from src.spark_loader import load_csv_to_raw_spark
from src.elt_pipeline import run_elt_transform_and_classify
from src.metrics import save_pipeline_metrics

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def run_pipeline(
    file_path: str,
    custom_run_id: str = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    reset_db: bool = False,
    db_name: str = MONGO_DB_NAME
) -> Dict[str, Any]:
    """
    Executes the complete Hybrid Data Pipeline from a single unified entry point.
    """
    overall_start_time = time.perf_counter()
    target_db_name = db_name or MONGO_DB_NAME

    # Step 1: Ensure MongoDB collections and indexes are initialized in target DB
    setup_mongodb_collections(db_name=target_db_name, drop_existing=reset_db)

    # Step 2: File Router Decision
    routing_decision = inspect_and_route(file_path, custom_run_id=custom_run_id)
    id_run = routing_decision["id_run"]
    selected_engine = routing_decision["selected_engine"]
    file_size_mb = routing_decision["file_size_mb"]

    # Step 3: Raw ELT Load (Load First, Transform Later)
    if selected_engine == ENGINE_PYTHON_BATCH:
        load_summary = load_csv_to_raw_batch(file_path, id_run, batch_size=batch_size, db_name=target_db_name)
        partitions_info = 1
    elif selected_engine == ENGINE_PYSPARK:
        load_summary = load_csv_to_raw_spark(file_path, id_run, db_name=target_db_name)
        partitions_info = load_summary.get("partitions", 8)
    else:
        raise ValueError(f"Unknown engine: {selected_engine}")

    # Step 4: Streaming Cleaning, Classification & Idempotent Upsert
    elt_summary = run_elt_transform_and_classify(id_run, db_name=target_db_name, batch_size=batch_size)

    total_pipeline_time = time.perf_counter() - overall_start_time
    total_read_rows = load_summary["read_rows"]
    overall_throughput = total_read_rows / total_pipeline_time if total_pipeline_time > 0 else 0.0

    # Step 5: Consolidate Metrics
    metrics = {
        "run_id": id_run,
        "database_name": target_db_name,
        "file_name": routing_decision["file_name"],
        "file_size_mb": file_size_mb,
        "threshold_mb": routing_decision["threshold_mb"],
        "used_engine": selected_engine,
        "engine_reason": routing_decision["reason"],
        "read_rows": total_read_rows,
        "loaded_raw": load_summary["loaded_raw"],
        "count_valid": elt_summary["valid_count"],
        "count_corrected": elt_summary["corrected_count"],
        "count_quarantine": elt_summary["quarantine_count"],
        "count_inserted": elt_summary["count_inserted"],
        "count_updated": elt_summary["count_updated"],
        "count_unchanged": elt_summary["count_unchanged"],
        "consistency_equation": elt_summary["consistency_check"],
        "batch_size": batch_size,
        "partitions": partitions_info,
        "seconds_elapsed": round(total_pipeline_time, 3),
        "throughput": round(overall_throughput, 2),
        "rule_case_counts": elt_summary["rule_distribution"],
        "error_case_counts": elt_summary["error_distribution"],
        "status": "SUCCESS" if elt_summary["consistency_check"] == "PASS" else "FAILED"
    }

    # Step 6: Save results report
    save_pipeline_metrics(metrics)

    logger.info("=" * 70)
    logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
    logger.info("=" * 70)
    logger.info(f"Run ID:               {metrics['run_id']}")
    logger.info(f"Database:             {metrics['database_name']}")
    logger.info(f"Engine:               {metrics['used_engine']} ({metrics['file_size_mb']} MB)")
    logger.info(f"Total Rows:           {metrics['read_rows']:,}")
    logger.info(f"Valid:                {metrics['count_valid']:,}")
    logger.info(f"Corrected:            {metrics['count_corrected']:,}")
    logger.info(f"Quarantined:          {metrics['count_quarantine']:,}")
    logger.info(f"Consistency Equation: {metrics['consistency_equation']} (Raw == Valid + Corrected + Quarantined)")
    logger.info(f"Validated Inserted:   {metrics['count_inserted']:,}")
    logger.info(f"Validated Updated:    {metrics['count_updated']:,}")
    logger.info(f"Total Time:           {metrics['seconds_elapsed']}s")
    logger.info(f"Overall Throughput:   {metrics['throughput']:,.2f} rows/s")
    logger.info("=" * 70)

    return metrics

def main():
    parser = argparse.ArgumentParser(description="Midterm Hybrid Data Pipeline Main Runner.")
    parser.add_argument("--input", "-i", default="data/orders_sample_100k.csv", help="Input CSV path")
    parser.add_argument("--run-id", help="Optional run id")
    parser.add_argument("--db-name", default=MONGO_DB_NAME, help="Target MongoDB Database Name")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size")
    parser.add_argument("--reset-db", action="store_true", help="Reset collections before running")
    args = parser.parse_args()

    try:
        run_pipeline(args.input, args.run_id, args.batch_size, args.reset_db, db_name=args.db_name)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()

