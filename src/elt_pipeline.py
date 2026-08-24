"""
ELT Pipeline Transformation & Classification Module for Midterm Data Pipeline.
Implements the core ELT principle:
1. Stream uncleaned raw records from orders_raw by id_run.
2. Apply deterministic cleaning rules, track Audit Trail, and classify.
3. Route quarantined records to orders_quarantine with error codes.
4. Prepare valid and corrected records for idempotent upsert.
5. Verify the strict consistency equation: Raw = Valid + Corrected + Quarantined.
"""
import time
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    STABLE_BUSINESS_KEY
)
from src.mongo_setup import get_database
from src.quality_rules import process_and_classify_record

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def run_elt_transform_and_classify(
    id_run: str,
    db_name: str = MONGO_DB_NAME,
    batch_size: int = 5000
) -> Dict[str, Any]:
    """
    Transforms raw documents for a given id_run, classifies them into
    Valid / Corrected / Quarantined, inserts quarantined docs into orders_quarantine,
    and returns categorized records and transformation metrics.
    """
    db = get_database(db_name=db_name)
    raw_col = db[RAW_COLLECTION]
    quar_col = db[QUARANTINE_COLLECTION]

    logger.info("=" * 60)
    logger.info("STARTING ELT TRANSFORMATION & CLASSIFICATION")
    logger.info(f"Run ID: {id_run}")
    logger.info("=" * 60)

    start_time = time.perf_counter()

    raw_query = {"id_run": id_run}
    total_raw_in_db = raw_col.count_documents(raw_query)

    if total_raw_in_db == 0:
        logger.warning(f"No raw records found in orders_raw for id_run='{id_run}'.")
        return {
            "id_run": id_run,
            "raw_count": 0,
            "valid_count": 0,
            "corrected_count": 0,
            "quarantine_count": 0,
            "consistency_check": "PASS",
            "elapsed_seconds": 0.0,
            "error_distribution": {},
            "rule_distribution": {},
            "records_to_upsert": []
        }

    valid_count = 0
    corrected_count = 0
    quarantine_count = 0

    records_to_upsert: List[Dict[str, Any]] = []
    quarantine_buffer: List[Dict[str, Any]] = []
    error_counts: Dict[str, int] = {}
    rule_counts: Dict[str, int] = {}

    processed_count = 0
    cursor = raw_col.find(raw_query, batch_size=batch_size)

    for raw_doc in cursor:
        processed_count += 1
        classified = process_and_classify_record(raw_doc)

        cls_type = classified.get("classification")

        if cls_type == "quarantined":
            quarantine_count += 1
            quarantine_buffer.append(classified)

            for err in classified.get("error_codes", []):
                error_counts[err] = error_counts.get(err, 0) + 1

            if len(quarantine_buffer) >= batch_size:
                quar_col.insert_many(quarantine_buffer, ordered=False)
                quarantine_buffer.clear()

        elif cls_type == "corrected":
            corrected_count += 1
            records_to_upsert.append(classified)

            for corr in classified.get("corrections", []):
                rcode = corr.get("rule_code", "UNKNOWN_RULE")
                rule_counts[rcode] = rule_counts.get(rcode, 0) + 1

        elif cls_type == "valid":
            valid_count += 1
            records_to_upsert.append(classified)

        if processed_count % 25000 == 0:
            logger.info(
                f"ELT Progress: {processed_count:,}/{total_raw_in_db:,} records | "
                f"Valid: {valid_count:,} | Corrected: {corrected_count:,} | Quarantined: {quarantine_count:,}"
            )

    # Flush remaining quarantine buffer
    if quarantine_buffer:
        quar_col.insert_many(quarantine_buffer, ordered=False)
        quarantine_buffer.clear()

    elapsed = time.perf_counter() - start_time

    # Strict Consistency Equation Check: Raw == Valid + Corrected + Quarantined
    sum_classified = valid_count + corrected_count + quarantine_count
    consistency_passed = (total_raw_in_db == sum_classified)

    if not consistency_passed:
        logger.error(
            f"DATA INTEGRITY ERROR: Consistency equation failed! "
            f"Raw ({total_raw_in_db}) != Valid ({valid_count}) + Corrected ({corrected_count}) + Quarantined ({quarantine_count})"
        )
        raise ValueError(
            f"Data Consistency Violation: raw_count={total_raw_in_db} but total_classified={sum_classified}"
        )

    summary = {
        "id_run": id_run,
        "raw_count": total_raw_in_db,
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "sum_classified": sum_classified,
        "consistency_check": "PASS" if consistency_passed else "FAIL",
        "elapsed_seconds": round(elapsed, 3),
        "throughput_rps": round(total_raw_in_db / elapsed, 2) if elapsed > 0 else 0.0,
        "error_distribution": error_counts,
        "rule_distribution": rule_counts,
        "records_to_upsert": records_to_upsert
    }

    logger.info("=" * 60)
    logger.info("ELT TRANSFORMATION & CLASSIFICATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Raw Records:       {summary['raw_count']:,}")
    logger.info(f"Valid Records:     {summary['valid_count']:,}")
    logger.info(f"Corrected Records: {summary['corrected_count']:,}")
    logger.info(f"Quarantined:       {summary['quarantine_count']:,}")
    logger.info(f"Consistency Check: {summary['consistency_check']} (Raw == Valid + Corrected + Quarantined)")
    logger.info(f"Elapsed Time:      {summary['elapsed_seconds']}s")
    logger.info(f"Processing Speed:  {summary['throughput_rps']:,.2f} rows/s")
    logger.info(f"Rules Triggered:   {summary['rule_distribution']}")
    logger.info(f"Quarantine Errors: {summary['error_distribution']}")
    logger.info("=" * 60)

    return summary

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m src.elt_pipeline <id_run>")
        sys.exit(1)
    run_elt_transform_and_classify(sys.argv[1])
