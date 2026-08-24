"""
ELT Pipeline Transformation & Classification Module for Midterm Data Pipeline.
Implements memory-safe streaming batch transformation and idempotent upsert:
1. Stream uncleaned raw records from orders_raw by id_run using MongoDB Cursor.
2. Apply deterministic cleaning rules, track Audit Trail, and classify.
3. Memory-safe batch buffers: flushes batches to MongoDB and clears RAM immediately (O(1) memory).
4. Idempotent Upsert for orders_validated (Business Key: id_order).
5. Idempotent Upsert for orders_quarantine (Key: id_run + source_row_number).
6. Verifies the strict consistency equation: Raw = Valid + Corrected + Quarantined.
"""
import time
import logging
from typing import Dict, Any, List
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from config.settings import (
    MONGO_DB_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    STABLE_BUSINESS_KEY,
    DEFAULT_BATCH_SIZE
)
from src.mongo_setup import get_database
from src.quality_rules import process_and_classify_record

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def flush_validated_batch(
    val_col,
    buffer: List[Dict[str, Any]],
    counts: Dict[str, int]
) -> None:
    """
    Executes idempotent bulk upsert on orders_validated using Stable Business Key (id_order).
    Directly updates inserted, updated, and unchanged counts from BulkWriteResult.
    """
    if not buffer:
        return

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

    try:
        res = val_col.bulk_write(ops, ordered=False)
        inserted = res.upserted_count
        updated = res.modified_count
        matched = res.matched_count
        unchanged = matched - updated

        counts["count_inserted"] += inserted
        counts["count_updated"] += updated
        counts["count_unchanged"] += unchanged

    except BulkWriteError as bwe:
        err_msg = f"BulkWriteError in validated upsert: {bwe.details}"
        logger.error(err_msg)
        raise

    buffer.clear()

def flush_quarantine_batch(
    quar_col,
    buffer: List[Dict[str, Any]],
    counts: Dict[str, int]
) -> None:
    """
    Executes idempotent bulk upsert on orders_quarantine using compound key (id_run, source_row_number).
    Guarantees re-running the same id_run does not create duplicate quarantine documents.
    """
    if not buffer:
        return

    ops = []
    for doc in buffer:
        ops.append(
            UpdateOne(
                filter={
                    "id_run": doc.get("id_run"),
                    "source_row_number": doc.get("source_row_number")
                },
                update={"$set": doc},
                upsert=True
            )
        )

    try:
        res = quar_col.bulk_write(ops, ordered=False)
        counts["quar_inserted"] += res.upserted_count
        counts["quar_updated"] += res.modified_count
        counts["quar_unchanged"] += (res.matched_count - res.modified_count)
    except BulkWriteError as bwe:
        err_msg = f"BulkWriteError in quarantine upsert: {bwe.details}"
        logger.error(err_msg)
        raise

    buffer.clear()

def run_elt_transform_and_classify(
    id_run: str,
    db_name: str = MONGO_DB_NAME,
    batch_size: int = DEFAULT_BATCH_SIZE
) -> Dict[str, Any]:
    """
    Memory-safe ELT streaming pipeline:
    Processes orders_raw cursor in fixed-size buffers, writes idempotent upserts,
    clears buffers immediately, and enforces strict consistency.
    """
    db = get_database(db_name=db_name)
    raw_col = db[RAW_COLLECTION]
    val_col = db[VALIDATED_COLLECTION]
    quar_col = db[QUARANTINE_COLLECTION]

    logger.info("=" * 60)
    logger.info("STARTING MEMORY-SAFE STREAMING ELT & IDEMPOTENT UPSERT")
    logger.info(f"Run ID:     {id_run}")
    logger.info(f"Batch Size: {batch_size:,}")
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
            "count_inserted": 0,
            "count_updated": 0,
            "count_unchanged": 0,
            "consistency_check": "PASS",
            "elapsed_seconds": 0.0
        }

    valid_count = 0
    corrected_count = 0
    quarantine_count = 0

    upsert_counts = {
        "count_inserted": 0,
        "count_updated": 0,
        "count_unchanged": 0,
        "quar_inserted": 0,
        "quar_updated": 0,
        "quar_unchanged": 0
    }

    validated_buffer: List[Dict[str, Any]] = []
    quarantine_buffer: List[Dict[str, Any]] = []
    error_distribution: Dict[str, int] = {}
    rule_distribution: Dict[str, int] = {}

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
                error_distribution[err] = error_distribution.get(err, 0) + 1

            if len(quarantine_buffer) >= batch_size:
                flush_quarantine_batch(quar_col, quarantine_buffer, upsert_counts)

        elif cls_type == "corrected":
            corrected_count += 1
            validated_buffer.append(classified)

            for corr in classified.get("corrections", []):
                rcode = corr.get("rule_code", "UNKNOWN_RULE")
                rule_distribution[rcode] = rule_distribution.get(rcode, 0) + 1

            if len(validated_buffer) >= batch_size:
                flush_validated_batch(val_col, validated_buffer, upsert_counts)

        elif cls_type == "valid":
            valid_count += 1
            validated_buffer.append(classified)

            if len(validated_buffer) >= batch_size:
                flush_validated_batch(val_col, validated_buffer, upsert_counts)

        if processed_count % 25000 == 0:
            logger.info(
                f"ELT Stream: {processed_count:,}/{total_raw_in_db:,} | "
                f"Valid: {valid_count:,} | Corrected: {corrected_count:,} | Quarantined: {quarantine_count:,}"
            )

    # Flush final remaining records
    flush_validated_batch(val_col, validated_buffer, upsert_counts)
    flush_quarantine_batch(quar_col, quarantine_buffer, upsert_counts)

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
        "count_inserted": upsert_counts["count_inserted"],
        "count_updated": upsert_counts["count_updated"],
        "count_unchanged": upsert_counts["count_unchanged"],
        "quarantine_inserted": upsert_counts["quar_inserted"],
        "quarantine_updated": upsert_counts["quar_updated"],
        "quarantine_unchanged": upsert_counts["quar_unchanged"],
        "consistency_check": "PASS" if consistency_passed else "FAIL",
        "elapsed_seconds": round(elapsed, 3),
        "throughput_rps": round(total_raw_in_db / elapsed, 2) if elapsed > 0 else 0.0,
        "error_distribution": error_distribution,
        "rule_distribution": rule_distribution
    }

    logger.info("=" * 60)
    logger.info("STREAMING ELT & IDEMPOTENT UPSERT SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Raw Ingested:          {summary['raw_count']:,}")
    logger.info(f"Valid Records:         {summary['valid_count']:,}")
    logger.info(f"Corrected Records:     {summary['corrected_count']:,}")
    logger.info(f"Quarantined Records:   {summary['quarantine_count']:,}")
    logger.info(f"Consistency Check:     {summary['consistency_check']}")
    logger.info(f"Validated Inserted:    {summary['count_inserted']:,}")
    logger.info(f"Validated Updated:     {summary['count_updated']:,}")
    logger.info(f"Validated Unchanged:   {summary['count_unchanged']:,}")
    logger.info(f"Quarantine Inserted:   {summary['quarantine_inserted']:,}")
    logger.info(f"Quarantine Unchanged:  {summary['quarantine_unchanged']:,}")
    logger.info(f"Total Elapsed Time:    {summary['elapsed_seconds']}s")
    logger.info(f"Streaming Throughput:  {summary['throughput_rps']:,.2f} rows/s")
    logger.info("=" * 60)

    return summary

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m src.elt_pipeline <id_run>")
        sys.exit(1)
    run_elt_transform_and_classify(sys.argv[1])
