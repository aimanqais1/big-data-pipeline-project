"""
Python Streaming Batch Loader for Midterm Data Pipeline.
Reads CSV via streaming chunks, builds untransformed raw documents with ELT metadata,
and inserts them into orders_raw in batches.
Guarantees O(1) memory consumption without calling list(reader).
"""
import os
import sys
import csv
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Generator, List

from pymongo import MongoClient
from pymongo.errors import PyMongoError, BulkWriteError

from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
    RAW_COLLECTION,
    DEFAULT_BATCH_SIZE,
    CSV_ENCODING
)
from src.mongo_setup import get_database

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def stream_csv_batches(
    file_path: str,
    id_run: str,
    batch_size: int = DEFAULT_BATCH_SIZE
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Generator that yields batches of raw ELT documents from CSV without loading full file into memory.
    """
    path_obj = Path(file_path).resolve()
    if not path_obj.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    row_number = 0
    batch = []

    with open(path_obj, mode="r", encoding=CSV_ENCODING, errors="replace", newline="") as fin:
        reader = csv.DictReader(fin)
        for row in reader:
            row_number += 1
            raw_doc = {
                "id_run": id_run,
                "source_file": path_obj.name,
                "source_row_number": row_number,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "engine_used": "python_batch",
                "raw_record": dict(row)  # Exact original strings preserved
            }
            batch.append(raw_doc)

            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

def load_csv_to_raw_batch(
    file_path: str,
    id_run: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    db_name: str = MONGO_DB_NAME
) -> Dict[str, Any]:
    """
    Loads raw CSV data into orders_raw collection using streaming batches.
    Collects performance metrics: time, records, throughput, and errors.
    """
    db = get_database(db_name=db_name)
    raw_col = db[RAW_COLLECTION]

    logger.info("=" * 60)
    logger.info(f"STARTING PYTHON STREAMING BATCH INGESTION -> orders_raw")
    logger.info(f"Run ID:     {id_run}")
    logger.info(f"File:       {file_path}")
    logger.info(f"Batch Size: {batch_size:,}")
    logger.info("=" * 60)

    total_rows = 0
    total_inserted = 0
    batch_index = 0
    batch_metrics = []
    errors_list = []

    start_total_time = time.perf_counter()

    try:
        for batch in stream_csv_batches(file_path, id_run, batch_size):
            batch_index += 1
            batch_len = len(batch)
            total_rows += batch_len

            batch_start = time.perf_counter()
            try:
                result = raw_col.insert_many(batch, ordered=False)
                inserted_count = len(result.inserted_ids)
                total_inserted += inserted_count
            except BulkWriteError as bwe:
                inserted_count = bwe.details.get("nInserted", 0)
                total_inserted += inserted_count
                err_msg = f"BulkWriteError in batch {batch_index}: {len(bwe.details.get('writeErrors', []))} write errors"
                logger.error(err_msg)
                errors_list.append(err_msg)
            except PyMongoError as pme:
                err_msg = f"PyMongoError in batch {batch_index}: {str(pme)}"
                logger.error(err_msg)
                errors_list.append(err_msg)

            batch_elapsed = time.perf_counter() - batch_start
            batch_throughput = batch_len / batch_elapsed if batch_elapsed > 0 else 0.0

            metric = {
                "batch_number": batch_index,
                "records": batch_len,
                "elapsed_seconds": round(batch_elapsed, 4),
                "throughput_rps": round(batch_throughput, 2),
                "errors_count": len(errors_list)
            }
            batch_metrics.append(metric)

            if batch_index % 5 == 0 or batch_len < batch_size:
                logger.info(
                    f"Batch #{batch_index:03d} | Rows: {batch_len:,} | "
                    f"Time: {batch_elapsed:.3f}s | Speed: {batch_throughput:,.0f} rows/s | "
                    f"Total Ingested: {total_inserted:,}"
                )

    except Exception as ex:
        logger.error(f"Critical error during streaming batch ingestion: {ex}")
        errors_list.append(str(ex))
        raise

    total_elapsed = time.perf_counter() - start_total_time
    overall_throughput = total_rows / total_elapsed if total_elapsed > 0 else 0.0

    summary = {
        "id_run": id_run,
        "engine_used": "python_batch",
        "file_name": Path(file_path).name,
        "read_rows": total_rows,
        "loaded_raw": total_inserted,
        "batches_count": batch_index,
        "batch_size": batch_size,
        "seconds_elapsed": round(total_elapsed, 3),
        "throughput_rps": round(overall_throughput, 2),
        "errors": errors_list
    }

    logger.info("=" * 60)
    logger.info("PYTHON BATCH INGESTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total Read Rows:    {summary['read_rows']:,}")
    logger.info(f"Total Loaded Raw:   {summary['loaded_raw']:,}")
    logger.info(f"Total Batches:      {summary['batches_count']}")
    logger.info(f"Elapsed Time:       {summary['seconds_elapsed']}s")
    logger.info(f"Overall Throughput: {summary['throughput_rps']:,.2f} rows/s")
    logger.info(f"Errors Encountered: {len(summary['errors'])}")
    logger.info("=" * 60)

    return summary

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.batch_loader <csv_path> [batch_size]")
        sys.exit(1)
    f_path = sys.argv[1]
    b_size = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_BATCH_SIZE
    res = load_csv_to_raw_batch(f_path, "test_manual_batch", b_size)
