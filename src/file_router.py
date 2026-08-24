"""
File Router module for Midterm Data Pipeline.
Inspects incoming CSV file metadata, computes exact file size in MB,
generates a unique id_run, and deterministically routes to either:
- Python Streaming Batch Loader (file_size <= SMALL_FILE_THRESHOLD_MB)
- PySpark Distributed DataFrame Loader (file_size > SMALL_FILE_THRESHOLD_MB)
"""
import os
import sys
import uuid
import argparse
import logging
from pathlib import Path
from typing import Dict, Any

from config.settings import SMALL_FILE_THRESHOLD_MB

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ENGINE_PYTHON_BATCH = "python_batch"
ENGINE_PYSPARK = "pyspark"

def inspect_and_route(file_path: str, custom_run_id: str = None) -> Dict[str, Any]:
    """
    Evaluate file size and return routing decision with a unique id_run.
    """
    path_obj = Path(file_path).resolve()
    if not path_obj.exists():
        raise FileNotFoundError(f"Input file not found at: {file_path}")

    if not path_obj.is_file():
        raise ValueError(f"Specified path is not a file: {file_path}")

    size_bytes = path_obj.stat().st_size
    size_mb = size_bytes / (1024 * 1024)

    run_id = custom_run_id or f"run_{uuid.uuid4().hex[:12]}"

    if size_mb <= SMALL_FILE_THRESHOLD_MB:
        selected_engine = ENGINE_PYTHON_BATCH
        reason = (
            f"File size ({size_mb:.2f} MB) is <= threshold ({SMALL_FILE_THRESHOLD_MB:.1f} MB). "
            f"Selected Python Streaming Batch engine for low memory footprint and rapid single-node processing."
        )
    else:
        selected_engine = ENGINE_PYSPARK
        reason = (
            f"File size ({size_mb:.2f} MB) exceeds threshold ({SMALL_FILE_THRESHOLD_MB:.1f} MB). "
            f"Selected PySpark Distributed DataFrame engine for parallel out-of-core execution."
        )

    decision = {
        "id_run": run_id,
        "file_name": path_obj.name,
        "file_path": str(path_obj),
        "file_size_bytes": size_bytes,
        "file_size_mb": round(size_mb, 2),
        "threshold_mb": SMALL_FILE_THRESHOLD_MB,
        "selected_engine": selected_engine,
        "reason": reason
    }

    logger.info("=" * 60)
    logger.info("FILE ROUTER DECISION")
    logger.info("=" * 60)
    logger.info(f"Run ID:          {decision['id_run']}")
    logger.info(f"File Name:       {decision['file_name']}")
    logger.info(f"File Size:       {decision['file_size_mb']} MB ({decision['file_size_bytes']:,} bytes)")
    logger.info(f"Threshold:       {decision['threshold_mb']} MB")
    logger.info(f"Selected Engine: {decision['selected_engine']}")
    logger.info(f"Reason:          {decision['reason']}")
    logger.info("=" * 60)

    return decision

def main():
    parser = argparse.ArgumentParser(description="Route file to processing engine based on file size.")
    parser.add_argument("--file", "-f", required=True, help="Path to input CSV file")
    parser.add_argument("--run-id", help="Optional custom run id")
    args = parser.parse_args()

    try:
        decision = inspect_and_route(args.file, args.run_id)
        print(f"\nRouter Output Engine: {decision['selected_engine']}")
    except Exception as e:
        logger.error(f"Routing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
