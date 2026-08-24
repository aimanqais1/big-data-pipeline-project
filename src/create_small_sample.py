"""
Reproducible Small Sample Generator for Midterm Data Pipeline.
Extracts an exact N-row sample from the huge CSV dataset using Python standard csv streaming
to properly handle quoted JSON columns and escape characters without loading the dataset into memory.
"""
import os
import sys
import csv
import argparse
import logging
from pathlib import Path

from config.settings import DATA_DIR, CSV_ENCODING

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def create_sample(
    input_path: str,
    output_path: str,
    target_rows: int = 100000
) -> dict:
    """
    Stream rows using csv.reader and write using csv.writer.
    Guarantees O(1) memory usage and strict CSV standard RFC-4180 compliance.
    """
    in_file = Path(input_path)
    out_file = Path(output_path)

    if not in_file.exists():
        raise FileNotFoundError(f"Input CSV file does not exist: {input_path}")

    out_file.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Generating small sample from: {input_path}")
    logger.info(f"Target rows: {target_rows:,} -> Output: {output_path}")

    rows_written = 0

    with open(in_file, mode="r", encoding=CSV_ENCODING, errors="replace", newline="") as fin, \
         open(out_file, mode="w", encoding="utf-8-sig", newline="") as fout:

        reader = csv.reader(fin)
        writer = csv.writer(fout, quoting=csv.QUOTE_MINIMAL)

        # 1. Read and write Header
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("Input CSV is empty.")
        
        writer.writerow(header)

        # 2. Stream and write row-by-row
        for row in reader:
            writer.writerow(row)
            rows_written += 1
            if rows_written >= target_rows:
                break

    out_size_mb = out_file.stat().st_size / (1024 * 1024)
    logger.info(f"Sample creation complete. Written {rows_written:,} rows. File size: {out_size_mb:.2f} MB")

    return {
        "input_file": str(in_file),
        "output_file": str(out_file),
        "rows_written": rows_written,
        "size_mb": round(out_size_mb, 2)
    }

def main():
    parser = argparse.ArgumentParser(description="Create a small CSV sample from a large CSV dataset.")
    parser.add_argument("--input", "-i", default="orders_huge_mixed_quality.csv", help="Input CSV path")
    parser.add_argument("--output", "-o", default="data/orders_sample_100k.csv", help="Output CSV path")
    parser.add_argument("--rows", "-r", type=int, default=100000, help="Number of data rows to sample")
    args = parser.parse_args()

    try:
        res = create_sample(args.input, args.output, args.rows)
        print("Sample Summary:", res)
    except Exception as e:
        logger.error(f"Error creating sample: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
