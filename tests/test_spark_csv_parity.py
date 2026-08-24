"""
Unit test verifying parity between Python csv.DictReader and PySpark CSV reader for quoted JSON strings.
"""
import csv
import unittest
from pathlib import Path
from src.spark_loader import EXPLICIT_CSV_SCHEMA, create_spark_session

class TestSparkCSVReaderParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark_session(master="local[2]", app_name="TestSparkCSVParity")
        cls.sample_path = Path("data/orders_sample_100k.csv").resolve()

    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            cls.spark.stop()

    def test_json_parity_first_100_rows(self):
        # 1. Read first 100 rows with Python csv.DictReader
        python_items = []
        with open(self.sample_path, mode="r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= 100:
                    break
                python_items.append(row["items_json"])

        # 2. Read first 100 rows with PySpark DataFrame
        df = (
            self.spark.read
            .format("csv")
            .schema(EXPLICIT_CSV_SCHEMA)
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("quote", "\"")
            .option("escape", "\"")
            .load(str(self.sample_path))
        )
        spark_rows = df.limit(100).collect()
        spark_items = [r["items_json"] for r in spark_rows]

        self.assertEqual(len(python_items), len(spark_items))
        for i in range(len(python_items)):
            self.assertEqual(
                python_items[i],
                spark_items[i],
                f"Mismatch at row {i}:\nPython: {python_items[i]}\nSpark:  {spark_items[i]}"
            )

if __name__ == "__main__":
    unittest.main()
