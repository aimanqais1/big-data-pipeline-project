"""
PySpark Distributed DataFrame Loader for Midterm Data Pipeline.
Processes large CSV datasets using Spark DataFrame API, enforces an Explicit Schema
(StringType for all raw fields to ensure data fidelity), constructs ELT raw documents,
and writes to MongoDB orders_raw collection in parallel via MongoDB Spark Connector.
"""
import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import (
    lit,
    col,
    struct,
    spark_partition_id,
    row_number,
    current_timestamp,
    date_format
)

from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
    RAW_COLLECTION,
    get_spark_classpath,
    SPARK_MONGO_CONNECTOR_VERSION,
    SPARK_SCALA_VERSION
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Explicit Schema: 17 columns all defined as StringType to prevent inferSchema data distortion
RAW_CSV_COLUMNS = [
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "delivery_cost",
    "payment_method",
    "payment_status",
    "payment_amount",
    "currency",
    "total_amount",
    "items_json"
]

EXPLICIT_CSV_SCHEMA = StructType([
    StructField(col_name, StringType(), nullable=True) for col_name in RAW_CSV_COLUMNS
])

def create_spark_session(
    app_name: str = "MidtermSparkLoader",
    master: str = "local[*]",
    db_name: str = MONGO_DB_NAME,
    collection_name: str = RAW_COLLECTION
) -> SparkSession:
    """
    Build and configure SparkSession with MongoDB Connector jars and connection properties.
    """
    mongo_write_uri = f"{MONGO_URI}{db_name}.{collection_name}"
    classpath = get_spark_classpath()

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.driver.extraClassPath", classpath)
        .config("spark.executor.extraClassPath", classpath)
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.mongodb.write.connection.uri", mongo_write_uri)
        .config("spark.mongodb.read.connection.uri", mongo_write_uri)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
    )
    return builder.getOrCreate()

def load_csv_to_raw_spark(
    file_path: str,
    id_run: str,
    db_name: str = MONGO_DB_NAME,
    master: str = "local[*]"
) -> Dict[str, Any]:
    """
    Read CSV via Spark DataFrame API with Explicit Schema and load into MongoDB orders_raw in parallel.
    """
    path_obj = Path(file_path).resolve()
    if not path_obj.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    logger.info("=" * 60)
    logger.info("STARTING PYSPARK DISTRIBUTED LOAD -> orders_raw")
    logger.info(f"Run ID:        {id_run}")
    logger.info(f"File:          {path_obj.name} ({path_obj.stat().st_size / (1024*1024):.2f} MB)")
    logger.info(f"Master:        {master}")
    logger.info(f"Scala Version: {SPARK_SCALA_VERSION} | Connector: {SPARK_MONGO_CONNECTOR_VERSION}")
    logger.info("=" * 60)

    spark = None
    start_time = time.perf_counter()

    try:
        spark = create_spark_session(
            app_name=f"MidtermLoader_{id_run}",
            master=master,
            db_name=db_name,
            collection_name=RAW_COLLECTION
        )

        # 1. Read CSV using Explicit Schema (No inferSchema)
        df_csv = (
            spark.read
            .format("csv")
            .schema(EXPLICIT_CSV_SCHEMA)
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("multiLine", "false")
            .option("quote", "\"")
            .option("escape", "\"")
            .load(str(path_obj))
        )

        input_partitions = df_csv.rdd.getNumPartitions()
        logger.info(f"Input partitions count: {input_partitions}")

        # 2. Compute Deterministic & Memory-Safe Sequential source_row_number (Int32, 1..N)
        df_with_pid = df_csv.withColumn("_pid", spark_partition_id())
        part_counts = df_with_pid.groupBy("_pid").count().orderBy("_pid").collect()

        offsets = []
        cum = 0
        for row in part_counts:
            offsets.append((int(row["_pid"]), int(cum)))
            cum += int(row["count"])

        offsets_df = spark.createDataFrame(offsets, ["_pid", "_offset"])
        w = Window.partitionBy("_pid").orderBy(lit(1))

        df_indexed = (
            df_with_pid.join(offsets_df, on="_pid")
            .withColumn("source_row_number", (col("_offset") + row_number().over(w)).cast(IntegerType()))
            .drop("_pid", "_offset")
        )

        # 3. Build ELT Raw Document Structure
        # Wrap all 17 CSV columns inside a struct named 'raw_record'
        df_raw = df_indexed.select(
            lit(id_run).alias("id_run"),
            lit(path_obj.name).alias("source_file"),
            col("source_row_number"),
            date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSxxx").alias("ingested_at"),
            lit("pyspark").alias("engine_used"),
            struct([col(c) for c in RAW_CSV_COLUMNS]).alias("raw_record")
        )

        # 4. Write parallel to MongoDB orders_raw via Connector
        logger.info("Writing DataFrame to MongoDB orders_raw via Spark Connector...")
        df_raw.write.format("mongodb").mode("append").save()

        # 5. Measure metrics
        record_count = cum
        elapsed_seconds = time.perf_counter() - start_time
        throughput = record_count / elapsed_seconds if elapsed_seconds > 0 else 0.0

        summary = {
            "id_run": id_run,
            "engine_used": "pyspark",
            "file_name": path_obj.name,
            "file_size_mb": round(path_obj.stat().st_size / (1024 * 1024), 2),
            "read_rows": record_count,
            "loaded_raw": record_count,
            "partitions": input_partitions,
            "seconds_elapsed": round(elapsed_seconds, 3),
            "throughput_rps": round(throughput, 2),
            "spark_version": spark.version,
            "scala_version": SPARK_SCALA_VERSION,
            "connector_version": SPARK_MONGO_CONNECTOR_VERSION,
            "errors": []
        }

        logger.info("=" * 60)
        logger.info("PYSPARK LOAD SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total Rows Ingested: {summary['loaded_raw']:,}")
        logger.info(f"Input Partitions:    {summary['partitions']}")
        logger.info(f"Elapsed Time:        {summary['seconds_elapsed']}s")
        logger.info(f"Throughput:          {summary['throughput_rps']:,.2f} rows/s")
        logger.info("=" * 60)

        return summary

    except Exception as e:
        logger.error(f"PySpark loading failed: {e}", exc_info=True)
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
                logger.info("SparkSession cleanly stopped.")
            except Exception as se:
                logger.warning(f"Error stopping SparkSession: {se}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.spark_loader <csv_path>")
        sys.exit(1)
    load_csv_to_raw_spark(sys.argv[1], "test_manual_spark")
