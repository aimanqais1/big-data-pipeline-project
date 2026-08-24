import os
import sys
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
DOCS_DIR = BASE_DIR / "docs"
JARS_DIR = BASE_DIR / "jars"

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "midterm_ecommerce")
MONGO_TIMEOUT_MS = int(os.getenv("MONGO_TIMEOUT_MS", "5000"))

# Collection Names
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"

# Engine & Processing Thresholds (Single Source of Truth)
SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "200.0"))

# Python Batch Loader Configuration
DEFAULT_BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))

# Java 17 Compatibility Path for Spark 4.2.0 (Eclipse Adoptium JDK 17)
JDK17_DEFAULT_PATH = r"C:\Users\MC\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.20.8-hotspot"
PYTHON_EXE = sys.executable

def configure_spark_env():
    """Configure runtime environment variables for PySpark and Java 17 compatibility."""
    current_java_home = os.environ.get("JAVA_HOME")
    if not current_java_home or not os.path.exists(os.path.join(current_java_home, "bin", "java.exe")):
        if os.path.exists(JDK17_DEFAULT_PATH):
            os.environ["JAVA_HOME"] = JDK17_DEFAULT_PATH
            java_bin = os.path.join(JDK17_DEFAULT_PATH, "bin")
            if java_bin not in os.environ.get("PATH", ""):
                os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")
    
    os.environ["PYSPARK_PYTHON"] = PYTHON_EXE
    os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXE

# Auto configure on import
configure_spark_env()

def get_spark_classpath() -> str:
    """Return local jar classpath containing MongoDB Spark Connector and its dependencies."""
    jar_files = [str(f) for f in JARS_DIR.glob("*.jar")]
    return ";".join(jar_files)

# Spark MongoDB Connector Verified Coordinate & Local Jars
SPARK_MONGO_CONNECTOR_VERSION = "10.4.0"
SPARK_SCALA_VERSION = "2.13"

# Stable Business Key
STABLE_BUSINESS_KEY = "id_order"

# CSV Encoding
CSV_ENCODING = "utf-8-sig"
