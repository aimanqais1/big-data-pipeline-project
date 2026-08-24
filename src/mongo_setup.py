"""
MongoDB setup and initialization module for the Midterm Data Pipeline.
Configures database connection, verifies accessibility, sets up collections,
and creates the required Unique Index on id_order in orders_validated.
"""
import sys
import logging
from typing import Dict, Any
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, OperationFailure

from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
    MONGO_TIMEOUT_MS,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    STABLE_BUSINESS_KEY
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def get_mongo_client(uri: str = MONGO_URI, timeout_ms: int = MONGO_TIMEOUT_MS) -> MongoClient:
    """Create and return a MongoDB client instance with timeout."""
    return MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)

def get_database(client: MongoClient = None, db_name: str = MONGO_DB_NAME):
    """Retrieve database object from client."""
    if client is None:
        client = get_mongo_client()
    return client[db_name]

def verify_connection(client: MongoClient = None) -> bool:
    """Ping MongoDB server to ensure active connection."""
    if client is None:
        client = get_mongo_client()
    try:
        client.admin.command('ping')
        return True
    except ConnectionFailure as e:
        logger.error(f"MongoDB connection failed: {e}")
        return False

def setup_mongodb_collections(db_name: str = MONGO_DB_NAME, drop_existing: bool = False) -> Dict[str, Any]:
    """
    Initialize required collections and indexes.
    - orders_raw: NO unique indexes, allows dirty data load first.
    - orders_validated: Unique index on STABLE_BUSINESS_KEY (id_order).
    - orders_quarantine: Index on run_id for fast audit/querying.
    """
    client = get_mongo_client()
    if not verify_connection(client):
        raise ConnectionFailure("Could not connect to MongoDB server.")

    db = client[db_name]

    if drop_existing:
        logger.warning(f"Dropping existing collections in database: {db_name}")
        db[RAW_COLLECTION].drop()
        db[VALIDATED_COLLECTION].drop()
        db[QUARANTINE_COLLECTION].drop()

    # 1. Raw collection: Ensure no unique constraints
    raw_col = db[RAW_COLLECTION]
    # Create non-unique index on id_run for query efficiency
    raw_col.create_index([("id_run", ASCENDING)], unique=False)

    # 2. Validated collection: Unique Index on id_order (Enforces business entity stability)
    val_col = db[VALIDATED_COLLECTION]
    val_col.create_index([(STABLE_BUSINESS_KEY, ASCENDING)], unique=True, name="uniq_id_order")

    # 3. Quarantine collection: Index on run_id and order_id (non-unique)
    quar_col = db[QUARANTINE_COLLECTION]
    quar_col.create_index([("id_run", ASCENDING)], unique=False)
    quar_col.create_index([(STABLE_BUSINESS_KEY, ASCENDING)], unique=False)

    indexes_created = {
        RAW_COLLECTION: [idx["name"] for idx in raw_col.list_indexes()],
        VALIDATED_COLLECTION: [idx["name"] for idx in val_col.list_indexes()],
        QUARANTINE_COLLECTION: [idx["name"] for idx in quar_col.list_indexes()],
    }

    logger.info("MongoDB setup successfully completed.")
    logger.info(f"Indexes created: {indexes_created}")
    return indexes_created

if __name__ == "__main__":
    drop_flag = "--reset" in sys.argv
    try:
        results = setup_mongodb_collections(drop_existing=drop_flag)
        print("MongoDB Setup Result:", results)
    except Exception as ex:
        logger.error(f"Setup failed: {ex}")
        sys.exit(1)
