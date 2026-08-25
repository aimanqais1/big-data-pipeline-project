"""
MongoDB setup and initialization module for the Midterm Data Pipeline.
Configures database connection, verifies accessibility, sets up collections,
and creates the required Unique Indexes:
- orders_validated: Unique Index on STABLE_BUSINESS_KEY (id_order).
- orders_quarantine: Unique Compound Index on (id_run, source_row_number) for Idempotent Quarantine.
- orders_raw: Non-unique Index on id_run for fast query without blocking raw ingestion.
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

# --------------------------------------------------------------------------
# Formal MongoDB JSON Schema Validator (orders_validated ONLY)
# --------------------------------------------------------------------------
VALIDATED_COLLECTION_VALIDATOR: Dict[str, Any] = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "id_order",
            "order_date",
            "status",
            "customer_id",
            "customer_name",
            "city",
            "district",
            "delivery_type",
            "delivery_cost",
            "payment_method",
            "payment_status",
            "currency",
            "total_amount",
            "items",
            "classification",
            "quality_status",
            "corrections",
            "id_run",
            "source_file",
            "source_row_number",
            "processed_at",
            "raw_record"
        ],
        "properties": {
            "_id": {"bsonType": "objectId"},
            "id_order": {"bsonType": "string", "minLength": 1},
            "order_date": {"bsonType": "string", "minLength": 1},
            "status": {"bsonType": "string", "minLength": 1},
            "customer_id": {"bsonType": "string", "minLength": 1},
            "customer_name": {"bsonType": "string"},
            "customer_phone": {"bsonType": ["string", "null"]},
            "customer_email": {"bsonType": ["string", "null"]},
            "city": {"bsonType": "string"},
            "district": {"bsonType": "string"},
            "delivery_type": {"bsonType": "string"},
            "delivery_cost": {"bsonType": "double", "minimum": 0.0},
            "payment_method": {"bsonType": "string"},
            "payment_status": {"bsonType": "string"},
            "payment_amount": {
                "bsonType": ["double", "null"],
                "minimum": 0.0
            },
            "currency": {"bsonType": "string"},
            "total_amount": {"bsonType": "double", "minimum": 0.0},
            "items": {
                "bsonType": "array",
                "minItems": 1,
                "items": {
                    "bsonType": "object",
                    "required": ["sku", "name", "qty", "unit_price", "total"],
                    "properties": {
                        "sku": {"bsonType": "string"},
                        "name": {"bsonType": "string"},
                        "qty": {"bsonType": ["int", "long"]},
                        "unit_price": {"bsonType": "double", "minimum": 0.0},
                        "total": {"bsonType": "double", "minimum": 0.0}
                    }
                }
            },
            "classification": {"bsonType": "string", "enum": ["valid", "corrected"]},
            "quality_status": {"bsonType": "string", "enum": ["valid", "corrected"]},
            "corrections": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": ["rule_code", "field", "original_value", "corrected_value", "reason"],
                    "properties": {
                        "rule_code": {
                            "bsonType": "string",
                            "enum": ["RULE_01", "RULE_02", "RULE_03", "RULE_04", "RULE_05", "RULE_06", "RULE_07", "RULE_08", "RULE_09", "RULE_10"]
                        },
                        "field": {"bsonType": "string"},
                        "original_value": {"bsonType": "string"},
                        "corrected_value": {"bsonType": ["string", "double", "int", "long"]},
                        "reason": {"bsonType": "string"}
                    }
                }
            },
            "id_run": {"bsonType": "string", "minLength": 1},
            "source_file": {"bsonType": "string", "minLength": 1},
            "source_row_number": {"bsonType": ["int", "long"], "minimum": 1},
            "processed_at": {"bsonType": "string", "minLength": 1},
            "raw_record": {"bsonType": "object"}
        }
    }
}

def _apply_collection_validator(db, col_name: str, validator: Dict[str, Any]):
    """Apply strict $jsonSchema validator to collection (create or collMod)."""
    existing_cols = db.list_collection_names()
    if col_name not in existing_cols:
        db.create_collection(
            col_name,
            validator=validator,
            validationLevel="strict",
            validationAction="error"
        )
    else:
        db.command({
            "collMod": col_name,
            "validator": validator,
            "validationLevel": "strict",
            "validationAction": "error"
        })

def setup_mongodb_collections(db_name: str = MONGO_DB_NAME, drop_existing: bool = False) -> Dict[str, Any]:
    """
    Initialize required collections, formal $jsonSchema validator on orders_validated, and unique indexes.
    - orders_raw: Raw ingestion layer (NO validator, NO unique index, non-unique index on id_run).
    - orders_validated: Strict formal $jsonSchema validator + unique index on id_order (uniq_id_order).
    - orders_quarantine: Quarantine layer (NO validator, unique compound index on (id_run, source_row_number)).
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

    # 1. Apply formal schema validator ONLY to orders_validated
    _apply_collection_validator(db, VALIDATED_COLLECTION, VALIDATED_COLLECTION_VALIDATOR)

    # 2. Raw collection: Non-unique index on id_run for fast streaming (NO validator, NO unique index)
    raw_col = db[RAW_COLLECTION]
    raw_col.create_index([("id_run", ASCENDING)], unique=False, name="idx_id_run")

    # 3. Validated collection: Unique Index on id_order (Enforces business entity stability)
    val_col = db[VALIDATED_COLLECTION]
    val_col.create_index([(STABLE_BUSINESS_KEY, ASCENDING)], unique=True, name="uniq_id_order")

    # 4. Quarantine collection: Unique Compound Index on (id_run, source_row_number) for idempotency
    quar_col = db[QUARANTINE_COLLECTION]
    quar_col.create_index(
        [("id_run", ASCENDING), ("source_row_number", ASCENDING)],
        unique=True,
        name="uniq_quarantine_run_row"
    )
    quar_col.create_index([(STABLE_BUSINESS_KEY, ASCENDING)], unique=False, name="idx_quar_order_id")

    indexes_created = {
        RAW_COLLECTION: [idx["name"] for idx in raw_col.list_indexes()],
        VALIDATED_COLLECTION: [idx["name"] for idx in val_col.list_indexes()],
        QUARANTINE_COLLECTION: [idx["name"] for idx in quar_col.list_indexes()],
    }

    logger.info("MongoDB setup successfully completed with formal schema on orders_validated & required indexes.")
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
