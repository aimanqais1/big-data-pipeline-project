"""
Unit Tests for MongoDB Schema Validation and Indexes (src/mongo_setup.py).
Verifies:
- orders_raw: NO validator, NO unique index, accepts dirty/raw records.
- orders_validated: Formal $jsonSchema validator + unique index on id_order.
- orders_quarantine: NO validator, unique compound index on (id_run, source_row_number).
"""
import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure, DuplicateKeyError
from src.mongo_setup import setup_mongodb_collections, get_mongo_client

TEST_DB_NAME = "midterm_ecommerce_schema_test"

@pytest.fixture(scope="module")
def schema_db():
    client = get_mongo_client()
    client.drop_database(TEST_DB_NAME)
    setup_mongodb_collections(db_name=TEST_DB_NAME, drop_existing=False)
    db = client[TEST_DB_NAME]
    yield db
    client.drop_database(TEST_DB_NAME)

def test_orders_raw_schema_and_indexes(schema_db):
    """orders_raw: NO validator, NO unique index, accepts raw record."""
    # 1. Check validator is None
    col_info = schema_db.command({"listCollections": 1, "filter": {"name": "orders_raw"}})
    validator = col_info["cursor"]["firstBatch"][0].get("options", {}).get("validator")
    assert validator is None

    # 2. Check indexes: NO unique index
    indexes = list(schema_db["orders_raw"].list_indexes())
    assert not any(idx.get("unique", False) for idx in indexes)
    assert any(idx["name"] == "idx_id_run" for idx in indexes)

    # 3. Insert real raw document -> PASS
    raw_doc = {
        "id_run": "test_run_01",
        "source_file": "test.csv",
        "source_row_number": 1,
        "ingested_at": "2025-01-15T10:00:00",
        "engine_used": "python_batch",
        "raw_record": {"order_id": "طلب-100", "any_dirty_field": "???"}
    }
    res = schema_db["orders_raw"].insert_one(raw_doc)
    assert res.inserted_id is not None

def test_orders_validated_schema_and_indexes(schema_db):
    """orders_validated: Formal $jsonSchema validator + Unique Index on id_order."""
    # 1. Check validator is installed
    col_info = schema_db.command({"listCollections": 1, "filter": {"name": "orders_validated"}})
    validator = col_info["cursor"]["firstBatch"][0].get("options", {}).get("validator")
    assert validator is not None
    assert "$jsonSchema" in validator

    # 2. Check unique index on id_order
    indexes = list(schema_db["orders_validated"].list_indexes())
    uniq_idx = next((idx for idx in indexes if idx["name"] == "uniq_id_order"), None)
    assert uniq_idx is not None
    assert uniq_idx.get("unique", False) is True

    # 3. Valid document insert -> PASS
    valid_doc = {
        "id_order": "طلب-90001",
        "order_date": "2025-01-15T10:00:00",
        "status": "مؤكد",
        "customer_id": "CUST-100",
        "customer_name": "أحمد علي",
        "customer_phone": "771234567",
        "customer_email": "ahmed@example.com",
        "city": "صنعاء",
        "district": "السبعين",
        "delivery_type": "عادي",
        "delivery_cost": 2000.0,
        "payment_method": "بطاقة",
        "payment_status": "تم الدفع",
        "payment_amount": 22000.0,
        "currency": "YER",
        "total_amount": 22000.0,
        "items": [
            {
                "sku": "SKU-1001",
                "name": "شاحن سريع",
                "qty": 2,
                "unit_price": 10000.0,
                "total": 20000.0
            }
        ],
        "classification": "valid",
        "quality_status": "valid",
        "corrections": [],
        "id_run": "test_run_01",
        "source_file": "test.csv",
        "source_row_number": 1,
        "processed_at": "2025-01-15T10:00:01",
        "raw_record": {"order_id": "طلب-90001"}
    }
    res = schema_db["orders_validated"].insert_one(valid_doc)
    assert res.inserted_id is not None

    # 4. Missing id_order -> REJECTED
    inv_no_id = dict(valid_doc)
    inv_no_id.pop("_id", None)
    inv_no_id.pop("id_order", None)
    with pytest.raises(OperationFailure, match="Document failed validation"):
        schema_db["orders_validated"].insert_one(inv_no_id)

    # 5. Negative payment_amount -> REJECTED
    inv_neg_pay = dict(valid_doc)
    inv_neg_pay.pop("_id", None)
    inv_neg_pay["id_order"] = "طلب-90002"
    inv_neg_pay["payment_amount"] = -22000.0
    with pytest.raises(OperationFailure, match="Document failed validation"):
        schema_db["orders_validated"].insert_one(inv_neg_pay)

    # 6. Negative total_amount -> REJECTED
    inv_neg_tot = dict(valid_doc)
    inv_neg_tot.pop("_id", None)
    inv_neg_tot["id_order"] = "طلب-90003"
    inv_neg_tot["total_amount"] = -5000.0
    with pytest.raises(OperationFailure, match="Document failed validation"):
        schema_db["orders_validated"].insert_one(inv_neg_tot)

    # 7. Negative delivery_cost -> REJECTED
    inv_neg_del = dict(valid_doc)
    inv_neg_del.pop("_id", None)
    inv_neg_del["id_order"] = "طلب-90004"
    inv_neg_del["delivery_cost"] = -1000.0
    with pytest.raises(OperationFailure, match="Document failed validation"):
        schema_db["orders_validated"].insert_one(inv_neg_del)

    # 8. Empty items array -> REJECTED
    inv_empty_items = dict(valid_doc)
    inv_empty_items.pop("_id", None)
    inv_empty_items["id_order"] = "طلب-90005"
    inv_empty_items["items"] = []
    with pytest.raises(OperationFailure, match="Document failed validation"):
        schema_db["orders_validated"].insert_one(inv_empty_items)

    # 9. Duplicate id_order -> REJECTED by Unique Index
    dup_doc = dict(valid_doc)
    dup_doc.pop("_id", None)
    dup_doc["source_row_number"] = 2
    with pytest.raises(DuplicateKeyError):
        schema_db["orders_validated"].insert_one(dup_doc)

def test_orders_quarantine_schema_and_indexes(schema_db):
    """orders_quarantine: NO validator, Unique Compound Index on (id_run, source_row_number)."""
    # 1. Check validator is None
    col_info = schema_db.command({"listCollections": 1, "filter": {"name": "orders_quarantine"}})
    validator = col_info["cursor"]["firstBatch"][0].get("options", {}).get("validator")
    assert validator is None

    # 2. Check compound unique index
    indexes = list(schema_db["orders_quarantine"].list_indexes())
    quar_idx = next((idx for idx in indexes if idx["name"] == "uniq_quarantine_run_row"), None)
    assert quar_idx is not None
    assert quar_idx.get("unique", False) is True

    # 3. Valid quarantine insert -> PASS
    quar_doc = {
        "id_run": "test_run_01",
        "source_file": "test.csv",
        "source_row_number": 10,
        "classification": "quarantined",
        "id_order": "UNKNOWN_ORDER",
        "error_codes": ["MISSING_ORDER_ID"],
        "quarantine_reason": "Record failed validation with errors: MISSING_ORDER_ID",
        "raw_record": {"order_id": ""},
        "detected_at": "2025-01-15T10:00:00"
    }
    res = schema_db["orders_quarantine"].insert_one(quar_doc)
    assert res.inserted_id is not None

    # 4. Duplicate (id_run, source_row_number) -> REJECTED by Unique Compound Index
    dup_quar = dict(quar_doc)
    dup_quar.pop("_id", None)
    with pytest.raises(DuplicateKeyError):
        schema_db["orders_quarantine"].insert_one(dup_quar)
