"""
Unit and Integration Tests for Phase 2 — Step 1B Query Module (`src/queries/order_queries.py`).
Verifies:
1. Query 01 (`orders_by_city_and_status`) returns expected structure and projection.
2. Query 02 (`customer_order_history`) returns expected structure and projection.
3. Query 03 (`high_value_orders_by_date_range`) accepts valid calendar-aware date ranges.
4. Query 03 rejects invalid date range ordering (`start_date > end_date`) and impossible dates (`2025-02-31`).
5. Query 04 (`orders_by_product_sku`) queries `items.sku` correctly.
6. Query 05 (`payment_settlement_audit`) applies payment filters and descending `total_amount` sort correctly.
7. `limit` parameter is strictly respected and invalid limits are rejected.
8. All query results are 100% JSON-serializable (`json.dumps`).
9. Strictly read-only: zero document writes and zero index modifications occur.
"""
import calendar
import json
from datetime import datetime
import pytest

from config.settings import MONGO_DB_NAME, VALIDATED_COLLECTION
from src.mongo_setup import get_database
from src.queries import (
    orders_by_city_and_status,
    customer_order_history,
    high_value_orders_by_date_range,
    orders_by_product_sku,
    payment_settlement_audit,
    list_available_queries,
    execute_query_by_name,
)


@pytest.fixture(scope="module")
def sample_doc():
    """Fetch a single existing validated document from midterm_ecommerce for dynamic parameter testing."""
    db = get_database(db_name=MONGO_DB_NAME)
    doc = db[VALIDATED_COLLECTION].find_one({"items.0": {"$exists": True}})
    assert doc is not None, f"Expected validated documents in {MONGO_DB_NAME}.{VALIDATED_COLLECTION}"
    return doc


def test_query_01_orders_by_city_and_status(sample_doc):
    """1. Query 01 returns expected structure, projection fields, and descending order_date sort."""
    target_city = sample_doc["city"]
    target_status = sample_doc["status"]

    res = orders_by_city_and_status(city=target_city, status=target_status, limit=5, db_name=MONGO_DB_NAME)

    assert res["query_name"] == "orders_by_city_and_status"
    assert res["collection"] == VALIDATED_COLLECTION
    assert res["parameters"] == {"city": target_city, "status": target_status, "limit": 5}
    assert isinstance(res["results"], list)
    assert res["count"] == len(res["results"])
    assert 1 <= res["count"] <= 5

    expected_keys = {
        "id_order", "order_date", "city", "district",
        "status", "delivery_type", "total_amount", "quality_status"
    }
    for item in res["results"]:
        assert "_id" not in item
        assert set(item.keys()) == expected_keys
        assert item["city"] == target_city
        assert item["status"] == target_status

    # Verify descending order_date sort
    dates = [r["order_date"] for r in res["results"]]
    assert dates == sorted(dates, reverse=True)


def test_query_02_customer_order_history(sample_doc):
    """2. Query 02 returns expected structure and matches target customer_id."""
    target_customer = sample_doc["customer_id"]

    res = customer_order_history(customer_id=target_customer, limit=10, db_name=MONGO_DB_NAME)

    assert res["query_name"] == "customer_order_history"
    assert res["collection"] == VALIDATED_COLLECTION
    assert res["parameters"] == {"customer_id": target_customer, "limit": 10}
    assert isinstance(res["results"], list)
    assert res["count"] == len(res["results"])
    assert 1 <= res["count"] <= 10

    expected_keys = {
        "id_order", "customer_id", "customer_name", "order_date",
        "status", "city", "payment_status", "total_amount", "quality_status"
    }
    for item in res["results"]:
        assert "_id" not in item
        assert set(item.keys()) == expected_keys
        assert item["customer_id"] == target_customer


def test_query_03_valid_date_range(sample_doc):
    """3. Query 03 accepts a valid calendar-aware date range and filters total_amount >= min_total."""
    dt = datetime.fromisoformat(sample_doc["order_date"])
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    start_iso = f"{dt.year:04d}-{dt.month:02d}-01T00:00:00"
    end_iso = f"{dt.year:04d}-{dt.month:02d}-{last_day:02d}T23:59:59"
    min_total = round(float(sample_doc["total_amount"]) * 0.5, 2)

    res = high_value_orders_by_date_range(
        start_date=start_iso,
        end_date=end_iso,
        min_total=min_total,
        limit=10,
        db_name=MONGO_DB_NAME
    )

    assert res["query_name"] == "high_value_orders_by_date_range"
    assert res["collection"] == VALIDATED_COLLECTION
    assert res["parameters"]["start_date"] == start_iso
    assert res["parameters"]["end_date"] == end_iso
    assert res["parameters"]["min_total"] == min_total
    assert 1 <= res["count"] <= 10

    expected_keys = {
        "id_order", "order_date", "customer_id", "city",
        "status", "payment_method", "total_amount"
    }
    for item in res["results"]:
        assert "_id" not in item
        assert set(item.keys()) == expected_keys
        assert start_iso <= item["order_date"] <= end_iso
        assert item["total_amount"] >= min_total

    dates = [r["order_date"] for r in res["results"]]
    assert dates == sorted(dates, reverse=True)


def test_query_03_rejects_invalid_date_range_and_impossible_dates():
    """4. Query 03 rejects start_date > end_date and impossible calendar dates like 2025-02-31."""
    # Reversed range: start_date > end_date
    with pytest.raises(ValueError, match="start_date .* must be <= end_date"):
        high_value_orders_by_date_range(
            start_date="2025-03-15T00:00:00",
            end_date="2025-03-01T00:00:00",
            min_total=1000.0,
            limit=5,
            db_name=MONGO_DB_NAME
        )

    # Impossible calendar date (Feb 31)
    with pytest.raises(ValueError, match="Invalid ISO-8601 date/timestamp"):
        high_value_orders_by_date_range(
            start_date="2025-02-01T00:00:00",
            end_date="2025-02-31T23:59:59",
            min_total=1000.0,
            limit=5,
            db_name=MONGO_DB_NAME
        )


def test_query_04_orders_by_product_sku(sample_doc):
    """5. Query 04 queries items.sku correctly and returns matching embedded items."""
    target_sku = sample_doc["items"][0]["sku"]

    res = orders_by_product_sku(sku=target_sku, limit=5, db_name=MONGO_DB_NAME)

    assert res["query_name"] == "orders_by_product_sku"
    assert res["collection"] == VALIDATED_COLLECTION
    assert res["parameters"] == {"sku": target_sku, "limit": 5}
    assert 1 <= res["count"] <= 5

    expected_keys = {"id_order", "order_date", "city", "status", "total_amount", "items"}
    for item in res["results"]:
        assert "_id" not in item
        assert set(item.keys()) == expected_keys
        skus_in_order = [line["sku"] for line in item["items"]]
        assert target_sku in skus_in_order


def test_query_05_payment_settlement_audit(sample_doc):
    """6. Query 05 applies payment_status, payment_method, and min_amount filters and sorts descending."""
    target_pay_status = sample_doc["payment_status"]
    target_pay_method = sample_doc["payment_method"]
    min_amt = round(float(sample_doc["total_amount"]) * 0.25, 2)

    res = payment_settlement_audit(
        payment_status=target_pay_status,
        payment_method=target_pay_method,
        min_amount=min_amt,
        limit=5,
        db_name=MONGO_DB_NAME
    )

    assert res["query_name"] == "payment_settlement_audit"
    assert res["collection"] == VALIDATED_COLLECTION
    assert res["parameters"] == {
        "payment_status": target_pay_status,
        "payment_method": target_pay_method,
        "min_amount": min_amt,
        "limit": 5,
    }
    assert 1 <= res["count"] <= 5

    expected_keys = {
        "id_order", "order_date", "customer_id", "city",
        "payment_method", "payment_status", "payment_amount",
        "total_amount", "corrections"
    }
    for item in res["results"]:
        assert "_id" not in item
        assert set(item.keys()) == expected_keys
        assert item["payment_status"] == target_pay_status
        assert item["payment_method"] == target_pay_method
        assert item["total_amount"] >= min_amt

    totals = [r["total_amount"] for r in res["results"]]
    assert totals == sorted(totals, reverse=True)


def test_limit_is_respected_and_validated(sample_doc):
    """7. Verify limit parameter caps returned results and rejects non-positive/invalid values."""
    res_limit_3 = orders_by_city_and_status(
        city=sample_doc["city"],
        status=sample_doc["status"],
        limit=3,
        db_name=MONGO_DB_NAME
    )
    assert res_limit_3["count"] <= 3
    assert len(res_limit_3["results"]) <= 3

    for invalid_limit in [0, -5, "10", True]:
        with pytest.raises(ValueError, match="limit"):
            orders_by_city_and_status(limit=invalid_limit, db_name=MONGO_DB_NAME)


def test_results_are_json_serializable():
    """8. Verify all 5 queries and registry metadata produce 100% JSON-serializable structures."""
    catalog = list_available_queries()
    assert len(catalog) == 5
    json.dumps(catalog, ensure_ascii=False)

    for q_meta in catalog:
        q_name = q_meta["name"]
        result = execute_query_by_name(q_name, db_name=MONGO_DB_NAME, limit=3)
        serialized = json.dumps(result, ensure_ascii=False)
        deserialized = json.loads(serialized)
        assert deserialized["query_name"] == q_name
        assert deserialized["count"] == len(deserialized["results"])


def test_read_only_and_no_indexes_created():
    """9. Verify executing all queries performs zero document writes and creates zero indexes."""
    db = get_database(db_name=MONGO_DB_NAME)
    col = db[VALIDATED_COLLECTION]

    count_before = col.estimated_document_count()
    indexes_before = [idx["name"] for idx in col.list_indexes()]

    for q_meta in list_available_queries():
        execute_query_by_name(q_meta["name"], db_name=MONGO_DB_NAME, limit=2)

    count_after = col.estimated_document_count()
    indexes_after = [idx["name"] for idx in col.list_indexes()]

    assert count_before == count_after
    assert indexes_before == indexes_after
    assert "_id_" in indexes_after
    assert "uniq_id_order" in indexes_after
