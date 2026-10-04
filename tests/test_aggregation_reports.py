"""
Phase 2 — Step 2: Automated Test Suite for MongoDB Aggregation Reports.
Tests `src/aggregations/reports.py` against `midterm_ecommerce.orders_validated`
verifying contract compliance, field schemas, sorting, limit enforcement,
input validation, JSON serializability, and strict read-only behavior.
"""
import json
import pytest

from config.settings import MONGO_DB_NAME, VALIDATED_COLLECTION
from src.mongo_setup import get_database
from src.aggregations import (
    AGGREGATION_REGISTRY,
    daily_sales_summary,
    execute_aggregation_by_name,
    list_available_aggregations,
    order_status_distribution,
    payment_method_analysis,
    sales_by_city,
    top_products,
)


def _assert_standard_report_contract(
    response: dict,
    expected_report_name: str,
    expected_keys: set,
) -> None:
    """Verify the standard Phase 2 Step 2 report response dictionary contract."""
    assert isinstance(response, dict)
    assert set(response.keys()) == {
        "report_name",
        "collection",
        "parameters",
        "count",
        "results",
    }
    assert response["report_name"] == expected_report_name
    assert response["collection"] == VALIDATED_COLLECTION
    assert isinstance(response["parameters"], dict)
    assert isinstance(response["results"], list)
    assert response["count"] == len(response["results"])

    # Verify strict JSON serializability (no ObjectId or unserializable BSON types)
    serialized = json.dumps(response, ensure_ascii=False)
    assert isinstance(serialized, str)

    for row in response["results"]:
        assert isinstance(row, dict)
        assert "_id" not in row
        assert set(row.keys()) == expected_keys


def test_daily_sales_summary_full_and_filtered():
    """Verify daily_sales_summary structure, sorting, and date-range filtering."""
    res_all = daily_sales_summary()
    _assert_standard_report_contract(
        res_all,
        expected_report_name="daily_sales_summary",
        expected_keys={"date", "order_count", "total_sales", "average_order_value"},
    )
    assert res_all["count"] > 0

    dates = [r["date"] for r in res_all["results"]]
    assert dates == sorted(dates), "daily_sales_summary must be sorted by date ascending"

    for row in res_all["results"]:
        assert isinstance(row["date"], str) and len(row["date"]) == 10
        assert isinstance(row["order_count"], int) and row["order_count"] > 0
        assert isinstance(row["total_sales"], float) and row["total_sales"] >= 0.0
        assert isinstance(row["average_order_value"], float) and row["average_order_value"] >= 0.0

    # Verify filtered date range and optional limit
    res_range = daily_sales_summary(
        start_date="2025-02-01",
        end_date="2025-02-05",
        limit=3,
    )
    _assert_standard_report_contract(
        res_range,
        expected_report_name="daily_sales_summary",
        expected_keys={"date", "order_count", "total_sales", "average_order_value"},
    )
    assert res_range["count"] == 3
    assert [r["date"] for r in res_range["results"]] == [
        "2025-02-01",
        "2025-02-02",
        "2025-02-03",
    ]


def test_daily_sales_summary_empty_and_validation():
    """Verify empty-result behavior and strict ISO date validation in daily_sales_summary."""
    res_empty = daily_sales_summary(
        start_date="1999-01-01",
        end_date="1999-01-31",
    )
    _assert_standard_report_contract(
        res_empty,
        expected_report_name="daily_sales_summary",
        expected_keys={"date", "order_count", "total_sales", "average_order_value"},
    )
    assert res_empty["count"] == 0
    assert res_empty["results"] == []

    # Invalid calendar date (February 31)
    with pytest.raises(ValueError, match="Invalid ISO-8601"):
        daily_sales_summary(start_date="2025-02-31")

    # Inverted date range (start_date > end_date)
    with pytest.raises(ValueError, match="must be <="):
        daily_sales_summary(start_date="2025-03-10", end_date="2025-03-01")

    # Invalid limit
    with pytest.raises(ValueError, match="positive integer"):
        daily_sales_summary(limit=0)


def test_sales_by_city():
    """Verify sales_by_city structure, metrics, and descending total_sales sort."""
    res = sales_by_city()
    _assert_standard_report_contract(
        res,
        expected_report_name="sales_by_city",
        expected_keys={"city", "order_count", "total_sales", "average_order_value"},
    )
    assert res["count"] > 0

    sales_values = [r["total_sales"] for r in res["results"]]
    assert sales_values == sorted(sales_values, reverse=True)

    for row in res["results"]:
        assert isinstance(row["city"], str) and len(row["city"]) > 0
        assert isinstance(row["order_count"], int) and row["order_count"] > 0
        assert isinstance(row["total_sales"], float) and row["total_sales"] >= 0.0
        assert isinstance(row["average_order_value"], float) and row["average_order_value"] >= 0.0


def test_payment_method_analysis():
    """Verify payment_method_analysis structure, metrics, and descending total_sales sort."""
    res = payment_method_analysis()
    _assert_standard_report_contract(
        res,
        expected_report_name="payment_method_analysis",
        expected_keys={
            "payment_method",
            "order_count",
            "total_sales",
            "average_order_value",
        },
    )
    assert res["count"] > 0

    sales_values = [r["total_sales"] for r in res["results"]]
    assert sales_values == sorted(sales_values, reverse=True)

    for row in res["results"]:
        assert isinstance(row["payment_method"], str) and len(row["payment_method"]) > 0
        assert isinstance(row["order_count"], int) and row["order_count"] > 0
        assert isinstance(row["total_sales"], float) and row["total_sales"] >= 0.0
        assert isinstance(row["average_order_value"], float) and row["average_order_value"] >= 0.0


def test_order_status_distribution():
    """Verify order_status_distribution structure and descending order_count sort."""
    res = order_status_distribution()
    _assert_standard_report_contract(
        res,
        expected_report_name="order_status_distribution",
        expected_keys={"status", "order_count", "total_sales"},
    )
    assert res["count"] > 0

    counts = [r["order_count"] for r in res["results"]]
    assert counts == sorted(counts, reverse=True)

    for row in res["results"]:
        assert isinstance(row["status"], str) and len(row["status"]) > 0
        assert isinstance(row["order_count"], int) and row["order_count"] > 0
        assert isinstance(row["total_sales"], float) and row["total_sales"] >= 0.0


def test_top_products_and_limit():
    """Verify top_products structure, descending total_sales sort, and limit enforcement."""
    res_default = top_products()
    _assert_standard_report_contract(
        res_default,
        expected_report_name="top_products",
        expected_keys={"sku", "product_name", "total_quantity", "total_sales", "order_count"},
    )
    assert 0 < res_default["count"] <= 10
    assert res_default["parameters"]["limit"] == 10

    sales_values = [r["total_sales"] for r in res_default["results"]]
    assert sales_values == sorted(sales_values, reverse=True)

    for row in res_default["results"]:
        assert isinstance(row["sku"], str)
        assert isinstance(row["product_name"], str) and len(row["product_name"]) > 0
        assert isinstance(row["total_quantity"], int)
        assert isinstance(row["total_sales"], float) and row["total_sales"] >= 0.0
        assert isinstance(row["order_count"], int) and row["order_count"] > 0

    # Verify top_products(limit=5) returns at most 5 rows
    res_limit5 = top_products(limit=5)
    _assert_standard_report_contract(
        res_limit5,
        expected_report_name="top_products",
        expected_keys={"sku", "product_name", "total_quantity", "total_sales", "order_count"},
    )
    assert res_limit5["count"] <= 5
    assert res_limit5["count"] == min(5, res_default["count"])
    assert res_limit5["parameters"]["limit"] == 5

    # Invalid limits must raise ValueError
    for bad_limit in (0, -3, True, "5"):
        with pytest.raises(ValueError, match="positive integer"):
            top_products(limit=bad_limit)  # type: ignore[arg-type]


def test_aggregation_registry_and_dispatcher():
    """Verify AGGREGATION_REGISTRY, list_available_aggregations, and execute_aggregation_by_name."""
    available = list_available_aggregations()
    assert available == [
        "daily_sales_summary",
        "sales_by_city",
        "payment_method_analysis",
        "order_status_distribution",
        "top_products",
    ]
    assert set(AGGREGATION_REGISTRY.keys()) == set(available)

    dispatched = execute_aggregation_by_name("top_products", limit=3)
    assert dispatched["report_name"] == "top_products"
    assert dispatched["count"] == 3

    with pytest.raises(ValueError, match="Unknown report_name"):
        execute_aggregation_by_name("non_existent_report")


def test_aggregations_strictly_read_only():
    """
    Verify that executing all 5 aggregation reports does NOT modify document counts,
    does NOT create Materialized View collections (`daily_sales_summary`, `top_products_summary`),
    and does NOT alter collection indexes.
    """
    db = get_database(db_name=MONGO_DB_NAME)
    col = db[VALIDATED_COLLECTION]

    count_before = col.count_documents({})
    collections_before = sorted(db.list_collection_names())
    indexes_before = sorted(col.index_information().keys())
    mv_daily_before = (
        db["daily_sales_summary"].count_documents({})
        if "daily_sales_summary" in collections_before
        else None
    )
    mv_products_before = (
        db["top_products_summary"].count_documents({})
        if "top_products_summary" in collections_before
        else None
    )

    # Execute all 5 reports
    r1 = daily_sales_summary()
    r2 = sales_by_city()
    r3 = payment_method_analysis()
    r4 = order_status_distribution()
    r5 = top_products(limit=5)

    count_after = col.count_documents({})
    collections_after = sorted(db.list_collection_names())
    indexes_after = sorted(col.index_information().keys())
    mv_daily_after = (
        db["daily_sales_summary"].count_documents({})
        if "daily_sales_summary" in collections_after
        else None
    )
    mv_products_after = (
        db["top_products_summary"].count_documents({})
        if "top_products_summary" in collections_after
        else None
    )

    assert count_before == count_after
    assert collections_before == collections_after
    assert mv_daily_before == mv_daily_after
    assert mv_products_before == mv_products_after
    assert indexes_before == indexes_after

    # Cross-check that order-level aggregations reconcile with total document count
    assert sum(x["order_count"] for x in r1["results"]) == count_before
    assert sum(x["order_count"] for x in r2["results"]) == count_before
    assert sum(x["order_count"] for x in r3["results"]) == count_before
    assert sum(x["order_count"] for x in r4["results"]) == count_before
    assert r5["count"] == 5
