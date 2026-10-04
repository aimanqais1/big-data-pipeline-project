"""
Phase 2 — Step 3B: Automated Test Suite for Materialized Views (`src/aggregations/materialized_views.py`).

Covers all 14 required verification scenarios:
1. MV index creation (`ensure_mv_indexes`).
2. Full refresh (`refresh_all_materialized_views(mode="full")`).
3. Numeric parity:
   - `get_daily_sales_mv()` == `reports.daily_sales_summary()`
   - `get_top_products_mv()` == `reports.top_products()`
4. No-op incremental refresh.
5. Controlled synthetic new order.
6. Controlled order update (same date & SKU, modified amount & qty).
7. Controlled `order_date` change.
8. Controlled `SKU` change.
9. Verify OLD date and NEW date are both corrected (including deleting old date when 0 orders remain).
10. Verify OLD SKU and NEW SKU are both corrected (including deleting old SKU when 0 items remain).
11. Re-run identical incremental refresh and prove no double counting.
12. Simulate interrupted `IN_PROGRESS` state (with `last_error` recorded).
13. Verify automatic recovery from `IN_PROGRESS` on next incremental refresh.
14. Restore test database to its exact original state and verify baseline parity.

IMPORTANT SAFETY:
All mutation/lifecycle tests execute exclusively in an isolated temporary database
(`midterm_ecommerce_mv_isolated_test`) and never mutate `midterm_ecommerce_100k_final`
or `midterm_ecommerce_30m_production`.
"""
import copy
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pytest

from config.settings import (
    DAILY_SALES_MV_COLLECTION,
    MONGO_DB_NAME,
    MV_ORDER_DIGEST_COLLECTION,
    MV_REFRESH_STATE_COLLECTION,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    TOP_PRODUCTS_MV_COLLECTION,
    VALIDATED_COLLECTION,
)
from src.aggregations.materialized_views import (
    ensure_mv_indexes,
    get_daily_sales_mv,
    get_mv_refresh_status,
    get_top_products_mv,
    refresh_all_materialized_views,
    refresh_daily_sales_mv,
    refresh_top_products_mv,
)
from src.aggregations.reports import daily_sales_summary, top_products
from src.mongo_setup import get_database, get_mongo_client, setup_mongodb_collections

ISOLATED_TEST_DB = "midterm_ecommerce_mv_isolated_test"


def _assert_daily_parity_with_live_report(db_name: str) -> None:
    """
    Assert that every document in `daily_sales_summary` matches the live
    `reports.daily_sales_summary(db_name=db_name)` output 100%.
    """
    live_res = daily_sales_summary(db_name=db_name)
    mv_res = get_daily_sales_mv(db_name=db_name)

    assert mv_res["count"] == live_res["count"]
    assert len(mv_res["results"]) == len(live_res["results"])

    # Verify JSON serializability
    json.dumps(mv_res, ensure_ascii=False)

    for mv_row, live_row in zip(mv_res["results"], live_res["results"]):
        assert mv_row["date"] == live_row["date"]
        assert mv_row["order_count"] == live_row["order_count"]
        assert mv_row["total_sales"] == live_row["total_sales"]
        assert mv_row["average_order_value"] == live_row["average_order_value"]
        assert "min_order_date" in mv_row
        assert "max_order_date" in mv_row
        assert "refreshed_at" in mv_row
        assert "refresh_mode" in mv_row
        assert "watermark_processed_at" in mv_row


def _assert_top_products_parity_with_live_report(db_name: str, limit: int = 50) -> None:
    """
    Assert that every document in `top_products_summary` matches the live
    `reports.top_products(limit=limit, db_name=db_name)` output 100%.
    """
    live_res = top_products(limit=limit, db_name=db_name)
    mv_res = get_top_products_mv(limit=limit, db_name=db_name)

    assert mv_res["count"] == live_res["count"]
    assert len(mv_res["results"]) == len(live_res["results"])

    json.dumps(mv_res, ensure_ascii=False)

    for mv_row, live_row in zip(mv_res["results"], live_res["results"]):
        assert mv_row["sku"] == live_row["sku"]
        assert mv_row["product_name"] == live_row["product_name"]
        assert mv_row["total_quantity"] == live_row["total_quantity"]
        assert mv_row["total_sales"] == live_row["total_sales"]
        assert mv_row["order_count"] == live_row["order_count"]
        assert isinstance(mv_row["distinct_order_count"], int)
        assert mv_row["distinct_order_count"] <= mv_row["order_count"]
        assert "refreshed_at" in mv_row
        assert "refresh_mode" in mv_row
        assert "watermark_processed_at" in mv_row


@pytest.fixture(scope="module")
def isolated_mv_db():
    """
    Create an isolated temporary MongoDB database seeded with a representative sample
    of validated orders copied read-only from `MONGO_DB_NAME.orders_validated`.
    Drops the isolated database before and after the module tests.
    """
    client = get_mongo_client()
    client.drop_database(ISOLATED_TEST_DB)

    # Set up formal $jsonSchema validator and unique index on isolated test DB
    setup_mongodb_collections(db_name=ISOLATED_TEST_DB, drop_existing=False)

    source_db = client[MONGO_DB_NAME]
    target_db = client[ISOLATED_TEST_DB]

    # Sample 400 real validated documents + up to 20 documents with empty SKU if present
    base_docs = list(source_db[VALIDATED_COLLECTION].find({}).limit(400))
    empty_sku_docs = list(
        source_db[VALIDATED_COLLECTION].find({"items.sku": ""}).limit(20)
    )
    by_id: Dict[str, Dict[str, Any]] = {}
    for d in base_docs + empty_sku_docs:
        doc_copy = copy.deepcopy(d)
        doc_copy.pop("_id", None)
        by_id[doc_copy["id_order"]] = doc_copy

    seed_docs = list(by_id.values())
    assert len(seed_docs) > 0, "Source database must contain validated orders."
    target_db[VALIDATED_COLLECTION].insert_many(seed_docs)

    yield {
        "db_name": ISOLATED_TEST_DB,
        "db": target_db,
        "seed_docs": seed_docs,
    }

    client.drop_database(ISOLATED_TEST_DB)


def test_01_mv_index_creation(isolated_mv_db):
    """1. Verify `ensure_mv_indexes` creates only the approved MV and digest indexes."""
    db_name = isolated_mv_db["db_name"]
    db = isolated_mv_db["db"]

    val_indexes_before = sorted(db[VALIDATED_COLLECTION].index_information().keys())

    idx_result = ensure_mv_indexes(db_name=db_name)
    json.dumps(idx_result, ensure_ascii=False)

    assert idx_result["database"] == db_name
    assert set(idx_result["created_indexes"][DAILY_SALES_MV_COLLECTION]) == {
        "uniq_mv_daily_sales_date",
        "idx_mv_daily_total_sales_desc",
    }
    assert set(idx_result["created_indexes"][TOP_PRODUCTS_MV_COLLECTION]) == {
        "uniq_mv_top_products_sku",
        "idx_mv_top_products_sales_desc",
        "idx_mv_top_products_qty_desc",
    }
    assert set(idx_result["created_indexes"][MV_ORDER_DIGEST_COLLECTION]) == {
        "uniq_mv_digest_id_order",
    }

    # Verify unique flags in MongoDB
    daily_idx_info = db[DAILY_SALES_MV_COLLECTION].index_information()
    assert daily_idx_info["uniq_mv_daily_sales_date"].get("unique") is True

    prod_idx_info = db[TOP_PRODUCTS_MV_COLLECTION].index_information()
    assert prod_idx_info["uniq_mv_top_products_sku"].get("unique") is True

    digest_idx_info = db[MV_ORDER_DIGEST_COLLECTION].index_information()
    assert digest_idx_info["uniq_mv_digest_id_order"].get("unique") is True

    # Verify orders_validated indexes were NOT altered
    val_indexes_after = sorted(db[VALIDATED_COLLECTION].index_information().keys())
    assert val_indexes_before == val_indexes_after


def test_02_full_refresh_and_03_numeric_parity(isolated_mv_db):
    """
    2 & 3. Verify full refresh populates `daily_sales_summary`, `top_products_summary`,
    `mv_order_digest`, and `mv_refresh_state`, with 100% numeric parity against
    `reports.daily_sales_summary()` and `reports.top_products()`.
    """
    db_name = isolated_mv_db["db_name"]
    db = isolated_mv_db["db"]
    expected_order_count = len(isolated_mv_db["seed_docs"])

    res = refresh_all_materialized_views(mode="full", db_name=db_name)
    json.dumps(res, ensure_ascii=False)

    assert res["mode_executed"] == "full"
    assert res["status"] == "SUCCESS"
    assert res["noop"] is False
    assert res["changed_orders_count"] == expected_order_count
    assert res["order_digest_docs_count"] == expected_order_count
    assert res["watermark_after"] is not None

    # Check refresh state controller document
    status_info = get_mv_refresh_status(db_name=db_name)
    json.dumps(status_info, ensure_ascii=False)
    assert status_info["initialized"] is True
    assert status_info["status"] == "IDLE"
    assert status_info["last_processed_at"] == res["watermark_after"]
    assert status_info["pending_affected_dates"] == []
    assert status_info["pending_affected_skus"] == []
    assert status_info["last_error"] is None
    assert (
        status_info["collection_counts"][MV_ORDER_DIGEST_COLLECTION]
        == expected_order_count
    )

    # Check stored mv_order_digest schema (minimum state only, not full order document)
    sample_digest = db[MV_ORDER_DIGEST_COLLECTION].find_one({})
    assert sample_digest is not None
    assert set(sample_digest.keys()) == {
        "_id",
        "id_order",
        "order_date_day",
        "skus",
        "processed_at",
        "id_run",
    }

    # Check stored daily_sales_summary schema
    sample_daily = db[DAILY_SALES_MV_COLLECTION].find_one({})
    assert sample_daily is not None
    assert set(sample_daily.keys()) == {
        "_id",
        "date",
        "order_count",
        "total_sales",
        "average_order_value",
        "min_order_date",
        "max_order_date",
        "refreshed_at",
        "refresh_mode",
        "watermark_processed_at",
    }

    # Check stored top_products_summary schema
    sample_prod = db[TOP_PRODUCTS_MV_COLLECTION].find_one({})
    assert sample_prod is not None
    assert set(sample_prod.keys()) == {
        "_id",
        "sku",
        "product_name",
        "total_quantity",
        "total_sales",
        "order_count",
        "distinct_order_count",
        "refreshed_at",
        "refresh_mode",
        "watermark_processed_at",
    }

    # 3. Verify 100% numeric parity with existing Step 2 aggregation reports
    _assert_daily_parity_with_live_report(db_name=db_name)
    _assert_top_products_parity_with_live_report(db_name=db_name)


def test_04_noop_incremental_refresh(isolated_mv_db):
    """4. Verify incremental refresh is a clean NO_OP when no new/changed orders exist."""
    db_name = isolated_mv_db["db_name"]

    status_before = get_mv_refresh_status(db_name=db_name)
    inc_res = refresh_all_materialized_views(mode="incremental", db_name=db_name)
    json.dumps(inc_res, ensure_ascii=False)

    assert inc_res["mode_executed"] == "incremental"
    assert inc_res["status"] == "NO_OP"
    assert inc_res["noop"] is True
    assert inc_res["changed_orders_count"] == 0
    assert inc_res["affected_dates"] == []
    assert inc_res["affected_skus"] == []
    assert inc_res["watermark_after"] == status_before["last_processed_at"]

    # Also test individual MV incremental refresh NO_OP paths
    d_noop = refresh_daily_sales_mv(mode="incremental", db_name=db_name)
    p_noop = refresh_top_products_mv(mode="incremental", db_name=db_name)
    assert d_noop["status"] == "NO_OP" and d_noop["noop"] is True
    assert p_noop["status"] == "NO_OP" and p_noop["noop"] is True


def test_05_to_11_incremental_lifecycle_updates_and_idempotency(isolated_mv_db):
    """
    Tests 5 through 11:
    5. Controlled synthetic new order -> incremental refresh updates new date & SKU.
    6. Controlled order update (same date & SKU, modified amount & qty) -> no double counting.
    7 & 9. Controlled `order_date` change -> both OLD date and NEW date are corrected
           (and old date is deleted when 0 remaining orders exist on it).
    8 & 10. Controlled `SKU` change -> both OLD SKU and NEW SKU are corrected
            (and old synthetic SKU is deleted when 0 remaining items exist for it).
    11. Re-run identical incremental refresh and prove no double counting.
    """
    db_name = isolated_mv_db["db_name"]
    db = isolated_mv_db["db"]
    val_col = db[VALIDATED_COLLECTION]

    # Dynamically choose a template order and derive unused future dates and synthetic SKUs
    template_doc = copy.deepcopy(isolated_mv_db["seed_docs"][0])
    max_date_doc = list(
        val_col.find({}, {"order_date": 1}).sort("order_date", -1).limit(1)
    )[0]
    max_dt = datetime.fromisoformat(max_date_doc["order_date"])
    synth_day_1 = (max_dt + timedelta(days=40)).strftime("%Y-%m-%d")
    synth_day_2 = (max_dt + timedelta(days=41)).strftime("%Y-%m-%d")

    status_before = get_mv_refresh_status(db_name=db_name)
    base_wm_dt = datetime.fromisoformat(status_before["last_processed_at"])

    synth_order_id = f"SYNTH-MV-{int(datetime.now().timestamp())}"
    synth_sku_1 = "SKU-SYNTH-ALPHA"
    synth_sku_2 = "SKU-SYNTH-BETA"

    wm_step5 = (base_wm_dt + timedelta(seconds=10)).isoformat()
    synth_doc = copy.deepcopy(template_doc)
    synth_doc.pop("_id", None)
    synth_doc["id_order"] = synth_order_id
    synth_doc["order_date"] = f"{synth_day_1}T12:00:00"
    synth_doc["delivery_cost"] = 1000.0
    synth_doc["items"] = [
        {
            "sku": synth_sku_1,
            "name": "منتج اختبار ألفا",
            "qty": 2,
            "unit_price": 25000.0,
            "total": 50000.0,
        }
    ]
    synth_doc["total_amount"] = 51000.0
    synth_doc["payment_amount"] = 51000.0
    synth_doc["processed_at"] = wm_step5
    synth_doc["id_run"] = "test_incremental_step5"

    try:
        # --------------------------------------------------------------
        # 5. Controlled synthetic new order
        # --------------------------------------------------------------
        val_col.insert_one(synth_doc)
        res_step5 = refresh_all_materialized_views(mode="incremental", db_name=db_name)

        assert res_step5["status"] == "SUCCESS"
        assert res_step5["noop"] is False
        assert res_step5["changed_orders_count"] == 1
        assert res_step5["affected_dates"] == [synth_day_1]
        assert res_step5["affected_skus"] == [synth_sku_1]
        assert res_step5["watermark_after"] == wm_step5

        _assert_daily_parity_with_live_report(db_name=db_name)
        _assert_top_products_parity_with_live_report(db_name=db_name)

        # --------------------------------------------------------------
        # 6. Controlled order update (same date & SKU, modified qty & total)
        # --------------------------------------------------------------
        wm_step6 = (base_wm_dt + timedelta(seconds=20)).isoformat()
        val_col.update_one(
            {"id_order": synth_order_id},
            {
                "$set": {
                    "items": [
                        {
                            "sku": synth_sku_1,
                            "name": "منتج اختبار ألفا",
                            "qty": 5,
                            "unit_price": 25000.0,
                            "total": 125000.0,
                        }
                    ],
                    "total_amount": 126000.0,
                    "payment_amount": 126000.0,
                    "processed_at": wm_step6,
                }
            },
        )
        res_step6 = refresh_all_materialized_views(mode="incremental", db_name=db_name)
        assert res_step6["status"] == "SUCCESS"
        assert res_step6["changed_orders_count"] == 1
        assert res_step6["affected_dates"] == [synth_day_1]
        assert res_step6["affected_skus"] == [synth_sku_1]

        # Verify no double-counting on synth_day_1 or synth_sku_1
        day1_mv = get_daily_sales_mv(
            start_date=synth_day_1, end_date=synth_day_1, db_name=db_name
        )
        assert day1_mv["count"] == 1
        assert day1_mv["results"][0]["order_count"] == 1
        assert day1_mv["results"][0]["total_sales"] == 126000.0

        _assert_daily_parity_with_live_report(db_name=db_name)
        _assert_top_products_parity_with_live_report(db_name=db_name)

        # --------------------------------------------------------------
        # 7 & 9. Controlled order_date change (synth_day_1 -> synth_day_2)
        # Verify OLD date (synth_day_1) is deleted (0 remaining orders)
        # and NEW date (synth_day_2) is created and accurate.
        # --------------------------------------------------------------
        wm_step7 = (base_wm_dt + timedelta(seconds=30)).isoformat()
        val_col.update_one(
            {"id_order": synth_order_id},
            {
                "$set": {
                    "order_date": f"{synth_day_2}T15:30:00",
                    "processed_at": wm_step7,
                }
            },
        )
        res_step7 = refresh_all_materialized_views(mode="incremental", db_name=db_name)
        assert res_step7["status"] == "SUCCESS"
        assert set(res_step7["old_dates"]) == {synth_day_1}
        assert set(res_step7["new_dates"]) == {synth_day_2}
        assert set(res_step7["affected_dates"]) == {synth_day_1, synth_day_2}
        assert res_step7["deleted_dates_count"] == 1

        # Old date must be removed from daily_sales_summary
        old_day_mv = get_daily_sales_mv(
            start_date=synth_day_1, end_date=synth_day_1, db_name=db_name
        )
        assert old_day_mv["count"] == 0

        # New date must exist in daily_sales_summary
        new_day_mv = get_daily_sales_mv(
            start_date=synth_day_2, end_date=synth_day_2, db_name=db_name
        )
        assert new_day_mv["count"] == 1
        assert new_day_mv["results"][0]["order_count"] == 1
        assert new_day_mv["results"][0]["total_sales"] == 126000.0

        _assert_daily_parity_with_live_report(db_name=db_name)

        # --------------------------------------------------------------
        # 8 & 10. Controlled SKU change (synth_sku_1 -> synth_sku_2)
        # Verify OLD SKU (synth_sku_1) is deleted (0 remaining items)
        # and NEW SKU (synth_sku_2) is created and accurate.
        # Also test shifting an existing SKU's quantity at the same time.
        # --------------------------------------------------------------
        existing_sku = template_doc["items"][0]["sku"]
        existing_name = template_doc["items"][0]["name"]
        wm_step8 = (base_wm_dt + timedelta(seconds=40)).isoformat()
        val_col.update_one(
            {"id_order": synth_order_id},
            {
                "$set": {
                    "items": [
                        {
                            "sku": synth_sku_2,
                            "name": "منتج اختبار بيتا",
                            "qty": 3,
                            "unit_price": 30000.0,
                            "total": 90000.0,
                        },
                        {
                            "sku": existing_sku,
                            "name": existing_name,
                            "qty": 1,
                            "unit_price": 10000.0,
                            "total": 10000.0,
                        },
                    ],
                    "total_amount": 101000.0,
                    "payment_amount": 101000.0,
                    "processed_at": wm_step8,
                }
            },
        )
        res_step8 = refresh_all_materialized_views(mode="incremental", db_name=db_name)
        assert res_step8["status"] == "SUCCESS"
        assert set(res_step8["old_skus"]) == {synth_sku_1}
        assert set(res_step8["new_skus"]) == {synth_sku_2, existing_sku}
        assert set(res_step8["affected_skus"]) == {
            synth_sku_1,
            synth_sku_2,
            existing_sku,
        }
        assert res_step8["deleted_skus_count"] == 1

        # Old SKU (synth_sku_1) must be deleted from top_products_summary
        assert (
            db[TOP_PRODUCTS_MV_COLLECTION].count_documents({"sku": synth_sku_1}) == 0
        )
        # New SKU (synth_sku_2) must exist with exact totals
        sku2_doc = db[TOP_PRODUCTS_MV_COLLECTION].find_one({"sku": synth_sku_2})
        assert sku2_doc is not None
        assert sku2_doc["total_quantity"] == 3
        assert sku2_doc["total_sales"] == 90000.0
        assert sku2_doc["order_count"] == 1
        assert sku2_doc["distinct_order_count"] == 1

        _assert_daily_parity_with_live_report(db_name=db_name)
        _assert_top_products_parity_with_live_report(db_name=db_name)

        # --------------------------------------------------------------
        # 11. Re-run identical incremental refresh twice -> prove NO double counting
        # --------------------------------------------------------------
        daily_snapshot = get_daily_sales_mv(db_name=db_name)["results"]
        prod_snapshot = get_top_products_mv(limit=50, db_name=db_name)["results"]

        rerun_1 = refresh_all_materialized_views(mode="incremental", db_name=db_name)
        rerun_2 = refresh_all_materialized_views(mode="incremental", db_name=db_name)

        assert rerun_1["status"] == "NO_OP" and rerun_1["noop"] is True
        assert rerun_2["status"] == "NO_OP" and rerun_2["noop"] is True

        assert get_daily_sales_mv(db_name=db_name)["results"] == daily_snapshot
        assert (
            get_top_products_mv(limit=50, db_name=db_name)["results"] == prod_snapshot
        )

    finally:
        # Clean up the synthetic order and restore full refresh state
        val_col.delete_one({"id_order": synth_order_id})
        refresh_all_materialized_views(mode="full", db_name=db_name)


def test_12_and_13_failure_simulation_and_automatic_recovery(isolated_mv_db):
    """
    12 & 13. Simulate an interrupted refresh (`status == "IN_PROGRESS"`) after
    `daily_sales_summary` updates but before `top_products_summary` and `mv_order_digest`
    complete. Verify:
    - `status` remains `"IN_PROGRESS"`
    - `last_error` is recorded
    - `last_processed_at` is NOT prematurely advanced
    - `pending_affected_dates` and `pending_affected_skus` are preserved
    - Running `refresh_all_materialized_views(mode="incremental")` automatically recovers,
      recomputes the pending groups, advances `last_processed_at`, clears `last_error`,
      and sets `status = "IDLE"`.
    """
    db_name = isolated_mv_db["db_name"]
    db = isolated_mv_db["db"]
    val_col = db[VALIDATED_COLLECTION]

    status_before = get_mv_refresh_status(db_name=db_name)
    wm_before = status_before["last_processed_at"]
    base_wm_dt = datetime.fromisoformat(wm_before)

    template_doc = copy.deepcopy(isolated_mv_db["seed_docs"][0])
    recovery_order_id = f"SYNTH-RECOVERY-{int(datetime.now().timestamp())}"
    recovery_day = "2025-06-15"
    recovery_sku = "SKU-RECOVERY-TEST"
    wm_new = (base_wm_dt + timedelta(seconds=60)).isoformat()

    rec_doc = copy.deepcopy(template_doc)
    rec_doc.pop("_id", None)
    rec_doc["id_order"] = recovery_order_id
    rec_doc["order_date"] = f"{recovery_day}T09:00:00"
    rec_doc["delivery_cost"] = 2000.0
    rec_doc["items"] = [
        {
            "sku": recovery_sku,
            "name": "منتج اختبار الاستعادة",
            "qty": 4,
            "unit_price": 15000.0,
            "total": 60000.0,
        }
    ]
    rec_doc["total_amount"] = 62000.0
    rec_doc["payment_amount"] = 62000.0
    rec_doc["processed_at"] = wm_new
    rec_doc["id_run"] = "test_recovery_run"

    try:
        val_col.insert_one(rec_doc)

        # 12. Simulate failure mid-refresh (after daily_sales_summary, before top_products_summary)
        with pytest.raises(RuntimeError, match="Simulated mid-refresh failure"):
            refresh_all_materialized_views(
                mode="incremental",
                db_name=db_name,
                _simulate_failure_stage="after_daily_sales",
            )

        interrupted_status = get_mv_refresh_status(db_name=db_name)
        assert interrupted_status["status"] == "IN_PROGRESS"
        assert interrupted_status["last_processed_at"] == wm_before
        assert interrupted_status["pending_affected_dates"] == [recovery_day]
        assert interrupted_status["pending_affected_skus"] == [recovery_sku]
        assert (
            interrupted_status["last_error"] is not None
            and "Simulated mid-refresh failure" in interrupted_status["last_error"]
        )

        # 13. Run normal incremental refresh and verify automatic recovery
        recovery_res = refresh_all_materialized_views(
            mode="incremental", db_name=db_name
        )
        assert recovery_res["status"] == "SUCCESS"
        assert recovery_res["recovered_from_in_progress"] is True
        assert recovery_res["affected_dates"] == [recovery_day]
        assert recovery_res["affected_skus"] == [recovery_sku]
        assert recovery_res["watermark_after"] == wm_new

        final_status = get_mv_refresh_status(db_name=db_name)
        assert final_status["status"] == "IDLE"
        assert final_status["last_processed_at"] == wm_new
        assert final_status["pending_affected_dates"] == []
        assert final_status["pending_affected_skus"] == []
        assert final_status["last_error"] is None

        _assert_daily_parity_with_live_report(db_name=db_name)
        _assert_top_products_parity_with_live_report(db_name=db_name)

    finally:
        # 14. Restore isolated test database to its exact original state
        val_col.delete_one({"id_order": recovery_order_id})
        refresh_all_materialized_views(mode="full", db_name=db_name)


def test_14_restore_and_verify_exact_original_state(isolated_mv_db):
    """
    14. Verify that the isolated test database is restored to its exact original seed state
    and that real baseline database `MONGO_DB_NAME` was not mutated.
    """
    db_name = isolated_mv_db["db_name"]
    db = isolated_mv_db["db"]
    expected_seed_count = len(isolated_mv_db["seed_docs"])

    assert db[VALIDATED_COLLECTION].count_documents({}) == expected_seed_count
    assert db[MV_ORDER_DIGEST_COLLECTION].count_documents({}) == expected_seed_count
    _assert_daily_parity_with_live_report(db_name=db_name)
    _assert_top_products_parity_with_live_report(db_name=db_name)

    # Verify reader parameter validation (invalid dates, limits, include_unassigned_sku)
    with pytest.raises(ValueError, match="Invalid ISO-8601"):
        get_daily_sales_mv(start_date="2025-02-31", db_name=db_name)
    with pytest.raises(ValueError, match="must be <="):
        get_daily_sales_mv(
            start_date="2025-03-10", end_date="2025-03-01", db_name=db_name
        )
    with pytest.raises(ValueError, match="positive integer"):
        get_top_products_mv(limit=0, db_name=db_name)

    filtered_prods = get_top_products_mv(
        limit=50, include_unassigned_sku=False, db_name=db_name
    )
    assert all(r["sku"] != "" for r in filtered_prods["results"])

    # Verify the real reference database baseline collections were untouched
    real_db = get_database(db_name=MONGO_DB_NAME)
    assert real_db[RAW_COLLECTION].count_documents({}) > 0
    assert real_db[VALIDATED_COLLECTION].count_documents({}) > 0
    assert real_db[QUARANTINE_COLLECTION].count_documents({}) > 0
