"""
Phase 2 — Step 3B: Materialized Views & Incremental Refresh Manager.

Implements the two persisted Materialized View collections:
1. `daily_sales_summary` (keyed by calendar `date` YYYY-MM-DD)
2. `top_products_summary` (keyed by product `sku`)

Supported by two control/state collections:
3. `mv_refresh_state` (single controller document `_id = "materialized_views_controller"`)
4. `mv_order_digest` (lightweight per-order dimension digest keyed by `id_order`)

Capabilities:
- Initial / Full refresh (`mode="full"`) via deterministic aggregation + `$merge`
- Incremental refresh (`mode="incremental"`) via `processed_at > last_processed_at` watermark
  and `mv_order_digest` dimension diffing (detecting both old and new `order_date` and `sku` values)
- Automatic pruning of affected dates or SKUs whose remaining order count drops to zero
- Idempotent execution (no blind `$inc`; repeated incremental refreshes with no new data are NO_OP)
- Crash / Interrupted refresh recovery (`status == "IN_PROGRESS"` reuses `pending_affected_dates`
  and `pending_affected_skus` before advancing `last_processed_at`)
"""
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from pymongo import ASCENDING, DESCENDING, ReplaceOne
from pymongo.database import Database
from pymongo.errors import PyMongoError

from config.settings import (
    DAILY_SALES_MV_COLLECTION,
    MONGO_DB_NAME,
    MV_ORDER_DIGEST_COLLECTION,
    MV_REFRESH_STATE_COLLECTION,
    TOP_PRODUCTS_MV_COLLECTION,
    VALIDATED_COLLECTION,
)
from src.mongo_setup import get_database

logger = logging.getLogger(__name__)

MV_CONTROLLER_ID = "materialized_views_controller"

# Approved Materialized View & Digest Index Specifications (Step 12)
MV_INDEX_SPECS: Dict[str, List[Dict[str, Any]]] = {
    DAILY_SALES_MV_COLLECTION: [
        {
            "name": "uniq_mv_daily_sales_date",
            "keys": [("date", ASCENDING)],
            "unique": True,
        },
        {
            "name": "idx_mv_daily_total_sales_desc",
            "keys": [("total_sales", DESCENDING), ("date", ASCENDING)],
            "unique": False,
        },
    ],
    TOP_PRODUCTS_MV_COLLECTION: [
        {
            "name": "uniq_mv_top_products_sku",
            "keys": [("sku", ASCENDING)],
            "unique": True,
        },
        {
            "name": "idx_mv_top_products_sales_desc",
            "keys": [("total_sales", DESCENDING), ("sku", ASCENDING)],
            "unique": False,
        },
        {
            "name": "idx_mv_top_products_qty_desc",
            "keys": [("total_quantity", DESCENDING), ("sku", ASCENDING)],
            "unique": False,
        },
    ],
    MV_ORDER_DIGEST_COLLECTION: [
        {
            "name": "uniq_mv_digest_id_order",
            "keys": [("id_order", ASCENDING)],
            "unique": True,
        },
    ],
}


# --------------------------------------------------------------------------
# Validation & Normalization Helpers
# --------------------------------------------------------------------------

def _validate_mode(mode: str) -> str:
    """Validate refresh mode is either 'full' or 'incremental'."""
    if not isinstance(mode, str):
        raise ValueError(f"Refresh 'mode' must be a string, got: {mode!r}")
    normalized = mode.strip().lower()
    if normalized not in ("full", "incremental"):
        raise ValueError(
            f"Invalid refresh mode: {mode!r}. Must be 'full' or 'incremental'."
        )
    return normalized


def _validate_limit(limit: int) -> int:
    """Ensure limit is a positive integer (rejecting booleans, <= 0, and non-ints)."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError(f"'limit' must be a positive integer, got: {limit!r}")
    return limit


def _parse_and_normalize_day_bound(date_str: str, field_name: str) -> Tuple[str, datetime]:
    """
    Validate that `date_str` is a valid calendar ISO-8601 date (`YYYY-MM-DD`)
    or timestamp (`YYYY-MM-DDTHH:MM:SS`) and return the 10-character `YYYY-MM-DD`
    day string along with its parsed calendar date.
    """
    if not isinstance(date_str, str) or not date_str.strip():
        raise ValueError(f"Parameter '{field_name}' must be a non-empty string.")
    cleaned = date_str.strip()
    try:
        parsed_dt = datetime.fromisoformat(cleaned)
    except ValueError as exc:
        raise ValueError(
            f"Invalid ISO-8601 date/timestamp for '{field_name}': {cleaned!r}. "
            f"Details: {exc}"
        ) from exc
    day_str = parsed_dt.strftime("%Y-%m-%d")
    day_dt = datetime.fromisoformat(day_str)
    return day_str, day_dt


def _derive_day_str(order_date_str: str) -> str:
    """Extract YYYY-MM-DD from an ISO-8601 order_date string."""
    if not order_date_str:
        return ""
    return order_date_str[:10]


# --------------------------------------------------------------------------
# Pipeline Builders (Step 4, Step 5, Step 6)
# --------------------------------------------------------------------------

def _build_daily_sales_mv_pipeline(
    refreshed_at: str,
    refresh_mode: str,
    watermark_processed_at: Optional[str],
    affected_dates: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Build the deterministic `$merge` aggregation pipeline for `daily_sales_summary`.
    Matches the exact semantics of `src.aggregations.reports.daily_sales_summary`.
    """
    pipeline: List[Dict[str, Any]] = []

    if affected_dates is not None:
        # Index-friendly range predicates on order_date combined with exact day filter
        or_ranges = [
            {
                "order_date": {
                    "$gte": f"{day}T00:00:00",
                    "$lte": f"{day}T23:59:59.999999",
                }
            }
            for day in affected_dates
        ]
        if or_ranges:
            pipeline.append({"$match": {"$or": or_ranges}})

    pipeline.append(
        {
            "$addFields": {
                "day": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": {"$toDate": "$order_date"},
                    }
                }
            }
        }
    )

    if affected_dates is not None:
        pipeline.append({"$match": {"day": {"$in": affected_dates}}})

    pipeline.extend(
        [
            {
                "$group": {
                    "_id": "$day",
                    "order_count": {"$sum": 1},
                    "total_sales": {"$sum": "$total_amount"},
                    "average_order_value": {"$avg": "$total_amount"},
                    "min_order_date": {"$min": "$order_date"},
                    "max_order_date": {"$max": "$order_date"},
                }
            },
            {
                "$project": {
                    "_id": "$_id",
                    "date": "$_id",
                    "order_count": 1,
                    "total_sales": {"$round": ["$total_sales", 2]},
                    "average_order_value": {"$round": ["$average_order_value", 2]},
                    "min_order_date": 1,
                    "max_order_date": 1,
                    "refreshed_at": {"$literal": refreshed_at},
                    "refresh_mode": {"$literal": refresh_mode},
                    "watermark_processed_at": {"$literal": watermark_processed_at},
                }
            },
            {"$sort": {"date": ASCENDING}},
            {
                "$merge": {
                    "into": DAILY_SALES_MV_COLLECTION,
                    "on": "date",
                    "whenMatched": "replace",
                    "whenNotMatched": "insert",
                }
            },
        ]
    )
    return pipeline


def _build_top_products_mv_pipeline(
    refreshed_at: str,
    refresh_mode: str,
    watermark_processed_at: Optional[str],
    affected_skus: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Build the deterministic `$merge` aggregation pipeline for `top_products_summary`.
    Matches the exact semantics of `src.aggregations.reports.top_products`:
    - `order_count` is line-item count (`$sum: 1` after `$unwind: "$items"`).
    - `distinct_order_count` is the count of unique `id_order` values.
    """
    pipeline: List[Dict[str, Any]] = []

    if affected_skus is not None:
        pipeline.append({"$match": {"items.sku": {"$in": affected_skus}}})

    pipeline.append({"$unwind": "$items"})

    if affected_skus is not None:
        pipeline.append({"$match": {"items.sku": {"$in": affected_skus}}})

    pipeline.extend(
        [
            {
                "$group": {
                    "_id": "$items.sku",
                    "product_name": {"$first": "$items.name"},
                    "total_quantity": {"$sum": "$items.qty"},
                    "total_sales": {"$sum": "$items.total"},
                    "order_count": {"$sum": 1},
                    "distinct_orders": {"$addToSet": "$id_order"},
                }
            },
            {
                "$project": {
                    "_id": "$_id",
                    "sku": "$_id",
                    "product_name": 1,
                    "total_quantity": 1,
                    "total_sales": {"$round": ["$total_sales", 2]},
                    "order_count": 1,
                    "distinct_order_count": {"$size": "$distinct_orders"},
                    "refreshed_at": {"$literal": refreshed_at},
                    "refresh_mode": {"$literal": refresh_mode},
                    "watermark_processed_at": {"$literal": watermark_processed_at},
                }
            },
            {"$sort": {"total_sales": DESCENDING, "sku": ASCENDING}},
            {
                "$merge": {
                    "into": TOP_PRODUCTS_MV_COLLECTION,
                    "on": "sku",
                    "whenMatched": "replace",
                    "whenNotMatched": "insert",
                }
            },
        ]
    )
    return pipeline


def _build_full_order_digest_pipeline() -> List[Dict[str, Any]]:
    """
    Build the `$merge` aggregation pipeline to populate `mv_order_digest`
    with minimal per-order dimension tracking fields:
    `_id`, `id_order`, `order_date_day`, `skus`, `processed_at`, `id_run`.
    """
    return [
        {
            "$project": {
                "_id": "$id_order",
                "id_order": "$id_order",
                "order_date_day": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": {"$toDate": "$order_date"},
                    }
                },
                "skus": {"$setUnion": ["$items.sku", []]},
                "processed_at": "$processed_at",
                "id_run": "$id_run",
            }
        },
        {
            "$merge": {
                "into": MV_ORDER_DIGEST_COLLECTION,
                "on": "id_order",
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        },
    ]


# --------------------------------------------------------------------------
# Internal Refresh Helpers
# --------------------------------------------------------------------------

def _get_watermark_snapshot(db: Database) -> Tuple[Optional[str], Optional[str], int]:
    """
    Inspect `orders_validated` to retrieve `(max_processed_at, latest_id_run, total_docs)`.
    """
    val_col = db[VALIDATED_COLLECTION]
    total_docs = val_col.count_documents({})
    if total_docs == 0:
        return None, None, 0

    agg = list(
        val_col.aggregate(
            [
                {"$sort": {"processed_at": DESCENDING}},
                {"$limit": 1},
                {"$project": {"_id": 0, "processed_at": 1, "id_run": 1}},
            ]
        )
    )
    if not agg:
        return None, None, total_docs
    return agg[0].get("processed_at"), agg[0].get("id_run"), total_docs


def _recompute_daily_sales_internal(
    db: Database,
    refreshed_at: str,
    refresh_mode: str,
    watermark_processed_at: Optional[str],
    affected_dates: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Execute the deterministic recomputation of `daily_sales_summary` (either full or
    restricted to `affected_dates`) and delete any affected/stale dates with 0 remaining orders.
    """
    val_col = db[VALIDATED_COLLECTION]
    mv_col = db[DAILY_SALES_MV_COLLECTION]

    if affected_dates is not None and len(affected_dates) == 0:
        return {
            "recomputed_dates": [],
            "deleted_dates_count": 0,
            "total_mv_docs": mv_col.count_documents({}),
        }

    pipeline = _build_daily_sales_mv_pipeline(
        refreshed_at=refreshed_at,
        refresh_mode=refresh_mode,
        watermark_processed_at=watermark_processed_at,
        affected_dates=affected_dates,
    )
    list(val_col.aggregate(pipeline))

    if affected_dates is None:
        # Full refresh: prune any date documents that were not touched in this full run
        del_res = mv_col.delete_many({"refreshed_at": {"$ne": refreshed_at}})
        deleted_count = del_res.deleted_count
        recomputed_dates = [
            d["date"]
            for d in mv_col.find({}, {"_id": 0, "date": 1}).sort("date", ASCENDING)
        ]
    else:
        # Incremental refresh: if an affected date has zero remaining orders in orders_validated,
        # it was not emitted by $group and still has an old refreshed_at -> delete it.
        del_res = mv_col.delete_many(
            {
                "date": {"$in": affected_dates},
                "refreshed_at": {"$ne": refreshed_at},
            }
        )
        deleted_count = del_res.deleted_count
        recomputed_dates = sorted(affected_dates)

    return {
        "recomputed_dates": recomputed_dates,
        "deleted_dates_count": deleted_count,
        "total_mv_docs": mv_col.count_documents({}),
    }


def _recompute_top_products_internal(
    db: Database,
    refreshed_at: str,
    refresh_mode: str,
    watermark_processed_at: Optional[str],
    affected_skus: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Execute the deterministic recomputation of `top_products_summary` (either full or
    restricted to `affected_skus`) and delete any affected/stale SKUs with 0 remaining items.
    """
    val_col = db[VALIDATED_COLLECTION]
    mv_col = db[TOP_PRODUCTS_MV_COLLECTION]

    if affected_skus is not None and len(affected_skus) == 0:
        return {
            "recomputed_skus": [],
            "deleted_skus_count": 0,
            "total_mv_docs": mv_col.count_documents({}),
        }

    pipeline = _build_top_products_mv_pipeline(
        refreshed_at=refreshed_at,
        refresh_mode=refresh_mode,
        watermark_processed_at=watermark_processed_at,
        affected_skus=affected_skus,
    )
    list(val_col.aggregate(pipeline))

    if affected_skus is None:
        del_res = mv_col.delete_many({"refreshed_at": {"$ne": refreshed_at}})
        deleted_count = del_res.deleted_count
        recomputed_skus = [
            d["sku"]
            for d in mv_col.find({}, {"_id": 0, "sku": 1}).sort("sku", ASCENDING)
        ]
    else:
        del_res = mv_col.delete_many(
            {
                "sku": {"$in": affected_skus},
                "refreshed_at": {"$ne": refreshed_at},
            }
        )
        deleted_count = del_res.deleted_count
        recomputed_skus = sorted(affected_skus)

    return {
        "recomputed_skus": recomputed_skus,
        "deleted_skus_count": deleted_count,
        "total_mv_docs": mv_col.count_documents({}),
    }


def _collect_incremental_changes(
    db: Database,
    last_processed_at: Optional[str],
    pending_dates: List[str],
    pending_skus: List[str],
) -> Dict[str, Any]:
    """
    Detect all orders in `orders_validated` with `processed_at > last_processed_at`,
    fetch their prior dimension digests from `mv_order_digest`, and compute:
    - `affected_dates = new_dates UNION old_dates UNION pending_dates`
    - `affected_skus = new_skus UNION old_skus UNION pending_skus`
    - `digest_updates`: list of new digest documents to upsert into `mv_order_digest`
    """
    val_col = db[VALIDATED_COLLECTION]
    digest_col = db[MV_ORDER_DIGEST_COLLECTION]

    query: Dict[str, Any] = {}
    if last_processed_at is not None:
        query = {"processed_at": {"$gt": last_processed_at}}

    changed_docs = list(
        val_col.find(
            query,
            {
                "_id": 0,
                "id_order": 1,
                "order_date": 1,
                "items.sku": 1,
                "processed_at": 1,
                "id_run": 1,
            },
        )
    )

    new_dates: Set[str] = set()
    new_skus: Set[str] = set()
    old_dates: Set[str] = set()
    old_skus: Set[str] = set()
    changed_id_orders: List[str] = []
    digest_updates: List[Dict[str, Any]] = []
    max_changed_processed_at: Optional[str] = last_processed_at
    latest_id_run: Optional[str] = None

    for doc in changed_docs:
        oid = doc["id_order"]
        changed_id_orders.append(oid)
        day_str = _derive_day_str(doc.get("order_date", ""))
        if day_str:
            new_dates.add(day_str)

        order_skus_set: Set[str] = set()
        for item in doc.get("items", []):
            sku_val = item.get("sku", "")
            if sku_val is None:
                sku_val = ""
            order_skus_set.add(sku_val)
            new_skus.add(sku_val)

        proc_at = doc.get("processed_at")
        if proc_at and (
            max_changed_processed_at is None or proc_at > max_changed_processed_at
        ):
            max_changed_processed_at = proc_at
            latest_id_run = doc.get("id_run")

        digest_updates.append(
            {
                "_id": oid,
                "id_order": oid,
                "order_date_day": day_str,
                "skus": sorted(order_skus_set),
                "processed_at": proc_at,
                "id_run": doc.get("id_run"),
            }
        )

    if changed_id_orders:
        prior_digests = list(
            digest_col.find(
                {"id_order": {"$in": changed_id_orders}},
                {"_id": 0, "id_order": 1, "order_date_day": 1, "skus": 1},
            )
        )
        for p_doc in prior_digests:
            old_day = p_doc.get("order_date_day")
            if old_day:
                old_dates.add(old_day)
            for s in p_doc.get("skus", []):
                old_skus.add(s)

    affected_dates = sorted(set(pending_dates) | new_dates | old_dates)
    affected_skus = sorted(set(pending_skus) | new_skus | old_skus)

    return {
        "changed_orders_count": len(changed_docs),
        "changed_id_orders": changed_id_orders,
        "new_dates": sorted(new_dates),
        "old_dates": sorted(old_dates),
        "new_skus": sorted(new_skus),
        "old_skus": sorted(old_skus),
        "affected_dates": affected_dates,
        "affected_skus": affected_skus,
        "digest_updates": digest_updates,
        "max_changed_processed_at": max_changed_processed_at,
        "latest_id_run": latest_id_run,
    }


def _upsert_order_digests(db: Database, digest_updates: List[Dict[str, Any]]) -> int:
    """Upsert changed order digest documents into `mv_order_digest`."""
    if not digest_updates:
        return 0
    digest_col = db[MV_ORDER_DIGEST_COLLECTION]
    ops = [
        ReplaceOne({"id_order": d["id_order"]}, d, upsert=True)
        for d in digest_updates
    ]
    res = digest_col.bulk_write(ops, ordered=False)
    return res.upserted_count + res.modified_count + res.matched_count


# --------------------------------------------------------------------------
# Approved Public Functions (Step 3)
# --------------------------------------------------------------------------

def ensure_mv_indexes(db_name: str = MONGO_DB_NAME) -> Dict[str, Any]:
    """
    Create the approved indexes on `daily_sales_summary`, `top_products_summary`,
    and `mv_order_digest`. Does NOT modify indexes on `orders_validated`.
    """
    db = get_database(db_name=db_name)
    created_indexes: Dict[str, List[str]] = {}
    all_indexes: Dict[str, List[Dict[str, Any]]] = {}

    for col_name, specs in MV_INDEX_SPECS.items():
        col = db[col_name]
        created_for_col: List[str] = []
        for spec in specs:
            idx_name = col.create_index(
                spec["keys"],
                name=spec["name"],
                unique=spec["unique"],
            )
            created_for_col.append(idx_name)
        created_indexes[col_name] = created_for_col
        all_indexes[col_name] = [
            {
                "name": idx["name"],
                "key": dict(idx["key"]),
                "unique": idx.get("unique", False),
            }
            for idx in col.list_indexes()
        ]

    return {
        "database": db_name,
        "created_indexes": created_indexes,
        "all_indexes": all_indexes,
    }


def refresh_daily_sales_mv(
    mode: str = "incremental",
    db_name: str = MONGO_DB_NAME,
) -> Dict[str, Any]:
    """
    Refresh `daily_sales_summary` in either 'full' or 'incremental' mode.
    When called in 'incremental' mode without prior refresh state, automatically
    initializes via 'full' refresh.
    """
    validated_mode = _validate_mode(mode)
    ensure_mv_indexes(db_name=db_name)
    db = get_database(db_name=db_name)
    state_col = db[MV_REFRESH_STATE_COLLECTION]
    state_doc = state_col.find_one({"_id": MV_CONTROLLER_ID})

    if validated_mode == "full" or state_doc is None:
        started_at = datetime.now().isoformat()
        max_proc_at, latest_run, total_docs = _get_watermark_snapshot(db)
        mv_stats = _recompute_daily_sales_internal(
            db=db,
            refreshed_at=started_at,
            refresh_mode="full",
            watermark_processed_at=max_proc_at,
            affected_dates=None,
        )
        return {
            "view_name": DAILY_SALES_MV_COLLECTION,
            "database": db_name,
            "mode_requested": validated_mode,
            "mode_executed": "full",
            "status": "SUCCESS",
            "noop": False,
            "recomputed_dates": mv_stats["recomputed_dates"],
            "deleted_dates_count": mv_stats["deleted_dates_count"],
            "document_count": mv_stats["total_mv_docs"],
            "watermark_processed_at": max_proc_at,
            "refreshed_at": started_at,
        }

    # Incremental mode with existing state
    last_proc_at = state_doc.get("last_processed_at")
    pending_dates = list(state_doc.get("pending_affected_dates", []))
    pending_skus = list(state_doc.get("pending_affected_skus", []))

    delta = _collect_incremental_changes(
        db=db,
        last_processed_at=last_proc_at,
        pending_dates=pending_dates,
        pending_skus=pending_skus,
    )
    affected_dates = delta["affected_dates"]

    if delta["changed_orders_count"] == 0 and len(affected_dates) == 0:
        return {
            "view_name": DAILY_SALES_MV_COLLECTION,
            "database": db_name,
            "mode_requested": "incremental",
            "mode_executed": "incremental",
            "status": "NO_OP",
            "noop": True,
            "recomputed_dates": [],
            "deleted_dates_count": 0,
            "document_count": db[DAILY_SALES_MV_COLLECTION].count_documents({}),
            "watermark_processed_at": last_proc_at,
            "refreshed_at": state_doc.get("last_successful_refresh_at"),
        }

    started_at = datetime.now().isoformat()
    # Persist pending groups before modifying the MV so any unread SKU changes are preserved
    state_col.update_one(
        {"_id": MV_CONTROLLER_ID},
        {
            "$set": {
                "status": "IN_PROGRESS",
                "last_refresh_mode": "incremental",
                "last_refresh_started_at": started_at,
                "pending_affected_dates": affected_dates,
                "pending_affected_skus": delta["affected_skus"],
            }
        },
        upsert=True,
    )

    mv_stats = _recompute_daily_sales_internal(
        db=db,
        refreshed_at=started_at,
        refresh_mode="incremental",
        watermark_processed_at=delta["max_changed_processed_at"],
        affected_dates=affected_dates,
    )

    return {
        "view_name": DAILY_SALES_MV_COLLECTION,
        "database": db_name,
        "mode_requested": "incremental",
        "mode_executed": "incremental",
        "status": "SUCCESS",
        "noop": False,
        "recomputed_dates": mv_stats["recomputed_dates"],
        "deleted_dates_count": mv_stats["deleted_dates_count"],
        "document_count": mv_stats["total_mv_docs"],
        "watermark_processed_at": delta["max_changed_processed_at"],
        "refreshed_at": started_at,
    }


def refresh_top_products_mv(
    mode: str = "incremental",
    db_name: str = MONGO_DB_NAME,
) -> Dict[str, Any]:
    """
    Refresh `top_products_summary` in either 'full' or 'incremental' mode.
    When called in 'incremental' mode without prior refresh state, automatically
    initializes via 'full' refresh.
    """
    validated_mode = _validate_mode(mode)
    ensure_mv_indexes(db_name=db_name)
    db = get_database(db_name=db_name)
    state_col = db[MV_REFRESH_STATE_COLLECTION]
    state_doc = state_col.find_one({"_id": MV_CONTROLLER_ID})

    if validated_mode == "full" or state_doc is None:
        started_at = datetime.now().isoformat()
        max_proc_at, latest_run, total_docs = _get_watermark_snapshot(db)
        mv_stats = _recompute_top_products_internal(
            db=db,
            refreshed_at=started_at,
            refresh_mode="full",
            watermark_processed_at=max_proc_at,
            affected_skus=None,
        )
        return {
            "view_name": TOP_PRODUCTS_MV_COLLECTION,
            "database": db_name,
            "mode_requested": validated_mode,
            "mode_executed": "full",
            "status": "SUCCESS",
            "noop": False,
            "recomputed_skus": mv_stats["recomputed_skus"],
            "deleted_skus_count": mv_stats["deleted_skus_count"],
            "document_count": mv_stats["total_mv_docs"],
            "watermark_processed_at": max_proc_at,
            "refreshed_at": started_at,
        }

    last_proc_at = state_doc.get("last_processed_at")
    pending_dates = list(state_doc.get("pending_affected_dates", []))
    pending_skus = list(state_doc.get("pending_affected_skus", []))

    delta = _collect_incremental_changes(
        db=db,
        last_processed_at=last_proc_at,
        pending_dates=pending_dates,
        pending_skus=pending_skus,
    )
    affected_skus = delta["affected_skus"]

    if delta["changed_orders_count"] == 0 and len(affected_skus) == 0:
        return {
            "view_name": TOP_PRODUCTS_MV_COLLECTION,
            "database": db_name,
            "mode_requested": "incremental",
            "mode_executed": "incremental",
            "status": "NO_OP",
            "noop": True,
            "recomputed_skus": [],
            "deleted_skus_count": 0,
            "document_count": db[TOP_PRODUCTS_MV_COLLECTION].count_documents({}),
            "watermark_processed_at": last_proc_at,
            "refreshed_at": state_doc.get("last_successful_refresh_at"),
        }

    started_at = datetime.now().isoformat()
    state_col.update_one(
        {"_id": MV_CONTROLLER_ID},
        {
            "$set": {
                "status": "IN_PROGRESS",
                "last_refresh_mode": "incremental",
                "last_refresh_started_at": started_at,
                "pending_affected_dates": delta["affected_dates"],
                "pending_affected_skus": affected_skus,
            }
        },
        upsert=True,
    )

    mv_stats = _recompute_top_products_internal(
        db=db,
        refreshed_at=started_at,
        refresh_mode="incremental",
        watermark_processed_at=delta["max_changed_processed_at"],
        affected_skus=affected_skus,
    )

    return {
        "view_name": TOP_PRODUCTS_MV_COLLECTION,
        "database": db_name,
        "mode_requested": "incremental",
        "mode_executed": "incremental",
        "status": "SUCCESS",
        "noop": False,
        "recomputed_skus": mv_stats["recomputed_skus"],
        "deleted_skus_count": mv_stats["deleted_skus_count"],
        "document_count": mv_stats["total_mv_docs"],
        "watermark_processed_at": delta["max_changed_processed_at"],
        "refreshed_at": started_at,
    }


def refresh_all_materialized_views(
    mode: str = "incremental",
    db_name: str = MONGO_DB_NAME,
    _simulate_failure_stage: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Unified controller for refreshing both Materialized Views (`daily_sales_summary`
    and `top_products_summary`), maintaining `mv_order_digest` and `mv_refresh_state`.

    Guarantees:
    - Full Refresh (`mode="full"`): populates both MVs, `mv_order_digest`, and `mv_refresh_state`,
      setting `status = "IDLE"` and `last_processed_at = current max(processed_at)`.
    - Incremental Refresh (`mode="incremental"`):
      1. Reads `mv_refresh_state` (falls back to 'full' if not initialized yet).
      2. Detects interrupted refresh (`status == "IN_PROGRESS"`) and recovers using
         `pending_affected_dates` and `pending_affected_skus`.
      3. Queries `orders_validated` for `processed_at > last_processed_at`.
      4. Returns clean `NO_OP` if no orders changed and no pending groups exist.
      5. Computes `affected_dates = new_dates UNION old_dates UNION pending_dates` and
         `affected_skus = new_skus UNION old_skus UNION pending_skus`.
      6. Persists `status = "IN_PROGRESS"` and pending groups BEFORE modifying the MVs.
      7. Recomputes only affected dates and affected SKUs, and updates `mv_order_digest`.
      8. Advances `last_processed_at`, clears pending groups, and sets `status = "IDLE"`
         ONLY after all 3 steps succeed.
    """
    t0 = time.perf_counter()
    validated_mode = _validate_mode(mode)
    ensure_mv_indexes(db_name=db_name)

    db = get_database(db_name=db_name)
    val_col = db[VALIDATED_COLLECTION]
    digest_col = db[MV_ORDER_DIGEST_COLLECTION]
    state_col = db[MV_REFRESH_STATE_COLLECTION]

    existing_state = state_col.find_one({"_id": MV_CONTROLLER_ID})
    effective_mode = validated_mode
    if effective_mode == "incremental" and (
        existing_state is None or existing_state.get("last_processed_at") is None
    ):
        logger.info(
            "No initialized watermark found in %s.%s; falling back to mode='full'.",
            db_name,
            MV_REFRESH_STATE_COLLECTION,
        )
        effective_mode = "full"

    started_at = datetime.now().isoformat()

    # ------------------------------------------------------------------
    # FULL REFRESH PATH (Step 8)
    # ------------------------------------------------------------------
    if effective_mode == "full":
        max_proc_at, latest_run, total_validated = _get_watermark_snapshot(db)
        watermark_before = (
            existing_state.get("last_processed_at") if existing_state else None
        )

        state_col.update_one(
            {"_id": MV_CONTROLLER_ID},
            {
                "$set": {
                    "status": "IN_PROGRESS",
                    "last_refresh_mode": "full",
                    "last_refresh_started_at": started_at,
                    "pending_affected_dates": [],
                    "pending_affected_skus": [],
                },
                "$setOnInsert": {
                    "last_processed_at": None,
                    "last_id_run": None,
                    "last_successful_refresh_at": None,
                    "validated_order_count_snapshot": 0,
                    "last_error": None,
                },
            },
            upsert=True,
        )

        try:
            daily_stats = _recompute_daily_sales_internal(
                db=db,
                refreshed_at=started_at,
                refresh_mode="full",
                watermark_processed_at=max_proc_at,
                affected_dates=None,
            )

            if _simulate_failure_stage == "after_daily_sales":
                raise RuntimeError(
                    "Simulated mid-refresh failure after daily_sales_summary."
                )

            products_stats = _recompute_top_products_internal(
                db=db,
                refreshed_at=started_at,
                refresh_mode="full",
                watermark_processed_at=max_proc_at,
                affected_skus=None,
            )

            if _simulate_failure_stage == "after_top_products":
                raise RuntimeError(
                    "Simulated mid-refresh failure after top_products_summary."
                )

            # Rebuild mv_order_digest cleanly so no orphan digest records remain
            digest_col.delete_many({})
            list(val_col.aggregate(_build_full_order_digest_pipeline()))
            digest_count = digest_col.count_documents({})

            completed_at = datetime.now().isoformat()
            state_col.update_one(
                {"_id": MV_CONTROLLER_ID},
                {
                    "$set": {
                        "status": "IDLE",
                        "last_processed_at": max_proc_at,
                        "last_id_run": latest_run,
                        "last_refresh_mode": "full",
                        "last_refresh_started_at": started_at,
                        "last_successful_refresh_at": completed_at,
                        "validated_order_count_snapshot": total_validated,
                        "pending_affected_dates": [],
                        "pending_affected_skus": [],
                        "last_error": None,
                    }
                },
                upsert=True,
            )
        except Exception as exc:
            logger.error("Error during full MV refresh: %s", exc)
            state_col.update_one(
                {"_id": MV_CONTROLLER_ID},
                {"$set": {"status": "IN_PROGRESS", "last_error": str(exc)}},
            )
            raise

        duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        return {
            "database": db_name,
            "mode_requested": validated_mode,
            "mode_executed": "full",
            "status": "SUCCESS",
            "noop": False,
            "recovered_from_in_progress": bool(
                existing_state and existing_state.get("status") == "IN_PROGRESS"
            ),
            "changed_orders_count": total_validated,
            "affected_dates": daily_stats["recomputed_dates"],
            "affected_skus": products_stats["recomputed_skus"],
            "daily_sales_docs_count": daily_stats["total_mv_docs"],
            "top_products_docs_count": products_stats["total_mv_docs"],
            "order_digest_docs_count": digest_count,
            "watermark_before": watermark_before,
            "watermark_after": max_proc_at,
            "refreshed_at": completed_at,
            "duration_ms": duration_ms,
        }

    # ------------------------------------------------------------------
    # INCREMENTAL REFRESH PATH (Step 9, Step 10, Step 11)
    # ------------------------------------------------------------------
    assert existing_state is not None
    watermark_before = existing_state.get("last_processed_at")
    was_in_progress = existing_state.get("status") == "IN_PROGRESS"
    pending_dates = (
        list(existing_state.get("pending_affected_dates", []))
        if was_in_progress or existing_state.get("pending_affected_dates")
        else []
    )
    pending_skus = (
        list(existing_state.get("pending_affected_skus", []))
        if was_in_progress or existing_state.get("pending_affected_skus")
        else []
    )

    delta = _collect_incremental_changes(
        db=db,
        last_processed_at=watermark_before,
        pending_dates=pending_dates,
        pending_skus=pending_skus,
    )

    changed_orders_count = delta["changed_orders_count"]
    affected_dates = delta["affected_dates"]
    affected_skus = delta["affected_skus"]

    # Step 9.4: Clean NO_OP when no changed orders and no pending recovery work
    if (
        not was_in_progress
        and changed_orders_count == 0
        and len(affected_dates) == 0
        and len(affected_skus) == 0
    ):
        duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        return {
            "database": db_name,
            "mode_requested": "incremental",
            "mode_executed": "incremental",
            "status": "NO_OP",
            "noop": True,
            "recovered_from_in_progress": False,
            "changed_orders_count": 0,
            "affected_dates": [],
            "affected_skus": [],
            "daily_sales_docs_count": db[DAILY_SALES_MV_COLLECTION].count_documents({}),
            "top_products_docs_count": db[TOP_PRODUCTS_MV_COLLECTION].count_documents({}),
            "order_digest_docs_count": digest_col.count_documents({}),
            "watermark_before": watermark_before,
            "watermark_after": watermark_before,
            "refreshed_at": existing_state.get("last_successful_refresh_at"),
            "duration_ms": duration_ms,
        }

    # Step 9.7: Persist status = IN_PROGRESS and pending groups BEFORE modifying MVs
    state_col.update_one(
        {"_id": MV_CONTROLLER_ID},
        {
            "$set": {
                "status": "IN_PROGRESS",
                "last_refresh_mode": "incremental",
                "last_refresh_started_at": started_at,
                "pending_affected_dates": affected_dates,
                "pending_affected_skus": affected_skus,
            }
        },
    )

    try:
        if _simulate_failure_stage == "before_mv_updates":
            raise RuntimeError(
                "Simulated mid-refresh failure after persisting pending groups."
            )

        # Step 9.8: Recompute only affected dates
        daily_stats = _recompute_daily_sales_internal(
            db=db,
            refreshed_at=started_at,
            refresh_mode="incremental",
            watermark_processed_at=delta["max_changed_processed_at"],
            affected_dates=affected_dates,
        )

        if _simulate_failure_stage == "after_daily_sales":
            raise RuntimeError(
                "Simulated mid-refresh failure after daily_sales_summary update."
            )

        # Step 9.9: Recompute only affected SKUs
        products_stats = _recompute_top_products_internal(
            db=db,
            refreshed_at=started_at,
            refresh_mode="incremental",
            watermark_processed_at=delta["max_changed_processed_at"],
            affected_skus=affected_skus,
        )

        if _simulate_failure_stage == "after_top_products":
            raise RuntimeError(
                "Simulated mid-refresh failure after top_products_summary update."
            )

        # Step 9.10: Update mv_order_digest for changed/new orders
        _upsert_order_digests(db=db, digest_updates=delta["digest_updates"])

        # Step 9.11: Only after all operations succeed, set status = IDLE,
        # advance last_processed_at, clear pending groups, and record timestamp.
        completed_at = datetime.now().isoformat()
        new_watermark = delta["max_changed_processed_at"] or watermark_before
        new_id_run = delta["latest_id_run"] or existing_state.get("last_id_run")
        total_validated = val_col.count_documents({})

        state_col.update_one(
            {"_id": MV_CONTROLLER_ID},
            {
                "$set": {
                    "status": "IDLE",
                    "last_processed_at": new_watermark,
                    "last_id_run": new_id_run,
                    "last_refresh_mode": "incremental",
                    "last_refresh_started_at": started_at,
                    "last_successful_refresh_at": completed_at,
                    "validated_order_count_snapshot": total_validated,
                    "pending_affected_dates": [],
                    "pending_affected_skus": [],
                    "last_error": None,
                }
            },
        )
    except Exception as exc:
        logger.error("Error during incremental MV refresh: %s", exc)
        state_col.update_one(
            {"_id": MV_CONTROLLER_ID},
            {
                "$set": {
                    "status": "IN_PROGRESS",
                    "last_error": str(exc),
                }
            },
        )
        raise

    duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    return {
        "database": db_name,
        "mode_requested": "incremental",
        "mode_executed": "incremental",
        "status": "SUCCESS",
        "noop": False,
        "recovered_from_in_progress": was_in_progress,
        "changed_orders_count": changed_orders_count,
        "new_dates": delta["new_dates"],
        "old_dates": delta["old_dates"],
        "affected_dates": affected_dates,
        "deleted_dates_count": daily_stats["deleted_dates_count"],
        "new_skus": delta["new_skus"],
        "old_skus": delta["old_skus"],
        "affected_skus": affected_skus,
        "deleted_skus_count": products_stats["deleted_skus_count"],
        "daily_sales_docs_count": daily_stats["total_mv_docs"],
        "top_products_docs_count": products_stats["total_mv_docs"],
        "order_digest_docs_count": digest_col.count_documents({}),
        "watermark_before": watermark_before,
        "watermark_after": new_watermark,
        "refreshed_at": completed_at,
        "duration_ms": duration_ms,
    }


def get_daily_sales_mv(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    db_name: str = MONGO_DB_NAME,
) -> Dict[str, Any]:
    """
    Read precomputed daily sales summary records from `daily_sales_summary`.
    Supports optional `start_date`, `end_date` (ISO-8601 date/timestamp), and `limit`.
    """
    validated_limit: Optional[int] = None
    if limit is not None:
        validated_limit = _validate_limit(limit)

    norm_start: Optional[str] = None
    norm_end: Optional[str] = None
    dt_start: Optional[datetime] = None
    dt_end: Optional[datetime] = None

    if start_date is not None:
        norm_start, dt_start = _parse_and_normalize_day_bound(start_date, "start_date")
    if end_date is not None:
        norm_end, dt_end = _parse_and_normalize_day_bound(end_date, "end_date")

    if dt_start is not None and dt_end is not None and dt_start > dt_end:
        raise ValueError(
            f"'start_date' ({norm_start}) must be <= 'end_date' ({norm_end})."
        )

    query: Dict[str, Any] = {}
    if norm_start is not None or norm_end is not None:
        date_filter: Dict[str, Any] = {}
        if norm_start is not None:
            date_filter["$gte"] = norm_start
        if norm_end is not None:
            date_filter["$lte"] = norm_end
        query["date"] = date_filter

    try:
        db = get_database(db_name=db_name)
        col = db[DAILY_SALES_MV_COLLECTION]
        cursor = col.find(query, {"_id": 0}).sort("date", ASCENDING)
        if validated_limit is not None:
            cursor = cursor.limit(validated_limit)
        raw_docs = list(cursor)
    except PyMongoError as exc:
        logger.error("MongoDB error in get_daily_sales_mv: %s", exc)
        raise

    results: List[Dict[str, Any]] = []
    for doc in raw_docs:
        item = dict(doc)
        item["order_count"] = int(item["order_count"])
        item["total_sales"] = round(float(item["total_sales"]), 2)
        item["average_order_value"] = round(float(item["average_order_value"]), 2)
        results.append(item)

    return {
        "report_name": DAILY_SALES_MV_COLLECTION,
        "collection": DAILY_SALES_MV_COLLECTION,
        "parameters": {
            "start_date": norm_start,
            "end_date": norm_end,
            "limit": validated_limit,
        },
        "count": len(results),
        "results": results,
    }


def get_top_products_mv(
    limit: int = 10,
    include_unassigned_sku: bool = True,
    db_name: str = MONGO_DB_NAME,
) -> Dict[str, Any]:
    """
    Read precomputed top product records from `top_products_summary` sorted by
    `total_sales` descending and `sku` ascending.
    """
    validated_limit = _validate_limit(limit)
    if not isinstance(include_unassigned_sku, bool):
        raise ValueError("'include_unassigned_sku' must be a boolean.")

    query: Dict[str, Any] = {}
    if not include_unassigned_sku:
        query["sku"] = {"$nin": ["", None]}

    try:
        db = get_database(db_name=db_name)
        col = db[TOP_PRODUCTS_MV_COLLECTION]
        cursor = (
            col.find(query, {"_id": 0})
            .sort([("total_sales", DESCENDING), ("sku", ASCENDING)])
            .limit(validated_limit)
        )
        raw_docs = list(cursor)
    except PyMongoError as exc:
        logger.error("MongoDB error in get_top_products_mv: %s", exc)
        raise

    results: List[Dict[str, Any]] = []
    for doc in raw_docs:
        item = dict(doc)
        item["total_quantity"] = int(item["total_quantity"])
        item["total_sales"] = round(float(item["total_sales"]), 2)
        item["order_count"] = int(item["order_count"])
        item["distinct_order_count"] = int(item["distinct_order_count"])
        results.append(item)

    return {
        "report_name": TOP_PRODUCTS_MV_COLLECTION,
        "collection": TOP_PRODUCTS_MV_COLLECTION,
        "parameters": {
            "limit": validated_limit,
            "include_unassigned_sku": include_unassigned_sku,
        },
        "count": len(results),
        "results": results,
    }


def get_mv_refresh_status(db_name: str = MONGO_DB_NAME) -> Dict[str, Any]:
    """
    Retrieve the current refresh controller document from `mv_refresh_state`
    along with live document counts for the Materialized View and digest collections.
    """
    db = get_database(db_name=db_name)
    state_doc = db[MV_REFRESH_STATE_COLLECTION].find_one({"_id": MV_CONTROLLER_ID})
    existing_cols = set(db.list_collection_names())

    daily_count = (
        db[DAILY_SALES_MV_COLLECTION].count_documents({})
        if DAILY_SALES_MV_COLLECTION in existing_cols
        else 0
    )
    products_count = (
        db[TOP_PRODUCTS_MV_COLLECTION].count_documents({})
        if TOP_PRODUCTS_MV_COLLECTION in existing_cols
        else 0
    )
    digest_count = (
        db[MV_ORDER_DIGEST_COLLECTION].count_documents({})
        if MV_ORDER_DIGEST_COLLECTION in existing_cols
        else 0
    )
    validated_count = (
        db[VALIDATED_COLLECTION].count_documents({})
        if VALIDATED_COLLECTION in existing_cols
        else 0
    )

    if state_doc is None:
        return {
            "database": db_name,
            "controller_id": MV_CONTROLLER_ID,
            "initialized": False,
            "status": "UNINITIALIZED",
            "last_processed_at": None,
            "last_id_run": None,
            "last_refresh_mode": None,
            "last_refresh_started_at": None,
            "last_successful_refresh_at": None,
            "validated_order_count_snapshot": 0,
            "pending_affected_dates": [],
            "pending_affected_skus": [],
            "last_error": None,
            "collection_counts": {
                DAILY_SALES_MV_COLLECTION: daily_count,
                TOP_PRODUCTS_MV_COLLECTION: products_count,
                MV_ORDER_DIGEST_COLLECTION: digest_count,
                VALIDATED_COLLECTION: validated_count,
            },
        }

    return {
        "database": db_name,
        "controller_id": state_doc.get("_id", MV_CONTROLLER_ID),
        "initialized": True,
        "status": state_doc.get("status"),
        "last_processed_at": state_doc.get("last_processed_at"),
        "last_id_run": state_doc.get("last_id_run"),
        "last_refresh_mode": state_doc.get("last_refresh_mode"),
        "last_refresh_started_at": state_doc.get("last_refresh_started_at"),
        "last_successful_refresh_at": state_doc.get("last_successful_refresh_at"),
        "validated_order_count_snapshot": state_doc.get(
            "validated_order_count_snapshot", 0
        ),
        "pending_affected_dates": list(state_doc.get("pending_affected_dates", [])),
        "pending_affected_skus": list(state_doc.get("pending_affected_skus", [])),
        "last_error": state_doc.get("last_error"),
        "collection_counts": {
            DAILY_SALES_MV_COLLECTION: daily_count,
            TOP_PRODUCTS_MV_COLLECTION: products_count,
            MV_ORDER_DIGEST_COLLECTION: digest_count,
            VALIDATED_COLLECTION: validated_count,
        },
    }
