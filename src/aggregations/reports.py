"""
Phase 2 — Step 2: Practical MongoDB Aggregation Reports Module.
Implements the 5 approved read-only analytical aggregation pipelines on `orders_validated`:
1. daily_sales_summary
2. sales_by_city
3. payment_method_analysis
4. order_status_distribution
5. top_products

Design Principles:
- Reuses `get_database` from `src.mongo_setup` and configuration from `config.settings`.
- Strictly read-only: uses only `collection.aggregate(pipeline)` on `orders_validated`.
- Does NOT create Materialized Views (`$out` / `$merge`), new collections, or indexes.
- Returns a consistent, JSON-serializable dictionary contract suitable for CLI, tests,
  scheduled jobs (Step 4), and FastAPI endpoints (Step 5).
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import PyMongoError

from config.settings import MONGO_DB_NAME, VALIDATED_COLLECTION
from src.mongo_setup import get_database

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Validation & Normalization Helpers
# --------------------------------------------------------------------------

def _validate_limit(limit: int) -> int:
    """Ensure limit is a positive integer (rejecting booleans, <= 0, and non-ints)."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError(f"'limit' must be a positive integer, got: {limit!r}")
    return limit


def _validate_non_empty_string(val: Any, field_name: str) -> str:
    """Ensure an explicitly supplied string parameter is non-empty."""
    if not isinstance(val, str) or not val.strip():
        raise ValueError(f"Parameter '{field_name}' must be a non-empty string.")
    return val.strip()


def _parse_and_normalize_iso_bound(
    date_str: str,
    field_name: str,
    is_end_bound: bool = False,
) -> Tuple[str, datetime]:
    """
    Validate that `date_str` is a valid calendar ISO-8601 date (`YYYY-MM-DD`)
    or timestamp (`YYYY-MM-DDTHH:MM:SS`). Rejects impossible dates like `2025-02-31`.
    If a 10-character date string (`YYYY-MM-DD`) is provided, normalizes it to
    `YYYY-MM-DDT00:00:00` (start bound) or `YYYY-MM-DDT23:59:59` (end bound)
    to align with the 19-byte ISO-8601 `order_date` strings stored in `orders_validated`.
    """
    cleaned = _validate_non_empty_string(date_str, field_name)
    try:
        parsed_dt = datetime.fromisoformat(cleaned)
    except ValueError as exc:
        raise ValueError(
            f"Invalid ISO-8601 date/timestamp for '{field_name}': {cleaned!r}. "
            f"Details: {exc}"
        ) from exc

    if len(cleaned) == 10:
        if is_end_bound:
            normalized_str = f"{cleaned}T23:59:59"
            parsed_dt = datetime.fromisoformat(normalized_str)
        else:
            normalized_str = f"{cleaned}T00:00:00"
            parsed_dt = datetime.fromisoformat(normalized_str)
    else:
        normalized_str = cleaned

    return normalized_str, parsed_dt


def _clean_numeric_fields(
    docs: List[Dict[str, Any]],
    int_fields: Tuple[str, ...] = (),
    float_fields: Tuple[str, ...] = (),
) -> List[Dict[str, Any]]:
    """
    Ensure aggregation output fields are clean, native Python `int` and `float`
    types for strict JSON serialization.
    """
    cleaned_docs: List[Dict[str, Any]] = []
    for doc in docs:
        item = dict(doc)
        item.pop("_id", None)
        for f in int_fields:
            if f in item and item[f] is not None:
                item[f] = int(item[f])
        for f in float_fields:
            if f in item and item[f] is not None:
                item[f] = round(float(item[f]), 2)
        cleaned_docs.append(item)
    return cleaned_docs


def _format_report_response(
    report_name: str,
    parameters: Dict[str, Any],
    results: List[Dict[str, Any]],
    collection_name: str = VALIDATED_COLLECTION,
) -> Dict[str, Any]:
    """Build the standardized, JSON-serializable aggregation report response."""
    return {
        "report_name": report_name,
        "collection": collection_name,
        "parameters": parameters,
        "count": len(results),
        "results": results,
    }


# --------------------------------------------------------------------------
# Approved Aggregation Reports (AGGREGATION 01 – AGGREGATION 05)
# --------------------------------------------------------------------------

def daily_sales_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    db_name: str = MONGO_DB_NAME,
) -> Dict[str, Any]:
    """
    AGGREGATION 01: Summarize order volume and revenue per calendar day.

    MongoDB stages:
    - Optional `$match` on `order_date` when `start_date` and/or `end_date` are provided.
    - `$addFields` deriving calendar `day` (`YYYY-MM-DD`) from ISO-8601 `order_date`
      via `$dateToString` + `$toDate`.
    - `$group` by `day` computing `order_count`, `total_sales`, and `average_order_value`.
    - `$project` shaping `date`, `order_count`, `total_sales`, and `average_order_value`.
    - `$sort` by `date` ascending.
    - Optional `$limit` when `limit` is provided.
    """
    validated_limit: Optional[int] = None
    if limit is not None:
        validated_limit = _validate_limit(limit)

    norm_start: Optional[str] = None
    norm_end: Optional[str] = None
    dt_start: Optional[datetime] = None
    dt_end: Optional[datetime] = None

    if start_date is not None:
        norm_start, dt_start = _parse_and_normalize_iso_bound(
            start_date, "start_date", is_end_bound=False
        )
    if end_date is not None:
        norm_end, dt_end = _parse_and_normalize_iso_bound(
            end_date, "end_date", is_end_bound=True
        )

    if dt_start is not None and dt_end is not None and dt_start > dt_end:
        raise ValueError(
            f"'start_date' ({norm_start}) must be <= 'end_date' ({norm_end})."
        )

    pipeline: List[Dict[str, Any]] = []

    if norm_start is not None or norm_end is not None:
        date_cond: Dict[str, Any] = {}
        if norm_start is not None:
            date_cond["$gte"] = norm_start
        if norm_end is not None:
            date_cond["$lte"] = norm_end
        pipeline.append({"$match": {"order_date": date_cond}})

    pipeline.extend([
        {
            "$addFields": {
                "day": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": {"$toDate": "$order_date"},
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$day",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$total_amount"},
                "average_order_value": {"$avg": "$total_amount"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id",
                "order_count": 1,
                "total_sales": {"$round": ["$total_sales", 2]},
                "average_order_value": {"$round": ["$average_order_value", 2]},
            }
        },
        {"$sort": {"date": ASCENDING}},
    ])

    if validated_limit is not None:
        pipeline.append({"$limit": validated_limit})

    try:
        db = get_database(db_name=db_name)
        col = db[VALIDATED_COLLECTION]
        raw_results = list(col.aggregate(pipeline))
        results = _clean_numeric_fields(
            raw_results,
            int_fields=("order_count",),
            float_fields=("total_sales", "average_order_value"),
        )
    except PyMongoError as exc:
        logger.error("MongoDB error in daily_sales_summary: %s", exc)
        raise

    return _format_report_response(
        report_name="daily_sales_summary",
        parameters={
            "start_date": norm_start,
            "end_date": norm_end,
            "limit": validated_limit,
        },
        results=results,
    )


def sales_by_city(db_name: str = MONGO_DB_NAME) -> Dict[str, Any]:
    """
    AGGREGATION 02: Analyze order volume and revenue across cities.

    MongoDB stages:
    - `$group` by `city` computing `order_count`, `total_sales`, and `average_order_value`.
    - `$project` shaping `city`, `order_count`, `total_sales`, and `average_order_value`.
    - `$sort` by `total_sales` descending.
    """
    pipeline: List[Dict[str, Any]] = [
        {
            "$group": {
                "_id": "$city",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$total_amount"},
                "average_order_value": {"$avg": "$total_amount"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "order_count": 1,
                "total_sales": {"$round": ["$total_sales", 2]},
                "average_order_value": {"$round": ["$average_order_value", 2]},
            }
        },
        {"$sort": {"total_sales": DESCENDING, "city": ASCENDING}},
    ]

    try:
        db = get_database(db_name=db_name)
        col = db[VALIDATED_COLLECTION]
        raw_results = list(col.aggregate(pipeline))
        results = _clean_numeric_fields(
            raw_results,
            int_fields=("order_count",),
            float_fields=("total_sales", "average_order_value"),
        )
    except PyMongoError as exc:
        logger.error("MongoDB error in sales_by_city: %s", exc)
        raise

    return _format_report_response(
        report_name="sales_by_city",
        parameters={},
        results=results,
    )


def payment_method_analysis(db_name: str = MONGO_DB_NAME) -> Dict[str, Any]:
    """
    AGGREGATION 03: Analyze usage and revenue across payment methods.

    MongoDB stages:
    - `$group` by `payment_method` computing `order_count`, `total_sales`,
      and `average_order_value`.
    - `$project` shaping `payment_method`, `order_count`, `total_sales`,
      and `average_order_value`.
    - `$sort` by `total_sales` descending.
    """
    pipeline: List[Dict[str, Any]] = [
        {
            "$group": {
                "_id": "$payment_method",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$total_amount"},
                "average_order_value": {"$avg": "$total_amount"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "payment_method": "$_id",
                "order_count": 1,
                "total_sales": {"$round": ["$total_sales", 2]},
                "average_order_value": {"$round": ["$average_order_value", 2]},
            }
        },
        {"$sort": {"total_sales": DESCENDING, "payment_method": ASCENDING}},
    ]

    try:
        db = get_database(db_name=db_name)
        col = db[VALIDATED_COLLECTION]
        raw_results = list(col.aggregate(pipeline))
        results = _clean_numeric_fields(
            raw_results,
            int_fields=("order_count",),
            float_fields=("total_sales", "average_order_value"),
        )
    except PyMongoError as exc:
        logger.error("MongoDB error in payment_method_analysis: %s", exc)
        raise

    return _format_report_response(
        report_name="payment_method_analysis",
        parameters={},
        results=results,
    )


def order_status_distribution(db_name: str = MONGO_DB_NAME) -> Dict[str, Any]:
    """
    AGGREGATION 04: Analyze operational order distribution by status.

    MongoDB stages:
    - `$group` by `status` computing `order_count` and `total_sales`.
    - `$project` shaping `status`, `order_count`, and `total_sales`.
    - `$sort` by `order_count` descending.
    """
    pipeline: List[Dict[str, Any]] = [
        {
            "$group": {
                "_id": "$status",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$total_amount"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "status": "$_id",
                "order_count": 1,
                "total_sales": {"$round": ["$total_sales", 2]},
            }
        },
        {"$sort": {"order_count": DESCENDING, "status": ASCENDING}},
    ]

    try:
        db = get_database(db_name=db_name)
        col = db[VALIDATED_COLLECTION]
        raw_results = list(col.aggregate(pipeline))
        results = _clean_numeric_fields(
            raw_results,
            int_fields=("order_count",),
            float_fields=("total_sales",),
        )
    except PyMongoError as exc:
        logger.error("MongoDB error in order_status_distribution: %s", exc)
        raise

    return _format_report_response(
        report_name="order_status_distribution",
        parameters={},
        results=results,
    )


def top_products(
    limit: int = 10,
    db_name: str = MONGO_DB_NAME,
) -> Dict[str, Any]:
    """
    AGGREGATION 05: Identify top-selling products across order line items.

    MongoDB stages:
    - `$unwind` on `items`.
    - `$group` by `items.sku` (capturing `product_name` from `items.name`)
      computing `total_quantity`, `total_sales`, and `order_count`.
    - `$project` shaping `sku`, `product_name`, `total_quantity`, `total_sales`,
      and `order_count`.
    - `$sort` by `total_sales` descending.
    - `$limit` configurable (default 10).
    """
    validated_limit = _validate_limit(limit)

    pipeline: List[Dict[str, Any]] = [
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.sku",
                "product_name": {"$first": "$items.name"},
                "total_quantity": {"$sum": "$items.qty"},
                "total_sales": {"$sum": "$items.total"},
                "order_count": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 0,
                "sku": "$_id",
                "product_name": 1,
                "total_quantity": 1,
                "total_sales": {"$round": ["$total_sales", 2]},
                "order_count": 1,
            }
        },
        {"$sort": {"total_sales": DESCENDING, "sku": ASCENDING}},
        {"$limit": validated_limit},
    ]

    try:
        db = get_database(db_name=db_name)
        col = db[VALIDATED_COLLECTION]
        raw_results = list(col.aggregate(pipeline))
        results = _clean_numeric_fields(
            raw_results,
            int_fields=("total_quantity", "order_count"),
            float_fields=("total_sales",),
        )
    except PyMongoError as exc:
        logger.error("MongoDB error in top_products: %s", exc)
        raise

    return _format_report_response(
        report_name="top_products",
        parameters={"limit": validated_limit},
        results=results,
    )


# --------------------------------------------------------------------------
# Aggregation Registry & Dispatcher (for CLI / Scheduled Jobs / FastAPI)
# --------------------------------------------------------------------------

AGGREGATION_REGISTRY: Dict[str, Any] = {
    "daily_sales_summary": daily_sales_summary,
    "sales_by_city": sales_by_city,
    "payment_method_analysis": payment_method_analysis,
    "order_status_distribution": order_status_distribution,
    "top_products": top_products,
}


def list_available_aggregations() -> List[str]:
    """Return the ordered list of all registered Phase 2 Step 2 aggregation reports."""
    return list(AGGREGATION_REGISTRY.keys())


def execute_aggregation_by_name(report_name: str, **kwargs: Any) -> Dict[str, Any]:
    """Dispatch and execute a registered aggregation report by name."""
    if report_name not in AGGREGATION_REGISTRY:
        raise ValueError(
            f"Unknown report_name: {report_name!r}. "
            f"Available reports: {list_available_aggregations()}"
        )
    return AGGREGATION_REGISTRY[report_name](**kwargs)
