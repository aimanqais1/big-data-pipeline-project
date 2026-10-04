"""
Phase 2 — Step 1B: Practical MongoDB Query Module.
Implements the 5 approved read-only analytical/operational queries on `orders_validated`:
1. orders_by_city_and_status
2. customer_order_history
3. high_value_orders_by_date_range
4. orders_by_product_sku
5. payment_settlement_audit

Design Principles:
- Reuses `get_database` from `src.mongo_setup` and configuration from `config.settings`.
- Strictly read-only (no inserts, updates, deletes, or index creation).
- Explicit parameters are preferred for production and API usage.
- Safe, dataset-independent dynamic fallbacks (including calendar-aware date bounds)
  are provided only when optional demonstration parameters are omitted (None).
- Returns a consistent JSON-serializable result dictionary for every query.
"""
import calendar
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from pymongo import DESCENDING
from pymongo.errors import PyMongoError

from config.settings import MONGO_DB_NAME, VALIDATED_COLLECTION
from src.mongo_setup import get_database

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Validation Helpers
# --------------------------------------------------------------------------

def _validate_limit(limit: int) -> int:
    """Ensure limit is a positive integer."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError(f"'limit' must be a positive integer, got: {limit!r}")
    return limit


def _validate_non_empty_string(val: Any, field_name: str) -> str:
    """Ensure an explicitly supplied string parameter is non-empty."""
    if not isinstance(val, str) or not val.strip():
        raise ValueError(f"Parameter '{field_name}' must be a non-empty string.")
    return val.strip()


def _validate_non_negative_number(val: Any, field_name: str) -> float:
    """Ensure a numeric threshold parameter is a valid non-negative number."""
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        raise ValueError(f"Parameter '{field_name}' must be a numeric value, got: {val!r}")
    num = float(val)
    if num < 0.0:
        raise ValueError(f"Parameter '{field_name}' must be >= 0.0, got: {num}")
    return num


def _parse_and_validate_iso_date(date_str: str, field_name: str) -> Tuple[str, datetime]:
    """
    Validate that a date string is a valid calendar ISO-8601 date or timestamp.
    Rejects impossible calendar dates (such as 2025-02-31).
    """
    cleaned = _validate_non_empty_string(date_str, field_name)
    try:
        parsed_dt = datetime.fromisoformat(cleaned)
    except ValueError as exc:
        raise ValueError(
            f"Invalid ISO-8601 date/timestamp for '{field_name}': {cleaned!r}. "
            f"Details: {exc}"
        ) from exc
    return cleaned, parsed_dt


def _derive_calendar_month_window(iso_date_str: str) -> Tuple[str, str]:
    """
    Given a valid ISO-8601 timestamp from `orders_validated`, compute a calendar-aware
    start_date and end_date covering that month using calendar.monthrange.
    """
    dt = datetime.fromisoformat(iso_date_str.strip())
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    start_iso = f"{dt.year:04d}-{dt.month:02d}-01T00:00:00"
    end_iso = f"{dt.year:04d}-{dt.month:02d}-{last_day:02d}T23:59:59"
    return start_iso, end_iso


def _format_query_response(
    query_name: str,
    parameters: Dict[str, Any],
    results: List[Dict[str, Any]],
    collection_name: str = VALIDATED_COLLECTION
) -> Dict[str, Any]:
    """Build the standardized, JSON-serializable query result contract."""
    return {
        "query_name": query_name,
        "collection": collection_name,
        "parameters": parameters,
        "count": len(results),
        "results": results,
    }


# --------------------------------------------------------------------------
# Approved Queries (QUERY 01 – QUERY 05)
# --------------------------------------------------------------------------

def orders_by_city_and_status(
    city: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 20,
    db_name: str = MONGO_DB_NAME
) -> Dict[str, Any]:
    """
    QUERY 01: Retrieve validated orders for a given city and status,
    ordered by newest order_date first.
    """
    validated_limit = _validate_limit(limit)
    db = get_database(db_name=db_name)
    col = db[VALIDATED_COLLECTION]

    resolved_city: Optional[str] = None
    resolved_status: Optional[str] = None

    if city is not None:
        resolved_city = _validate_non_empty_string(city, "city")
    if status is not None:
        resolved_status = _validate_non_empty_string(status, "status")

    if resolved_city is None or resolved_status is None:
        sample_filter: Dict[str, Any] = {}
        if resolved_city is not None:
            sample_filter["city"] = resolved_city
        if resolved_status is not None:
            sample_filter["status"] = resolved_status

        sample_doc = col.find_one(sample_filter, {"_id": 0, "city": 1, "status": 1})
        if not sample_doc:
            raise ValueError(
                "Could not resolve demonstration defaults for 'city'/'status' because "
                "no matching documents exist in orders_validated. Please provide explicit parameters."
            )
        if resolved_city is None:
            resolved_city = sample_doc["city"]
        if resolved_status is None:
            resolved_status = sample_doc["status"]

    query_filter = {
        "city": resolved_city,
        "status": resolved_status,
    }
    projection = {
        "_id": 0,
        "id_order": 1,
        "order_date": 1,
        "city": 1,
        "district": 1,
        "status": 1,
        "delivery_type": 1,
        "total_amount": 1,
        "quality_status": 1,
    }
    sort_spec = [("order_date", DESCENDING)]

    try:
        cursor = col.find(query_filter, projection).sort(sort_spec).limit(validated_limit)
        results = list(cursor)
    except PyMongoError as exc:
        logger.error(f"Database error in orders_by_city_and_status: {exc}")
        raise

    return _format_query_response(
        query_name="orders_by_city_and_status",
        parameters={"city": resolved_city, "status": resolved_status, "limit": validated_limit},
        results=results,
    )


def customer_order_history(
    customer_id: Optional[str] = None,
    limit: int = 20,
    db_name: str = MONGO_DB_NAME
) -> Dict[str, Any]:
    """
    QUERY 02: Retrieve validated orders for a specific customer,
    ordered by newest order_date first.
    """
    validated_limit = _validate_limit(limit)
    db = get_database(db_name=db_name)
    col = db[VALIDATED_COLLECTION]

    if customer_id is not None:
        resolved_customer_id = _validate_non_empty_string(customer_id, "customer_id")
    else:
        sample_doc = col.find_one({}, {"_id": 0, "customer_id": 1})
        if not sample_doc or not sample_doc.get("customer_id"):
            raise ValueError(
                "Could not resolve demonstration default for 'customer_id' from orders_validated. "
                "Please provide an explicit 'customer_id'."
            )
        resolved_customer_id = sample_doc["customer_id"]

    query_filter = {
        "customer_id": resolved_customer_id,
    }
    projection = {
        "_id": 0,
        "id_order": 1,
        "customer_id": 1,
        "customer_name": 1,
        "order_date": 1,
        "status": 1,
        "city": 1,
        "payment_status": 1,
        "total_amount": 1,
        "quality_status": 1,
    }
    sort_spec = [("order_date", DESCENDING)]

    try:
        cursor = col.find(query_filter, projection).sort(sort_spec).limit(validated_limit)
        results = list(cursor)
    except PyMongoError as exc:
        logger.error(f"Database error in customer_order_history: {exc}")
        raise

    return _format_query_response(
        query_name="customer_order_history",
        parameters={"customer_id": resolved_customer_id, "limit": validated_limit},
        results=results,
    )


def high_value_orders_by_date_range(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_total: Optional[float] = None,
    limit: int = 20,
    db_name: str = MONGO_DB_NAME
) -> Dict[str, Any]:
    """
    QUERY 03: Retrieve validated orders inside an explicit ISO-8601 date range
    where total_amount >= min_total, ordered by order_date descending.
    """
    validated_limit = _validate_limit(limit)
    db = get_database(db_name=db_name)
    col = db[VALIDATED_COLLECTION]

    resolved_start: Optional[str] = None
    resolved_end: Optional[str] = None
    resolved_min_total: Optional[float] = None

    if start_date is not None:
        resolved_start, _ = _parse_and_validate_iso_date(start_date, "start_date")
    if end_date is not None:
        resolved_end, _ = _parse_and_validate_iso_date(end_date, "end_date")
    if min_total is not None:
        resolved_min_total = _validate_non_negative_number(min_total, "min_total")

    if resolved_start is None or resolved_end is None or resolved_min_total is None:
        sample_doc = col.find_one({}, {"_id": 0, "order_date": 1, "total_amount": 1})
        if not sample_doc or not sample_doc.get("order_date"):
            raise ValueError(
                "Could not resolve demonstration defaults for high_value_orders_by_date_range. "
                "Please provide explicit 'start_date', 'end_date', and 'min_total'."
            )
        auto_start, auto_end = _derive_calendar_month_window(sample_doc["order_date"])
        if resolved_start is None:
            resolved_start = auto_start
        if resolved_end is None:
            resolved_end = auto_end
        if resolved_min_total is None:
            sample_amt = float(sample_doc.get("total_amount", 0.0))
            resolved_min_total = round(sample_amt * 0.5, 2)

    # Validate date range ordering on both parsed datetime objects and ISO strings
    _, dt_start = _parse_and_validate_iso_date(resolved_start, "start_date")
    _, dt_end = _parse_and_validate_iso_date(resolved_end, "end_date")
    if dt_start > dt_end or resolved_start > resolved_end:
        raise ValueError(
            f"Invalid date range: start_date ({resolved_start!r}) must be <= end_date ({resolved_end!r})."
        )

    query_filter = {
        "order_date": {
            "$gte": resolved_start,
            "$lte": resolved_end,
        },
        "total_amount": {
            "$gte": float(resolved_min_total),
        },
    }
    projection = {
        "_id": 0,
        "id_order": 1,
        "order_date": 1,
        "customer_id": 1,
        "city": 1,
        "status": 1,
        "payment_method": 1,
        "total_amount": 1,
    }
    sort_spec = [("order_date", DESCENDING)]

    try:
        cursor = col.find(query_filter, projection).sort(sort_spec).limit(validated_limit)
        results = list(cursor)
    except PyMongoError as exc:
        logger.error(f"Database error in high_value_orders_by_date_range: {exc}")
        raise

    return _format_query_response(
        query_name="high_value_orders_by_date_range",
        parameters={
            "start_date": resolved_start,
            "end_date": resolved_end,
            "min_total": float(resolved_min_total),
            "limit": validated_limit,
        },
        results=results,
    )


def orders_by_product_sku(
    sku: Optional[str] = None,
    limit: int = 20,
    db_name: str = MONGO_DB_NAME
) -> Dict[str, Any]:
    """
    QUERY 04: Retrieve validated orders containing a specific SKU
    inside items[], ordered by order_date descending.
    """
    validated_limit = _validate_limit(limit)
    db = get_database(db_name=db_name)
    col = db[VALIDATED_COLLECTION]

    if sku is not None:
        resolved_sku = _validate_non_empty_string(sku, "sku")
    else:
        sample_doc = col.find_one({"items.0": {"$exists": True}}, {"_id": 0, "items": 1})
        if not sample_doc or not sample_doc.get("items"):
            raise ValueError(
                "Could not resolve demonstration default for 'sku' from orders_validated. "
                "Please provide an explicit 'sku'."
            )
        resolved_sku = sample_doc["items"][0]["sku"]

    query_filter = {
        "items.sku": resolved_sku,
    }
    projection = {
        "_id": 0,
        "id_order": 1,
        "order_date": 1,
        "city": 1,
        "status": 1,
        "total_amount": 1,
        "items": 1,
    }
    sort_spec = [("order_date", DESCENDING)]

    try:
        cursor = col.find(query_filter, projection).sort(sort_spec).limit(validated_limit)
        results = list(cursor)
    except PyMongoError as exc:
        logger.error(f"Database error in orders_by_product_sku: {exc}")
        raise

    return _format_query_response(
        query_name="orders_by_product_sku",
        parameters={"sku": resolved_sku, "limit": validated_limit},
        results=results,
    )


def payment_settlement_audit(
    payment_status: Optional[str] = None,
    payment_method: Optional[str] = None,
    min_amount: float = 0.0,
    limit: int = 20,
    db_name: str = MONGO_DB_NAME
) -> Dict[str, Any]:
    """
    QUERY 05: Retrieve orders matching payment status, payment method,
    and minimum total amount, ordered by total_amount descending.
    """
    validated_limit = _validate_limit(limit)
    validated_min_amount = _validate_non_negative_number(min_amount, "min_amount")
    db = get_database(db_name=db_name)
    col = db[VALIDATED_COLLECTION]

    resolved_pay_status: Optional[str] = None
    resolved_pay_method: Optional[str] = None

    if payment_status is not None:
        resolved_pay_status = _validate_non_empty_string(payment_status, "payment_status")
    if payment_method is not None:
        resolved_pay_method = _validate_non_empty_string(payment_method, "payment_method")

    if resolved_pay_status is None or resolved_pay_method is None:
        sample_filter: Dict[str, Any] = {}
        if resolved_pay_status is not None:
            sample_filter["payment_status"] = resolved_pay_status
        if resolved_pay_method is not None:
            sample_filter["payment_method"] = resolved_pay_method

        sample_doc = col.find_one(
            sample_filter,
            {"_id": 0, "payment_status": 1, "payment_method": 1}
        )
        if not sample_doc:
            raise ValueError(
                "Could not resolve demonstration defaults for 'payment_status'/'payment_method' "
                "from orders_validated. Please provide explicit parameters."
            )
        if resolved_pay_status is None:
            resolved_pay_status = sample_doc["payment_status"]
        if resolved_pay_method is None:
            resolved_pay_method = sample_doc["payment_method"]

    query_filter = {
        "payment_status": resolved_pay_status,
        "payment_method": resolved_pay_method,
        "total_amount": {
            "$gte": float(validated_min_amount),
        },
    }
    projection = {
        "_id": 0,
        "id_order": 1,
        "order_date": 1,
        "customer_id": 1,
        "city": 1,
        "payment_method": 1,
        "payment_status": 1,
        "payment_amount": 1,
        "total_amount": 1,
        "corrections": 1,
    }
    sort_spec = [("total_amount", DESCENDING)]

    try:
        cursor = col.find(query_filter, projection).sort(sort_spec).limit(validated_limit)
        results = list(cursor)
    except PyMongoError as exc:
        logger.error(f"Database error in payment_settlement_audit: {exc}")
        raise

    return _format_query_response(
        query_name="payment_settlement_audit",
        parameters={
            "payment_status": resolved_pay_status,
            "payment_method": resolved_pay_method,
            "min_amount": float(validated_min_amount),
            "limit": validated_limit,
        },
        results=results,
    )


# --------------------------------------------------------------------------
# Registry & Dispatcher Helpers (Reusable by FastAPI, CLI, and Explain Runner)
# --------------------------------------------------------------------------

QUERY_REGISTRY: Dict[str, Dict[str, Any]] = {
    "orders_by_city_and_status": {
        "name": "orders_by_city_and_status",
        "description": "Retrieve validated orders for a given city and status, ordered by newest order_date first.",
        "parameters": ["city", "status", "limit"],
        "function": orders_by_city_and_status,
    },
    "customer_order_history": {
        "name": "customer_order_history",
        "description": "Retrieve validated orders for a specific customer_id, ordered by newest order_date first.",
        "parameters": ["customer_id", "limit"],
        "function": customer_order_history,
    },
    "high_value_orders_by_date_range": {
        "name": "high_value_orders_by_date_range",
        "description": "Retrieve validated orders inside an explicit ISO-8601 date range where total_amount >= min_total.",
        "parameters": ["start_date", "end_date", "min_total", "limit"],
        "function": high_value_orders_by_date_range,
    },
    "orders_by_product_sku": {
        "name": "orders_by_product_sku",
        "description": "Retrieve validated orders containing a specific product SKU inside items[].",
        "parameters": ["sku", "limit"],
        "function": orders_by_product_sku,
    },
    "payment_settlement_audit": {
        "name": "payment_settlement_audit",
        "description": "Retrieve validated orders matching payment_status, payment_method, and minimum total_amount.",
        "parameters": ["payment_status", "payment_method", "min_amount", "limit"],
        "function": payment_settlement_audit,
    },
}


def list_available_queries() -> List[Dict[str, Any]]:
    """Return metadata for all registered practical queries."""
    return [
        {
            "name": meta["name"],
            "description": meta["description"],
            "parameters": meta["parameters"],
        }
        for meta in QUERY_REGISTRY.values()
    ]


def execute_query_by_name(
    query_name: str,
    db_name: str = MONGO_DB_NAME,
    **kwargs: Any
) -> Dict[str, Any]:
    """Execute one of the 5 registered queries by its canonical name."""
    if query_name not in QUERY_REGISTRY:
        valid_names = ", ".join(QUERY_REGISTRY.keys())
        raise KeyError(f"Unknown query '{query_name}'. Available queries: {valid_names}")
    func = QUERY_REGISTRY[query_name]["function"]
    return func(db_name=db_name, **kwargs)
