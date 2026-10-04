"""
Phase 2 — Step 5: Unified FastAPI Route Definitions (`src/api/routes.py`).

Implements the 10 required endpoints by delegating directly to the existing
Phase 1 and Phase 2 modules:
1. GET  /health
2. POST /ingest
3. POST /indexes
4. GET  /queries
5. GET  /queries/{name}
6. GET  /aggregations
7. GET  /aggregations/{name}
8. POST /refresh-mv
9. GET  /jobs
10. POST /jobs/{name}/run
"""
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Depends, Query, Request
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError

import src.aggregations.materialized_views as mv_module
import src.aggregations.reports as reports_module
import src.jobs.job_runner as job_runner_module
import src.main as main_pipeline_module
import src.mongo_setup as mongo_setup_module
import src.queries.index_manager as index_manager_module
import src.queries.order_queries as order_queries_module
from config.settings import (
    BASE_DIR,
    DATA_DIR,
    MONGO_DB_NAME,
)
from src.api.schemas import HealthResponse, IngestRequest, RefreshMVRequest

logger = logging.getLogger("src.api")

router = APIRouter()

PROTECTED_30M_DB = "midterm_ecommerce_30m_production"
FORBIDDEN_OPERATOR_TOKENS = ("$", "{", "}", "$where", "$gt", "$lt", "$ne", "$in", "$nin", "$regex")


# --------------------------------------------------------------------------
# Dependencies & Security Validators
# --------------------------------------------------------------------------

def get_target_db_name() -> str:
    """
    Server-side database target dependency.
    Defaults to `MONGO_DB_NAME` (`midterm_ecommerce_100k_final`) and strictly forbids
    targeting the protected 30M production database.
    """
    db_name = MONGO_DB_NAME
    if db_name == PROTECTED_30M_DB:
        raise PermissionError(
            f"Targeting protected production database '{PROTECTED_30M_DB}' is forbidden."
        )
    return db_name


def _validate_safe_scalar_string(value: Optional[str], param_name: str) -> Optional[str]:
    """
    Ensure a user-supplied string query parameter does not contain NoSQL operator
    injection tokens (such as `$where`, `$ne`, `{`, `}`).
    """
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"Query parameter '{param_name}' must not be empty.")
    if cleaned.startswith("$") or "{" in cleaned or "}" in cleaned:
        raise ValueError(
            f"Invalid characters or MongoDB operator syntax in parameter '{param_name}'."
        )
    for tok in FORBIDDEN_OPERATOR_TOKENS:
        if tok in cleaned:
            raise ValueError(
                f"Forbidden operator token in parameter '{param_name}'."
            )
    return cleaned


def _reject_unapproved_query_params(request: Request, allowed_params: set) -> None:
    """
    Reject any request that supplies unknown or operator-injected query parameter keys
    (e.g., `?$where=...` or `?db_name=...`).
    """
    for key in request.query_params.keys():
        if key.startswith("$") or "{" in key or "}" in key:
            raise ValueError(f"Forbidden query parameter key: {key!r}")
        if key not in allowed_params:
            raise ValueError(
                f"Unsupported query parameter '{key}' for this endpoint. "
                f"Allowed parameters: {sorted(allowed_params)}"
            )


def _validate_and_resolve_ingest_path(raw_file_path: str) -> Path:
    """
    Validate and resolve `file_path` for `POST /ingest`:
    1. Rejects null bytes and path traversal (`..`).
    2. Resolves path and enforces containment within `DATA_DIR` (`D:\\Big Data\\data`).
    3. Enforces `.csv` extension.
    4. Verifies the file exists and is a regular file.
    Note: Does NOT impose a 200 MB file-size cap; Phase 1's `inspect_and_route`
    handles engine selection (`<= 200 MB` -> Python Batch, `> 200 MB` -> PySpark).
    """
    if not isinstance(raw_file_path, str) or not raw_file_path.strip():
        raise ValueError("'file_path' must be a non-empty string.")
    if "\x00" in raw_file_path:
        raise ValueError("Invalid null byte in 'file_path'.")

    raw_path = Path(raw_file_path.strip())
    if ".." in raw_path.parts:
        raise ValueError("Path traversal ('..') is not allowed in 'file_path'.")

    data_dir_resolved = DATA_DIR.resolve()
    base_dir_resolved = BASE_DIR.resolve()

    if raw_path.is_absolute():
        candidate = raw_path.resolve()
    else:
        # Support either "data/sample.csv" or "sample.csv" inside DATA_DIR
        if raw_path.parts and raw_path.parts[0].lower() == "data":
            candidate = (base_dir_resolved / raw_path).resolve()
        else:
            candidate = (data_dir_resolved / raw_path).resolve()

    if not candidate.is_relative_to(data_dir_resolved):
        raise ValueError(
            "Security violation: 'file_path' must reside inside the project 'data/' directory."
        )

    if candidate.suffix.lower() != ".csv":
        raise ValueError("Only '.csv' files inside 'data/' are permitted for ingestion.")

    if not candidate.exists():
        raise FileNotFoundError(f"CSV file not found in data directory: {candidate.name}")

    if not candidate.is_file():
        raise ValueError(f"Specified path is not a regular file: {candidate.name}")

    return candidate


# --------------------------------------------------------------------------
# 1. GET /health
# --------------------------------------------------------------------------

@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Check API and MongoDB connectivity health",
    tags=["System"],
)
def health_check(db_name: str = Depends(get_target_db_name)) -> Any:
    """
    Confirm API availability and verify active MongoDB connectivity without exposing
    credentials or connection strings.
    """
    try:
        client = mongo_setup_module.get_mongo_client()
        is_ok = mongo_setup_module.verify_connection(client)
    except Exception as exc:
        logger.error("Health check connection error: %s", exc)
        is_ok = False

    if not is_ok:
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "database": db_name,
                "mongodb": "disconnected",
                "error_type": "DatabaseUnavailable",
                "detail": "MongoDB server is unreachable.",
            },
        )

    return {
        "status": "ok",
        "database": db_name,
        "mongodb": "connected",
    }


# --------------------------------------------------------------------------
# 2. POST /ingest
# --------------------------------------------------------------------------

@router.post(
    "/ingest",
    summary="Execute Phase 1 ingestion pipeline on a validated CSV file in data/",
    tags=["Ingestion"],
)
def ingest_csv(
    payload: IngestRequest,
    db_name: str = Depends(get_target_db_name),
) -> Dict[str, Any]:
    """
    Validate request and delegate directly to the existing Phase 1 ingestion gateway
    (`src.main.run_pipeline`) with `reset_db=False`.
    """
    safe_csv_path = _validate_and_resolve_ingest_path(payload.file_path)
    if payload.custom_run_id is not None:
        _validate_safe_scalar_string(payload.custom_run_id, "custom_run_id")

    metrics = main_pipeline_module.run_pipeline(
        file_path=str(safe_csv_path),
        custom_run_id=payload.custom_run_id,
        batch_size=payload.batch_size,
        reset_db=False,
        db_name=db_name,
    )

    return {
        "status": "SUCCESS",
        "database": db_name,
        "reset_db": False,
        "ingestion_metrics": metrics,
    }


# --------------------------------------------------------------------------
# 3. POST /indexes
# --------------------------------------------------------------------------

@router.post(
    "/indexes",
    summary="Ensure the 3 approved Phase 2 analytical indexes on orders_validated",
    tags=["Indexes"],
)
def ensure_indexes(db_name: str = Depends(get_target_db_name)) -> Dict[str, Any]:
    """
    Delegate directly to `src.queries.index_manager.create_phase2_indexes`.
    Idempotently ensures `idx_city_status_date`, `idx_customer_date`, and `idx_order_date_desc`.
    """
    res = index_manager_module.create_phase2_indexes(db_name=db_name)
    return {
        "status": "SUCCESS",
        **res,
    }


# --------------------------------------------------------------------------
# 4. GET /queries
# --------------------------------------------------------------------------

@router.get(
    "/queries",
    summary="List all registered Phase 2 analytical queries",
    tags=["Queries"],
)
def get_available_queries() -> Dict[str, Any]:
    """Return available query definitions from `src.queries.order_queries.list_available_queries`."""
    queries = order_queries_module.list_available_queries()
    return {
        "count": len(queries),
        "queries": queries,
    }


# --------------------------------------------------------------------------
# 5. GET /queries/{name}
# --------------------------------------------------------------------------

@router.get(
    "/queries/{name}",
    summary="Execute a registered Phase 2 query by name",
    tags=["Queries"],
)
def run_registered_query(
    name: str,
    request: Request,
    city: Optional[str] = Query(default=None, description="City filter (orders_by_city_and_status)."),
    status: Optional[str] = Query(default=None, description="Order status filter (orders_by_city_and_status)."),
    customer_id: Optional[str] = Query(default=None, description="Customer ID filter (customer_order_history)."),
    start_date: Optional[str] = Query(default=None, description="Start ISO-8601 date/timestamp (high_value_orders_by_date_range)."),
    end_date: Optional[str] = Query(default=None, description="End ISO-8601 date/timestamp (high_value_orders_by_date_range)."),
    min_total: Optional[float] = Query(default=None, ge=0.0, description="Minimum total_amount (high_value_orders_by_date_range)."),
    sku: Optional[str] = Query(default=None, description="Product SKU filter (orders_by_product_sku)."),
    payment_status: Optional[str] = Query(default=None, description="Payment status filter (payment_settlement_audit)."),
    payment_method: Optional[str] = Query(default=None, description="Payment method filter (payment_settlement_audit)."),
    min_amount: Optional[float] = Query(default=None, ge=0.0, description="Minimum total_amount filter (payment_settlement_audit)."),
    limit: int = Query(default=20, ge=1, le=500, description="Maximum number of documents to return (1..500)."),
    db_name: str = Depends(get_target_db_name),
) -> Dict[str, Any]:
    """
    Execute one registered query from `QUERY_REGISTRY` by delegating to
    `src.queries.order_queries.execute_query_by_name`.
    Rejects unknown query names, unapproved parameters, and MongoDB operator injection.
    """
    clean_name = _validate_safe_scalar_string(name, "name")
    if clean_name not in order_queries_module.QUERY_REGISTRY:
        valid_names = ", ".join(order_queries_module.QUERY_REGISTRY.keys())
        raise KeyError(f"Unknown query '{clean_name}'. Available queries: {valid_names}")

    allowed_for_query = set(order_queries_module.QUERY_REGISTRY[clean_name]["parameters"])
    _reject_unapproved_query_params(request, allowed_for_query)

    candidate_params: Dict[str, Any] = {
        "city": _validate_safe_scalar_string(city, "city"),
        "status": _validate_safe_scalar_string(status, "status"),
        "customer_id": _validate_safe_scalar_string(customer_id, "customer_id"),
        "start_date": _validate_safe_scalar_string(start_date, "start_date"),
        "end_date": _validate_safe_scalar_string(end_date, "end_date"),
        "min_total": min_total,
        "sku": _validate_safe_scalar_string(sku, "sku"),
        "payment_status": _validate_safe_scalar_string(payment_status, "payment_status"),
        "payment_method": _validate_safe_scalar_string(payment_method, "payment_method"),
        "min_amount": min_amount,
        "limit": limit,
    }

    filtered_kwargs = {
        k: v
        for k, v in candidate_params.items()
        if k in allowed_for_query and v is not None
    }

    return order_queries_module.execute_query_by_name(
        clean_name,
        db_name=db_name,
        **filtered_kwargs,
    )


# --------------------------------------------------------------------------
# 6. GET /aggregations
# --------------------------------------------------------------------------

@router.get(
    "/aggregations",
    summary="List all registered Phase 2 aggregation reports",
    tags=["Aggregations"],
)
def get_available_aggregations() -> Dict[str, Any]:
    """Return available aggregation report names from `src.aggregations.reports.list_available_aggregations`."""
    report_names = reports_module.list_available_aggregations()
    return {
        "count": len(report_names),
        "aggregations": report_names,
    }


# --------------------------------------------------------------------------
# 7. GET /aggregations/{name}
# --------------------------------------------------------------------------

AGGREGATION_ALLOWED_PARAMS: Dict[str, set] = {
    "daily_sales_summary": {"start_date", "end_date", "limit"},
    "sales_by_city": set(),
    "payment_method_analysis": set(),
    "order_status_distribution": set(),
    "top_products": {"limit"},
}


@router.get(
    "/aggregations/{name}",
    summary="Execute a registered Phase 2 aggregation report by name",
    tags=["Aggregations"],
)
def run_registered_aggregation(
    name: str,
    request: Request,
    start_date: Optional[str] = Query(default=None, description="Start ISO-8601 date (daily_sales_summary)."),
    end_date: Optional[str] = Query(default=None, description="End ISO-8601 date (daily_sales_summary)."),
    limit: Optional[int] = Query(default=None, ge=1, le=500, description="Result limit (daily_sales_summary / top_products)."),
    db_name: str = Depends(get_target_db_name),
) -> Dict[str, Any]:
    """
    Execute one registered aggregation report by delegating to
    `src.aggregations.reports.execute_aggregation_by_name`.
    """
    clean_name = _validate_safe_scalar_string(name, "name")
    if clean_name not in reports_module.AGGREGATION_REGISTRY:
        valid_names = ", ".join(reports_module.list_available_aggregations())
        raise KeyError(
            f"Unknown aggregation report '{clean_name}'. Available reports: {valid_names}"
        )

    allowed_params = AGGREGATION_ALLOWED_PARAMS.get(clean_name, set())
    _reject_unapproved_query_params(request, allowed_params)

    call_kwargs: Dict[str, Any] = {"db_name": db_name}
    if "start_date" in allowed_params and start_date is not None:
        call_kwargs["start_date"] = _validate_safe_scalar_string(start_date, "start_date")
    if "end_date" in allowed_params and end_date is not None:
        call_kwargs["end_date"] = _validate_safe_scalar_string(end_date, "end_date")
    if "limit" in allowed_params and limit is not None:
        call_kwargs["limit"] = limit

    return reports_module.execute_aggregation_by_name(clean_name, **call_kwargs)


# --------------------------------------------------------------------------
# 8. POST /refresh-mv
# --------------------------------------------------------------------------

@router.post(
    "/refresh-mv",
    summary="Trigger incremental Materialized View refresh",
    tags=["Materialized Views"],
)
def refresh_materialized_views_endpoint(
    payload: Optional[RefreshMVRequest] = Body(default=None),
    db_name: str = Depends(get_target_db_name),
) -> Dict[str, Any]:
    """
    Trigger Materialized View refresh by delegating directly to
    `src.aggregations.materialized_views.refresh_all_materialized_views`
    with `mode='incremental'` (public API is incremental-only).
    """
    _ = payload
    return mv_module.refresh_all_materialized_views(
        mode="incremental",
        db_name=db_name,
    )


# --------------------------------------------------------------------------
# 9. GET /jobs
# --------------------------------------------------------------------------

@router.get(
    "/jobs",
    summary="List all registered Phase 2 scheduled jobs and schedules",
    tags=["Scheduled Jobs"],
)
def get_registered_jobs() -> Dict[str, Any]:
    """Return all registered scheduled jobs from `src.jobs.job_runner.list_jobs`."""
    jobs = job_runner_module.list_jobs()
    return {
        "count": len(jobs),
        "jobs": jobs,
    }


# --------------------------------------------------------------------------
# 10. POST /jobs/{name}/run
# --------------------------------------------------------------------------

@router.post(
    "/jobs/{name}/run",
    summary="Manually execute a registered scheduled job by name",
    tags=["Scheduled Jobs"],
)
def run_registered_job_endpoint(
    name: str,
    db_name: str = Depends(get_target_db_name),
) -> Dict[str, Any]:
    """
    Execute one registered scheduled job by delegating directly to
    `src.jobs.job_runner.run_job`.
    """
    clean_name = _validate_safe_scalar_string(name, "name")
    if clean_name not in job_runner_module.JOBS:
        valid_names = ", ".join(job_runner_module.JOBS.keys())
        raise KeyError(
            f"Unknown scheduled job '{clean_name}'. Available jobs: {valid_names}"
        )

    return job_runner_module.run_job(clean_name, db_name=db_name, raise_on_error=True)
