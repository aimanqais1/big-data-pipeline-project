"""
Phase 2 — Step 5: Automated Test Suite for the Unified FastAPI Interface (`tests/test_fastapi_app.py`).

All database-backed tests run against an isolated temporary MongoDB database
(`test_phase2_fastapi_db`) via FastAPI dependency overrides and clean up after execution.
Never touches `midterm_ecommerce_30m_production` or mutates `midterm_ecommerce_100k_final`.
"""
import copy
from pathlib import Path
from typing import Generator

import pytest
from fastapi.testclient import TestClient
from pymongo.errors import PyMongoError

import src.api.routes as api_routes
import src.jobs.job_runner as job_runner_module
import src.main as main_pipeline_module
from config.settings import (
    DAILY_SALES_MV_COLLECTION,
    DATA_DIR,
    MONGO_DB_NAME,
    TOP_PRODUCTS_MV_COLLECTION,
    VALIDATED_COLLECTION,
)
from src.api.app import app, create_app
from src.api.routes import get_target_db_name
from src.mongo_setup import get_database, get_mongo_client, setup_mongodb_collections

TEST_DB_NAME = "test_phase2_fastapi_db"


@pytest.fixture(scope="module")
def isolated_api_client() -> Generator[TestClient, None, None]:
    """
    Provide a FastAPI TestClient wired to `test_phase2_fastapi_db` via dependency override,
    seeded with a read-only slice of 200 validated orders from `MONGO_DB_NAME`.
    """
    client = get_mongo_client()
    client.drop_database(TEST_DB_NAME)
    setup_mongodb_collections(db_name=TEST_DB_NAME, drop_existing=False)

    source_db = client[MONGO_DB_NAME]
    target_db = client[TEST_DB_NAME]

    sample_docs = list(source_db[VALIDATED_COLLECTION].find({}).limit(200))
    seed_docs = []
    for d in sample_docs:
        item = copy.deepcopy(d)
        item.pop("_id", None)
        seed_docs.append(item)

    assert len(seed_docs) > 0, "Expected validated documents in source database."
    target_db[VALIDATED_COLLECTION].insert_many(seed_docs)

    app.dependency_overrides[get_target_db_name] = lambda: TEST_DB_NAME
    try:
        with TestClient(app, raise_server_exceptions=False) as test_client:
            yield test_client
    finally:
        app.dependency_overrides.clear()
        client.drop_database(TEST_DB_NAME)


def test_health_endpoint_ok(isolated_api_client: TestClient) -> None:
    """1. GET /health returns 200, status='ok', mongodb='connected', and leaks no URI."""
    resp = isolated_api_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {
        "status": "ok",
        "database": TEST_DB_NAME,
        "mongodb": "connected",
    }
    assert "mongodb://" not in resp.text


def test_openapi_and_docs_available(isolated_api_client: TestClient) -> None:
    """2. GET /docs and GET /openapi.json return 200 and expose all 10 required routes."""
    docs_resp = isolated_api_client.get("/docs")
    assert docs_resp.status_code == 200
    assert "swagger-ui" in docs_resp.text.lower()

    openapi_resp = isolated_api_client.get("/openapi.json")
    assert openapi_resp.status_code == 200
    schema = openapi_resp.json()
    paths = set(schema.get("paths", {}).keys())
    expected_paths = {
        "/health",
        "/ingest",
        "/indexes",
        "/queries",
        "/queries/{name}",
        "/aggregations",
        "/aggregations/{name}",
        "/refresh-mv",
        "/jobs",
        "/jobs/{name}/run",
    }
    assert expected_paths.issubset(paths)


def test_list_queries_endpoint(isolated_api_client: TestClient) -> None:
    """3. GET /queries lists all 5 registered analytical queries."""
    resp = isolated_api_client.get("/queries")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 5
    names = {q["name"] for q in body["queries"]}
    assert names == {
        "orders_by_city_and_status",
        "customer_order_history",
        "high_value_orders_by_date_range",
        "orders_by_product_sku",
        "payment_settlement_audit",
    }


def test_execute_query_endpoint_success(isolated_api_client: TestClient) -> None:
    """4. GET /queries/{name} executes a registered query against the target DB."""
    db = get_database(db_name=TEST_DB_NAME)
    sample_doc = db[VALIDATED_COLLECTION].find_one({})
    assert sample_doc is not None

    resp = isolated_api_client.get(
        "/queries/orders_by_city_and_status",
        params={"city": sample_doc["city"], "status": sample_doc["status"], "limit": 5},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["query_name"] == "orders_by_city_and_status"
    assert body["count"] >= 1
    assert len(body["results"]) == body["count"]


def test_execute_query_unknown_returns_404(isolated_api_client: TestClient) -> None:
    """5. GET /queries/{name} returns 404 for unknown query names."""
    resp = isolated_api_client.get("/queries/nonexistent_query")
    assert resp.status_code == 404
    body = resp.json()
    assert body["status"] == "error"
    assert body["error_type"] == "NotFound"
    assert "Unknown query" in body["detail"]


def test_execute_query_rejects_nosql_injection(isolated_api_client: TestClient) -> None:
    """6. GET /queries/{name} rejects NoSQL operator injection tokens and unapproved params."""
    resp_op = isolated_api_client.get(
        "/queries/orders_by_city_and_status",
        params={"city": "$ne", "status": "DELIVERED"},
    )
    assert resp_op.status_code == 400
    assert resp_op.json()["error_type"] == "ValidationError"

    resp_where = isolated_api_client.get(
        "/queries/orders_by_city_and_status?$where=1"
    )
    assert resp_where.status_code == 400
    assert resp_where.json()["error_type"] == "ValidationError"

    resp_extra = isolated_api_client.get(
        "/queries/orders_by_city_and_status?city=Sanaa&db_name=other_db"
    )
    assert resp_extra.status_code == 400
    assert resp_extra.json()["error_type"] == "ValidationError"


def test_execute_query_invalid_limit_returns_422(isolated_api_client: TestClient) -> None:
    """7. GET /queries/{name} with out-of-bounds limit returns 422."""
    resp = isolated_api_client.get(
        "/queries/orders_by_city_and_status",
        params={"city": "Sanaa", "status": "DELIVERED", "limit": 999999},
    )
    assert resp.status_code == 422


def test_list_aggregations_endpoint(isolated_api_client: TestClient) -> None:
    """8. GET /aggregations lists all 5 registered aggregation reports."""
    resp = isolated_api_client.get("/aggregations")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 5
    assert set(body["aggregations"]) == {
        "daily_sales_summary",
        "sales_by_city",
        "payment_method_analysis",
        "order_status_distribution",
        "top_products",
    }


def test_execute_aggregation_endpoint_success(isolated_api_client: TestClient) -> None:
    """9. GET /aggregations/{name} executes registered aggregation reports."""
    resp_city = isolated_api_client.get("/aggregations/sales_by_city")
    assert resp_city.status_code == 200
    body_city = resp_city.json()
    assert body_city["report_name"] == "sales_by_city"
    assert body_city["count"] >= 1

    resp_top = isolated_api_client.get("/aggregations/top_products", params={"limit": 3})
    assert resp_top.status_code == 200
    body_top = resp_top.json()
    assert body_top["report_name"] == "top_products"
    assert 1 <= body_top["count"] <= 3


def test_execute_aggregation_unknown_returns_404(isolated_api_client: TestClient) -> None:
    """10. GET /aggregations/{name} returns 404 for unknown report names."""
    resp = isolated_api_client.get("/aggregations/nonexistent_report")
    assert resp.status_code == 404
    body = resp.json()
    assert body["status"] == "error"
    assert body["error_type"] == "NotFound"


def test_indexes_endpoint_ensures_three_phase2_indexes(isolated_api_client: TestClient) -> None:
    """11. POST /indexes ensures the 3 approved Phase 2 analytical indexes."""
    resp = isolated_api_client.post("/indexes")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "SUCCESS"
    assert body["database"] == TEST_DB_NAME
    assert set(body["created_indexes"]) == {
        "idx_city_status_date",
        "idx_customer_date",
        "idx_order_date_desc",
    }


def test_refresh_mv_endpoint_incremental_only(isolated_api_client: TestClient) -> None:
    """
    12. POST /refresh-mv is incremental-only on the public API:
    - No body -> incremental (200)
    - {"mode": "incremental"} -> incremental (200)
    - {"mode": "full"} -> rejected with 422
    """
    resp_no_body = isolated_api_client.post("/refresh-mv")
    assert resp_no_body.status_code == 200
    body_no_body = resp_no_body.json()
    assert body_no_body["status"] in ("SUCCESS", "NO_OP")
    assert body_no_body["mode_requested"] == "incremental"
    assert body_no_body["database"] == TEST_DB_NAME

    resp_inc = isolated_api_client.post("/refresh-mv", json={"mode": "incremental"})
    assert resp_inc.status_code == 200
    body_inc = resp_inc.json()
    assert body_inc["status"] in ("SUCCESS", "NO_OP")
    assert body_inc["mode_requested"] == "incremental"
    assert body_inc["database"] == TEST_DB_NAME

    db = get_database(db_name=TEST_DB_NAME)
    assert db[DAILY_SALES_MV_COLLECTION].count_documents({}) >= 1
    assert db[TOP_PRODUCTS_MV_COLLECTION].count_documents({}) >= 1

    resp_full = isolated_api_client.post("/refresh-mv", json={"mode": "full"})
    assert resp_full.status_code == 422

    resp_bad = isolated_api_client.post("/refresh-mv", json={"mode": "drop_all"})
    assert resp_bad.status_code == 422


def test_list_jobs_endpoint(isolated_api_client: TestClient) -> None:
    """13. GET /jobs returns both registered Phase 2 scheduled jobs."""
    resp = isolated_api_client.get("/jobs")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 2
    job_names = {j["name"] for j in body["jobs"]}
    assert job_names == {"refresh_materialized_views", "generate_aggregation_report"}


def test_run_job_endpoint_success_and_unknown_404(
    isolated_api_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """14. POST /jobs/{name}/run runs registered jobs and returns 404 for unknown jobs."""
    monkeypatch.setattr(job_runner_module, "REPORTS_DIR", tmp_path)

    resp_mv = isolated_api_client.post("/jobs/refresh_materialized_views/run")
    assert resp_mv.status_code == 200
    assert resp_mv.json()["status"] == "SUCCESS"
    assert resp_mv.json()["database"] == TEST_DB_NAME

    resp_agg = isolated_api_client.post("/jobs/generate_aggregation_report/run")
    assert resp_agg.status_code == 200
    assert resp_agg.json()["status"] == "SUCCESS"
    assert (tmp_path / "scheduled_aggregation_report.json").exists()

    resp_unknown = isolated_api_client.post("/jobs/unknown_job_name/run")
    assert resp_unknown.status_code == 404
    assert resp_unknown.json()["error_type"] == "NotFound"


def test_ingest_endpoint_delegates_to_run_pipeline_with_reset_false(
    isolated_api_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    15. POST /ingest delegates directly to `src.main.run_pipeline` with `reset_db=False`,
    and forbids `reset_db` or `db_name` in request payload (`extra='forbid'`).
    """
    temp_csv = DATA_DIR / "_temp_api_ingest_test.csv"
    temp_csv.write_text(
        "order_id,order_date,status,customer_id,customer_name,customer_phone,customer_email,city,district,delivery_type,delivery_cost,payment_method,payment_status,payment_amount,currency,total_amount,items_json\n"
        'ORD-API-ING-9901,2025-03-15T12:00:00Z,DELIVERED,CUST-API-9901,Ahmad Ali,771234567,ahmad@example.com,Sanaa,Hadda,Express,2000.0,Wallet,PAID,32000.0,YER,32000.0,"[{""sku"":""SKU-1009"",""name"":""Fast Charger"",""qty"":3,""unit_price"":10000.0,""total"":30000.0}]"\n',
        encoding="utf-8",
    )

    recorded_calls = []
    real_run_pipeline = main_pipeline_module.run_pipeline

    def spy_run_pipeline(*args, **kwargs):
        recorded_calls.append(kwargs.copy())
        return real_run_pipeline(*args, **kwargs)

    # Prevent overwriting reports/results.json during test
    monkeypatch.setattr(main_pipeline_module, "save_pipeline_metrics", lambda m: None)
    monkeypatch.setattr(main_pipeline_module, "run_pipeline", spy_run_pipeline)

    try:
        resp = isolated_api_client.post(
            "/ingest",
            json={
                "file_path": "data/_temp_api_ingest_test.csv",
                "custom_run_id": "api_test_run_01",
                "batch_size": 500,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "SUCCESS"
        assert body["database"] == TEST_DB_NAME
        assert body["reset_db"] is False
        assert body["ingestion_metrics"]["read_rows"] == 1

        assert len(recorded_calls) == 1
        assert recorded_calls[0]["reset_db"] is False
        assert recorded_calls[0]["db_name"] == TEST_DB_NAME
        assert recorded_calls[0]["batch_size"] == 500

        # Verify caller cannot pass reset_db or db_name (rejected by extra="forbid")
        resp_forbid_reset = isolated_api_client.post(
            "/ingest",
            json={"file_path": "data/_temp_api_ingest_test.csv", "reset_db": True},
        )
        assert resp_forbid_reset.status_code == 422

        resp_forbid_db = isolated_api_client.post(
            "/ingest",
            json={"file_path": "data/_temp_api_ingest_test.csv", "db_name": "other_db"},
        )
        assert resp_forbid_db.status_code == 422
    finally:
        if temp_csv.exists():
            temp_csv.unlink()


def test_ingest_endpoint_allows_large_csv_over_200mb_to_reach_run_pipeline(
    isolated_api_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    16. A legitimate CSV >200 MB inside `data/` is NOT rejected by FastAPI;
    it reaches `src.main.run_pipeline` with `reset_db=False` so Phase 1's
    `inspect_and_route` can route it to PySpark.
    Uses a mocked `run_pipeline` and a temporary sparse file (no actual Spark ingestion).
    """
    import os

    temp_large_csv = DATA_DIR / "_temp_api_large_205mb_test.csv"
    with open(temp_large_csv, "wb") as f:
        os.truncate(f.fileno(), 205 * 1024 * 1024)

    recorded_calls = []

    def mock_run_pipeline(*args, **kwargs):
        recorded_calls.append(kwargs.copy())
        return {
            "run_id": kwargs.get("custom_run_id") or "mock_spark_run",
            "database_name": kwargs.get("db_name"),
            "file_name": temp_large_csv.name,
            "file_size_mb": 205.0,
            "threshold_mb": 200.0,
            "used_engine": "pyspark",
            "read_rows": 0,
            "status": "SUCCESS",
        }

    monkeypatch.setattr(main_pipeline_module, "run_pipeline", mock_run_pipeline)

    try:
        assert temp_large_csv.stat().st_size > 200 * 1024 * 1024
        resp = isolated_api_client.post(
            "/ingest",
            json={
                "file_path": "data/_temp_api_large_205mb_test.csv",
                "custom_run_id": "spark_routing_verify_01",
                "batch_size": 10000,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "SUCCESS"
        assert body["reset_db"] is False
        assert body["ingestion_metrics"]["used_engine"] == "pyspark"

        assert len(recorded_calls) == 1
        assert recorded_calls[0]["reset_db"] is False
        assert recorded_calls[0]["db_name"] == TEST_DB_NAME
        assert Path(recorded_calls[0]["file_path"]) == temp_large_csv.resolve()
    finally:
        if temp_large_csv.exists():
            temp_large_csv.unlink()


def test_ingest_endpoint_rejects_path_traversal_outside_data_dir_and_non_csv(
    isolated_api_client: TestClient,
) -> None:
    """
    17. POST /ingest rejects path traversal (`..`), paths outside `DATA_DIR`,
    and non-CSV extensions.
    """
    bad_payloads = [
        {"file_path": "../config/settings.py"},
        {"file_path": "C:/Windows/win.ini"},
        {"file_path": "data/notes.txt"},
    ]
    for payload in bad_payloads:
        resp = isolated_api_client.post("/ingest", json=payload)
        assert resp.status_code == 400, f"Expected 400 for payload {payload}, got {resp.status_code}"
        body = resp.json()
        assert body["status"] == "error"
        assert body["error_type"] == "ValidationError"


def test_database_error_returns_sanitized_503(
    isolated_api_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    17. Simulated PyMongoError returns 503 with sanitized JSON and never exposes
    tracebacks or connection URIs.
    """
    def broken_index_call(*args, **kwargs):
        raise PyMongoError("Connection refused to mongodb://secret_user:secret_pass@127.0.0.1:27018")

    monkeypatch.setattr(api_routes.index_manager_module, "create_phase2_indexes", broken_index_call)

    resp = isolated_api_client.post("/indexes")
    assert resp.status_code == 503
    body = resp.json()
    assert body == {
        "status": "error",
        "error_type": "DatabaseUnavailable",
        "detail": "Database operation failed or MongoDB service is temporarily unavailable.",
    }
    assert "secret_pass" not in resp.text
    assert "Traceback" not in resp.text


def test_create_app_is_deterministic_and_protect_30m_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `create_app()` does not start background threads and blocks 30M DB target."""
    fresh_app = create_app()
    assert fresh_app.title == "Big Data E-Commerce Pipeline — Unified API"

    monkeypatch.setattr(api_routes, "MONGO_DB_NAME", "midterm_ecommerce_30m_production")
    with pytest.raises(PermissionError, match="midterm_ecommerce_30m_production"):
        api_routes.get_target_db_name()
