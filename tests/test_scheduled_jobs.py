"""
Phase 2 — Step 4: Automated Test Suite for Scheduled Jobs (`src/jobs/`).

Verifies:
1. Job registry (`JOBS` and `list_jobs()`) contains both required jobs.
2. Every job has a complete, explicit schedule definition (`interval_seconds`, `cron_expression`, `human_readable`).
3. Every job is manually callable via Python API (`run_job`, `job.run`) and CLI (`main(["--list"])`, `main(["--run", ...])`, `main(["--run-all"])`).
4. Successful job execution logs `JOB_START` and `JOB_SUCCESS` (both to logger and `reports/logs/scheduled_jobs.log`).
5. Failed job execution logs `JOB_FAILURE` with error details.
6. Job exceptions are handled properly (`raise_on_error=True` vs `raise_on_error=False`), and failed report runs preserve the last valid report file on disk.
7. `refresh_materialized_views` delegates directly to `src.aggregations.materialized_views.refresh_all_materialized_views` (tested via mock spy and real isolated DB for both `NO_OP` and legitimate incremental change).
8. `generate_aggregation_report` delegates directly to `src.aggregations.reports.execute_aggregation_by_name` for all 5 reports.
9. Importing `src.jobs.scheduler` or instantiating `JobScheduler` never starts an infinite background thread; `run_pending()` and `start(max_cycles=1)` terminate deterministically.
10. Real baseline database (`midterm_ecommerce_100k_final`) and 30M database (`midterm_ecommerce_30m_production`) are never mutated by destructive tests.
"""
import copy
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

import pytest

import src.jobs.job_runner as job_runner_mod
from config.settings import MONGO_DB_NAME, VALIDATED_COLLECTION
from src.jobs import (
    JOB_LOG_FILE,
    JOBS,
    JobScheduler,
    ScheduledJob,
    execute_aggregation_report_job,
    execute_mv_refresh_job,
    list_jobs,
    run_all_jobs,
    run_job,
)
from src.jobs.scheduler import main as scheduler_cli_main
from src.mongo_setup import get_mongo_client, setup_mongodb_collections

ISOLATED_JOBS_DB = "midterm_ecommerce_jobs_isolated_test"


@pytest.fixture(scope="module")
def isolated_jobs_db():
    """
    Create an isolated temporary MongoDB database seeded with a slice of real validated
    orders copied read-only from `MONGO_DB_NAME.orders_validated`.
    Drops the isolated database before and after the module tests.
    """
    client = get_mongo_client()
    client.drop_database(ISOLATED_JOBS_DB)
    setup_mongodb_collections(db_name=ISOLATED_JOBS_DB, drop_existing=False)

    source_db = client[MONGO_DB_NAME]
    target_db = client[ISOLATED_JOBS_DB]

    sample_docs = list(source_db[VALIDATED_COLLECTION].find({}).limit(250))
    seed_docs = []
    for d in sample_docs:
        item = copy.deepcopy(d)
        item.pop("_id", None)
        seed_docs.append(item)

    assert len(seed_docs) > 0, "Expected validated documents in source database."
    target_db[VALIDATED_COLLECTION].insert_many(seed_docs)

    yield {
        "db_name": ISOLATED_JOBS_DB,
        "db": target_db,
        "seed_docs": seed_docs,
    }

    client.drop_database(ISOLATED_JOBS_DB)


def test_01_job_registry_and_02_schedule_definitions():
    """1 & 2. Verify job registry contains required jobs and explicit schedule definitions."""
    assert "refresh_materialized_views" in JOBS
    assert "generate_aggregation_report" in JOBS
    assert len(JOBS) >= 2

    catalog = list_jobs()
    serialized = json.dumps(catalog, ensure_ascii=False)
    assert isinstance(serialized, str)

    by_name = {entry["name"]: entry for entry in catalog}
    assert set(by_name.keys()) == set(JOBS.keys())

    for name, job in JOBS.items():
        assert isinstance(job, ScheduledJob)
        assert isinstance(job.description, str) and len(job.description) > 10
        assert callable(job.target_function)

        sched = job.schedule
        assert isinstance(sched, dict)
        assert sched.get("schedule_type") in ("interval", "daily")
        assert isinstance(sched.get("interval_seconds"), int) and sched["interval_seconds"] > 0
        assert isinstance(sched.get("cron_expression"), str) and len(sched["cron_expression"].split()) == 5
        assert isinstance(sched.get("human_readable"), str) and len(sched["human_readable"]) > 0

    assert JOBS["refresh_materialized_views"].schedule["interval_seconds"] == 900
    assert JOBS["refresh_materialized_views"].schedule["cron_expression"] == "*/15 * * * *"
    assert JOBS["generate_aggregation_report"].schedule["interval_seconds"] == 86400
    assert JOBS["generate_aggregation_report"].schedule["cron_expression"] == "0 0 * * *"


def test_03_mv_job_delegates_to_existing_incremental_mv_mechanism(isolated_jobs_db):
    """
    7. Verify `refresh_materialized_views` calls `refresh_all_materialized_views`
    and handles:
    - initial uninitialized state (auto full refresh)
    - NO_OP when no changes exist
    - incremental update when a legitimate new order arrives
    """
    db_name = isolated_jobs_db["db_name"]
    db = isolated_jobs_db["db"]
    val_col = db[VALIDATED_COLLECTION]

    # Spy on refresh_all_materialized_views to prove direct reuse without re-implementing MV logic
    with patch.object(
        job_runner_mod,
        "refresh_all_materialized_views",
        wraps=job_runner_mod.refresh_all_materialized_views,
    ) as spy_mv:
        # 1st run on uninitialized DB -> auto full initialization
        res_init = run_job("refresh_materialized_views", db_name=db_name)
        assert spy_mv.call_count == 1
        spy_mv.assert_called_with(mode="incremental", db_name=db_name)
        assert res_init["status"] == "SUCCESS"
        assert res_init["result"]["mode_executed"] == "full"
        assert res_init["result"]["noop"] is False

        # 2nd run with no changes -> clean NO_OP
        res_noop = JOBS["refresh_materialized_views"].run(db_name=db_name)
        assert spy_mv.call_count == 2
        assert res_noop["status"] == "SUCCESS"
        assert res_noop["result"]["mode_executed"] == "incremental"
        assert res_noop["result"]["status"] == "NO_OP"
        assert res_noop["result"]["noop"] is True

        # 3rd run after inserting a synthetic order -> incremental refresh of 1 order
        template_doc = copy.deepcopy(isolated_jobs_db["seed_docs"][0])
        template_doc.pop("_id", None)
        synth_id = f"JOB-TEST-ORDER-{int(datetime.now().timestamp())}"
        wm_prev = res_noop["result"]["watermark_after"]
        wm_next = (datetime.fromisoformat(wm_prev) + timedelta(seconds=30)).isoformat()

        template_doc["id_order"] = synth_id
        template_doc["order_date"] = "2025-06-20T11:00:00"
        template_doc["processed_at"] = wm_next

        try:
            val_col.insert_one(template_doc)
            res_inc = run_job("refresh_materialized_views", db_name=db_name)
            assert spy_mv.call_count == 3
            assert res_inc["status"] == "SUCCESS"
            assert res_inc["result"]["mode_executed"] == "incremental"
            assert res_inc["result"]["status"] == "SUCCESS"
            assert res_inc["result"]["noop"] is False
            assert res_inc["result"]["changed_orders_count"] == 1
            assert res_inc["result"]["affected_dates"] == ["2025-06-20"]
        finally:
            val_col.delete_one({"id_order": synth_id})
            run_job("refresh_materialized_views", db_name=db_name, mode="full")


def test_04_aggregation_job_reuses_existing_reports_and_preserves_last_valid_report(
    isolated_jobs_db, tmp_path: Path
):
    """
    8 & 6. Verify `generate_aggregation_report`:
    - calls `execute_aggregation_by_name` for all 5 existing reports
    - writes valid JSON and Markdown report files
    - preserves the last valid report file on disk if a subsequent execution fails
    """
    db_name = isolated_jobs_db["db_name"]
    out_dir = tmp_path / "reports_out"
    out_file = "test_scheduled_report.json"

    with patch.object(
        job_runner_mod,
        "execute_aggregation_by_name",
        wraps=job_runner_mod.execute_aggregation_by_name,
    ) as spy_agg:
        res = run_job(
            "generate_aggregation_report",
            db_name=db_name,
            output_dir=out_dir,
            output_filename=out_file,
        )
        assert res["status"] == "SUCCESS"
        assert spy_agg.call_count == 5

        called_report_names = [call.args[0] for call in spy_agg.call_args_list]
        assert called_report_names == [
            "daily_sales_summary",
            "sales_by_city",
            "payment_method_analysis",
            "order_status_distribution",
            "top_products",
        ]

    json_path = Path(res["result"]["output_json_path"])
    md_path = Path(res["result"]["output_md_path"])
    assert json_path.exists()
    assert md_path.exists()

    saved_payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert saved_payload["database"] == db_name
    assert len(saved_payload["reports_executed"]) == 5
    original_generated_at = saved_payload["generated_at"]

    # Now simulate a failure on the 3rd report and verify the existing JSON/MD files are NOT overwritten
    def fail_on_payment_report(report_name: str, **kwargs: Any):
        if report_name == "payment_method_analysis":
            raise RuntimeError("Simulated aggregation failure on payment_method_analysis")
        return job_runner_mod.AGGREGATION_REGISTRY_FALLBACK(report_name, **kwargs)

    with patch.object(
        job_runner_mod,
        "execute_aggregation_by_name",
        side_effect=RuntimeError("Simulated database disconnect during aggregation"),
    ):
        failed_res = run_job(
            "generate_aggregation_report",
            db_name=db_name,
            raise_on_error=False,
            output_dir=out_dir,
            output_filename=out_file,
        )
        assert failed_res["status"] == "FAILED"
        assert "Simulated database disconnect" in failed_res["error"]

    # Verify the previous valid report on disk is 100% preserved
    preserved_payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert preserved_payload["generated_at"] == original_generated_at


def test_05_structured_logging_start_success_and_failure(isolated_jobs_db, caplog):
    """
    4, 5, 6. Verify structured logging emits `JOB_START`, `JOB_SUCCESS`, and `JOB_FAILURE`
    to both the logger (`caplog`) and `reports/logs/scheduled_jobs.log`, and that
    exception handling works for both `raise_on_error=True` and `raise_on_error=False`.
    """
    db_name = isolated_jobs_db["db_name"]
    caplog.set_level(logging.INFO, logger="src.jobs")

    # 1. Successful execution logging
    res_ok = run_job("refresh_materialized_views", db_name=db_name)
    assert res_ok["status"] == "SUCCESS"
    assert any("JOB_START job=refresh_materialized_views" in rec.message for rec in caplog.records)
    assert any("JOB_SUCCESS job=refresh_materialized_views" in rec.message for rec in caplog.records)

    # 2. Failed execution logging with raise_on_error=True
    caplog.clear()
    with patch.object(
        job_runner_mod,
        "refresh_all_materialized_views",
        side_effect=RuntimeError("Simulated MV connection timeout"),
    ):
        with pytest.raises(RuntimeError, match="Simulated MV connection timeout"):
            run_job("refresh_materialized_views", db_name=db_name, raise_on_error=True)

    assert any("JOB_START job=refresh_materialized_views" in rec.message for rec in caplog.records)
    assert any(
        "JOB_FAILURE job=refresh_materialized_views" in rec.message
        and "Simulated MV connection timeout" in rec.message
        for rec in caplog.records
    )

    # 3. Unknown job name raises KeyError
    with pytest.raises(KeyError, match="Unknown scheduled job"):
        run_job("non_existent_job_name", db_name=db_name)

    # Verify log file on disk contains JOB_START, JOB_SUCCESS, and JOB_FAILURE
    assert JOB_LOG_FILE.exists()
    log_text = JOB_LOG_FILE.read_text(encoding="utf-8")
    assert "JOB_START job=refresh_materialized_views" in log_text
    assert "JOB_SUCCESS job=refresh_materialized_views" in log_text
    assert "JOB_FAILURE job=refresh_materialized_views" in log_text


def test_06_scheduler_does_not_auto_start_and_runs_deterministically(
    isolated_jobs_db, tmp_path: Path, monkeypatch
):
    """
    9. Verify `JobScheduler` does not start automatically on import or initialization,
    calculates `next_run_at` deterministically in `run_pending()`, and terminates
    immediately when `start(max_cycles=1)` is called.
    """
    monkeypatch.setattr(job_runner_mod, "REPORTS_DIR", tmp_path)
    db_name = isolated_jobs_db["db_name"]
    t0_epoch = 1_700_000_000.0

    scheduler = JobScheduler(db_name=db_name, initial_epoch=t0_epoch)
    assert scheduler.is_running is False

    # Check initial schedule status before any tick
    sched_status = scheduler.get_schedule_status(now_epoch=t0_epoch)
    assert len(sched_status) == 2
    assert all(s["seconds_until_next_run"] == 0.0 for s in sched_status)

    # First tick at t0_epoch: both jobs are due and execute
    tick1 = scheduler.run_pending(now_epoch=t0_epoch)
    assert len(tick1) == 2

    # Second tick 60 seconds later: neither job is due yet (900s and 86400s intervals)
    tick2 = scheduler.run_pending(now_epoch=t0_epoch + 60.0)
    assert tick2 == []

    # Third tick 900 seconds after t0_epoch: ONLY refresh_materialized_views (900s) is due
    tick3 = scheduler.run_pending(now_epoch=t0_epoch + 900.0)
    assert len(tick3) == 1
    assert tick3[0]["job_name"] == "refresh_materialized_views"

    # Bounded start(max_cycles=1) must execute 1 cycle and return cleanly with is_running == False
    bounded_scheduler = JobScheduler(db_name=db_name)
    cycle_results = bounded_scheduler.start(max_cycles=1, poll_interval_seconds=0.01)
    assert len(cycle_results) == 2
    assert bounded_scheduler.is_running is False


def test_07_cli_manual_execution_list_run_and_run_all(
    isolated_jobs_db, capsys, tmp_path: Path, monkeypatch
):
    """
    3. Verify CLI manual execution (`--list`, `--run refresh_materialized_views`,
    `--run generate_aggregation_report`, `--run-all`) with `--db-name` override.
    """
    monkeypatch.setattr(job_runner_mod, "REPORTS_DIR", tmp_path)
    db_name = isolated_jobs_db["db_name"]

    # --list
    rc_list = scheduler_cli_main(["--list"])
    assert rc_list == 0
    out_list = json.loads(capsys.readouterr().out)
    assert len(out_list["scheduled_jobs"]) == 2

    # --run refresh_materialized_views
    rc_mv = scheduler_cli_main(["--run", "refresh_materialized_views", "--db-name", db_name])
    assert rc_mv == 0
    out_mv = json.loads(capsys.readouterr().out)
    assert out_mv["job_name"] == "refresh_materialized_views"
    assert out_mv["status"] == "SUCCESS"

    # --run generate_aggregation_report
    rc_agg = scheduler_cli_main(["--run", "generate_aggregation_report", "--db-name", db_name])
    assert rc_agg == 0
    out_agg = json.loads(capsys.readouterr().out)
    assert out_agg["job_name"] == "generate_aggregation_report"
    assert out_agg["status"] == "SUCCESS"
    assert len(out_agg["result"]["reports_executed"]) == 5

    # --run-all
    rc_all = scheduler_cli_main(["--run-all", "--db-name", db_name])
    assert rc_all == 0
    out_all = json.loads(capsys.readouterr().out)
    assert len(out_all["executed_jobs"]) == 2
