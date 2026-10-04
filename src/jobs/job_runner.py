"""
Phase 2 — Step 4: Scheduled Job Runner & Job Registry.

Implements the common job contract (`ScheduledJob`), structured job execution logging
(`JOB_START`, `JOB_SUCCESS`, `JOB_FAILURE`), and the two approved Phase 2 scheduled jobs:

1. `refresh_materialized_views`:
   - Schedule: Every 15 minutes (`*/15 * * * *`, `interval_seconds = 900`)
   - Reuses `refresh_all_materialized_views(mode="incremental", db_name=db_name)`
     from `src.aggregations.materialized_views`.

2. `generate_aggregation_report`:
   - Schedule: Daily at 00:00 UTC (`0 0 * * *`, `interval_seconds = 86400`)
   - Reuses `list_available_aggregations()` and `execute_aggregation_by_name()`
     from `src.aggregations.reports` to execute all 5 analytical aggregation reports
     and atomically write `reports/scheduled_aggregation_report.json` and `.md`
     (preserving the last valid report on disk if an execution error occurs).
"""
import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from config.settings import MONGO_DB_NAME, REPORTS_DIR
from src.aggregations.materialized_views import refresh_all_materialized_views
from src.aggregations.reports import (
    execute_aggregation_by_name,
    list_available_aggregations,
)

JOB_LOG_DIR = REPORTS_DIR / "logs"
JOB_LOG_FILE = JOB_LOG_DIR / "scheduled_jobs.log"
DEFAULT_REPORT_FILENAME = "scheduled_aggregation_report.json"


def get_job_logger() -> logging.Logger:
    """
    Configure and return the structured logger for scheduled jobs.
    Writes to both standard console logging and `reports/logs/scheduled_jobs.log`.
    """
    logger = logging.getLogger("src.jobs")
    logger.setLevel(logging.INFO)

    JOB_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path_str = str(JOB_LOG_FILE.resolve())

    has_file_handler = any(
        isinstance(h, logging.FileHandler)
        and getattr(h, "baseFilename", None) == log_path_str
        for h in logger.handlers
    )
    if not has_file_handler:
        file_handler = logging.FileHandler(log_path_str, mode="a", encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


logger = get_job_logger()


# --------------------------------------------------------------------------
# Concrete Job Task Implementations (Reusing Existing Phase 2 Functions)
# --------------------------------------------------------------------------

def execute_mv_refresh_job(
    db_name: str = MONGO_DB_NAME,
    mode: str = "incremental",
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    JOB 1 Task: Execute incremental (or full) Materialized View refresh by delegating
    directly to `src.aggregations.materialized_views.refresh_all_materialized_views`.
    """
    return refresh_all_materialized_views(mode=mode, db_name=db_name, **kwargs)


def _format_aggregation_markdown_report(payload: Dict[str, Any]) -> str:
    """Build a human-readable Markdown summary for the scheduled aggregation report."""
    lines: List[str] = [
        "# Scheduled Aggregation Report — Phase 2 Analytical Summary",
        f"**Database:** `{payload.get('database')}`  ",
        f"**Generated At:** `{payload.get('generated_at')}`  ",
        f"**Reports Executed:** `{len(payload.get('reports_executed', []))}`",
        "",
        "---",
        "",
        "## 1. Report Row Counts Summary",
        "| Report Name | Result Count |",
        "| :--- | ---: |",
    ]
    for r_name in payload.get("reports_executed", []):
        r_count = payload.get("report_row_counts", {}).get(r_name, 0)
        lines.append(f"| `{r_name}` | `{r_count:,}` |")

    lines.extend(["", "---", "", "## 2. Analytical Highlights", ""])

    reports_map = payload.get("reports", {})
    for r_name in payload.get("reports_executed", []):
        r_data = reports_map.get(r_name, {})
        sample_rows = r_data.get("results", [])[:5]
        lines.append(f"### `{r_name}` (Showing top/first {len(sample_rows)} of {r_data.get('count', 0)} rows)")
        lines.append("```json")
        lines.append(json.dumps(sample_rows, ensure_ascii=False, indent=2))
        lines.append("```")
        lines.append("")

    return "\n".join(lines)


def _atomic_write_text(target_path: Path, content: str) -> None:
    """
    Atomically write text to `target_path` via a temporary file in the same directory,
    ensuring any previous valid report file is preserved if writing fails.
    """
    target_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(target_path.parent),
        prefix=f".{target_path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, mode="w", encoding="utf-8") as tmp_file:
            tmp_file.write(content)
        os.replace(tmp_name, target_path)
    except Exception:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise


def execute_aggregation_report_job(
    db_name: str = MONGO_DB_NAME,
    output_filename: str = DEFAULT_REPORT_FILENAME,
    output_dir: Optional[Path] = None,
    top_products_limit: int = 10,
) -> Dict[str, Any]:
    """
    JOB 2 Task: Execute all 5 approved Phase 2 aggregation reports via
    `src.aggregations.reports.execute_aggregation_by_name` and atomically persist
    the consolidated JSON and Markdown artifacts into `reports/`.
    If any report execution fails, raises an exception BEFORE touching existing report files.
    """
    target_dir = Path(output_dir) if output_dir is not None else REPORTS_DIR
    target_dir.mkdir(parents=True, exist_ok=True)

    json_path = target_dir / output_filename
    md_filename = (
        output_filename[:-5] + ".md"
        if output_filename.endswith(".json")
        else f"{output_filename}.md"
    )
    md_path = target_dir / md_filename

    report_names = list_available_aggregations()
    reports_output: Dict[str, Dict[str, Any]] = {}
    report_row_counts: Dict[str, int] = {}

    for report_name in report_names:
        call_kwargs: Dict[str, Any] = {"db_name": db_name}
        if report_name == "top_products":
            call_kwargs["limit"] = top_products_limit

        rep_res = execute_aggregation_by_name(report_name, **call_kwargs)
        reports_output[report_name] = rep_res
        report_row_counts[report_name] = int(rep_res.get("count", 0))

    generated_at = datetime.now(timezone.utc).isoformat()
    payload: Dict[str, Any] = {
        "report_suite": "phase2_scheduled_aggregation_report",
        "database": db_name,
        "generated_at": generated_at,
        "reports_executed": report_names,
        "report_row_counts": report_row_counts,
        "output_json_path": str(json_path.resolve()),
        "output_md_path": str(md_path.resolve()),
        "reports": reports_output,
    }

    json_text = json.dumps(payload, ensure_ascii=False, indent=2)
    md_text = _format_aggregation_markdown_report(payload)

    _atomic_write_text(json_path, json_text)
    _atomic_write_text(md_path, md_text)

    return payload


# --------------------------------------------------------------------------
# Common Job Contract & Registry
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class ScheduledJob:
    """Common contract for all Phase 2 scheduled jobs."""

    name: str
    description: str
    schedule: Dict[str, Any]
    target_function: Callable[..., Dict[str, Any]]

    def run(
        self,
        db_name: str = MONGO_DB_NAME,
        raise_on_error: bool = True,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Execute this scheduled job through the structured lifecycle wrapper."""
        return run_job(
            self.name,
            db_name=db_name,
            raise_on_error=raise_on_error,
            **kwargs,
        )


JOBS: Dict[str, ScheduledJob] = {
    "refresh_materialized_views": ScheduledJob(
        name="refresh_materialized_views",
        description=(
            "Run incremental Materialized View refresh for daily_sales_summary and "
            "top_products_summary using watermark and order digest tracking."
        ),
        schedule={
            "schedule_type": "interval",
            "interval_seconds": 900,
            "cron_expression": "*/15 * * * *",
            "human_readable": "Every 15 minutes",
        },
        target_function=execute_mv_refresh_job,
    ),
    "generate_aggregation_report": ScheduledJob(
        name="generate_aggregation_report",
        description=(
            "Execute all 5 approved MongoDB aggregation reports on orders_validated "
            "and write consolidated JSON and Markdown summary artifacts to reports/."
        ),
        schedule={
            "schedule_type": "daily",
            "interval_seconds": 86400,
            "cron_expression": "0 0 * * *",
            "human_readable": "Daily at 00:00 UTC",
        },
        target_function=execute_aggregation_report_job,
    ),
}


def list_jobs() -> List[Dict[str, Any]]:
    """Return metadata for all registered scheduled jobs in a JSON-serializable list."""
    return [
        {
            "name": job.name,
            "description": job.description,
            "schedule": dict(job.schedule),
            "target_function": job.target_function.__name__,
        }
        for job in JOBS.values()
    ]


def _build_log_summary(job_name: str, result: Dict[str, Any]) -> str:
    """Extract concise key-value telemetry for the JOB_SUCCESS log line."""
    if job_name == "refresh_materialized_views":
        return (
            f"mv_status={result.get('status')} "
            f"mode={result.get('mode_executed')} "
            f"noop={result.get('noop')} "
            f"changed_orders={result.get('changed_orders_count')} "
            f"daily_docs={result.get('daily_sales_docs_count')} "
            f"product_docs={result.get('top_products_docs_count')}"
        )
    if job_name == "generate_aggregation_report":
        return (
            f"reports_executed={len(result.get('reports_executed', []))} "
            f"row_counts={json.dumps(result.get('report_row_counts', {}), ensure_ascii=False)} "
            f"output_json={result.get('output_json_path')}"
        )
    return "completed=true"


def run_job(
    job_name: str,
    db_name: str = MONGO_DB_NAME,
    raise_on_error: bool = True,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Look up and execute a registered scheduled job by name, emitting structured
    `JOB_START`, `JOB_SUCCESS`, or `JOB_FAILURE` logs with start/end timestamps
    and duration in milliseconds.
    """
    if job_name not in JOBS:
        valid_names = ", ".join(JOBS.keys())
        raise KeyError(
            f"Unknown scheduled job: {job_name!r}. Available jobs: {valid_names}"
        )

    job = JOBS[job_name]
    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.perf_counter()
    cron_expr = job.schedule.get("cron_expression", "")

    job_logger = get_job_logger()
    job_logger.info(
        "JOB_START job=%s started_at=%s db_name=%s schedule=\"%s\"",
        job.name,
        started_at,
        db_name,
        cron_expr,
    )

    try:
        task_result = job.target_function(db_name=db_name, **kwargs)
        ended_at = datetime.now(timezone.utc).isoformat()
        duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        summary_info = _build_log_summary(job.name, task_result)

        job_logger.info(
            "JOB_SUCCESS job=%s started_at=%s ended_at=%s duration_ms=%.2f status=SUCCESS %s",
            job.name,
            started_at,
            ended_at,
            duration_ms,
            summary_info,
        )

        return {
            "job_name": job.name,
            "description": job.description,
            "schedule": dict(job.schedule),
            "database": db_name,
            "status": "SUCCESS",
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_ms": duration_ms,
            "error": None,
            "result": task_result,
        }
    except Exception as exc:
        ended_at = datetime.now(timezone.utc).isoformat()
        duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)

        job_logger.error(
            "JOB_FAILURE job=%s started_at=%s ended_at=%s duration_ms=%.2f status=FAILED error=%r",
            job.name,
            started_at,
            ended_at,
            duration_ms,
            str(exc),
        )

        if raise_on_error:
            raise

        return {
            "job_name": job.name,
            "description": job.description,
            "schedule": dict(job.schedule),
            "database": db_name,
            "status": "FAILED",
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_ms": duration_ms,
            "error": str(exc),
            "result": None,
        }


def run_all_jobs(
    db_name: str = MONGO_DB_NAME,
    raise_on_error: bool = True,
) -> List[Dict[str, Any]]:
    """Sequentially execute all registered scheduled jobs in `JOBS`."""
    results: List[Dict[str, Any]] = []
    for job_name in JOBS:
        res = run_job(job_name, db_name=db_name, raise_on_error=raise_on_error)
        results.append(res)
    return results
