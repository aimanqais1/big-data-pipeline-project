"""
Phase 2 — Step 4: Scheduled Jobs Package (`src.jobs`).

Exposes the scheduled job registry, job contract, structured execution helpers,
and deterministic scheduler.
"""
from src.jobs.job_runner import (
    DEFAULT_REPORT_FILENAME,
    JOB_LOG_FILE,
    JOBS,
    ScheduledJob,
    execute_aggregation_report_job,
    execute_mv_refresh_job,
    get_job_logger,
    list_jobs,
    run_all_jobs,
    run_job,
)
from src.jobs.scheduler import JobScheduler

__all__ = [
    "JOBS",
    "ScheduledJob",
    "JobScheduler",
    "list_jobs",
    "run_job",
    "run_all_jobs",
    "execute_mv_refresh_job",
    "execute_aggregation_report_job",
    "get_job_logger",
    "JOB_LOG_FILE",
    "DEFAULT_REPORT_FILENAME",
]
