"""
Phase 2 — Step 4: Lightweight Deterministic Scheduler & Manual CLI Runner.

Provides:
1. `JobScheduler`:
   - Test-safe, deterministic schedule coordinator for `JOBS`.
   - NEVER starts an infinite background thread on module import or instantiation.
   - Supports single-tick evaluation via `run_pending(now_epoch=...)` and bounded
     or explicit loop execution via `start(max_cycles=...)`.
2. CLI Manual Execution Interface (`python -m src.jobs.scheduler`):
   - `--list`: Display all registered jobs and their schedules.
   - `--run <job_name>`: Manually execute a specific job (`refresh_materialized_views`
     or `generate_aggregation_report`).
   - `--run-all`: Manually execute all registered jobs sequentially.
   - `--db-name <db_name>`: Override target MongoDB database name.
   - `--daemon`: Explicitly start the periodic scheduler loop (never runs unless passed).
"""
import argparse
import json
import sys
import threading
import time
from typing import Any, Dict, List, Optional

from config.settings import MONGO_DB_NAME
from src.jobs.job_runner import (
    JOBS,
    ScheduledJob,
    get_job_logger,
    list_jobs,
    run_all_jobs,
    run_job,
)

logger = get_job_logger()


class JobScheduler:
    """
    Lightweight, deterministic scheduler for registered Phase 2 jobs.
    Does NOT start any background thread or loop upon import or initialization.
    """

    def __init__(
        self,
        jobs: Optional[Dict[str, ScheduledJob]] = None,
        db_name: str = MONGO_DB_NAME,
        initial_epoch: Optional[float] = None,
    ) -> None:
        self.jobs: Dict[str, ScheduledJob] = dict(jobs) if jobs is not None else dict(JOBS)
        self.db_name: str = db_name
        self.is_running: bool = False
        self._stop_event = threading.Event()

        base_now = initial_epoch if initial_epoch is not None else time.time()
        # By default, jobs are immediately eligible on the first scheduler tick
        self.next_run_at: Dict[str, float] = {name: base_now for name in self.jobs}
        self.last_run_results: Dict[str, Dict[str, Any]] = {}

    def get_schedule_status(self, now_epoch: Optional[float] = None) -> List[Dict[str, Any]]:
        """Return the schedule metadata and next-run countdown for all registered jobs."""
        current = now_epoch if now_epoch is not None else time.time()
        status_list: List[Dict[str, Any]] = []
        for name, job in self.jobs.items():
            next_ts = self.next_run_at.get(name, current)
            last_res = self.last_run_results.get(name)
            status_list.append(
                {
                    "name": job.name,
                    "description": job.description,
                    "schedule": dict(job.schedule),
                    "next_run_epoch": round(next_ts, 3),
                    "seconds_until_next_run": max(0.0, round(next_ts - current, 3)),
                    "last_status": last_res.get("status") if last_res else None,
                    "last_started_at": last_res.get("started_at") if last_res else None,
                }
            )
        return status_list

    def run_pending(
        self,
        now_epoch: Optional[float] = None,
        raise_on_error: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Evaluate all registered jobs against `now_epoch` and execute any job whose
        `next_run_at <= current`. Advances `next_run_at` by `interval_seconds` for
        each executed job.
        """
        current = now_epoch if now_epoch is not None else time.time()
        executed_results: List[Dict[str, Any]] = []

        for name, job in self.jobs.items():
            due_at = self.next_run_at.get(name, current)
            if current >= due_at:
                res = run_job(
                    name,
                    db_name=self.db_name,
                    raise_on_error=raise_on_error,
                )
                interval_sec = float(job.schedule.get("interval_seconds", 900))
                self.next_run_at[name] = current + interval_sec
                self.last_run_results[name] = res
                executed_results.append(res)

        return executed_results

    def start(
        self,
        max_cycles: Optional[int] = None,
        poll_interval_seconds: float = 1.0,
        raise_on_error: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Explicitly start the scheduler loop.
        When `max_cycles` is provided (e.g., `max_cycles=1` in tests or bounded demos),
        executes at most `max_cycles` polling cycles and returns cleanly.
        """
        self.is_running = True
        self._stop_event.clear()
        cycles_run = 0
        all_results: List[Dict[str, Any]] = []

        logger.info(
            "SCHEDULER_START db_name=%s jobs=%s max_cycles=%s",
            self.db_name,
            list(self.jobs.keys()),
            max_cycles,
        )

        try:
            while not self._stop_event.is_set():
                cycle_results = self.run_pending(raise_on_error=raise_on_error)
                all_results.extend(cycle_results)
                cycles_run += 1

                if max_cycles is not None and cycles_run >= max_cycles:
                    break

                self._stop_event.wait(timeout=max(0.01, poll_interval_seconds))
        finally:
            self.is_running = False
            logger.info(
                "SCHEDULER_STOP db_name=%s cycles_run=%d jobs_executed=%d",
                self.db_name,
                cycles_run,
                len(all_results),
            )

        return all_results

    def stop(self) -> None:
        """Signal the scheduler loop to stop cleanly."""
        self._stop_event.set()
        self.is_running = False


def build_arg_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser for manual and scheduled job execution."""
    parser = argparse.ArgumentParser(
        description="Phase 2 Scheduled Jobs Runner & CLI Controller."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--list",
        action="store_true",
        help="List all registered scheduled jobs and their schedule definitions.",
    )
    group.add_argument(
        "--run",
        metavar="JOB_NAME",
        help="Manually execute a specific registered job by name.",
    )
    group.add_argument(
        "--run-all",
        action="store_true",
        help="Manually execute all registered scheduled jobs sequentially.",
    )
    group.add_argument(
        "--daemon",
        action="store_true",
        help="Explicitly start the periodic scheduler loop.",
    )

    parser.add_argument(
        "--db-name",
        default=MONGO_DB_NAME,
        help=f"Target MongoDB database name (default: {MONGO_DB_NAME}).",
    )
    parser.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
        help="Refresh mode when running refresh_materialized_views (default: incremental).",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="Optional maximum polling cycles when running with --daemon.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        help="Polling interval in seconds when running with --daemon (default: 5.0).",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point for `python -m src.jobs.scheduler`."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        if args.list:
            jobs_catalog = list_jobs()
            print(json.dumps({"scheduled_jobs": jobs_catalog}, ensure_ascii=False, indent=2))
            return 0

        if args.run:
            extra_kwargs: Dict[str, Any] = {}
            if args.run == "refresh_materialized_views" and args.mode:
                extra_kwargs["mode"] = args.mode
            res = run_job(args.run, db_name=args.db_name, raise_on_error=True, **extra_kwargs)
            print(json.dumps(res, ensure_ascii=False, indent=2))
            return 0

        if args.run_all:
            results = run_all_jobs(db_name=args.db_name, raise_on_error=True)
            print(json.dumps({"executed_jobs": results}, ensure_ascii=False, indent=2))
            return 0

        if args.daemon:
            scheduler = JobScheduler(db_name=args.db_name)
            results = scheduler.start(
                max_cycles=args.max_cycles,
                poll_interval_seconds=args.poll_interval,
                raise_on_error=False,
            )
            print(
                json.dumps(
                    {"daemon_completed": True, "executed_jobs_count": len(results)},
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

        return 0
    except Exception as exc:
        logger.error("CLI job execution failed: %s", exc)
        print(
            json.dumps({"status": "FAILED", "error": str(exc)}, ensure_ascii=False, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
