"""Scheduled jobs: drift monitoring, retention, dead-letter alerting.

A plain asyncio loop rather than APScheduler or ``pg_cron``, for one reason that
decides the design:

**The free tier sleeps.** Render's free web service spins down after 15 minutes
idle, and Neon's free compute scales to zero. A timer that only fires while the
process is awake will silently skip every window it slept through, and
``pg_cron`` has the identical problem on a suspended Neon instance -- with the
added twist that you cannot tell it did.

So last-run state lives in the ``job_runs`` **table**, not in memory. On wake,
any job whose last successful run is older than its interval runs immediately
("catch-up"), and the table is the audit trail of what actually ran and when.
That makes a skipped window visible instead of invisible.

The scheduler presents the same ``run()``/``request_stop()`` surface as a stream
consumer so the supervisor treats it identically.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select

from obs_sdk.schema import Severity

from ..alerts import AlertKind, dead_letter_dedupe_key, raise_alert
from ..db import Database, get_db, table_of, upsert
from ..drift.monitor import DriftMonitor
from ..jobs import retention
from ..logging import get_logger, log_context
from ..models import DeadLetter, JobRun, WorkerHeartbeat
from ..settings import Settings, get_settings

log = get_logger("obs_platform.scheduler")

JobFn = Callable[[], Awaitable[dict[str, Any]]]


@dataclass
class Job:
    name: str
    interval_seconds: int
    run: JobFn
    enabled: bool = True
    #: Run at startup if the last successful run is older than the interval.
    catch_up: bool = True


class Scheduler:
    """Runs periodic jobs with persistent, catch-up-aware scheduling."""

    role = "scheduler"

    def __init__(
        self,
        settings: Settings | None = None,
        database: Database | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._db = database
        self.consumer_name = f"{socket.gethostname()}:{os.getpid()}"
        self.drift = DriftMonitor(self.settings)
        self._stop = asyncio.Event()
        self._started_at = datetime.now(UTC)
        self.jobs = self._build_jobs()
        self.runs = 0
        self.failures = 0

    @property
    def db(self) -> Database:
        return self._db if self._db is not None else get_db()

    @property
    def name(self) -> str:
        return f"{self.consumer_name}#{self.role}"

    def _build_jobs(self) -> list[Job]:
        return [
            Job(
                name="drift_monitor",
                interval_seconds=self.settings.drift_interval_seconds,
                run=self._run_drift,
                enabled=self.settings.drift_enabled,
            ),
            Job(
                name="retention_purge",
                interval_seconds=self.settings.retention_interval_seconds,
                run=self._run_retention,
                enabled=self.settings.retention_days > 0,
            ),
            Job(
                name="dead_letter_watch",
                interval_seconds=max(300, self.settings.retention_interval_seconds // 4),
                run=self._run_dead_letter_watch,
            ),
        ]

    # -- lifecycle ---------------------------------------------------------
    def request_stop(self) -> None:
        if not self._stop.is_set():
            log.info("scheduler.stop_requested")
            self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    async def run(self) -> None:
        enabled = [j for j in self.jobs if j.enabled]
        log.info("scheduler.started", jobs=[j.name for j in enabled])
        await self._heartbeat("running")
        try:
            while not self._stop.is_set():
                await self.tick()
                # 30s granularity is plenty for jobs measured in minutes, and
                # keeps SIGTERM responsive.
                await self._sleep(min(30, min((j.interval_seconds for j in enabled), default=30)))
        finally:
            await self._heartbeat("stopped")
            log.info("scheduler.stopped", runs=self.runs, failures=self.failures)

    async def tick(self) -> list[str]:
        """Run every job that is due. Returns the names that ran."""
        ran: list[str] = []
        for job in self.jobs:
            if not job.enabled or self._stop.is_set():
                continue
            if not await self._is_due(job):
                continue
            await self._execute(job)
            ran.append(job.name)
        if ran:
            await self._heartbeat("running")
        return ran

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)

    # -- scheduling state --------------------------------------------------
    async def _is_due(self, job: Job) -> bool:
        async with self.db.read_session() as session:
            row = (
                await session.execute(select(JobRun).where(JobRun.job_name == job.name))
            ).scalar_one_or_none()
            last_finished = row.last_finished_at if row else None

        if last_finished is None:
            # Never run. catch_up decides whether that means "now" or "after
            # one interval" -- for retention and drift, now is correct.
            return job.catch_up

        if last_finished.tzinfo is None:
            last_finished = last_finished.replace(tzinfo=UTC)
        return datetime.now(UTC) - last_finished >= timedelta(seconds=job.interval_seconds)

    async def _execute(self, job: Job) -> None:
        started = datetime.now(UTC)
        clock = time.perf_counter()
        with log_context(job=job.name):
            try:
                detail = await job.run()
                status, error = "ok", None
            except Exception as exc:
                # One failing job must not stop the others, and must not stop
                # the loop -- the next tick tries again.
                self.failures += 1
                status, error = "error", f"{type(exc).__name__}: {exc}"[:1000]
                detail = {}
                log.exception("scheduler.job_failed", job=job.name)
            else:
                self.runs += 1
                log.info(
                    "scheduler.job_completed",
                    job=job.name,
                    duration_ms=int((time.perf_counter() - clock) * 1000),
                    **{k: v for k, v in detail.items() if not isinstance(v, (dict, list))},
                )
        await self._record(job, started, status, error, detail)

    async def _record(
        self,
        job: Job,
        started: datetime,
        status: str,
        error: str | None,
        detail: dict[str, Any],
    ) -> None:
        async with self.db.session() as session:
            row = (
                await session.execute(select(JobRun).where(JobRun.job_name == job.name))
            ).scalar_one_or_none()
            run_count = (row.run_count if row else 0) + 1
            await upsert(
                session,
                table_of(JobRun),
                {
                    "job_name": job.name,
                    "last_started_at": started,
                    # Only a SUCCESSFUL run advances the schedule. A failing job
                    # that still moved the clock forward would look "recently
                    # run" while never actually doing anything.
                    "last_finished_at": datetime.now(UTC)
                    if status == "ok"
                    else (row.last_finished_at if row else None),
                    "last_status": status,
                    "last_error": error,
                    "run_count": run_count,
                    "detail": detail,
                },
                index_elements=["job_name"],
                update_columns=[
                    "last_started_at",
                    "last_finished_at",
                    "last_status",
                    "last_error",
                    "run_count",
                    "detail",
                ],
            )

    # -- the jobs ----------------------------------------------------------
    async def _run_drift(self) -> dict[str, Any]:
        async with self.db.session() as session:
            results = await self.drift.run_once(session)
        drifted = [r for r in results if r.drifted]
        return {
            "evaluated": len(results),
            "drifted": len(drifted),
            "tenants": sorted({r.tenant_id for r in results}),
        }

    async def _run_retention(self) -> dict[str, Any]:
        report = await retention.purge(self.db, self.settings)
        return report.as_dict()

    async def _run_dead_letter_watch(self) -> dict[str, Any]:
        """Unreplayed dead letters are a silent failure until someone is told."""
        async with self.db.session() as session:
            rows = (
                await session.execute(
                    select(DeadLetter.consumer_group, func.count())
                    .where(DeadLetter.replayed_at.is_(None))
                    .group_by(DeadLetter.consumer_group)
                )
            ).all()
            for consumer_group, count in rows:
                if not count:
                    continue
                await raise_alert(
                    session,
                    tenant_id="_platform",
                    kind=AlertKind.DEAD_LETTER,
                    severity=Severity.HIGH if count > 10 else Severity.MEDIUM,
                    title=f"{count} unreplayed dead letters in {consumer_group}",
                    detail={"consumer_group": consumer_group, "count": int(count)},
                    dedupe_key=dead_letter_dedupe_key(str(consumer_group)),
                )
        return {"groups": len(rows), "total": sum(int(c) for _, c in rows)}

    # -- heartbeat ---------------------------------------------------------
    async def _heartbeat(self, status: str) -> None:
        try:
            async with self.db.session() as session:
                await upsert(
                    session,
                    table_of(WorkerHeartbeat),
                    {
                        "name": self.name,
                        "role": self.role,
                        "host": socket.gethostname()[:200],
                        "pid": os.getpid(),
                        "status": status,
                        "started_at": self._started_at,
                        "last_seen_at": datetime.now(UTC),
                        "last_success_at": datetime.now(UTC) if self.runs else None,
                        "processed": self.runs,
                        "failed": self.failures,
                        "dead_lettered": 0,
                        "detail": {"jobs": [j.name for j in self.jobs if j.enabled]},
                    },
                    index_elements=["name"],
                    update_columns=[
                        "role",
                        "host",
                        "pid",
                        "status",
                        "last_seen_at",
                        "last_success_at",
                        "processed",
                        "failed",
                        "detail",
                    ],
                )
        except Exception:
            log.debug("scheduler.heartbeat_failed", exc_info=True)


__all__ = ["Job", "Scheduler"]
