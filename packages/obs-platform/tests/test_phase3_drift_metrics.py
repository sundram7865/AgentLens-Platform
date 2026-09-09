"""Phase 3 -- drift monitor, scheduled jobs, and the read API the dashboard uses.

The drift tests are the interesting ones. They check not just "does it compute a
z-score" but the two judgement calls behind it: a baseline is captured
deliberately (never inferred), and an improvement is not an incident.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from obs_platform.alerts import AlertKind
from obs_platform.drift.monitor import DriftMonitor
from obs_platform.jobs import retention
from obs_platform.models import (
    Alert,
    DriftBaseline,
    DriftSnapshot,
    EvalScore,
    GuardrailFinding,
    JobRun,
    Span,
    Trace,
)
from obs_platform.workers.scheduler import Scheduler


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
async def seed_scores(
    database, tenant_id: str, metric: str, values: list[float], hours_ago: float = 1.0
) -> None:
    created = datetime.now(UTC) - timedelta(hours=hours_ago)
    async with database.session() as session:
        for index, value in enumerate(values):
            session.add(
                EvalScore(
                    trace_id=f"tr_{tenant_id}_{metric}_{hours_ago}_{index}",
                    tenant_id=tenant_id,
                    metric=metric,
                    score=value,
                    backend="heuristic",
                    created_at=created,
                )
            )


async def seed_trace(
    database,
    trace_id: str,
    tenant_id: str = "org_1",
    latency_ms: int = 500,
    status: str = "ok",
    cost_micros: int = 100,
    days_ago: float = 0.0,
    flagged: bool = False,
) -> None:
    created = datetime.now(UTC) - timedelta(days=days_ago)
    async with database.session() as session:
        session.add(
            Trace(
                trace_id=trace_id,
                tenant_id=tenant_id,
                name="agent_run",
                service="supportpilot",
                status=status,
                started_at=created,
                ended_at=created,
                latency_ms=latency_ms,
                total_tokens=1200,
                prompt_tokens=1000,
                completion_tokens=200,
                cost_micros=cost_micros,
                span_count=3,
                input={"question": "where is my order"},
                output={"answer": "tomorrow"},
                attributes={},
                guardrail_status="flagged" if flagged else "clean",
                guardrail_flags={},
                created_at=created,
                updated_at=created,
            )
        )


# --------------------------------------------------------------------------- #
# Drift
# --------------------------------------------------------------------------- #
class TestDriftMonitor:
    async def test_no_baseline_and_healthy_scores_is_not_drift(self, database, settings):
        monitor = DriftMonitor(settings)
        await seed_scores(database, "org_1", "faithfulness", [0.9] * 25)
        async with database.session() as session:
            result = await monitor.evaluate(session, "org_1", "faithfulness")
        assert result is not None
        assert result.drifted is False
        assert result.baseline_mean is None

    async def test_too_few_samples_returns_nothing(self, database, settings):
        """A mean over three scores is noise; alerting on it mutes the monitor."""
        settings.drift_min_samples = 20
        await seed_scores(database, "org_1", "faithfulness", [0.2, 0.3, 0.1])
        monitor = DriftMonitor(settings)
        async with database.session() as session:
            assert await monitor.evaluate(session, "org_1", "faithfulness") is None

    async def test_baseline_capture_requires_enough_samples(self, database, settings):
        settings.drift_min_samples = 20
        monitor = DriftMonitor(settings)
        await seed_scores(database, "org_1", "faithfulness", [0.9] * 5)
        async with database.session() as session:
            assert await monitor.capture_baseline(session, "org_1", "faithfulness") is None

    async def test_degradation_against_a_baseline_is_detected(self, database, settings):
        settings.drift_min_samples = 20
        settings.drift_z_threshold = 2.5
        monitor = DriftMonitor(settings)

        # A healthy period, captured deliberately.
        await seed_scores(
            database, "org_1", "faithfulness", [0.90, 0.92, 0.88, 0.91] * 6, hours_ago=20
        )
        async with database.session() as session:
            baseline = await monitor.capture_baseline(session, "org_1", "faithfulness", hours=48)
        assert baseline is not None and baseline.mean == pytest.approx(0.9025, abs=0.01)

        # Then quality falls.
        await seed_scores(
            database, "org_1", "faithfulness", [0.55, 0.60, 0.52, 0.58] * 6, hours_ago=1
        )
        async with database.session() as session:
            result = await monitor.evaluate(session, "org_1", "faithfulness")

        assert result is not None
        assert result.drifted is True
        assert result.z_score is not None and result.z_score > 2.5
        assert "fell to" in result.reason

    async def test_improvement_is_not_drift(self, database, settings):
        """One-sided on purpose: a better model must not page anyone at 3am."""
        settings.drift_min_samples = 20
        monitor = DriftMonitor(settings)
        await seed_scores(
            database, "org_1", "faithfulness", [0.60, 0.62, 0.58, 0.61] * 6, hours_ago=20
        )
        async with database.session() as session:
            await monitor.capture_baseline(session, "org_1", "faithfulness", hours=48)
        await seed_scores(
            database, "org_1", "faithfulness", [0.95, 0.96, 0.94, 0.97] * 6, hours_ago=1
        )
        async with database.session() as session:
            result = await monitor.evaluate(session, "org_1", "faithfulness")
        assert result is not None and result.drifted is False

    async def test_absolute_floor_catches_a_never_good_deployment(self, database, settings):
        """A baseline captured during a bad period never drifts from itself."""
        settings.drift_min_samples = 20
        settings.drift_absolute_floor = 0.60
        monitor = DriftMonitor(settings)
        await seed_scores(
            database, "org_1", "faithfulness", [0.30, 0.32, 0.28, 0.31] * 6, hours_ago=20
        )
        async with database.session() as session:
            await monitor.capture_baseline(session, "org_1", "faithfulness", hours=48)
        await seed_scores(
            database, "org_1", "faithfulness", [0.30, 0.31, 0.29, 0.30] * 6, hours_ago=1
        )
        async with database.session() as session:
            result = await monitor.evaluate(session, "org_1", "faithfulness")
        assert result is not None
        assert result.drifted is True
        assert "absolute floor" in result.reason

    async def test_zero_variance_baseline_does_not_explode(self, database, settings):
        """Identical early scores would otherwise make every z-score infinite."""
        settings.drift_min_samples = 20
        monitor = DriftMonitor(settings)
        await seed_scores(database, "org_1", "faithfulness", [0.8] * 24, hours_ago=20)
        async with database.session() as session:
            baseline = await monitor.capture_baseline(session, "org_1", "faithfulness", hours=48)
        assert baseline is not None and baseline.stddev == 0.0
        await seed_scores(database, "org_1", "faithfulness", [0.79] * 24, hours_ago=1)
        async with database.session() as session:
            result = await monitor.evaluate(session, "org_1", "faithfulness")
        assert result is not None and result.z_score is not None
        assert abs(result.z_score) < 1e6

    async def test_capturing_a_new_baseline_retires_the_old_one(self, database, settings):
        settings.drift_min_samples = 5
        monitor = DriftMonitor(settings)
        await seed_scores(database, "org_1", "faithfulness", [0.9] * 10, hours_ago=2)
        async with database.session() as session:
            await monitor.capture_baseline(session, "org_1", "faithfulness")
        async with database.session() as session:
            await monitor.capture_baseline(session, "org_1", "faithfulness", note="after retrain")

        async with database.session() as session:
            rows = list((await session.execute(select(DriftBaseline))).scalars())
        assert len(rows) == 2, "history is kept, not overwritten"
        assert sum(1 for r in rows if r.is_active) == 1

    async def test_run_once_writes_snapshots_and_alerts(self, database, settings):
        settings.drift_min_samples = 20
        settings.drift_absolute_floor = 0.60
        monitor = DriftMonitor(settings)
        await seed_scores(database, "org_1", "faithfulness", [0.2] * 24, hours_ago=1)
        async with database.session() as session:
            results = await monitor.run_once(session)

        assert any(r.drifted for r in results)
        async with database.session() as session:
            snapshots = list((await session.execute(select(DriftSnapshot))).scalars())
            alerts = list(
                (
                    await session.execute(select(Alert).where(Alert.kind == AlertKind.DRIFT))
                ).scalars()
            )
        assert snapshots and alerts


# --------------------------------------------------------------------------- #
# Retention
# --------------------------------------------------------------------------- #
class TestRetention:
    async def test_old_traces_and_their_children_are_purged(self, database, settings):
        settings.retention_days = 30
        await seed_trace(database, "tr_old", days_ago=45)
        await seed_trace(database, "tr_new", days_ago=1)
        async with database.session() as session:
            session.add(
                Span(
                    trace_id="tr_old",
                    span_id="sp_old",
                    tenant_id="org_1",
                    kind="llm",
                    name="x",
                    status="ok",
                    input={},
                    output={},
                    attributes={},
                    created_at=datetime.now(UTC) - timedelta(days=45),
                )
            )
            session.add(
                GuardrailFinding(
                    trace_id="tr_old",
                    span_id="sp_old",
                    tenant_id="org_1",
                    detector="pii",
                    finding_type="EMAIL_ADDRESS",
                    severity="medium",
                    score=0.9,
                    field="input.q",
                    excerpt="a***@b.com",
                    created_at=datetime.now(UTC) - timedelta(days=45),
                )
            )

        report = await retention.purge(database, settings)

        async with database.session() as session:
            traces = {t.trace_id for t in (await session.execute(select(Trace))).scalars()}
            spans = list((await session.execute(select(Span))).scalars())
            findings = list((await session.execute(select(GuardrailFinding))).scalars())
        assert traces == {"tr_new"}
        assert spans == [] and findings == []
        assert report.deleted["traces"] == 1

    async def test_purge_is_a_no_op_when_nothing_is_old(self, database, settings):
        settings.retention_days = 30
        await seed_trace(database, "tr_recent", days_ago=1)
        report = await retention.purge(database, settings)
        assert report.total == 0

    async def test_audit_log_outlives_the_data_it_describes(self, database, settings):
        """The compliance artifact is small and must survive the trace."""
        from obs_platform.models import AuditLog

        settings.retention_days = 10
        async with database.session() as session:
            session.add(
                AuditLog(
                    actor_email="a@b.com",
                    action="trace.read",
                    resource_type="trace",
                    resource_id="tr_old",
                    detail={},
                    created_at=datetime.now(UTC) - timedelta(days=20),
                )
            )
        await retention.purge(database, settings)
        async with database.session() as session:
            rows = list((await session.execute(select(AuditLog))).scalars())
        assert len(rows) == 1, "audit rows must not be purged on the trace schedule"

    async def test_batching_handles_more_rows_than_one_batch(self, database, settings):
        settings.retention_days = 1
        settings.retention_batch_size = 3
        for index in range(7):
            await seed_trace(database, f"tr_bulk_{index}", days_ago=10)
        report = await retention.purge(database, settings)
        assert report.deleted["traces"] == 7
        assert report.batches >= 3


# --------------------------------------------------------------------------- #
# Scheduler
# --------------------------------------------------------------------------- #
class TestScheduler:
    async def test_jobs_run_and_record_their_state(self, database, settings):
        settings.drift_enabled = True
        scheduler = Scheduler(settings=settings, database=database)
        ran = await scheduler.tick()
        assert "drift_monitor" in ran
        assert "retention_purge" in ran

        async with database.session() as session:
            rows = {r.job_name: r for r in (await session.execute(select(JobRun))).scalars()}
        assert rows["drift_monitor"].last_status == "ok"
        assert rows["drift_monitor"].last_finished_at is not None

    async def test_a_job_is_not_rerun_before_its_interval(self, database, settings):
        scheduler = Scheduler(settings=settings, database=database)
        await scheduler.tick()
        assert await scheduler.tick() == []

    async def test_overdue_job_runs_immediately_on_wake(self, database, settings):
        """The free tier sleeps; a timer that only ticks while awake skips windows."""
        settings.drift_interval_seconds = 300
        scheduler = Scheduler(settings=settings, database=database)
        await scheduler.tick()

        async with database.session() as session:
            row = (
                await session.execute(select(JobRun).where(JobRun.job_name == "drift_monitor"))
            ).scalar_one()
            row.last_finished_at = datetime.now(UTC) - timedelta(hours=2)

        assert "drift_monitor" in await scheduler.tick()

    async def test_a_failing_job_does_not_advance_the_schedule(self, database, settings):
        """A job that failed must retry, not look 'recently run' forever."""
        scheduler = Scheduler(settings=settings, database=database)

        async def boom() -> dict:
            raise RuntimeError("neon unreachable")

        scheduler.jobs = [
            j.__class__(name="drift_monitor", interval_seconds=300, run=boom)
            for j in scheduler.jobs[:1]
        ]
        await scheduler.tick()

        async with database.session() as session:
            row = (
                await session.execute(select(JobRun).where(JobRun.job_name == "drift_monitor"))
            ).scalar_one()
        assert row.last_status == "error"
        assert row.last_finished_at is None
        assert "neon unreachable" in (row.last_error or "")
        assert await scheduler.tick() == ["drift_monitor"], "a failed job must retry"

    async def test_one_failing_job_does_not_block_the_others(self, database, settings):
        scheduler = Scheduler(settings=settings, database=database)

        async def boom() -> dict:
            raise RuntimeError("nope")

        scheduler.jobs[0].run = boom
        ran = await scheduler.tick()
        assert len(ran) == len([j for j in scheduler.jobs if j.enabled])

    async def test_dead_letters_raise_an_alert(self, database, settings):
        from obs_platform.models import DeadLetter

        async with database.session() as session:
            session.add(
                DeadLetter(
                    stream="obs:events",
                    consumer_group="obs-storage",
                    message_id="1-1",
                    error_type="ValueError",
                    error_message="bad",
                    payload={},
                )
            )
        scheduler = Scheduler(settings=settings, database=database)
        await scheduler._execute(next(j for j in scheduler.jobs if j.name == "dead_letter_watch"))

        async with database.session() as session:
            alerts = list(
                (
                    await session.execute(select(Alert).where(Alert.kind == AlertKind.DEAD_LETTER))
                ).scalars()
            )
        assert alerts


# --------------------------------------------------------------------------- #
# Metrics API
# --------------------------------------------------------------------------- #
class TestMetricsApi:
    async def test_overview(self, client, database):
        for index, latency in enumerate([100, 200, 300, 400, 900]):
            await seed_trace(
                database,
                f"tr_m{index}",
                latency_ms=latency,
                status="error" if index == 4 else "ok",
                flagged=index == 0,
            )
        await seed_scores(database, "org_1", "faithfulness", [0.8, 0.9])

        body = (await client.get("/v1/metrics/overview?hours=24")).json()
        assert body["traces"] == 5
        assert body["errors"] == 1
        assert body["error_rate"] == 0.2
        assert body["flagged"] == 1
        assert body["p50_latency_ms"] == 300
        assert body["p95_latency_ms"] == 900
        assert body["cost_usd"] == pytest.approx(0.0005)
        assert body["eval_scores"]["faithfulness"] == pytest.approx(0.85)

    async def test_overview_scopes_to_tenant(self, client, database):
        await seed_trace(database, "tr_a", tenant_id="org_1")
        await seed_trace(database, "tr_b", tenant_id="org_2")
        assert (await client.get("/v1/metrics/overview?tenant_id=org_2")).json()["traces"] == 1

    async def test_empty_window_returns_zeros_not_an_error(self, client):
        body = (await client.get("/v1/metrics/overview?hours=1")).json()
        assert body["traces"] == 0
        assert body["error_rate"] == 0.0
        assert body["p50_latency_ms"] is None

    async def test_timeseries_buckets(self, client, database):
        for index in range(4):
            await seed_trace(database, f"tr_ts{index}", latency_ms=100 * (index + 1))
        series = (await client.get("/v1/metrics/timeseries?hours=24&bucket=hour")).json()
        assert series
        assert sum(point["traces"] for point in series) == 4
        assert all(point["p50_latency_ms"] is not None for point in series)


# --------------------------------------------------------------------------- #
# Alerts API
# --------------------------------------------------------------------------- #
class TestAlertsApi:
    async def test_list_and_acknowledge(self, client, database):
        async with database.session() as session:
            session.add(
                Alert(
                    tenant_id="org_1",
                    kind=AlertKind.GUARDRAIL,
                    severity="critical",
                    title="PII: CREDIT_CARD in output.answer",
                    detail={},
                    status="open",
                    dedupe_key="k1",
                )
            )
        listing = (await client.get("/v1/alerts")).json()
        assert listing["items"][0]["severity"] == "critical"
        alert_id = listing["items"][0]["id"]

        acked = (await client.post(f"/v1/alerts/{alert_id}/acknowledge?resolve=true")).json()
        assert acked["status"] == "resolved"
        assert acked["acknowledged_by"]
        assert (await client.get("/v1/alerts?status=open")).json()["items"] == []

    async def test_acknowledging_a_missing_alert_is_404(self, client):
        assert (await client.post("/v1/alerts/999999/acknowledge")).status_code == 404

    async def test_baseline_capture_refuses_without_enough_data(self, client, settings):
        settings.drift_min_samples = 20
        response = await client.post(
            "/v1/drift/baseline",
            json={"tenant_id": "org_1", "metric": "faithfulness", "hours": 24},
        )
        assert response.status_code == 409
        assert "at least 20 scores" in response.json()["error"]["message"]

    async def test_baseline_capture_succeeds_with_data(self, client, database, settings):
        settings.drift_min_samples = 5
        await seed_scores(database, "org_1", "faithfulness", [0.9] * 10)
        response = await client.post(
            "/v1/drift/baseline",
            json={
                "tenant_id": "org_1",
                "metric": "faithfulness",
                "hours": 24,
                "note": "post-launch healthy window",
            },
        )
        assert response.status_code == 200
        assert response.json()["sample_count"] == 10

        baselines = (await client.get("/v1/drift/baselines")).json()
        assert baselines[0]["note"] == "post-launch healthy window"
