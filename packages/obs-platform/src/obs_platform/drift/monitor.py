"""Drift monitor.

A **scheduled process**, not another stream consumer. Drift is a property of a
window of scores, not of any single event; hanging it off the event stream would
mean recomputing the same window on every message and make its cadence depend on
traffic volume, which is exactly backwards -- you most want a drift check on the
quiet day when nobody is watching.

Two independent triggers, because they catch different failures:

**Relative (z-score)** -- the window mean has moved away from a *captured*
baseline. Catches "the model got worse than it used to be for this tenant",
whatever good meant for them.

**Absolute floor** -- the window mean is below a hard floor regardless of the
baseline. Catches a deployment that was never good: a baseline captured during a
bad period makes "consistently poor" look perfectly healthy, because it never
drifts from itself.

The baseline is captured **deliberately**, from a window an operator has looked
at and judged healthy. A baseline that recomputes itself from recent data cannot
detect drift at all -- it drifts along with it.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from obs_sdk.schema import Severity

from ..alerts import AlertKind, drift_dedupe_key, raise_alert
from ..evals.judge import METRICS
from ..logging import get_logger
from ..models import DriftBaseline, DriftSnapshot, EvalScore
from ..settings import Settings

log = get_logger("obs_platform.drift")

# A baseline with (near) zero variance would make every z-score infinite. This
# floor says "scores are never more precise than +/- 0.02" and keeps a tenant
# whose first 20 scores happened to be identical from alerting on every window.
MIN_STDDEV = 0.02


@dataclass
class DriftResult:
    tenant_id: str
    metric: str
    window_start: datetime
    window_end: datetime
    mean: float
    stddev: float
    sample_count: int
    baseline_mean: float | None = None
    baseline_stddev: float | None = None
    z_score: float | None = None
    drifted: bool = False
    reason: str = ""

    def as_row(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "metric": self.metric,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "mean": self.mean,
            "stddev": self.stddev,
            "sample_count": self.sample_count,
            "baseline_mean": self.baseline_mean,
            "baseline_stddev": self.baseline_stddev,
            "z_score": self.z_score,
            "drifted": self.drifted,
            "created_at": datetime.now(UTC),
        }


class DriftMonitor:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    # -- baselines ---------------------------------------------------------
    async def capture_baseline(
        self,
        session: AsyncSession,
        tenant_id: str,
        metric: str,
        hours: int = 24,
        note: str = "",
    ) -> DriftBaseline | None:
        """Freeze the current window as the healthy reference for a tenant/metric."""
        window_end = datetime.now(UTC)
        window_start = window_end - timedelta(hours=hours)
        scores = await self._scores(session, tenant_id, metric, window_start, window_end)
        if len(scores) < self.settings.drift_min_samples:
            log.info(
                "drift.baseline_too_few_samples",
                tenant_id=tenant_id,
                metric=metric,
                have=len(scores),
                need=self.settings.drift_min_samples,
            )
            return None

        # Retire the previous baseline instead of deleting it: comparing today's
        # alert against the baseline that was active when it fired is the first
        # question anyone asks.
        for previous in await self._active_baselines(session, tenant_id, metric):
            previous.is_active = False

        baseline = DriftBaseline(
            tenant_id=tenant_id,
            metric=metric,
            mean=round(statistics.fmean(scores), 6),
            stddev=round(_stddev(scores), 6),
            sample_count=len(scores),
            window_start=window_start,
            window_end=window_end,
            is_active=True,
            note=note[:500],
        )
        session.add(baseline)
        log.info(
            "drift.baseline_captured",
            tenant_id=tenant_id,
            metric=metric,
            mean=baseline.mean,
            stddev=baseline.stddev,
            samples=baseline.sample_count,
        )
        return baseline

    async def _active_baselines(
        self, session: AsyncSession, tenant_id: str, metric: str
    ) -> list[DriftBaseline]:
        return list(
            (
                await session.execute(
                    select(DriftBaseline).where(
                        DriftBaseline.tenant_id == tenant_id,
                        DriftBaseline.metric == metric,
                        DriftBaseline.is_active.is_(True),
                    )
                )
            ).scalars()
        )

    # -- evaluation --------------------------------------------------------
    async def evaluate(
        self, session: AsyncSession, tenant_id: str, metric: str
    ) -> DriftResult | None:
        window_end = datetime.now(UTC)
        window_start = window_end - timedelta(hours=self.settings.drift_window_hours)
        scores = await self._scores(session, tenant_id, metric, window_start, window_end)
        if len(scores) < self.settings.drift_min_samples:
            # A mean over three scores is noise, and alerting on noise is how a
            # monitor gets muted.
            return None

        mean = round(statistics.fmean(scores), 6)
        result = DriftResult(
            tenant_id=tenant_id,
            metric=metric,
            window_start=window_start,
            window_end=window_end,
            mean=mean,
            stddev=round(_stddev(scores), 6),
            sample_count=len(scores),
        )

        baselines = await self._active_baselines(session, tenant_id, metric)
        if baselines:
            baseline = baselines[0]
            spread = max(baseline.stddev, MIN_STDDEV)
            standard_error = spread / math.sqrt(len(scores))
            # One-sided: only a DROP is drift. A model that got better does not
            # need to page anyone at 3am.
            z = (baseline.mean - mean) / standard_error if standard_error else 0.0
            result.baseline_mean = baseline.mean
            result.baseline_stddev = baseline.stddev
            result.z_score = round(z, 4)
            if z >= self.settings.drift_z_threshold:
                result.drifted = True
                result.reason = (
                    f"{metric} fell to {mean:.3f} from a baseline of "
                    f"{baseline.mean:.3f} (z={z:.2f} over {len(scores)} scores)"
                )

        if not result.drifted and mean < self.settings.drift_absolute_floor:
            result.drifted = True
            result.reason = (
                f"{metric} is {mean:.3f}, below the absolute floor of "
                f"{self.settings.drift_absolute_floor:.2f} over {len(scores)} scores"
            )
        return result

    async def run_once(self, session: AsyncSession) -> list[DriftResult]:
        """Evaluate every tenant and metric that has recent scores."""
        results: list[DriftResult] = []
        for tenant_id in await self._active_tenants(session):
            for metric in METRICS:
                result = await self.evaluate(session, tenant_id, metric)
                if result is None:
                    continue
                session.add(DriftSnapshot(**result.as_row()))
                results.append(result)
                if result.drifted:
                    await self._alert(session, result)
        return results

    async def _alert(self, session: AsyncSession, result: DriftResult) -> None:
        await raise_alert(
            session,
            tenant_id=result.tenant_id,
            kind=AlertKind.DRIFT,
            severity=Severity.HIGH if result.mean < 0.5 else Severity.MEDIUM,
            title=f"Quality drift: {result.reason}",
            detail={
                "metric": result.metric,
                "window_mean": result.mean,
                "window_stddev": result.stddev,
                "sample_count": result.sample_count,
                "baseline_mean": result.baseline_mean,
                "baseline_stddev": result.baseline_stddev,
                "z_score": result.z_score,
                "window_hours": self.settings.drift_window_hours,
            },
            # One alert per tenant/metric/day: drift persists for hours, and a
            # new row every five minutes would bury everything else.
            dedupe_key=drift_dedupe_key(result.tenant_id, result.metric),
        )

    # -- queries -----------------------------------------------------------
    async def _scores(
        self,
        session: AsyncSession,
        tenant_id: str,
        metric: str,
        window_start: datetime,
        window_end: datetime,
    ) -> list[float]:
        rows = (
            await session.execute(
                select(EvalScore.score).where(
                    EvalScore.tenant_id == tenant_id,
                    EvalScore.metric == metric,
                    EvalScore.created_at >= window_start,
                    EvalScore.created_at <= window_end,
                )
            )
        ).scalars()
        return [float(value) for value in rows]

    async def _active_tenants(self, session: AsyncSession) -> list[str]:
        cutoff = datetime.now(UTC) - timedelta(hours=self.settings.drift_window_hours)
        rows = (
            await session.execute(
                select(EvalScore.tenant_id)
                .where(EvalScore.created_at >= cutoff)
                .group_by(EvalScore.tenant_id)
                .having(func.count() >= self.settings.drift_min_samples)
            )
        ).scalars()
        return [str(value) for value in rows]


def _stddev(values: list[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


__all__ = ["MIN_STDDEV", "DriftMonitor", "DriftResult"]
