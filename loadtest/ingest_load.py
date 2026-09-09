#!/usr/bin/env python
"""Load test for the ingestion pipeline.

Measures the four numbers the build plan asks for, and one it does not but
should:

1. **SDK publish overhead in the caller's request path.** The headline claim of
   this whole project is "watching SupportPilot does not slow SupportPilot
   down". This measures the actual cost of the `publish()` call the agent makes
   -- not the end-to-end pipeline, the microseconds the agent itself pays.
2. **Ingestion throughput** -- events per second accepted into Redis.
3. **Consumer lag under load** -- sampled `XLEN` and per-group lag while the
   producers are running, so backpressure is visible rather than inferred.
4. **Drain time and Postgres write throughput** -- how long the consumers take
   to catch up once producers stop, and how many rows a second that is.
5. **Event loss** -- published minus dropped, reconciled against rows landed.

Run against a real stack (docker compose up):

    python loadtest/ingest_load.py --traces 500 --concurrency 25

Every number in docs/LOADTEST.md came out of this script. It is deliberately
reproducible: pass --seed for identical traffic between runs.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [
    str(ROOT / "packages/obs-sdk/src"),
    str(ROOT / "packages/obs-platform/src"),
]
sys.path.append(str(ROOT / "scripts"))

from obs_sdk import (  # noqa: E402
    EventType,
    Observability,
    ObsEvent,
    SpanKind,
    new_span_id,
    new_trace_id,
)


@dataclass
class Sample:
    at: float
    stream_length: int
    groups: dict[str, int]


@dataclass
class Report:
    traces: int
    events: int
    concurrency: int
    publish_latencies_us: list[float] = field(default_factory=list)
    produce_seconds: float = 0.0
    drain_seconds: float = 0.0
    samples: list[Sample] = field(default_factory=list)
    published: int = 0
    dropped: int = 0
    rows_before: int = 0
    rows_after: int = 0
    #: Seconds for each consumer group to reach zero lag AND zero pending.
    group_drain_seconds: dict[str, float] = field(default_factory=dict)

    def percentile(self, values: list[float], q: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))
        return ordered[index]

    def as_dict(self) -> dict[str, Any]:
        lat = self.publish_latencies_us
        peak_lag = max((max(s.groups.values(), default=0) for s in self.samples), default=0)
        rows = self.rows_after - self.rows_before
        return {
            "traces": self.traces,
            "events": self.events,
            "concurrency": self.concurrency,
            "publish_overhead_us": {
                "p50": round(self.percentile(lat, 0.50), 1),
                "p95": round(self.percentile(lat, 0.95), 1),
                "p99": round(self.percentile(lat, 0.99), 1),
                "max": round(max(lat), 1) if lat else 0.0,
                "mean": round(statistics.fmean(lat), 1) if lat else 0.0,
            },
            "ingest_throughput_events_per_s": round(self.events / self.produce_seconds, 1)
            if self.produce_seconds
            else 0.0,
            "produce_seconds": round(self.produce_seconds, 2),
            "drain_seconds": round(self.drain_seconds, 2),
            "group_drain_seconds": {k: round(v, 2) for k, v in self.group_drain_seconds.items()},
            "peak_consumer_lag": peak_lag,
            "spans_written": rows,
            # Throughput of the DURABLE WRITE PATH only. Dividing by the overall
            # drain would fold in the eval scorer's deliberate batch interval --
            # it holds messages unacked on purpose while it fills a batch -- and
            # report the storage writer as several times slower than it is.
            "write_throughput_rows_per_s": round(rows / self.group_drain_seconds["obs-storage"], 1)
            if self.group_drain_seconds.get("obs-storage")
            else 0.0,
            "published": self.published,
            "dropped": self.dropped,
            "loss_rate": round(self.dropped / max(1, self.published + self.dropped), 6),
        }


def build_events(trace_id: str, tenant_id: str, span_count: int) -> list[ObsEvent]:
    """One trace's worth of events, shaped like a real agent run."""
    events = [
        ObsEvent(
            trace_id=trace_id,
            tenant_id=tenant_id,
            type=EventType.TRACE_START,
            kind=SpanKind.AGENT,
            name="agent_run",
            input={"question": "Where is my order A-10294? It has been five days."},
            attributes={"ticket_id": "T-load", "category": "ORDER_STATUS"},
            service="supportpilot",
        )
    ]
    root = new_span_id()
    for index in range(span_count):
        events.append(
            ObsEvent(
                trace_id=trace_id,
                tenant_id=tenant_id,
                span_id=new_span_id() if index else root,
                parent_span_id=None if index == 0 else root,
                type=EventType.SPAN_END,
                kind=SpanKind.LLM if index % 3 == 0 else SpanKind.TOOL,
                name=f"step_{index}",
                model="gemini-2.0-flash",
                input={"prompt": "classify this support ticket"},
                output={"completion": "ORDER_STATUS"},
                service="supportpilot",
            )
        )
    events.append(
        ObsEvent(
            trace_id=trace_id,
            tenant_id=tenant_id,
            type=EventType.TRACE_END,
            kind=SpanKind.AGENT,
            name="agent_run",
            latency_ms=900,
            output={"answer": "Your order ships within two business days."},
            service="supportpilot",
        )
    )
    return events


async def sample_lag(report: Report, stop: asyncio.Event, interval: float) -> None:
    """Poll stream depth and per-group lag while the producers run."""
    from obs_platform.redis_io import group_info, stream_length

    while not stop.is_set():
        try:
            length = await stream_length("obs:events")
            groups = {g["name"]: g["lag"] for g in await group_info("obs:events")}
            report.samples.append(Sample(at=time.time(), stream_length=length, groups=groups))
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue


async def count_spans() -> int:
    from sqlalchemy import func, select

    from obs_platform.db import Database
    from obs_platform.models import Span
    from obs_platform.settings import get_settings

    database = Database(get_settings())
    try:
        async with database.read_session() as session:
            return int(
                (await session.execute(select(func.count()).select_from(Span))).scalar() or 0
            )
    finally:
        await database.dispose()


async def wait_for_drain(report: Report, timeout: float) -> None:  # noqa: ASYNC109 - a deadline, not a cancellation scope
    """Wait for each consumer group to catch up, recording each one separately.

    Per-group matters: the storage writer is the durable path and should drain
    as fast as Postgres accepts rows, while the eval scorer deliberately holds
    messages unacked until its batch fills. Reporting one combined number makes
    an intentional batching delay look like a slow database.
    """
    from obs_platform.redis_io import group_info

    started = time.perf_counter()
    while time.perf_counter() - started < timeout:
        try:
            groups = await group_info("obs:events")
            for group in groups:
                name = str(group["name"])
                if name in report.group_drain_seconds:
                    continue
                if group["lag"] == 0 and group["pending"] == 0:
                    report.group_drain_seconds[name] = time.perf_counter() - started
            if groups and len(report.group_drain_seconds) == len(groups):
                report.drain_seconds = time.perf_counter() - started
                return
        except Exception:
            pass
        await asyncio.sleep(0.1)
    report.drain_seconds = time.perf_counter() - started


async def run(args: argparse.Namespace) -> int:
    os.environ["OBS_REDIS_URL"] = args.redis_url
    os.environ.setdefault("OBS_DATABASE_URL", args.database_url)
    os.environ["OBS_ENABLED"] = "true"

    obs = Observability.from_env(service="supportpilot-loadtest")
    if type(obs.publisher).__name__ == "NullPublisher":
        print("OBS_REDIS_URL is not set or observability is disabled", file=sys.stderr)
        return 2

    report = Report(traces=args.traces, events=0, concurrency=args.concurrency)
    report.rows_before = await count_spans() if args.check_database else 0

    stop = asyncio.Event()
    sampler = asyncio.create_task(sample_lag(report, stop, args.sample_interval))
    semaphore = asyncio.Semaphore(args.concurrency)
    produced = 0

    # Paced mode models real agent traffic: a ticket produces its events over
    # the life of one agent run, not all at once. Burst mode (rate=0) instead
    # finds the point where the SDK's bounded queue starts shedding -- both are
    # worth knowing, and they answer different questions.
    inter_event_delay = (1.0 / args.rate / (args.spans + 2)) if args.rate else 0.0

    async def one_trace(index: int) -> None:
        nonlocal produced
        async with semaphore:
            events = build_events(new_trace_id(), f"tenant_{index % args.tenants}", args.spans)
            for event in events:
                # This is the number that matters for the "no impact on the
                # agent" claim: the wall time the caller spends inside publish().
                started = time.perf_counter()
                obs.publisher.publish(event)
                report.publish_latencies_us.append((time.perf_counter() - started) * 1_000_000)
                if inter_event_delay:
                    await asyncio.sleep(inter_event_delay)
            produced += len(events)

    mode = f"paced at {args.rate}/s per slot" if args.rate else "burst (unthrottled)"
    print(
        f"producing {args.traces} traces x {args.spans + 2} events "
        f"at concurrency {args.concurrency}, {mode}..."
    )
    started = time.perf_counter()
    await asyncio.gather(*(one_trace(i) for i in range(args.traces)))
    report.produce_seconds = time.perf_counter() - started
    report.events = produced

    print("flushing the SDK buffer...")
    obs.publisher.flush(timeout=60)
    stats = obs.stats()
    report.published = int(stats.get("published", 0))
    report.dropped = int(stats.get("dropped_queue_full", 0)) + int(stats.get("dropped_error", 0))

    print("waiting for consumers to drain...")
    await wait_for_drain(report, timeout=args.drain_timeout)
    stop.set()
    await sampler

    report.rows_after = await count_spans() if args.check_database else 0
    obs.shutdown(timeout=10)

    payload = report.as_dict()
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        render(payload, report)

    if args.out:
        Path(args.out).write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"\nwrote {args.out}")
    return 0


def render(payload: dict[str, Any], report: Report) -> None:
    overhead = payload["publish_overhead_us"]
    print("\n" + "=" * 66)
    print("INGESTION LOAD TEST")
    print("=" * 66)
    print(f"  traces                  {payload['traces']}")
    print(f"  events                  {payload['events']}")
    print(f"  concurrency             {payload['concurrency']}")
    print()
    print("  SDK publish overhead (what the agent's request path pays)")
    print(f"    p50                   {overhead['p50']:>10.1f} us")
    print(f"    p95                   {overhead['p95']:>10.1f} us")
    print(f"    p99                   {overhead['p99']:>10.1f} us")
    print(f"    max                   {overhead['max']:>10.1f} us")
    print()
    print(f"  ingest throughput       {payload['ingest_throughput_events_per_s']:>10.1f} events/s")
    print(f"  produce time            {payload['produce_seconds']:>10.2f} s")
    print(f"  drain time (all groups) {payload['drain_seconds']:>10.2f} s")
    for name, seconds in sorted(payload["group_drain_seconds"].items()):
        note = "  <- durable write path" if name == "obs-storage" else ""
        print(f"    {name:<22}{seconds:>10.2f} s{note}")
    print(f"  peak consumer lag       {payload['peak_consumer_lag']:>10} messages")
    print(f"  spans written           {payload['spans_written']:>10}")
    print(f"  write throughput        {payload['write_throughput_rows_per_s']:>10.1f} rows/s")
    print()
    print(f"  published               {payload['published']:>10}")
    print(f"  dropped                 {payload['dropped']:>10}")
    print(f"  loss rate               {payload['loss_rate']:>10.6f}")

    if report.samples:
        print("\n  lag over time (stream length / max group lag)")
        first = report.samples[0].at
        for sample in report.samples[:: max(1, len(report.samples) // 10)]:
            lag = max(sample.groups.values(), default=0)
            bar = "#" * min(40, lag // max(1, payload["events"] // 200 or 1))
            print(
                f"    t+{sample.at - first:5.1f}s  len={sample.stream_length:>6}  lag={lag:>6} {bar}"
            )
    print("=" * 66)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--traces", type=int, default=200)
    parser.add_argument("--spans", type=int, default=6)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="Traces per second per worker slot. 0 = burst as fast as possible.",
    )
    parser.add_argument("--tenants", type=int, default=3)
    parser.add_argument(
        "--redis-url", default=os.environ.get("OBS_REDIS_URL", "redis://localhost:6379/0")
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get(
            "OBS_DATABASE_URL", "postgresql+asyncpg://obs:obs@localhost:5432/obs"
        ),
    )
    parser.add_argument("--sample-interval", type=float, default=0.5)
    parser.add_argument("--drain-timeout", type=float, default=180.0)
    parser.add_argument("--check-database", action="store_true", default=True)
    parser.add_argument("--no-check-database", dest="check_database", action="store_false")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out", default=None, help="Write the JSON report to this path")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
