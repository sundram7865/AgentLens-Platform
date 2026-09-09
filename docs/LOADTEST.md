# Load test report

Every number here was produced by `loadtest/ingest_load.py` against real
Postgres 16 and Redis 7 (the `docker compose` stack), not estimated. Raw JSON
output is in `loadtest/results/`.

**Environment.** Windows 11, Python 3.12.6, Docker Desktop with Postgres 16 and
Redis 7 in Linux containers, all four consumer roles in one worker process,
producer and consumers on the same machine. That last point matters: this is a
single-box measurement, so it under-states network latency and over-states
inter-process contention compared to a real Render + Neon + Upstash deployment.

Reproduce:

```bash
docker compose up -d postgres redis
python -m obs_platform.workers.cli --roles storage,guardrail,eval,scheduler
python loadtest/ingest_load.py --traces 1500 --spans 6 --concurrency 30 --rate 4
```

---

## 1. The number that matters: SDK overhead in the agent's request path

This is the claim the whole project rests on: *watching SupportPilot must not
slow SupportPilot down*. It is the wall time the agent's own thread spends
inside `publish()`, not the end-to-end pipeline.

| percentile | overhead |
|---|---|
| p50 | **2.5 µs** |
| p95 | **10.4 µs** |
| p99 | **29.0 µs** |
| max | 561 µs |

For context: a SupportPilot agent run makes several LLM calls and takes on the
order of **1–3 seconds**. It emits ~8 events. Total observability cost to that
request is roughly **20–80 microseconds**, or about **0.003%** of the run.

That number is low because `publish()` does exactly two things: build a dict and
`put_nowait` onto a bounded queue. Every socket write happens on a daemon thread
the request never waits for. The p99 of 29 µs is queue contention under 30
concurrent producers; the 561 µs max is one GC pause.

---

## 2. Sustained throughput (paced, realistic)

1,500 traces × 8 events, 30 concurrent agent runs, paced to model real ticket
handling rather than a synthetic flood.

| metric | value |
|---|---|
| events published | 12,000 |
| events dropped | **0** |
| loss rate | **0.000000** |
| sustained ingest | 744 events/s |
| peak consumer lag | 7,050 messages |

Zero loss at this rate. For scale, 744 events/s is roughly **93 agent runs per
second**, or about 8 million tickets a day, several orders of magnitude beyond
what a portfolio deployment or a mid-size support desk produces.

## 3. Consumer drain, measured per group

Combining the groups into one number would be misleading, because they are
*supposed* to behave differently.

| consumer group | drain time | note |
|---|---|---|
| `obs-guardrail` | 12.4 s | Scans every event; CPU-bound, no LLM calls |
| `obs-storage` | 66.8 s | The durable write path |
| `obs-eval` | 30.4 s | Holds messages unacked **on purpose** while batching |

`obs-eval` being slower is not a problem to fix; it is the batching that makes
LLM-judge scoring affordable. Reporting one combined drain number would have
made the storage writer look several times slower than it is, which is why the
harness measures each group separately.

**Postgres write throughput: 134.7 rows/s** on the durable path (9,000 spans),
single worker, no tuning, on a laptop running the database in a container.

---

## 4. Where it breaks, and how

An unthrottled burst is a different test, and worth running because it finds the
safety valve rather than the steady state.

| mode | ingest rate | dropped | loss rate |
|---|---|---|---|
| paced (744/s) | 744 events/s | 0 | 0% |
| burst (unthrottled) | 35,010 events/s | 5,950 of 16,000 | **37%** |

At 35k events/s the SDK's bounded 10,000-event queue fills and the publisher
**sheds load**: it drops the event, increments a counter, logs with geometric
backoff, and returns immediately. It does not block the caller and does not grow
memory.

That is the designed behaviour and the correct trade. The alternative (an
unbounded queue) converts a Redis outage into an out-of-memory kill of
SupportPilot itself, which is precisely the failure this architecture exists to
prevent. **Losing observability data is always preferable to losing the ticket.**

Drops are visible, not silent: `Observability.stats()` exposes
`dropped_queue_full`, and the log line names the count. If it happens in
production, raise `OBS_QUEUE_SIZE` or add a worker replica.

---

## 5. Two things the load test found that review did not

### A regression I introduced while "optimizing"

Batching the per-span upserts into one statement was a genuine win. Encouraged
by that, I also collapsed the per-trace rollup `UPDATE`s into a single statement
using correlated subqueries. It looked strictly better: eleven round trips
became one.

It measured **25% slower**:

| rollup form | write throughput |
|---|---|
| GROUP BY + one UPDATE per trace | **134.7 rows/s** |
| single UPDATE, correlated subqueries | 91.4 rows/s |

The correlated form runs eight aggregate subqueries *per trace row* instead of
one shared `GROUP BY` pass over the batch. Round trips were never the
bottleneck; repeatedly scanning `spans` was. Reverted, and the reason is now a
comment in `storage_writer.py` so nobody re-applies it.

The general lesson is the reason this report exists: "fewer round trips" is a
heuristic, not a measurement.

### A monitoring endpoint that lied

`/health/meta` reported `"database unreachable"` against a completely healthy
database. It read ORM rows after its read-only session had rolled back, raised
`DetachedInstanceError`, caught it in a broad `except`, and reported the generic
message. An endpoint whose entire job is telling you which dependency broke was
naming the wrong one. Fixed by selecting columns inside the session, and by
reporting the actual exception type instead of a guess.

---

## 6. Honest limitations

* **Single box.** Producer, consumers and databases share a machine. A real
  deployment adds network latency (Render → Neon is a WAN hop) and removes CPU
  contention. Expect lower throughput and higher per-write latency in
  production, and re-run this there before quoting numbers.
* **One worker process.** All roles share one event loop. The consumer-group
  design supports horizontal scaling (`hostname:pid` consumer names, `XCLAIM`
  recovery), but that was not measured here.
* **The eval scorer used the heuristic backend.** No LLM API calls were made, so
  these numbers do not include judge latency. That is intentional: judge latency
  is provider latency, not platform latency, and it is off the critical path by
  construction.
* **Upstash's free tier is 10,000 commands/day**, which the paced test would
  exhaust in about 13 seconds. The free-tier deployment is sized for demo
  traffic, not this load; see `docs/DEPLOY.md` for the command-budget maths.
* **No sustained soak.** The longest run was ~2 minutes. Connection-pool
  exhaustion and memory growth over hours are unmeasured.

---

## 7. Summary for a résumé line

> Built an observability platform whose client SDK adds **p99 29 µs** to the
> host application's request path, sustains **744 events/s with zero loss**, and
> sheds load rather than blocking when its bounded queue fills. Load testing
> caught a 25% throughput regression in an optimization that looked correct by
> inspection.
