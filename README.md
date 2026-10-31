# AgentLens

Traces, PII and prompt-injection guardrails, LLM-judge evaluation and quality-drift
detection for LLM agents. Built to watch
[SupportPilot](https://github.com/sundram7865/SupportPilot) (live:
[supportpilot-tau.vercel.app](https://supportpilot-tau.vercel.app)), an agentic
customer-support platform, and usable with any LangChain or LangGraph agent.

[![CI](https://github.com/sundram7865/AgentLens-Platform/actions/workflows/ci.yml/badge.svg)](https://github.com/sundram7865/AgentLens-Platform/actions/workflows/ci.yml)

The client SDK costs the watched application **about 2 µs per event at the
median and under 50 µs at p99**. A 12,000-event burst arrived with **every span
reconciled in Postgres**, and when producers outrun it the SDK sheds load rather
than blocking. Every number in this repository has its raw output in
[`docs/evidence/`](docs/evidence/); [`docs/LOADTEST.md`](docs/LOADTEST.md) has the
method, the caveats, and the deadlock that measuring it end to end uncovered.

```
SupportPilot (or any agent)
  └─ obs-sdk  ── XADD ──▶ Redis Stream ──┬──▶ storage writer  ──▶ Postgres
     2 deps                              ├──▶ guardrail scanner   (traces, spans,
     never raises                        └──▶ eval scorer          findings, scores)
     never blocks                                                       │
                                          scheduler ── drift ───────────┤
                                                    └─ retention        │
                                                                        ▼
                                          Next.js dashboard ◀── read-only API
```

---

## Why it is built this way

Most of the interesting decisions here are about failure, not features.

**The SDK cannot hurt the app it watches.** `publish()` appends to a bounded
queue and returns; every socket write happens on a daemon thread. Redis being
down means dropped events and a log line, never a failed ticket. Every callback
is wrapped, so a bug in the SDK degrades observability rather than breaking a
customer's support request. It survives `fork()` too: a child process gets a
fresh queue and thread instead of a lock nobody will ever release.

**Redelivery is assumed, not feared.** Redis Streams are at-least-once, so a
restarted consumer *will* see the same message twice. `spans` carries a unique
constraint on `(trace_id, span_id)`; span starts insert with `ON CONFLICT DO
NOTHING` and span ends with `DO UPDATE`, so ordering does not matter either.
Trace rollups are recomputed from `spans` rather than incremented, because an
increment is not idempotent.

**A poison message is bounded.** After 5 deliveries a message moves to
`dead_letters` and is acked. An infinitely retried message is a self-inflicted
outage that also starves real work.

**Redaction happens server-side.** A `viewer` gets masked bytes from the API
itself. `curl` with their token returns the same masked bytes as the dashboard
does. Hiding a field in React is cosmetic, not redaction. Secrets stay masked
even for admins, because nobody needs to read a live API key out of a trace.

**The one thing that costs money is capped.** LLM-judge evaluation is sampled
deterministically (`blake2b(trace_id) % 100`, so retries agree), batched into one
request per 10 traces, and checked against a per-tenant budget *before* the call.
Past the cap, sampling drops to zero and an alert is written instead of tokens
being spent.

**The platform watches itself.** `/health/meta` reports consumer lag, last
successful write, worker heartbeats and dead-letter count, and every container
has a health check that means something for its role. A tool that watches other
systems and cannot notice its own failure is worse than no tool, because it is
trusted.

---

## Run it

```bash
git clone https://github.com/sundram7865/AgentLens-Platform.git && cd AgentLens-Platform
cp .env.example .env
docker compose up -d
```

Postgres 16, Redis 7, the API and a worker come up. The API runs migrations on
boot; the worker waits for the API to be healthy, so it never races them.

```bash
# Python tooling for the scripts and tests
python -m venv .venv && source .venv/bin/activate   # .venv\Scripts\activate on Windows
pip install -r packages/obs-platform/requirements-dev.txt
pip install --no-deps -e packages/obs-sdk -e packages/obs-platform

# Agent traffic shaped exactly like SupportPilot's integration produces it --
# PII, injections and failures included, because a demo where nothing is
# flagged demonstrates nothing
python scripts/traffic_sim.py --traces 50

curl localhost:8000/health/meta      # lag, workers, freshness: "status": "ok"
```

Dashboard:

```bash
cd apps/dashboard && npm ci && npm run dev   # localhost:3000
# or: docker compose --profile dashboard up -d
```

Sign in with `OBS_BOOTSTRAP_ADMIN_EMAIL` / `OBS_BOOTSTRAP_ADMIN_PASSWORD` from
your `.env` (`admin@example.com` / `admin12345` locally; change both before
deploying).

## Test it

Three layers, each proving something the one below it cannot:

```bash
# unit -- SQLite + fakeredis, no containers
python -m pytest

docker compose up -d postgres redis

# integration -- real Postgres and Redis, driving the consumers directly
OBS_INTEGRATION=1 OBS_ENVIRONMENT=test python -m pytest -m integration

# end-to-end -- real `obs-worker` and `uvicorn` processes, over HTTP
OBS_E2E=1 python -m pytest -m e2e -p no:cacheprovider
```

(`make test`, `make test-integration`, `make test-e2e`, `make test-all` wrap
these where `make` is available.) The integration and e2e layers use their own
`obs_test` database on the same server, created on first use, so running them
never touches the data the dashboard is showing you.

342 tests: 304 unit, 25 integration, 11 end-to-end and 2 fork-safety tests
that need a POSIX `fork()` and run in CI on Linux. Latest run:
[`docs/evidence/tests.txt`](docs/evidence/tests.txt).

The default suite runs against SQLite and fakeredis on purpose. A test suite
that needs Docker is a test suite that stops being run, and this keeps the inner
loop fast. But that default cannot see a whole class of bug, so the other two
layers exist and **all three run in CI** on every push:

* **Integration** covers what only a real engine does: `ON CONFLICT` against a
  real unique index, JSONB round-tripping, `percentile_cont` (SQLite has no such
  function), real `XPENDING`/`XCLAIM` semantics, actual concurrency rather
  than SQLite's serialised writers, and a spawned process publishing to a real
  stream.
* **End-to-end** migrates a cold database, starts the worker and the API as
  separate processes and talks to them over HTTP: the only layer that proves a
  worker started from the command line picks up what a *different* process
  published, that SIGTERM finishes the in-flight batch, and that RBAC redaction
  holds on the wire rather than only in a unit assertion.

The layering is not decoration. The eval scorer raced the storage writer, and
the trace-API dependency leaked a connection on every 404: neither was visible
to the unit suite, and each was found by the layer above it.
[`docs/DECISIONS.md`](docs/DECISIONS.md) lists every bug found by running the
system rather than reading it.

---

## Wiring it to your agent

If you use LangChain or LangGraph models:

```python
from obs_sdk import Observability

obs = Observability.from_env(service="my-agent")
graph.invoke(state, config={"callbacks": [obs.handler(tenant_id=str(org.id))]})
```

For hand-written code, `obs.tracer()` gives `trace()` and `span()` context
managers that join the same trace tree.

**SupportPilot** calls Gemini directly rather than through LangChain, and runs its
graph on a worker thread or in a spawned process, so it is wired differently:
through the one function every SupportPilot step, Gemini call and UrbanKart
request already passes through. [`integrations/supportpilot/`](integrations/supportpilot/)
has the drop-in module and a `git apply`-able patch against SupportPilot `main`,
with the reasoning. It is tested through a replica of SupportPilot's real
control flow, against real Redis from a spawned process, and was run against the
real SupportPilot agent: [`docs/evidence/supportpilot_real_run.txt`](docs/evidence/supportpilot_real_run.txt).

With `OBS_REDIS_URL` unset, every SDK call is a no-op, so an integration can be
merged before the platform is deployed.

---

## What is in the box

| | |
|---|---|
| **Ingestion** | LangChain/LangGraph callback handler (full hook set, including the error hooks), manual tracer for non-LangChain code, buffered `XADD ... MAXLEN ~` publisher, fork-safe |
| **Guardrails** | Checksum-validated PII (Luhn, IBAN mod-97, Aadhaar Verhoeff, US SSN), prompt-injection detection with a **measured** threshold, optional Presidio for NER |
| **Evaluation** | Faithfulness and answer-relevancy via batched LLM judge, deterministic sampling, per-tenant budget cap, free heuristic backend when no key is set |
| **Cost** | Per-span cost from a price table with its source and date, overridable without a deploy; unpriced models are reported, not shown as free |
| **Drift** | Scheduled monitor with a deliberately captured baseline, z-score plus an absolute floor |
| **API** | Read-only, keyset-paginated, RBAC with server-side redaction, audit log, sliding-window rate limit |
| **Dashboard** | Next.js 15 server components, hand-written SVG charts, 173 B of route-specific JS per page (106 kB first load including React) |
| **Ops** | Dead letters, `SIGTERM` handling, role-aware container health checks, structured JSON logs with `trace_id`, retention job, `pg_dump` backups with a tested restore |

---

## Documentation

| Document | What it covers |
|---|---|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | How the pieces fit, and the trade-offs behind each |
| [`docs/DECISIONS.md`](docs/DECISIONS.md) | Where this deviates from the original plan, and every bug found by running it |
| [`docs/LOADTEST.md`](docs/LOADTEST.md) | Measured numbers, the method, and where the system breaks |
| [`docs/TUNING.md`](docs/TUNING.md) | The measured injection threshold and PII engine memory, with honest caveats |
| [`docs/evidence/`](docs/evidence/) | Raw output behind every number |
| [`docs/DEPLOY.md`](docs/DEPLOY.md) | Free-tier deployment, including the command-budget maths |
| [`docs/RUNBOOK.md`](docs/RUNBOOK.md) | What to do when something breaks |
| [`docs/DEMO.md`](docs/DEMO.md) | A 90-second walkthrough |
| [`integrations/supportpilot/`](integrations/supportpilot/) | The SupportPilot integration, the patch, and why it is shaped that way |

## Deploying free

Render (API + embedded workers) · Neon (Postgres) · Upstash (Redis) · Vercel
(dashboard). All permanent free tiers; [`render.yaml`](render.yaml) is a
one-click blueprint. The only recurring cost in the entire stack is LLM judge
tokens, which are off by default and budget-capped when on.

Two free-tier realities the configuration handles rather than ignores: Render's
free tier has no background-worker service type (so the consumers can run inside
the API process; same code, same shutdown path), and Upstash's free tier bills
10,000 commands/day (so the blocking read is stretched to 30 s).
[`docs/DEPLOY.md`](docs/DEPLOY.md) shows the arithmetic.

## Deliberately out of scope

Multi-region, high availability, and horizontal autoscaling. At this scale they
would be decoration. The consumer-group design supports horizontal scaling:
`hostname:pid` consumer names, `XCLAIM` recovery of abandoned messages, and an
end-to-end test that a second worker joins the same group. Throughput beyond one
worker has not been measured, and this document does not claim otherwise.

## Licence

MIT.
