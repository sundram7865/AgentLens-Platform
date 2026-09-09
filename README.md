# AI Observability & Guardrails Platform

Traces, PII/prompt-injection guardrails, LLM-judge evaluation and quality-drift
detection for LLM agents, built to watch [SupportPilot](#) in production, and
usable with any LangChain or LangGraph agent.

The client SDK adds **29 µs at p99** to the watched application's request path,
sustains **744 events/s with zero loss**, and sheds load rather than blocking
when its buffer fills. Those are measured numbers, not estimates.
[`docs/LOADTEST.md`](docs/LOADTEST.md) has the methodology and the caveats.

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
customer's support request.

**Redelivery is assumed, not feared.** Redis Streams are at-least-once, so a
restarted consumer *will* see the same message twice. `spans` carries a unique
constraint on `(trace_id, span_id)`; span starts insert with `ON CONFLICT DO
NOTHING` and span ends with `DO UPDATE`, so ordering does not matter either.
Trace rollups are recomputed from `spans` rather than incremented, because an
increment is not idempotent.

**A poison message is bounded.** After N redeliveries a message moves to
`dead_letters` and is acked. An infinitely retried message is a self-inflicted
outage that also starves real work.

**Redaction happens server-side.** A `viewer` gets masked bytes from the API
itself. `curl` with their token returns the same masked bytes as the dashboard
does. Hiding a field in React is cosmetic, not redaction. Secrets stay masked
even for admins, because nobody needs to read a live API key out of a trace.

**The one thing that costs money is capped.** LLM-judge evaluation is sampled
deterministically (`blake2b(trace_id) % 100`, so retries agree), batched into one
request per N traces, and checked against a per-tenant budget *before* the call.
Past the cap, sampling drops to zero and an alert is written instead of tokens
being spent.

**The platform watches itself.** `/health/meta` reports consumer lag, last
successful write, worker heartbeats and dead-letter count. A tool that watches
other systems and cannot notice its own failure is worse than no tool, because
it is trusted.

---

## Run it

```bash
git clone <this repo> && cd obs-platform
cp .env.example .env
docker compose up
```

That is the whole setup. Postgres 16, Redis 7, the API and a worker come up;
migrations run automatically on the API's first boot.

```bash
# Generate realistic agent traffic (PII, injections and failures included,
# because a demo where nothing is flagged demonstrates nothing)
python scripts/traffic_sim.py --traces 50

curl localhost:8000/health/meta | jq        # lag, workers, freshness
```

Dashboard:

```bash
cd apps/dashboard && npm install && npm run dev   # localhost:3000
# or: docker compose --profile dashboard up
```

Sign in with `OBS_BOOTSTRAP_ADMIN_EMAIL` / `OBS_BOOTSTRAP_ADMIN_PASSWORD` from
your `.env`.

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
these where `make` is available.)

The default suite runs against SQLite and fakeredis on purpose. A test suite
that needs Docker is a test suite that stops being run, and this keeps the inner
loop fast. But that default cannot see a whole class of bug, so the other two
layers exist and **all three run in CI** on every push:

* **Integration** covers what only a real engine does: `ON CONFLICT` against a
  real unique index, JSONB round-tripping, `percentile_cont` (SQLite has no such
  function), real `XPENDING`/`XCLAIM` semantics, and actual concurrency rather
  than SQLite's serialised writers.
* **End-to-end** starts the worker and the API as separate processes against a
  cold-migrated database and talks to them over HTTP, the only layer that
  proves a worker started from the command line picks up what a *different*
  process published, that SIGTERM finishes the in-flight batch, and that RBAC
  redaction holds on the wire rather than only in a unit assertion.

The layering is not decoration. The eval scorer raced the storage writer, and
the trace-API dependency leaked a connection on every 404: neither was visible
to the unit suite, and each was found by the layer above it.

---

## Wiring it to your agent

One line, if you use LangChain or LangGraph:

```python
from obs_sdk import Observability

obs = Observability.from_env(service="supportpilot")
graph.invoke(state, config={"callbacks": [obs.handler(tenant_id=str(org.id))]})
```

For SupportPilot specifically, [`integrations/supportpilot/`](integrations/supportpilot/)
has a drop-in module and the exact four call sites, written against its actual
layout (`app/modules/agent`, the tool gateway, approvals, the lifespan hook).
That module is exercised by this repository's own test suite, so it is not
untested copy-paste.

With `OBS_REDIS_URL` unset, every SDK call is a no-op, so the integration can
be merged before the platform is deployed.

---

## What is in the box

| | |
|---|---|
| **Ingestion** | LangChain/LangGraph callback handler (full hook set, including the error hooks), manual tracer for non-LangChain code, buffered `XADD ... MAXLEN ~` publisher |
| **Guardrails** | Checksum-validated PII (Luhn, IBAN mod-97, Aadhaar Verhoeff, US SSN), prompt-injection detection with a **measured** threshold, optional Presidio for NER |
| **Evaluation** | Faithfulness and answer-relevancy via batched LLM judge, deterministic sampling, per-tenant budget cap, free heuristic backend when no key is set |
| **Drift** | Scheduled monitor with a deliberately captured baseline, z-score plus an absolute floor |
| **API** | Read-only, keyset-paginated, RBAC with server-side redaction, audit log, sliding-window rate limit |
| **Dashboard** | Next.js 15 server components, hand-written SVG charts, 173 B of JS per page |
| **Ops** | Dead letters, `SIGTERM` handling, structured JSON logs with `trace_id`, retention job, `pg_dump` backups with a tested restore |

---

## Documentation

| Document | What it covers |
|---|---|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | How the pieces fit, and the trade-offs behind each |
| [`docs/TUNING.md`](docs/TUNING.md) | The measured injection threshold, PII engine memory, sampling maths, with honest caveats |
| [`docs/LOADTEST.md`](docs/LOADTEST.md) | Real numbers, and two bugs the load test found |
| [`docs/DEPLOY.md`](docs/DEPLOY.md) | Free-tier deployment, including the command-budget maths |
| [`docs/RUNBOOK.md`](docs/RUNBOOK.md) | What to do when something breaks |
| [`docs/DECISIONS.md`](docs/DECISIONS.md) | Where this deviates from the original plan, and why |
| [`docs/DEMO.md`](docs/DEMO.md) | A 90-second walkthrough |

## Deploying free

Render (API + embedded workers) · Neon (Postgres) · Upstash (Redis) · Vercel
(dashboard). All permanent free tiers; [`render.yaml`](render.yaml) is a
one-click blueprint. The only recurring cost in the entire stack is LLM judge
tokens, which are off by default and budget-capped when on.

Two free-tier realities the configuration handles rather than ignores: Render's
free tier has no background-worker service type (so the consumers can run inside
the API process; same code, same shutdown path), and Upstash's free tier bills
10,000 commands/day (so the blocking read is stretched to 30s, costing ~2,880
commands/day at idle). [`docs/DEPLOY.md`](docs/DEPLOY.md) shows the arithmetic.

## Deliberately out of scope

Multi-region, high availability, and horizontal autoscaling. At this scale they
would be decoration. The consumer-group design supports horizontal scaling:
`hostname:pid` consumer names, `XCLAIM` recovery of abandoned messages; it just
has not been measured beyond one worker, and this document does not claim
otherwise.

## Licence

MIT.
#   O B S _ P L A T F O R M  
 