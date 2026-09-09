# Architecture

## The shape

```
┌─────────────────────────────┐
│ SupportPilot (or any agent) │
│   obs-sdk                   │   pydantic + redis. Nothing else.
│   ├ callback handler        │   One span per LangGraph node / LLM / tool call
│   ├ manual tracer           │   For the tool gateway and other non-LangChain code
│   └ buffered publisher      │   put_nowait -> bounded queue -> daemon thread
└──────────────┬──────────────┘
               │ XADD obs:events MAXLEN ~ 100000
               ▼
       ┌───────────────┐
       │ Redis Stream  │  One stream, three independent consumer groups
       └───┬───────┬───┬─────────────┐
           │       │                 │
    obs-storage  obs-guardrail   obs-eval
           │       │                 │
           ▼       ▼                 ▼
    ┌──────────────────────────────────────┐
    │ Postgres                             │
    │  traces · spans · guardrail_findings │
    │  eval_scores · alerts · tenant_usage │
    │  drift_* · dead_letters · audit_log  │
    └──────────────┬───────────────────────┘
                   │ read-only
       ┌───────────▼──────────┐      ┌──────────────┐
       │ FastAPI (RBAC,       │◀─────│ Next.js      │
       │ redaction, audit)    │      │ dashboard    │
       └──────────────────────┘      └──────────────┘

    scheduler (same process or its own): drift monitor · retention · DLQ watch
```

## Why a stream and not direct writes

The agent could write to Postgres directly. It should not:

* **Latency coupling.** A slow database would become slow ticket handling. The
  stream decouples them; the agent's cost is a queue append.
* **Backpressure has somewhere to go.** A stream absorbs a burst that would
  otherwise exhaust the database's connection pool, which on Neon's free tier
  is shared with the agent itself.
* **Three consumers, one write.** Storage, guardrails and evaluation each read
  the same events at their own pace through separate consumer groups. One
  falling behind never starves the others.
* **Replay.** The stream retains ~100k events, so a consumer bug can be fixed
  and the affected window reprocessed. `spans` is idempotent, so replay is safe.

## Three consumer groups, one stream

| Group | Runs on | Cost | Falls behind when |
|---|---|---|---|
| `obs-storage` | every event | Postgres writes | the database is slow |
| `obs-guardrail` | every event | CPU (regex, checksums) | never, in practice |
| `obs-eval` | sampled `trace.end` only | **LLM tokens** | deliberately, while batching |

`obs-eval` holding messages unacked is a feature: it is what makes batched
judging affordable. Measuring one combined "drain time" across all three hides
that and makes the storage writer look slow ([`LOADTEST.md`](LOADTEST.md) §3).

## Idempotency, concretely

Redelivery is normal: every restart, deploy and transient error causes it.

| Write | Mechanism |
|---|---|
| `span.start` | `INSERT ... ON CONFLICT (trace_id, span_id) DO NOTHING` |
| `span.end` | `INSERT ... ON CONFLICT DO UPDATE`: terminal state always wins, whatever the arrival order |
| trace row | read-modify-write with convergent merge: timestamps take min/max, status only escalates, payloads only overwrite when non-empty |
| rollups | **recomputed** from `spans`, never incremented |
| findings | unique on `(trace_id, span_id, detector, type, field, offset)` |
| eval scores | unique on `(trace_id, metric)` |
| alerts | unique on `(tenant_id, dedupe_key)`: collapses an alert storm into one row |

The trace-row merge is convergent rather than locked, so two consumer replicas
processing different halves of one trace agree without coordination.

## Schema evolution

Every event carries `schema_version`. Consumers call `parse_event()`, which
walks any supported version forward to the current shape before validating, and
`extra="allow"` lets an *older* consumer read a *newer* producer's events
instead of dying on an unknown field. Both directions are tested.

Migrations are additive: every column added to an existing table is nullable or
carries a server default, so a consumer built before the migration keeps
inserting successfully mid-rollout. A test asserts this about the migration
files themselves, not just by convention.

## Where the money goes

Exactly one component spends money: the eval scorer. Four things bound it.

1. **Deterministic sampling.** `blake2b(trace_id) % 100 < rate`. Not `random()`,
   which would re-dice on redelivery and make coverage unrepeatable; not
   `hash()`, which Python randomises per process.
2. **Batching.** One request grades N traces. At this size the rubric is most of
   the prompt, so writing it once instead of N times is most of the saving.
3. **A budget cap checked before the call**, with atomic SQL increments so
   concurrent replicas cannot lose an update and let the cap drift.
4. **Cost recorded on every score row.** Observability spend that hides from its
   own dashboard is precisely the failure this platform exists to prevent.

## Security boundaries

* **Redaction is server-side.** The response body is built from already-masked
  values. `curl` gets the same bytes as the browser. Two levels: `viewer` gets
  PII masked, `admin` does not, but secrets (API keys, JWTs, private keys,
  credentialed URLs) are masked for *both*, because a screenshot of a live key
  is a credential leak regardless of role.
* **Tenant scoping is enforced in the dependency**, not per-endpoint. A scoped
  principal asking for another tenant gets a 403 on a list and a **404** on a
  single trace; a 403 there would confirm the trace exists.
* **The dashboard is read-only** except for acknowledging an alert. The read
  path uses a session that rolls back on exit, so a stray write cannot commit.
* **Audit rows record whether the response was redacted.** "Alice opened
  tr_9f2c" is trivia; "and it contained unmasked customer PII" is the artifact a
  compliance review actually asks for.

## Deployment shapes

The same code runs three ways, differing only in configuration:

| Shape | Config | When |
|---|---|---|
| One process | `OBS_EMBED_WORKERS_IN_API=true` | Render free tier (no worker service type) |
| Two processes | separate `obs-worker` container | Local compose, paid plans |
| N processes | one role per service | Scale-out; each role has its own group |

The supervisor, the graceful-shutdown path and the consumer code are identical
in all three. Only the process boundary moves.

## Known limits

* One worker process was measured; horizontal scaling is designed for
  (`hostname:pid` consumer names, `XCLAIM` recovery) but unmeasured.
* Percentiles use `percentile_cont` on Postgres and a capped in-process
  computation on SQLite. The SQLite path is for tests and degrades to
  approximate rather than exact on large windows.
* The injection detector's recall comes mostly from rules, not similarity. A
  genuinely novel phrasing that matches no rule is missed, mitigated by
  detection data living in JSON files, so adding a signature is a data change.
* Presidio (NER for names) is optional because of image size and cold-start
  time, not because it does not fit. Measured numbers in [`TUNING.md`](TUNING.md).
