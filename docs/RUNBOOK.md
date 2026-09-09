# Runbook

Start here: `curl $API/health/meta | jq`. It reports consumer lag, last
successful write, worker heartbeats and dead-letter count, and names the failing
dependency rather than guessing.

---

## Traces are not appearing

Work outwards from the producer.

**1. Is the SDK enabled and reaching Redis?**

```python
from app.common.observability import observability_stats
observability_stats()
# {"published": 0, "connect_failures": 12, "last_error": "ConnectionError(...)"}
```

* `published: 0` and `connect_failures: 0` → the SDK is disabled. Check
  `OBS_ENABLED` and `OBS_REDIS_URL`.
* `connect_failures > 0` → wrong URL, wrong password, or egress blocked. Upstash
  needs `rediss://` (TLS).
* `dropped_queue_full > 0` → the buffer filled. Redis is slow or unreachable, or
  the agent is producing faster than 10,000 buffered events. Raise
  `OBS_QUEUE_SIZE`.

**2. Are events in the stream?**

```bash
redis-cli -u "$OBS_REDIS_URL" XLEN obs:events
redis-cli -u "$OBS_REDIS_URL" XINFO GROUPS obs:events
```

Events present but `lag` growing → the consumer is down or behind (below).
Stream empty → the problem is upstream, in the SDK.

**3. Is the consumer running?**

```bash
curl -s $API/health/meta | jq '.workers'
```

No workers listed → the worker process is not running, or
`OBS_EMBED_WORKERS_IN_API` is false with no separate worker service.
`seconds_since_seen` large → the worker is wedged; restart it.

---

## Consumer lag is growing

```bash
curl -s $API/health/meta | jq '.streams.groups'
```

| Group behind | Meaning | Action |
|---|---|---|
| `obs-storage` | Postgres is the bottleneck | Check Neon compute state; raise `OBS_CONSUMER_BATCH_SIZE`; add a replica |
| `obs-guardrail` | CPU-bound scanning | Rare. Check for pathologically large payloads |
| `obs-eval` | **Usually not a problem** | It holds messages while batching. Only investigate if lag exceeds `eval_batch_size` by a lot |

Lag on *every* group at once usually means the whole worker process is down, not
three separate problems.

Adding capacity is horizontal: each replica gets a distinct `hostname:pid`
consumer name and Redis distributes messages across them:

```bash
python -m obs_platform.workers.cli --roles storage    # extra storage capacity only
```

---

## Dead letters are accumulating

`/health/meta` reports `unreplayed_dead_letters`, and the scheduler raises an
alert.

```sql
SELECT error_type, count(*), max(created_at)
FROM dead_letters WHERE replayed_at IS NULL
GROUP BY error_type ORDER BY 2 DESC;

SELECT payload FROM dead_letters WHERE replayed_at IS NULL LIMIT 1;
```

| `error_type` | Cause | Action |
|---|---|---|
| `SchemaTooOldError` | Producer older than `MIN_SUPPORTED_SCHEMA_VERSION` | Upgrade the SDK in the agent |
| `ValidationError` | Payload does not match the schema | Usually a hand-crafted or corrupted event |
| `MaxDeliveryAttemptsExceeded` | Failed `max_delivery_attempts` times | A consumer bug; read the payload |
| `JSONDecodeError` | Truncated or non-JSON `data` field | Something other than the SDK is writing to the stream |

After fixing the cause, replay by re-publishing the stored payloads:

```python
import asyncio, json
from sqlalchemy import select
from obs_platform.db import get_db
from obs_platform.models import DeadLetter
from obs_platform.redis_io import get_redis

async def replay():
    redis = get_redis()
    async with get_db().session() as s:
        rows = (await s.execute(
            select(DeadLetter).where(DeadLetter.replayed_at.is_(None))
        )).scalars().all()
        for row in rows:
            await redis.xadd("obs:events",
                             {"v": str(row.payload.get("schema_version", 2)),
                              "data": json.dumps(row.payload)})
            row.replayed_at = __import__("datetime").datetime.now(
                __import__("datetime").UTC)

asyncio.run(replay())
```

Replay is safe: `spans` is idempotent, so anything already stored is a no-op.

---

## A tenant stopped being scored

Expected when the budget cap engages. Confirm:

```sql
SELECT tenant_id, period_key, cost_micros/1e6 AS usd, total_tokens, calls
FROM tenant_usage WHERE period_kind = 'day' ORDER BY cost_micros DESC LIMIT 10;

SELECT * FROM alerts WHERE kind = 'budget' AND status = 'open';
```

Traces will show `eval_status = 'skipped_budget'`. Raise
`OBS_BUDGET_DAILY_USD_PER_TENANT` or wait for the period to roll over. This is
the guard rail working, not a fault.

---

## Drift alert fired

```bash
curl -s "$API/v1/drift?tenant_id=<t>&metric=faithfulness" -H "Authorization: Bearer $TOKEN" | jq
curl -s "$API/v1/drift/baselines" -H "Authorization: Bearer $TOKEN" | jq
```

Two triggers, and which one fired matters:

* **z-score**: quality dropped relative to the captured baseline. Look for a
  model version change, a prompt change, or a retrieval regression.
* **absolute floor**: quality is below the floor regardless of baseline. If the
  baseline was captured during a bad period it will never trigger the z-score;
  this is what catches that.

If the baseline is stale (after a deliberate model or prompt change), capture a
new one from a window you have actually reviewed:

```bash
curl -X POST $API/v1/drift/baseline -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"acme","metric":"faithfulness","hours":24,"note":"after gemini-2.0 upgrade"}'
```

Only an admin can do this, and it is audited. A baseline that refreshes itself
cannot detect drift; it drifts along with the data.

---

## Database is filling up

Neon free is 0.5 GB.

```sql
SELECT relname, pg_size_pretty(pg_total_relation_size(relid))
FROM pg_catalog.pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC;

SELECT job_name, last_finished_at, last_status, detail
FROM job_runs WHERE job_name = 'retention_purge';
```

`last_status = 'error'` or a stale `last_finished_at` means retention is not
running. Note a failed job deliberately does **not** advance
`last_finished_at`, so it retries rather than appearing recently-run.

Lower `OBS_RETENTION_DAYS` and let the job catch up; it deletes in bounded
batches, so it will not take a long lock.

---

## Backup and restore

Neon's free PITR window is **6 hours**. That covers "I deleted it five minutes
ago", not "nobody noticed for two days". For that, weekly dumps:

```bash
OBS_DATABASE_DIRECT_URL='postgresql://...' ./scripts/backup.sh
OBS_DATABASE_DIRECT_URL='postgresql://...' ./scripts/backup.sh --verify   # restores into a scratch DB
```

Restore:

```bash
# Everything, into a fresh database
createdb obs_restored
pg_restore --dbname="postgresql://.../obs_restored" --no-owner --no-privileges \
           backups/obs-<stamp>.dump

# One table (e.g. traces clobbered, everything else fine)
pg_restore --dbname="$PGURL" --data-only --table=traces --no-owner \
           backups/obs-<stamp>.dump

# Inspect without restoring
pg_restore --list backups/obs-<stamp>.dump
```

**Run `--verify` at least once.** An untested backup is a hope, not a plan.

---

## Rolling back a bad deploy

Events are still in Redis (`MAXLEN ~ 50000`), so a rollback loses nothing that
has not already been consumed:

1. Roll back the service on Render.
2. If a migration must be reversed: `alembic downgrade -1` using the **direct**
   connection string. Every migration in this project has a tested `downgrade()`.
3. Restart the consumers. Unacked messages are redelivered; idempotent writes
   make that a no-op.

## Emergency: turn observability off entirely

In the *watched* application:

```env
OBS_ENABLED=false
```

Every SDK call becomes a no-op on the next request. No platform redeploy, no
restart ordering. Tickets are handled exactly as they were before the
integration existed.
