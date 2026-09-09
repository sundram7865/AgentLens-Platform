# Wiring SupportPilot to the observability platform

Four call sites and one dependency. SupportPilot's behaviour does not change:
this only observes.

The guarantee that makes this safe to merge before the platform is even
deployed: with `OBS_REDIS_URL` unset, every function here is a no-op. No
network calls, no threads, no failure mode.

---

## 1. Install the SDK

`obs-sdk` depends on `pydantic` and `redis`. SupportPilot already has both, so
this adds no new transitive packages.

```bash
# apps/api/requirements.txt
obs-sdk @ git+https://github.com/<you>/obs-platform.git#subdirectory=packages/obs-sdk
```

Or vendor it; it is four files.

Copy `observability.py` (next to this README) to
`apps/api/app/common/observability.py`.

## 2. Configure

```env
# apps/api/.env
OBS_ENABLED=true
OBS_REDIS_URL=rediss://default:<password>@<name>.upstash.io:6379
OBS_SERVICE=supportpilot
OBS_ENVIRONMENT=production
OBS_CAPTURE_CONTENT=true   # false sends shapes and sizes but no prompt text
```

The same Redis the platform's workers read. Nothing else is shared: SupportPilot
never talks to the platform's Postgres or its API.

---

## 3. The four call sites

### 3.1 Agent runs: `app/modules/agent/service.py`

The one that matters. Everything else is optional detail.

```python
from app.common.observability import agent_config, new_agent_trace_id

async def run_agent(db, organization, ticket, agent_run):
    trace_id = new_agent_trace_id()          # once per request, never a global
    agent_run.trace_id = trace_id            # optional: link your row to the trace

    result = await graph.ainvoke(
        initial_state,
        config=agent_config(
            organization_id=organization.id,
            ticket_id=ticket.id,
            trace_id=trace_id,
            agent_run_id=agent_run.id,
        ),
    )
    return result
```

If you already pass a `config`, merge instead of replacing; LangGraph uses the
same dict for its own `configurable` keys:

```python
config = {**existing_config, **agent_config(...)}
```

That single change gives you a span per LangGraph node
(`load_context_step`, `retrieve_knowledge_step`, `classify_ticket_step`,
`detect_risk_step`, `plan_tools_step`, `execute_tools_node`,
`draft_response_step`, `decision_step`), a span per LLM call with token counts
and cost, a span per retriever call with the retrieved chunks, and error spans
for failures.

**Storing `trace_id` on `agent_runs` is worth the migration.** It turns "this
run misbehaved" into a dashboard link, and it is one nullable column:

```python
# alembic
op.add_column("agent_runs", sa.Column("trace_id", sa.String(64), nullable=True))
```

### 3.2 Tool gateway: `app/modules/tools/gateway.py`

The agent's LLM calls are visible without this, but the *tool executions* are
where refunds actually happen. They go through your gateway, not LangChain, so
they need explicit instrumentation.

```python
from app.common.observability import observe_tool_execution

with observe_tool_execution(
    organization_id=organization.id,
    tool_name=tool.name,               # urbankart_request_refund
    scope=tool.scope,                  # READ_ONLY | HIGH_RISK_WRITE
    arguments=normalised_args,
    ticket_id=ticket.id,
) as span:
    result = await provider_client.call(...)
    span.set_output(status=result.status, execution_id=str(execution.id))
```

These spans join the same trace as the agent run, because both read the same
contextvar. So the trace detail view shows the decision *and* the tool call that
followed it, rather than an agent that apparently issued a refund out of thin air.

### 3.3 Approvals: `app/modules/approvals/service.py`

Optional, and the highest-signal optional one. It records the human decision in
the same trace as the AI recommendation.

```python
from app.common.observability import get_tracer
from obs_sdk import SpanKind

tracer = get_tracer(organization.id)
with tracer.span(
    "approval_decision",
    kind=SpanKind.GUARDRAIL,
    trace_id=agent_run.trace_id,      # attach to the original run
    attributes={
        "approval_state": "APPROVED",
        "ticket_id": str(ticket.id),
        "tool_scope": execution.scope,
    },
) as span:
    span.set_output(approved_by=str(current_user.id))
```

### 3.4 Shutdown: `app/main.py`

Without this, the last few seconds of traces before every deploy are lost,
which is exactly the window a bad deploy shows up in.

```python
from contextlib import asynccontextmanager
from app.common.observability import shutdown_observability

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    shutdown_observability(timeout=5)
```

Optionally surface the publisher's counters on your existing `/health`, so a
full buffer is visible in the app you already watch:

```python
from app.common.observability import observability_stats

@app.get("/health")
async def health():
    return {"status": "ok", "observability": observability_stats()}
```

---

## 4. Verify

```bash
# 1. Submit a ticket and run the agent in SupportPilot as normal.
# 2. Confirm the events reached Redis:
redis-cli -u "$OBS_REDIS_URL" XLEN obs:events

# 3. Confirm they landed:
curl -s "$OBS_API_URL/v1/traces?limit=1" -H "Authorization: Bearer $TOKEN" | jq '.items[0]'

# 4. Confirm the tenant is your organization id, not "default":
curl -s "$OBS_API_URL/v1/traces/tenants" -H "Authorization: Bearer $TOKEN" | jq
```

If `XLEN` is 0, the SDK is disabled or cannot reach Redis. Check
`observability_stats()`: `connect_failures` and `last_error` say which.

---

## 5. What this does *not* do

Stated explicitly, because an observability integration that quietly changes
behaviour is worse than none:

* **It never blocks or slows a request.** `publish()` is a bounded-queue append;
  measured p99 is 29 µs (`docs/LOADTEST.md`). All I/O is on a daemon thread.
* **It never raises.** Every callback is wrapped. A bug in the SDK degrades
  observability, never a customer's ticket.
* **It never changes an agent decision.** No blocking, no rewriting, no
  filtering. The platform detects a prompt injection *after the fact* and raises
  an alert; SupportPilot's own risk detection and approval gates remain the
  thing that actually stops it.
* **It sends no data to third parties.** Events go to your Redis and your
  Postgres. The only external call in the whole platform is the optional eval
  judge, which runs on the platform side, is sampled, and is budget-capped.

## 6. Turning it off

```env
OBS_ENABLED=false
```

No redeploy of the platform, no code change, no restart ordering to think about.
The next request handles the ticket exactly as it did before this integration
existed.
