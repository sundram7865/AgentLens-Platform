# Wiring SupportPilot to AgentLens

[SupportPilot](https://github.com/sundram7865/SupportPilot) (live:
[supportpilot-tau.vercel.app](https://supportpilot-tau.vercel.app)) is the agent
AgentLens was built to watch. This folder is the whole integration:

| File | What it is |
|---|---|
| `observability.py` | The drop-in module. Goes to `apps/api/app/common/observability.py`. |
| `supportpilot.patch` | The complete change, as `git diff` against SupportPilot `main` @ `ca317cd`. |

```bash
# in a SupportPilot checkout
git apply /path/to/AgentLens-Platform/integrations/supportpilot/supportpilot.patch
```

With `OBS_REDIS_URL` unset every call in the module is a no-op (no thread, no
socket, no queue), so the patch is safe to merge before AgentLens is deployed.

---

## Why it hooks `trace_operation`, not a LangChain callback

An earlier version of this integration passed a LangChain callback handler in the
graph's `config`. Checked against SupportPilot's real code, that design was
wrong in three ways, and none of them showed up in tests that used a made-up
host:

| What SupportPilot actually does | What the callback design got |
|---|---|
| Calls Gemini through `google.generativeai` directly (`agent/llm.py`), not a LangChain chat model | Node spans, but **zero LLM spans**: no prompts, models, tokens or cost |
| Runs `graph.invoke` on a `ThreadPoolExecutor` thread, or with `AGENT_EXECUTION_MODE=process` in a **spawned** interpreter | Spawn pickles `config`; a handler owning a thread, lock and queue does not pickle, so **every process-mode run would fail** |
| Has no `tools/gateway.py`, no `run_agent`, never calls `ainvoke` | Call sites that do not exist |

What SupportPilot *does* have is one function every agent step, every Gemini call
and every UrbanKart request already passes through:
`app.core.resilience.trace_operation`, used by `run_logged_step`,
`LLMClient.generate_json` / `generate_text` and `UrbanKartClient._request`.
Wrapping it instruments all three, in whatever thread or process they run.

## How one run stays one trace

The trace id is a pure function of the run: `"tr_" + agent_run_id.hex`.

* Every agent step's metadata already carries `agent_run_id` and
  `organization_id`, so a step computes the trace id itself, in the executor
  thread or the spawned child, with nothing passed in.
* The step then sets the trace context, so the Gemini and UrbanKart calls nested
  inside it join it as child spans.
* A run that pauses for approval and resumes later keeps its `agent_run_id`, so
  the resume lands in the same trace.
* An operation outside any run (knowledge ingestion, say) emits nothing, rather
  than an orphan trace with no tenant.

## The patch, call site by call site

| File | Change |
|---|---|
| `app/common/observability.py` | New. The module in this folder. |
| `app/core/resilience.py` | `trace_operation` wraps the existing LangSmith body (renamed `_langsmith_trace_operation`) in `observe_operation`. |
| `app/modules/agent/llm.py` | One line after each `generate_content`: `record_llm_usage(response, model_name)`. Token counts exist only on the response, which those helpers discard. |
| `app/modules/agent/service.py` | `observe_agent_run(...)` around the three `_invoke_graph_with_timeout` calls (run, resume, replay). It records the ticket as the question and `draft_response` as the answer, which is what the eval scorer grades. |
| `app/modules/agent/nodes.py` | `_run_async_from_sync`'s fallback thread runs under `contextvars.copy_context()`. That path is the resume after approval, where approved refunds execute; without it their UrbanKart calls fall out of the trace. |
| `app/main.py` | `shutdown_observability()` in the lifespan teardown, so the last seconds before a deploy are not lost. |
| `requirements.txt` | `obs-sdk` as an archive URL (the `python:3.11-slim` image has no git). Pin it to a commit SHA. |
| `.env.example` | The `OBS_*` settings below. |

## Configure

```env
# apps/api/.env
OBS_ENABLED=true
OBS_REDIS_URL=rediss://default:<password>@<name>.upstash.io:6379   # AgentLens's Redis
OBS_SERVICE=supportpilot
OBS_ENVIRONMENT=production
OBS_CAPTURE_CONTENT=true    # false: shapes and sizes only, no ticket or prompt text
```

SupportPilot only ever writes to that Redis stream. It never talks to AgentLens's
Postgres or its API.

## What a SupportPilot trace contains

| Span | Kind | Carries |
|---|---|---|
| `ticket_agent` (the trace) | agent | ticket subject + description as `question`, `draft_response` as `answer`, decision, risk, category, `phase` (run / resume / replay) |
| `agent_step:<step>` | chain | only the state keys that step produces (e.g. `classify_ticket` → category, priority, confidence), not the whole `AgentState` fifteen times |
| `agent_step:retrieve_knowledge` | chain | the retrieved chunks as `documents[].content`, which faithfulness is graded against |
| `llm:generate_json` / `llm:generate_text` | llm | the model that actually answered (SupportPilot walks a fallback chain on quota errors), prompt and completion tokens from `usage_metadata`, cost priced by the platform |
| `provider:<METHOD>:<endpoint>` | tool | the UrbanKart request and response |

Cost uses Google's published prices for SupportPilot's model chain (see
`packages/obs-platform/src/obs_platform/pricing.py` for the source and date).
`gemini-3.5-flash` and `gemini-3.7-flash` are in SupportPilot's chain but not on
Google's price page, so they are left unpriced and reported on `/health/meta`
under `unknown_models` rather than silently shown as free. Add them with
`OBS_PRICING_JSON` once they are listed.

## What it does not do

* **It never blocks or slows a request.** `publish()` is a bounded-queue append;
  all I/O is on a daemon thread. See `docs/LOADTEST.md` for the measured cost.
* **It never raises into SupportPilot.** Every entry point swallows its own
  failures; a test replaces the publisher with one that throws and the agent run
  still completes.
* **It never changes a decision.** AgentLens flags a prompt injection after the
  fact. SupportPilot's own `PromptInjectionGuard`, risk detection and approval
  gates remain what actually stops one.

## How this is tested

SupportPilot is another repository, so AgentLens's suite is this module's only
CI. `packages/obs-platform/tests/supportpilot_replica.py` reproduces the parts of
SupportPilot the module touches (`trace_operation`, `run_logged_step`, the
Gemini client and its model chain, the UrbanKart client, `_run_async_from_sync`,
the executor thread and the spawn entry point), each annotated with its source
file. The tests drive the real module through that replica:

* `tests/test_supportpilot_integration.py`: one run is one trace across the
  executor thread; LLM spans nest in their step and carry model and tokens; the
  fallback model is the one priced; pause and resume share a trace; six
  concurrent runs for different organizations do not braid; a broken publisher
  never reaches the agent; the storage writer and eval scorer produce a complete
  sample (question, answer, retrieved context) from a real run.
* `tests/integration/test_supportpilot_process_mode.py`: a spawned child, as in
  `AGENT_EXECUTION_MODE=process`, publishes to a real Redis and its events
  arrive with the right trace id, tenant, model and tokens.

It was also run against SupportPilot itself; `docs/evidence/` has the output.

## Turning it off

```env
OBS_ENABLED=false
```

No redeploy of AgentLens, no restart ordering. The next ticket is handled
exactly as before the patch existed.
