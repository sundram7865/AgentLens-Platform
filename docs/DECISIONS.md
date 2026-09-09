# Decisions

Where this build deviates from the original plan, and why. Each entry is a place
where following the plan literally would have produced something that did not
work, did not deploy, or claimed more than it had measured.

---

## 1. Keycloak is not in the default stack

**Plan:** "OIDC via self-hosted Keycloak in `docker-compose` (free), or NextAuth
against a free-tier IdP."

**Built:** local JWT auth (Argon2id + HS256) as the default, with **optional**
OIDC via JWKS that works with Keycloak, Auth0 or Clerk. Keycloak is present as a
`docker compose --profile keycloak` service for exercising that path.

**Why:** Keycloak needs its own database and ~1 GB of RAM. The whole free-tier
target is 512 MB. Making it the default would mean the deployment the README
calls free is not. The OIDC verification path is fully implemented and strict
(signature, issuer, audience, expiry, `alg: none` rejected), so pointing this at
SupportPilot's existing Clerk tenant is configuration, not code.

## 2. RAGAS is not the default eval backend

**Plan:** "Use RAGAS's current `ragas.metrics.collections` API. Pin the exact
version."

**Built:** three backends behind one interface: a direct Anthropic judge
(default when a key is set), a free deterministic heuristic (default without
one), and RAGAS via `ragas.metrics.collections` when importable.

**Why:** two concrete reasons, not preference.

* **The budget cap cannot see RAGAS's spend.** RAGAS drives its own LLM client,
  so token usage is not returned to the caller. The plan also asks for a
  per-tenant budget cap enforced before each batch; those two requirements are
  in direct tension. The direct judge reports `usage.input_tokens` on every call,
  which is what makes the cap real rather than decorative.
* **Batching.** RAGAS scores one sample per call. The direct judge grades a whole
  batch in one request, where the rubric is written once instead of N times.

RAGAS remains selectable (`OBS_EVAL_BACKEND=ragas`), installed from the
`ragas` extra and pinned there to an exact version rather than a range;
this is the one dependency whose API has moved under the project twice. The heuristic backend exists
so the pipeline is demonstrable end to end with no API key; it labels itself
`heuristic` in the `backend` column of every row so nobody mistakes it for a
model judge.

## 3. Injection detection defaults to lexical, not sentence-transformers

**Plan:** "Embed the incoming prompt (reuse the sentence-transformers you already
used in FinFlow), cosine-similarity against ~30 stored known-injection
embeddings."

**Built:** high-precision rules plus TF-IDF cosine similarity over a 40-entry
corpus, implemented in **pure Python**. sentence-transformers is available via
`OBS_INJECTION_BACKEND=embeddings`.

**Why:** forty short documents do not justify numpy and scikit-learn (~100 MB)
or sentence-transformers plus a model (~250 MB resident) on a 512 MB box. More
importantly, the measurement showed similarity alone was not the main signal:
paraphrased attacks score ~0.33 on lexical similarity while the rules score them
0.85–0.95. Recall came from rules. Shipping embeddings as the default would have
added 250 MB for the weaker half of the detector.

The honest caveat is in [`TUNING.md`](TUNING.md): an attack phrasing that matches
no rule and no signature is missed, and embeddings would genuinely help there.

## 4. The threshold was measured, and the obvious pick was wrong

**Plan:** "Tune the threshold against real test strings; don't ship a guessed
number."

**Built:** `scripts/tune_injection_threshold.py` sweeps 20 held-out attacks (not
in the signature corpus) against 40 benign support messages that include
deliberate near-misses.

**What the measurement changed:** the first run scored **40% recall**. That was
not a threshold problem; the rules were too narrow. Broadening them took recall
to 100% at 0% false positives across a plateau from 0.40 to 0.85.

Then a second correction: argmax-F1 picks **0.37**, the lowest threshold in that
plateau, which sits 0.008 above the highest benign score. One slightly unusual
ticket becomes a false positive. The shipped value is **0.61**, the plateau
midpoint, with ~0.24 of margin on both sides. `choose_operating_point()`
implements that rather than argmax.

## 5. Presidio's real problem is not memory

**Plan:** "Test its memory footprint locally before assuming it fits a free-tier
deploy box."

**Measured** (`scripts/measure_pii_memory.py`): builtin **59 MB** resident,
Presidio **154 MB**. Both fit in 512 MB. The assumption that Presidio would not
fit was wrong.

**What the measurement actually found** was worse and less obvious: Presidio's
default NLP engine is `en_core_web_lg`, a **400 MB model it downloads lazily on
the first `analyze()` call**. Left alone, the first flagged trace on a fresh
deploy triggers a 400 MB download mid-request. `PresidioPiiEngine` now pins
`en_core_web_sm` explicitly.

Presidio remains opt-in for image size and cold-start time (a defensible
trade-off) rather than because it does not fit, which was not true.

## 6. The built-in PII engine is not "just regex"

The plan treats Presidio as the PII detector and a fallback as a compromise. The
built-in engine validates every high-severity identifier with its own checksum:
Luhn for cards, mod-97 for IBAN, Verhoeff for Aadhaar, structural rules for US
SSN. That is what stops the 16-digit order numbers in every support ticket from
being reported as credit cards; a pinned test.

It also adds Aadhaar and Indian PAN, which Presidio's US/EN-tuned default
recognisers do not cover, and which matter for an India-facing ecommerce support
product. When Presidio *is* installed the two are **merged**, not swapped: built-in
matches win on overlap, Presidio contributes the NER entities (person, location)
that regex genuinely cannot do.

## 7. Sorting and paginating on `created_at`, not `started_at`

The trace list sorts by ingestion time rather than agent start time. `started_at`
is nullable (a trace whose envelope never closed has none), and a NULL sort key
breaks a keyset cursor for exactly the malformed traces you most want to inspect.
`created_at` is never NULL and always increasing, so one index serves both the
sort and the time filter. In practice they differ by under a second.

## 8. No SupportPilot rebuild

SupportPilot exists. This repository ships the SDK plus a drop-in module written
against its actual layout, and `scripts/traffic_sim.py`, a generator that emits
the same *shape* of trace (same LangGraph step names, same tools, same decision
outcomes) so the platform is demonstrable and load-testable without the real
agent, an LLM key, or a provider account.

The drop-in module is exercised by this repository's test suite, so it is not
untested copy-paste.

---

## Things the plan got right that were tempting to skip

* **`schema_version` from day one.** Cheap up front; the v1→v2 upgrade path and
  the `extra="allow"` forward tolerance are both tested in both directions.
* **The unique constraint on `(trace_id, span_id)`.** Redelivery happened
  constantly during development, every worker restart.
* **Alembic before any data existed.** The drift test (models vs migrations)
  caught real divergence twice.
* **Separating the try/except around the work from the one around `XACK`.** This
  is the difference between a poison message looping forever and a database blip
  losing a batch, and both paths have tests.
* **`hostname:pid` consumer names.** Free, and the thing that makes horizontal
  scaling possible later rather than a rewrite.

## Things found only by running it

Neither of these was visible in review:

* **The consumer loop could busy-spin at 100% CPU** and starve its own heartbeat
  when `XREADGROUP` returned without suspending: an `await` that completes
  synchronously does not yield.
* **A "fewer round trips" optimization measured 25% slower.** Collapsing
  per-trace rollup UPDATEs into one correlated-subquery statement does eight
  aggregate passes per trace instead of one shared `GROUP BY`. Reverted, with the
  measurement recorded in the code so nobody re-applies it.
* **`/health/meta` reported "database unreachable" against a healthy database**:
  it read ORM rows after its read-only session had rolled back. A monitoring
  endpoint naming the wrong dependency is worse than one that is simply down.
* **The judge raced the storage writer and lost, silently.** `obs-eval` and
  `obs-storage` read the same stream in different consumer groups with nothing
  ordering them, so `trace.end` reaches the judge before the row it describes
  reaches Postgres. The judge read the database, found nothing, marked the trace
  `not_scorable` and acked, permanently. In the end-to-end run one of four
  sampled traces was stuck at `pending` and another was mislabelled. The judge
  now distinguishes "not there yet" from "nothing to grade" and retries the
  former for up to `eval_ready_timeout_seconds`. Every unit test drained the
  writer first, so none of them could ever have seen it.
* **Merging a trace across two batches crashed on a naive timestamp.** SQLite has
  no timezone type and returns naive datetimes from a `DateTime(timezone=True)`
  column, so comparing the incoming `started_at` with the stored one raised
  `TypeError` and failed the whole batch. It only fires when `trace.start` and
  `trace.end` arrive in *different* batches, the normal case for any trace
  longer than one poll, which is why every existing test missed it. Found by
  the regression test written for the bug above.
* **Every 404 parked a database connection.** The read-session dependency was
  written as `async for s in get_read_session(): yield s`, which reads as a
  harmless re-export. FastAPI throws a route's exception into that generator at
  its `yield`; the exception propagates out of the wrapper and leaves the inner
  generator suspended inside its own `async with`, so the cleanup that returns
  the connection runs only if something later collects it *while an event loop
  is still running*. Against Neon's pooled connection limit that is a slow
  strangle driven by traffic nobody controls: clients asking for things that
  are not there. Awaiting the context manager directly makes the release
  deterministic. Found because the unit suite stopped exiting: the parked
  connection kept aiosqlite's non-daemon worker thread alive and the interpreter
  waited on it forever. The regression test drives the dependency directly
  rather than through HTTP, because refcounting can collect the leak inside the
  same test and make an HTTP-level assertion pass for the wrong reason.
* **The SDK leaked an `atexit` hook per publisher.** Registered per instance,
  never unregistered on `close()`, so an application that builds a tracer per
  worker or per reload pays up to `timeout` seconds per dead hook at shutdown.
* **`pip-audit` was failing and nobody was looking.** 20 advisories across three
  pinned packages, including PyJWT, the library that verifies every token in
  the auth path. Upgraded to FastAPI 0.141 / Starlette 1.6, PyJWT 2.13 and
  python-multipart 0.0.32; all three test layers pass on the new stack. This is
  gotcha #14 landing exactly as predicted, which is the argument for having the
  job at all.
* **The pins and the ranges had drifted apart, twice.** `requirements*.txt` pins
  what CI installs; `pyproject.toml` declares what the package claims to be
  compatible with. Nothing linked them, so `structlog` sat at 26.1.0 against a
  declared `<26` and `aiosqlite` at 0.22.1 against `<0.22`. CI never noticed,
  because CI installs the pins. The person who breaks is whoever installs by
  range and gets a version this project has never tested. `test_dependency_pins`
  now checks every pin against its declared range.
