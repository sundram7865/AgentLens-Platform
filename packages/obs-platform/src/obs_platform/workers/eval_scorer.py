"""Eval scorer -- the only component in this platform that spends money.

Four things keep that spend predictable, and all four are load-bearing:

**Deterministic sampling.** ``hash(trace_id) % 100 < rate``, via blake2b so the
answer survives a process restart (Python randomises ``hash()`` per process).
A redelivered message reaches the same verdict, so a trace is never scored
twice, and "why wasn't this trace scored?" has an answer you can check by hand.

**Batching.** Sampled traces accumulate until ``eval_batch_size`` or
``eval_batch_interval_seconds``, then one request grades the whole batch. The
rubric is most of the prompt at this size, so writing it once instead of N times
is most of the saving -- and it is one round trip instead of N.

**A per-tenant budget cap checked before the call.** Over the cap, the tenant's
effective sample rate is zero and an alert is written instead of tokens spent.

**Cost recorded on every row.** ``eval_scores.cost_micros`` is what the judging
itself cost. Observability spend that hides from its own dashboard is exactly
the failure this platform exists to prevent.

Buffered messages stay **unacked** until they are scored, and the buffer is
flushed on SIGTERM before the process exits -- otherwise every redeploy would
silently drop whatever was mid-batch.

**And it waits for the writer.** This consumer and the storage writer read the
same stream in two different groups, concurrently, with nothing ordering them:
``trace.end`` reaches the judge before the row it describes reaches Postgres
often enough to matter. Grading straight off the database therefore has to
tolerate a trace that is not there yet, or is there without its answer. Such a
trace goes back in the buffer and is retried for up to
``eval_ready_timeout_seconds`` rather than being written off as ungradable --
see ``_readiness``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, update

from obs_sdk.schema import EventType

from ..db import table_of, upsert
from ..evals.budget import check_budget, record_spend, report_exhausted
from ..evals.judge import EvalSample, JudgeRun, build_judge
from ..evals.sampling import should_sample
from ..logging import get_logger
from ..models import EvalScore, Span, Trace
from ..settings import Settings
from .base import BatchOutcome, StreamConsumer, StreamMessage

log = get_logger("obs_platform.eval")

_QUESTION_KEYS = ("question", "query", "prompt", "input", "text", "message", "ticket")
_ANSWER_KEYS = ("answer", "completion", "response", "output", "reply", "draft", "text")
_CONTEXT_SPAN_HINTS = ("retriev", "knowledge", "search", "context")

# How often to look again at a trace that is only waiting for the storage
# writer. Deliberately far shorter than ``eval_batch_interval_seconds``: that
# interval exists to find a trace some company to share a judge call with, and a
# deferred trace already has its batch. It is waiting on another process, and
# that wait is normally milliseconds.
_READY_POLL_SECONDS = 1.0


@dataclass
class _Buffered:
    """One sampled trace waiting to be judged, and the message that carried it."""

    message_id: str
    trace_id: str
    tenant_id: str
    #: When this trace first entered the buffer. A retry does not reset it, so
    #: the readiness wait is bounded overall rather than per attempt.
    first_seen: float
    attempts: int = 0


@dataclass
class _Readiness:
    """What a batch of trace ids looks like in the database at this moment."""

    samples: list[EvalSample] = field(default_factory=list)
    #: The storage writer has not caught up with these. Retry them.
    waiting: set[str] = field(default_factory=set)
    #: These are complete and there is nothing in them to grade. Final.
    unscorable: set[str] = field(default_factory=set)


class EvalScorer(StreamConsumer):
    """Consumer group ``obs-eval``."""

    role = "eval"

    def __init__(self, settings: Settings | None = None, **kwargs: Any) -> None:
        super().__init__(settings=settings, **kwargs)
        self.judge = build_judge(self.settings)
        self._buffer: list[_Buffered] = []
        # Message ids currently held in the buffer. Buffered messages are
        # deliberately left unacked until they are scored, so Redis is entitled
        # to redeliver one while we still have it; without this set that would
        # buffer -- and pay to grade -- the same trace twice.
        self._buffered_ids: set[str] = set()
        self._last_flush = time.monotonic()

    # -- lifecycle ---------------------------------------------------------
    async def _tick(self) -> None:
        await super()._tick()
        # The buffer must drain on a timer too: with no new traffic, process()
        # is never called and a half-full batch would sit unscored forever.
        await self._maybe_flush()

    async def teardown(self) -> None:
        """SIGTERM: score what is buffered before exiting, or it is lost."""
        if not self._buffer:
            return
        log.info("eval.flushing_on_shutdown", buffered=len(self._buffer))
        acked = await self._flush()
        if acked:
            # Ack here as well as score. Grading these and then dropping the
            # acks would leave the messages pending, and the process that
            # replaces this one would grade -- and pay for -- the same traces
            # again on the very next deploy.
            try:
                await self.redis.xack(self.stream, self.group, *acked)
            except Exception:
                log.exception("eval.shutdown_ack_failed", count=len(acked))

    # -- ingestion ---------------------------------------------------------
    async def process(self, messages: list[StreamMessage]) -> BatchOutcome:
        parsed, outcome = self.parse(messages)
        if not parsed:
            return outcome

        rate = self.settings.eval_sample_rate
        enabled = self.settings.evals_enabled

        for item in parsed:
            event = item.event
            # Only a completed trace is scorable: mid-flight spans have no answer.
            if event.type is not EventType.TRACE_END:
                outcome.ack.append(item.message_id)
                continue
            if not enabled or not should_sample(event.trace_id, rate):
                outcome.ack.append(item.message_id)
                continue
            if item.message_id in self._buffered_ids:
                continue
            self._buffered_ids.add(item.message_id)
            self._buffer.append(
                _Buffered(
                    message_id=item.message_id,
                    trace_id=event.trace_id,
                    tenant_id=event.tenant_id,
                    first_seen=time.monotonic(),
                )
            )

        if len(self._buffer) >= self.settings.eval_batch_size:
            acked = await self._flush()
            outcome.ack.extend(acked)
        return outcome

    async def _maybe_flush(self) -> None:
        if not self._buffer:
            self._last_flush = time.monotonic()
            return
        if len(self._buffer) >= self.settings.eval_batch_size:
            due = True
        else:
            interval = float(self.settings.eval_batch_interval_seconds)
            if any(entry.attempts for entry in self._buffer):
                interval = min(interval, _READY_POLL_SECONDS)
            due = time.monotonic() - self._last_flush >= interval
        if due:
            acked = await self._flush()
            if acked:
                try:
                    await self.redis.xack(self.stream, self.group, *acked)
                except Exception:
                    log.exception("eval.ack_failed", count=len(acked))

    # -- scoring -----------------------------------------------------------
    async def _flush(self) -> list[str]:
        """Score everything buffered. Returns the message ids safe to ack.

        Traces the storage writer has not caught up with go back in the buffer
        instead of being acked, so a later pass grades them.
        """
        if not self._buffer:
            return []
        batch, self._buffer = self._buffer, []
        self._last_flush = time.monotonic()

        by_tenant: dict[str, list[_Buffered]] = {}
        for entry in batch:
            entry.attempts += 1
            by_tenant.setdefault(entry.tenant_id, []).append(entry)

        acked: list[str] = []
        for tenant_id, entries in by_tenant.items():
            try:
                done, deferred = await self._score_tenant(tenant_id, entries)
            except Exception:
                # Transient (API timeout, database blip): leave these unacked so
                # Redis redelivers them. The delivery counter still bounds it --
                # a permanently failing batch ends up in dead_letters. Drop them
                # from the held set too, or the redelivery would be ignored as a
                # duplicate of a trace nothing is holding any more.
                log.exception("eval.tenant_batch_failed", tenant_id=tenant_id, count=len(entries))
                for entry in entries:
                    self._buffered_ids.discard(entry.message_id)
                continue
            acked.extend(done)
            self._buffer.extend(deferred)
        for message_id in acked:
            self._buffered_ids.discard(message_id)
        return acked

    async def _score_tenant(
        self, tenant_id: str, entries: list[_Buffered]
    ) -> tuple[list[str], list[_Buffered]]:
        """Grade one tenant's slice of a batch.

        Returns the message ids that reached a final answer (score, budget skip,
        or genuinely ungradable) and the entries still waiting on the writer.
        """
        trace_ids = [entry.trace_id for entry in entries]
        message_ids = [entry.message_id for entry in entries]

        async with self.db.session() as session:
            status = await check_budget(session, tenant_id, self.settings)
            if not status.allowed:
                await report_exhausted(session, tenant_id, status)
                await session.execute(
                    update(Trace)
                    .where(Trace.trace_id.in_(trace_ids))
                    .values(eval_status="skipped_budget")
                )
                log.info(
                    "eval.skipped_over_budget",
                    tenant_id=tenant_id,
                    traces=len(trace_ids),
                    reason=status.reason,
                )
                # Acked on purpose: not scoring is the correct final outcome, so
                # redelivering would just re-run the budget check forever.
                return message_ids, []

            readiness = await self._readiness(session, trace_ids)

        # A trace whose row has not landed yet is not a trace without an answer.
        # Give the storage writer a bounded head start before writing one off:
        # marking it not_scorable is permanent, and it would be a lie.
        now = time.monotonic()
        timeout = self.settings.eval_ready_timeout_seconds
        deferred = [
            entry
            for entry in entries
            if entry.trace_id in readiness.waiting and now - entry.first_seen < timeout
        ]
        deferred_ids = {entry.message_id for entry in deferred}
        final_ids = [message_id for message_id in message_ids if message_id not in deferred_ids]

        # Waited the full window and the row still is not usable. Something is
        # wrong upstream -- the storage writer is stalled, or that trace was
        # dead-lettered -- so say so loudly rather than only in a status column.
        abandoned = {
            entry.trace_id
            for entry in entries
            if entry.trace_id in readiness.waiting and entry.message_id not in deferred_ids
        }
        if abandoned:
            log.warning(
                "eval.storage_never_caught_up",
                tenant_id=tenant_id,
                traces=sorted(abandoned),
                waited_seconds=timeout,
            )
        if deferred:
            log.debug("eval.awaiting_storage", tenant_id=tenant_id, traces=len(deferred))

        unscorable = sorted(readiness.unscorable | abandoned)
        if unscorable:
            async with self.db.session() as session:
                await session.execute(
                    update(Trace)
                    .where(Trace.trace_id.in_(unscorable))
                    .values(eval_status="not_scorable")
                )

        if not readiness.samples:
            return final_ids, deferred

        run = await self.judge.score_batch(readiness.samples)
        await self._persist(tenant_id, run, readiness.samples)
        return final_ids, deferred

    async def _readiness(self, session: Any, trace_ids: list[str]) -> _Readiness:
        """Sort a batch of trace ids into gradable, not-yet-there, and hopeless."""
        traces = list(
            (await session.execute(select(Trace).where(Trace.trace_id.in_(trace_ids)))).scalars()
        )
        spans = list(
            (await session.execute(select(Span).where(Span.trace_id.in_(trace_ids)))).scalars()
        )
        spans_by_trace: dict[str, list[Span]] = {}
        for span in spans:
            spans_by_trace.setdefault(span.trace_id, []).append(span)

        by_id = {trace.trace_id: trace for trace in traces}
        readiness = _Readiness()
        for trace_id in trace_ids:
            trace = by_id.get(trace_id)
            if trace is None:
                # We read trace.end off the stream before the writer wrote it.
                readiness.waiting.add(trace_id)
                continue
            sample = build_sample(trace, spans_by_trace.get(trace_id, []))
            if sample.scorable:
                readiness.samples.append(sample)
            elif trace.eval_status == "not_sampled":
                # The storage writer stamps eval_status the moment it merges
                # trace.end, and only then. Still "not_sampled" on a trace we
                # know *was* sampled means the end -- and with it the answer --
                # has not been merged yet. Any other value means it has, and
                # this trace genuinely has nothing to grade.
                readiness.waiting.add(trace_id)
            else:
                readiness.unscorable.add(trace_id)
        return readiness

    async def _persist(self, tenant_id: str, run: JudgeRun, samples: list[EvalSample]) -> None:
        now = datetime.now(UTC)
        scored: list[str] = []
        failed: list[str] = []

        async with self.db.session() as session:
            for verdict in run.verdicts:
                if verdict.error or not verdict.scores:
                    failed.append(verdict.trace_id)
                    continue
                # Cost is attributed evenly across the batch: one request graded
                # them all, so no single trace "owns" the spend.
                share = run.cost_micros // max(1, len(run.verdicts))
                for score in verdict.scores:
                    await upsert(
                        session,
                        table_of(EvalScore),
                        {
                            "trace_id": verdict.trace_id,
                            "tenant_id": tenant_id,
                            "metric": score.metric,
                            "score": score.score,
                            "reason": score.reason[:2000],
                            "backend": run.backend,
                            "judge_model": run.model[:120],
                            "prompt_tokens": run.prompt_tokens // max(1, len(run.verdicts)),
                            "completion_tokens": run.completion_tokens // max(1, len(run.verdicts)),
                            "cost_micros": share // max(1, len(verdict.scores)),
                            "created_at": now,
                        },
                        index_elements=["trace_id", "metric"],
                        update_columns=[
                            "score",
                            "reason",
                            "backend",
                            "judge_model",
                            "prompt_tokens",
                            "completion_tokens",
                            "cost_micros",
                            "created_at",
                        ],
                    )
                scored.append(verdict.trace_id)

            if scored:
                await session.execute(
                    update(Trace).where(Trace.trace_id.in_(scored)).values(eval_status="scored")
                )
            if failed:
                await session.execute(
                    update(Trace).where(Trace.trace_id.in_(failed)).values(eval_status="error")
                )

            if run.prompt_tokens or run.completion_tokens or run.cost_micros:
                await record_spend(
                    session,
                    tenant_id=tenant_id,
                    prompt_tokens=run.prompt_tokens,
                    completion_tokens=run.completion_tokens,
                    cost_micros=run.cost_micros,
                    calls=run.calls,
                )

        log.info(
            "eval.batch_scored",
            tenant_id=tenant_id,
            backend=run.backend,
            model=run.model,
            traces=len(samples),
            scored=len(scored),
            failed=len(failed),
            prompt_tokens=run.prompt_tokens,
            completion_tokens=run.completion_tokens,
            cost_usd=round(run.cost_micros / 1_000_000, 6),
        )


# --------------------------------------------------------------------------- #
# Sample construction
# --------------------------------------------------------------------------- #
def _first_text(payload: dict[str, Any] | None, keys: tuple[str, ...]) -> str:
    if not payload:
        return ""
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
        if isinstance(value, list) and value:
            first = value[0]
            if isinstance(first, str) and first.strip():
                return first
            if isinstance(first, dict):
                nested = _first_text(first, keys)
                if nested:
                    return nested
        if isinstance(value, dict):
            nested = _first_text(value, keys)
            if nested:
                return nested
    for value in payload.values():
        if isinstance(value, str) and value.strip():
            return value
    return ""


def extract_contexts(spans: list[Span]) -> list[str]:
    """Pull retrieved context out of retriever spans.

    Faithfulness is meaningless without the context the answer was supposed to
    be grounded in, which is why the SDK's retriever hook captures document text
    rather than only a count.
    """
    contexts: list[str] = []
    for span in spans:
        is_retriever = span.kind == "retriever" or any(
            hint in (span.name or "").lower() for hint in _CONTEXT_SPAN_HINTS
        )
        if not is_retriever:
            continue
        documents = (span.output or {}).get("documents")
        if isinstance(documents, list):
            for document in documents:
                if isinstance(document, dict) and document.get("content"):
                    contexts.append(str(document["content"]))
                elif isinstance(document, str):
                    contexts.append(document)
        elif isinstance((span.output or {}).get("context"), str):
            contexts.append(str(span.output["context"]))
    return contexts[:20]


def build_sample(trace: Trace, spans: list[Span]) -> EvalSample:
    return EvalSample(
        trace_id=trace.trace_id,
        tenant_id=trace.tenant_id,
        question=_first_text(trace.input, _QUESTION_KEYS),
        answer=_first_text(trace.output, _ANSWER_KEYS),
        contexts=extract_contexts(spans),
    )


__all__ = ["EvalScorer", "build_sample", "extract_contexts"]
