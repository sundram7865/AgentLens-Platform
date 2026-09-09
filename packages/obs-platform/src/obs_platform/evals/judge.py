"""LLM-as-judge scoring for faithfulness and answer relevancy.

Only those two metrics run on live traffic, and that is a deliberate limit:
both are computable from what a trace already contains -- the question, the
retrieved context, and the answer. Context precision and recall need a reference
answer, which live traffic does not have. Forcing them here would produce a
number that looks like a metric and means nothing; they belong in an offline
eval set against golden questions (SupportPilot already has that surface).

Three backends behind one interface:

``judge`` (default when a key is configured)
    Direct Anthropic call with a JSON schema. **One request scores the whole
    batch** rather than one request per trace: the rubric is written once
    instead of N times, which is most of the token cost at this size, and it is
    one round trip instead of N.

``heuristic`` (default with no key)
    Deterministic lexical grounding -- no network, no spend, no API key. Used in
    CI and for the demo. It is a weak proxy for a real judge and says so in the
    ``backend`` column of every row it writes, so nobody mistakes it for one.

``ragas``
    Uses ``ragas.metrics.collections`` when that module is importable. The legacy
    ``ragas.metrics`` classes are deprecated and the library's API has churned
    repeatedly, so the version is pinned and the import is guarded rather than
    assumed.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from typing import Any, Protocol

from ..logging import get_logger
from ..pricing import cost_micros

log = get_logger("obs_platform.judge")

FAITHFULNESS = "faithfulness"
ANSWER_RELEVANCY = "answer_relevancy"
METRICS = (FAITHFULNESS, ANSWER_RELEVANCY)

MAX_CONTEXT_CHARS = 6_000
MAX_ANSWER_CHARS = 4_000
MAX_QUESTION_CHARS = 1_000


@dataclass
class EvalSample:
    """Everything the judge needs about one trace."""

    trace_id: str
    tenant_id: str
    question: str
    answer: str
    contexts: list[str] = field(default_factory=list)

    def truncated(self, max_context_chars: int = MAX_CONTEXT_CHARS) -> EvalSample:
        budget = max_context_chars
        contexts: list[str] = []
        for chunk in self.contexts:
            if budget <= 0:
                break
            contexts.append(chunk[:budget])
            budget -= len(chunk)
        return EvalSample(
            trace_id=self.trace_id,
            tenant_id=self.tenant_id,
            question=self.question[:MAX_QUESTION_CHARS],
            answer=self.answer[:MAX_ANSWER_CHARS],
            contexts=contexts,
        )

    @property
    def scorable(self) -> bool:
        """A trace with no answer cannot be graded, and grading it would burn tokens."""
        return bool(self.question.strip() and self.answer.strip())


@dataclass
class MetricScore:
    metric: str
    score: float
    reason: str = ""


@dataclass
class JudgeVerdict:
    trace_id: str
    scores: list[MetricScore] = field(default_factory=list)
    error: str = ""


@dataclass
class JudgeRun:
    """The result of one batch, including what the judging itself cost."""

    verdicts: list[JudgeVerdict] = field(default_factory=list)
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost_micros: int = 0
    model: str = ""
    backend: str = ""
    calls: int = 0


class Judge(Protocol):
    backend: str
    model: str

    async def score_batch(self, samples: list[EvalSample]) -> JudgeRun: ...


# --------------------------------------------------------------------------- #
# Heuristic backend -- deterministic, free, honest about being a proxy
# --------------------------------------------------------------------------- #
_WORD = re.compile(r"[a-z0-9']+")
_STOPWORDS = frozenset(
    [
        "a",
        "an",
        "the",
        "and",
        "or",
        "but",
        "if",
        "then",
        "than",
        "that",
        "this",
        "these",
        "those",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "of",
        "in",
        "on",
        "at",
        "to",
        "for",
        "from",
        "with",
        "without",
        "by",
        "as",
        "it",
        "its",
        "i",
        "you",
        "we",
        "they",
        "he",
        "she",
        "them",
        "my",
        "your",
        "our",
        "do",
        "does",
        "did",
        "done",
        "have",
        "has",
        "had",
        "will",
        "would",
        "can",
        "could",
        "should",
        "may",
        "might",
        "must",
        "not",
        "no",
        "yes",
        "please",
        "thank",
        "thanks",
        "hi",
        "hello",
        "about",
        "into",
        "over",
        "under",
        "again",
        "very",
        "just",
        "so",
        "such",
        "more",
        "most",
    ]
)


def _content_words(text: str) -> set[str]:
    return {w for w in _WORD.findall(text.lower()) if w not in _STOPWORDS and len(w) > 2}


class HeuristicJudge:
    """Lexical grounding: how much of the answer is supported by the context.

    Not a substitute for a model judge. It catches the failure that matters most
    -- an answer asserting things that appear nowhere in the retrieved context --
    and it costs nothing, which is what makes the pipeline demonstrable end to
    end without an API key.
    """

    backend = "heuristic"
    model = "lexical-overlap-v1"

    async def score_batch(self, samples: list[EvalSample]) -> JudgeRun:
        verdicts = []
        for sample in samples:
            answer_words = _content_words(sample.answer)
            context_words = _content_words(" ".join(sample.contexts))
            question_words = _content_words(sample.question)

            if not answer_words:
                verdicts.append(JudgeVerdict(trace_id=sample.trace_id, error="empty answer"))
                continue

            grounded = len(answer_words & context_words) / len(answer_words)
            # No retrieved context at all is not automatically unfaithful -- a
            # greeting or a clarifying question is fine -- so it scores neutral
            # rather than zero, which would poison the drift baseline.
            faithfulness = grounded if context_words else 0.5

            overlap = len(answer_words & question_words)
            relevancy = min(1.0, overlap / max(1, min(len(question_words), 8)))
            # A short answer to a long question is usually a deflection.
            length_penalty = min(1.0, len(answer_words) / 12)
            relevancy = round(relevancy * (0.5 + 0.5 * length_penalty), 4)

            verdicts.append(
                JudgeVerdict(
                    trace_id=sample.trace_id,
                    scores=[
                        MetricScore(
                            FAITHFULNESS,
                            round(min(1.0, faithfulness), 4),
                            f"{len(answer_words & context_words)}/{len(answer_words)} "
                            "answer terms appear in the retrieved context",
                        ),
                        MetricScore(
                            ANSWER_RELEVANCY,
                            relevancy,
                            f"{overlap} question terms addressed in the answer",
                        ),
                    ],
                )
            )
        return JudgeRun(verdicts=verdicts, model=self.model, backend=self.backend, calls=0)


# --------------------------------------------------------------------------- #
# Anthropic backend
# --------------------------------------------------------------------------- #
JUDGE_SYSTEM = """You grade the output of a customer-support AI agent.

For each item you receive the customer's question, the context retrieved from \
the company's knowledge base and order system, and the agent's answer.

Score two metrics from 0.0 to 1.0:

faithfulness
  How much of the answer is supported by the retrieved context. An answer that \
  asserts a fact absent from the context scores low even if the fact is true. \
  An answer that only asks a clarifying question or greets the customer, and \
  asserts nothing, scores 1.0 -- it invents nothing.

answer_relevancy
  How directly the answer addresses the question actually asked. A correct but \
  off-topic reply, or a generic deflection, scores low.

Be strict and consistent: these scores are tracked over time and a drifting \
rubric produces a false drift alert. Give a one-sentence reason for each score, \
citing the specific claim or gap that decided it."""

JUDGE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "faithfulness": {"type": "number"},
                    "faithfulness_reason": {"type": "string"},
                    "answer_relevancy": {"type": "number"},
                    "answer_relevancy_reason": {"type": "string"},
                },
                "required": [
                    "id",
                    "faithfulness",
                    "faithfulness_reason",
                    "answer_relevancy",
                    "answer_relevancy_reason",
                ],
                "additionalProperties": False,
            },
        }
    },
    "required": ["results"],
    "additionalProperties": False,
}


def build_batch_prompt(samples: list[EvalSample]) -> str:
    """Render the batch. Item ids are positional, never the trace id.

    Trace ids are opaque but they are still our identifiers; there is no reason
    to send them to a third party, and a short positional id costs fewer tokens.
    """
    blocks = []
    for index, sample in enumerate(samples):
        context = "\n---\n".join(sample.contexts) if sample.contexts else "(no context retrieved)"
        blocks.append(
            f'<item id="{index}">\n'
            f"<question>{sample.question}</question>\n"
            f"<context>{context}</context>\n"
            f"<answer>{sample.answer}</answer>\n"
            f"</item>"
        )
    return (
        f"Grade all {len(samples)} items. Return one result object per item, "
        f'with "id" matching the item id.\n\n' + "\n\n".join(blocks)
    )


class AnthropicJudge:
    """One structured request per batch."""

    backend = "judge"

    def __init__(
        self,
        api_key: str,
        model: str = "claude-haiku-4-5",
        timeout: float = 30.0,
        max_context_chars: int = MAX_CONTEXT_CHARS,
    ) -> None:
        import anthropic

        self._anthropic = anthropic
        self.model = model
        self.max_context_chars = max_context_chars
        self.client = anthropic.AsyncAnthropic(api_key=api_key, timeout=timeout, max_retries=2)

    async def score_batch(self, samples: list[EvalSample]) -> JudgeRun:
        usable = [s.truncated(self.max_context_chars) for s in samples if s.scorable]
        if not usable:
            return JudgeRun(model=self.model, backend=self.backend)

        prompt = build_batch_prompt(usable)
        try:
            response = await self._create(prompt)
        except Exception as exc:
            log.warning("judge.call_failed", error=repr(exc)[:300], model=self.model)
            raise

        text = "".join(
            block.text for block in response.content if getattr(block, "type", "") == "text"
        )
        prompt_tokens = int(getattr(response.usage, "input_tokens", 0) or 0)
        completion_tokens = int(getattr(response.usage, "output_tokens", 0) or 0)

        verdicts = _parse_verdicts(text, usable)
        return JudgeRun(
            verdicts=verdicts,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            cost_micros=cost_micros(self.model, prompt_tokens, completion_tokens),
            model=self.model,
            backend=self.backend,
            calls=1,
        )

    async def _create(self, prompt: str) -> Any:
        kwargs: dict[str, Any] = {
            "model": self.model,
            "max_tokens": 4096,
            "system": JUDGE_SYSTEM,
            "messages": [{"role": "user", "content": prompt}],
        }
        try:
            return await self.client.messages.create(
                **kwargs,
                output_config={"format": {"type": "json_schema", "schema": JUDGE_SCHEMA}},
            )
        except self._anthropic.BadRequestError as exc:
            # Structured outputs are not available on every model or deployment.
            # Falling back to an instructed JSON response keeps scoring working
            # instead of silently producing no evals at all.
            log.info("judge.structured_output_unavailable", error=str(exc)[:200])
            kwargs["system"] = (
                JUDGE_SYSTEM + "\n\nRespond with JSON only, matching: " + json.dumps(JUDGE_SCHEMA)
            )
            return await self.client.messages.create(**kwargs)


def _parse_verdicts(text: str, samples: list[EvalSample]) -> list[JudgeVerdict]:
    """Map the model's positional results back onto trace ids, defensively."""
    payload = _extract_json(text)
    results = payload.get("results", []) if isinstance(payload, dict) else []
    by_id: dict[str, dict[str, Any]] = {}
    for item in results:
        if isinstance(item, dict) and "id" in item:
            by_id[str(item["id"])] = item

    verdicts: list[JudgeVerdict] = []
    for index, sample in enumerate(samples):
        item = by_id.get(str(index))
        if item is None:
            # A judge that skips an item must not silently drop the trace; the
            # error is recorded so the trace can be retried or investigated.
            verdicts.append(JudgeVerdict(trace_id=sample.trace_id, error="no result returned"))
            continue
        scores = []
        for metric, reason_key in (
            (FAITHFULNESS, "faithfulness_reason"),
            (ANSWER_RELEVANCY, "answer_relevancy_reason"),
        ):
            value = _clamp(item.get(metric))
            if value is None:
                continue
            scores.append(MetricScore(metric, value, str(item.get(reason_key, ""))[:1000]))
        verdicts.append(
            JudgeVerdict(
                trace_id=sample.trace_id,
                scores=scores,
                error="" if scores else "no parseable scores",
            )
        )
    return verdicts


def _clamp(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return round(min(1.0, max(0.0, number)), 4)


def _extract_json(text: str) -> dict[str, Any]:
    """Parse JSON that may be wrapped in prose or a fenced code block."""
    text = text.strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    candidate = fenced.group(1) if fenced else None
    if candidate is None:
        start, end = text.find("{"), text.rfind("}")
        candidate = text[start : end + 1] if start >= 0 and end > start else None
    if not candidate:
        return {}
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        log.warning("judge.unparseable_response", preview=text[:200])
        return {}


# --------------------------------------------------------------------------- #
# RAGAS backend (optional)
# --------------------------------------------------------------------------- #
class RagasJudge:  # pragma: no cover - only when ragas>=0.3 is installed
    """Uses ``ragas.metrics.collections``, not the deprecated metric classes."""

    backend = "ragas"

    def __init__(self, api_key: str, model: str = "claude-haiku-4-5") -> None:
        from ragas.metrics.collections import (
            AnswerRelevancy,
            Faithfulness,
        )

        self.model = model
        self._faithfulness = Faithfulness()
        self._relevancy = AnswerRelevancy()

    async def score_batch(self, samples: list[EvalSample]) -> JudgeRun:
        verdicts = []
        for sample in samples:
            faithfulness = await self._faithfulness.ascore(
                user_input=sample.question,
                response=sample.answer,
                retrieved_contexts=sample.contexts,
            )
            relevancy = await self._relevancy.ascore(
                user_input=sample.question, response=sample.answer
            )
            verdicts.append(
                JudgeVerdict(
                    trace_id=sample.trace_id,
                    scores=[
                        MetricScore(FAITHFULNESS, float(faithfulness), "ragas"),
                        MetricScore(ANSWER_RELEVANCY, float(relevancy), "ragas"),
                    ],
                )
            )
        # RAGAS drives its own LLM client, so token usage is not visible here.
        # The budget guard therefore cannot see this spend -- which is exactly
        # why `judge` is the default backend.
        return JudgeRun(
            verdicts=verdicts, model=self.model, backend=self.backend, calls=len(samples)
        )


# --------------------------------------------------------------------------- #
# Selection
# --------------------------------------------------------------------------- #
def build_judge(settings: Any) -> Judge:
    """Pick a backend from config, degrading to the heuristic rather than failing."""
    backend = settings.eval_backend
    api_key = settings.eval_api_key
    provider = settings.eval_provider

    if backend == "heuristic" or provider == "none" or not api_key:
        if backend in ("ragas", "judge"):
            log.warning(
                "judge.no_api_key_falling_back",
                requested=backend,
                hint="set OBS_EVAL_PROVIDER and OBS_EVAL_API_KEY to enable LLM judging",
            )
        return HeuristicJudge()

    if backend == "ragas":
        try:
            return RagasJudge(api_key=api_key, model=settings.eval_model)
        except Exception as exc:
            log.warning("judge.ragas_unavailable", error=str(exc)[:200])
            if backend == "ragas":
                return HeuristicJudge()

    if provider == "anthropic":
        return AnthropicJudge(
            api_key=api_key,
            model=settings.eval_model,
            timeout=settings.eval_timeout_seconds,
            max_context_chars=settings.eval_max_context_chars,
        )

    log.warning("judge.unsupported_provider_falling_back", provider=provider)
    return HeuristicJudge()


__all__ = [
    "ANSWER_RELEVANCY",
    "FAITHFULNESS",
    "METRICS",
    "AnthropicJudge",
    "EvalSample",
    "HeuristicJudge",
    "Judge",
    "JudgeRun",
    "JudgeVerdict",
    "MetricScore",
    "RagasJudge",
    "build_batch_prompt",
    "build_judge",
]
