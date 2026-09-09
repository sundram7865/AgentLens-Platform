"""PII detection.

Two engines behind one interface:

``builtin`` (default)
    The checksum-validated pattern registry in :mod:`.patterns`. Zero extra
    dependencies, a few megabytes of RSS, and high precision on structured
    identifiers because every high-severity hit is confirmed by its own checksum.

``presidio`` (optional, ``pip install 'obs-platform[presidio]'``)
    Microsoft Presidio plus a spaCy model. Adds NER, which is the one thing the
    built-in engine genuinely cannot do: free-text **person names** and
    locations.

    The memory cost was measured, not assumed (``scripts/measure_pii_memory.py``):
    **59 MB** resident for builtin, **154 MB** for Presidio with the small model.
    Both fit inside a 512 MB free-tier box, so Presidio is opt-in for image size
    and cold-start time rather than because it does not fit.

    What genuinely does not fit is Presidio's **own default**, ``en_core_web_lg``
    -- a 400 MB model it downloads *lazily, on the first analyze() call*. Left
    alone, the first flagged trace on a fresh deploy triggers a 400 MB download
    mid-request. ``PresidioPiiEngine`` pins ``en_core_web_sm`` explicitly for
    exactly that reason.

``auto`` prefers Presidio when it imports and falls back silently. Selecting
``presidio`` explicitly and having it fail raises, because silently running a
weaker detector than the operator configured is worse than a startup error.

Presidio's default recognisers are English/US-tuned. That is a real scope limit
for a platform watching an India-facing support product, which is why the
built-in registry adds Aadhaar (Verhoeff-checked) and PAN and why both engines'
results are merged rather than one replacing the other.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import Any

from obs_sdk.schema import Severity

from ..logging import get_logger
from .patterns import PATTERNS_BY_NAME, Match, scan_text

log = get_logger("obs_platform.guardrails.pii")

# Presidio entity -> (severity, score floor). Only entities the built-in engine
# does not already cover better; overlapping ones would double-report.
PRESIDIO_ENTITIES: dict[str, Severity] = {
    "PERSON": Severity.MEDIUM,
    "LOCATION": Severity.LOW,
    "NRP": Severity.MEDIUM,
    "DATE_TIME": Severity.INFO,
    "MEDICAL_LICENSE": Severity.HIGH,
    "US_DRIVER_LICENSE": Severity.HIGH,
    "US_PASSPORT": Severity.CRITICAL,
    "UK_NHS": Severity.CRITICAL,
    "IN_PASSPORT": Severity.CRITICAL,
    "IN_VOTER": Severity.HIGH,
}

PRESIDIO_MIN_SCORE = 0.6


@dataclass
class PiiResult:
    matches: list[Match]
    engine: str

    @property
    def detected(self) -> bool:
        return bool(self.matches)

    @property
    def max_severity(self) -> Severity:
        if not self.matches:
            return Severity.INFO
        return Severity.max_of([m.severity for m in self.matches])

    def types(self) -> list[str]:
        seen: dict[str, None] = {}
        for match in self.matches:
            seen.setdefault(match.pattern, None)
        return list(seen)


class BuiltinPiiEngine:
    """Checksum-validated pattern matching. No model, no download, no spaCy."""

    name = "builtin"

    def analyze(self, text: str) -> list[Match]:
        return scan_text(text)


class PresidioPiiEngine:  # pragma: no cover - only when the extra is installed
    """Presidio analyzer, merged with the built-in registry.

    Merged rather than substituted: Presidio brings NER for names, the built-in
    registry brings Aadhaar/PAN and checksum confirmation that Presidio's
    US-tuned recognisers do not do as strictly.
    """

    name = "presidio"

    #: Presidio's own default is en_core_web_lg -- a 400 MB model it downloads
    #: LAZILY, on the first analyze() call. On a free-tier box that means a cold
    #: start silently attempting a 400 MB download mid-request and timing out.
    #: Pinning the small model (~12 MB) makes the dependency explicit and the
    #: startup deterministic. Override with OBS_PII_SPACY_MODEL if you have the
    #: memory and want the accuracy.
    DEFAULT_MODEL = "en_core_web_sm"

    def __init__(self, language: str = "en", model_name: str | None = None) -> None:
        import os

        from presidio_analyzer import AnalyzerEngine
        from presidio_analyzer.nlp_engine import NlpEngineProvider

        self.language = language
        self.model_name = model_name or os.environ.get("OBS_PII_SPACY_MODEL", self.DEFAULT_MODEL)
        provider = NlpEngineProvider(
            nlp_configuration={
                "nlp_engine_name": "spacy",
                "models": [{"lang_code": language, "model_name": self.model_name}],
            }
        )
        self.analyzer = AnalyzerEngine(
            nlp_engine=provider.create_engine(), supported_languages=[language]
        )
        self.builtin = BuiltinPiiEngine()

    def analyze(self, text: str) -> list[Match]:
        matches = self.builtin.analyze(text)
        covered = [(m.start, m.end) for m in matches]
        try:
            results = self.analyzer.analyze(
                text=text, language=self.language, entities=list(PRESIDIO_ENTITIES)
            )
        except Exception:
            log.warning("pii.presidio_failed", exc_info=True)
            return matches

        for result in results:
            if result.score < PRESIDIO_MIN_SCORE:
                continue
            if any(result.start < end and start < result.end for start, end in covered):
                continue
            severity = PRESIDIO_ENTITIES.get(result.entity_type, Severity.LOW)
            if severity is Severity.INFO:
                continue
            value = text[result.start : result.end]
            matches.append(
                Match(
                    pattern=result.entity_type,
                    severity=severity,
                    score=float(result.score),
                    start=result.start,
                    end=result.end,
                    value=value,
                    masked="*" * min(len(value), 12),
                )
            )
        return sorted(matches, key=lambda m: m.start)


def build_engine(engine: str = "auto", language: str = "en") -> Any:
    if engine in ("auto", "presidio"):
        try:
            return PresidioPiiEngine(language=language)
        except Exception as exc:
            if engine == "presidio":
                raise RuntimeError(
                    "OBS_PII_ENGINE=presidio but presidio-analyzer is not importable. "
                    "Install with: pip install 'obs-platform[presidio]' && "
                    "python -m spacy download en_core_web_sm"
                ) from exc
            log.info("pii.presidio_unavailable_using_builtin", reason=str(exc)[:200])
    return BuiltinPiiEngine()


@functools.lru_cache(maxsize=2)
def get_engine(engine: str = "auto", language: str = "en") -> Any:
    return build_engine(engine, language)


def scan(text: str, engine: str = "auto", language: str = "en") -> PiiResult:
    detector = get_engine(engine, language)
    return PiiResult(matches=detector.analyze(text or ""), engine=detector.name)


def describe(match: Match) -> str:
    pattern = PATTERNS_BY_NAME.get(match.pattern)
    return pattern.description if pattern else match.pattern.replace("_", " ").title()


__all__ = [
    "PRESIDIO_ENTITIES",
    "BuiltinPiiEngine",
    "PiiResult",
    "PresidioPiiEngine",
    "build_engine",
    "describe",
    "get_engine",
    "scan",
]
