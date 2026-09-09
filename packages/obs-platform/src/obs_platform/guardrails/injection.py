"""Prompt-injection detection.

Two signals, combined by taking the stronger:

**High-precision rules** for the recognisable attack shapes -- instruction
override, prompt exfiltration, jailbreak personas, approval bypass, credential
requests, chat-template forgery. Rules carry most of the recall, and each one is
written to require an explicit attack noun so that ordinary support phrasing
("ignore my last message", "could a manager override the 30 day window") does
not trip it.

**Similarity** to a corpus of ~40 known injection phrasings, for near-duplicates
and copy-pasted jailbreaks the rules have not been taught yet. The default
backend computes TF-IDF over word unigrams/bigrams and character 4-grams in
**pure Python** -- 40 short vectors do not justify pulling numpy and
scikit-learn (~100 MB) onto a 512 MB box. ``OBS_INJECTION_BACKEND=embeddings``
swaps in sentence-transformers where the memory is available; that is strictly
better at paraphrase and is a one-line install.

Character n-grams and the normaliser below also defeat the cheap evasions:
``i g n o r e``, ``1gn0re``, unicode look-alikes.

The threshold is **measured, not guessed**. ``scripts/tune_injection_threshold.py``
sweeps it against held-out attacks that are deliberately absent from the corpus,
plus benign support messages that include near-misses. The current default and
the numbers behind it are in ``docs/TUNING.md``.
"""

from __future__ import annotations

import json
import math
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from functools import lru_cache
from itertools import pairwise
from pathlib import Path
from typing import Any

from obs_sdk.schema import Severity

DATA_DIR = Path(__file__).parent / "data"

MAX_SCAN_CHARS = 20_000


@dataclass(frozen=True)
class Signature:
    text: str
    category: str
    severity: Severity


@dataclass
class InjectionResult:
    """The verdict for one piece of text."""

    score: float = 0.0
    detected: bool = False
    category: str = ""
    severity: Severity = Severity.INFO
    matched_signature: str = ""
    rule_hits: list[str] = field(default_factory=list)
    backend: str = "lexical"

    def as_dict(self) -> dict[str, Any]:
        return {
            "score": round(self.score, 4),
            "detected": self.detected,
            "category": self.category,
            "severity": self.severity.value,
            "matched_signature": self.matched_signature[:160],
            "rule_hits": self.rule_hits,
            "backend": self.backend,
        }


# --------------------------------------------------------------------------- #
# High-precision rules
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Rule:
    name: str
    regex: re.Pattern[str]
    category: str
    severity: Severity
    score: float


RULES: tuple[Rule, ...] = (
    # -- instruction override -------------------------------------------------
    Rule(
        "ignore_previous",
        re.compile(
            r"\b(?:ignore|disregard|forget|drop|discard|set\s+aside|put\s+aside|override|"
            r"stop\s+following|no\s+longer\s+follow)\b[^.\n]{0,50}?"
            r"\b(?:previous|prior|earlier|above|preceding|initial|original|first|all)\b[^.\n]{0,40}?"
            r"\b(?:instruction|instructions|prompt|prompts|rule|rules|guideline|guidelines|"
            r"direction|directions|polic(?:y|ies)|constraint|constraints|restriction|restrictions)\b",
            re.IGNORECASE,
        ),
        "instruction_override",
        Severity.HIGH,
        0.95,
    ),
    Rule(
        # The reverse word order: "the rules you were given earlier -- set them aside".
        # Every branch requires an explicit rule-noun, which is what keeps
        # "ignore my last message" and "forget the previous ticket" out.
        "rules_dismissed",
        re.compile(
            r"\b(?:rules?|instructions?|guidelines?|polic(?:y|ies)|restrictions?)\b[^.\n]{0,60}?"
            r"\b(?:you\s+were\s+given|above|earlier|before\s+this)\b[^.\n]{0,40}?"
            r"\b(?:ignore|set\s+aside|disregard|do\s+not\s+apply|forget)\b"
            r"|\b(?:set\s+aside|disregard|ignore|forget)\b[^.\n]{0,30}?"
            r"\b(?:rules?|instructions?|guidelines?|polic(?:y|ies))\b[^.\n]{0,40}?"
            r"\b(?:you\s+were\s+given|given\s+to\s+you|above|earlier)\b",
            re.IGNORECASE,
        ),
        "instruction_override",
        Severity.HIGH,
        0.93,
    ),
    Rule(
        "new_rules_declared",
        re.compile(
            r"\b(?:your\s+new\s+(?:rule|instruction|policy|directive)|new\s+directive"
            r"|from\s+now\s+on\s+you\s+(?:must|will|should|are))\b",
            re.IGNORECASE,
        ),
        "instruction_override",
        Severity.HIGH,
        0.9,
    ),
    Rule(
        "policy_overwritten",
        re.compile(
            r"\b(?:forget|ignore|disregard|override)\b[^.\n]{0,25}?\b(?:the\s+)?polic(?:y|ies)\b",
            re.IGNORECASE,
        ),
        "instruction_override",
        Severity.HIGH,
        0.9,
    ),
    # -- prompt / context exfiltration ----------------------------------------
    Rule(
        "system_prompt_exfiltration",
        re.compile(
            r"\b(?:reveal|print|repeat|show|output|display|dump|quote|expose|leak)\b[^.\n]{0,50}?"
            r"\b(?:system\s*prompt|initial\s*(?:instructions?|configuration)|your\s+instructions?"
            r"|context\s*window|configuration\s+text|hidden\s+(?:text|rules?|instructions?|prompt))\b",
            re.IGNORECASE,
        ),
        "prompt_exfiltration",
        Severity.HIGH,
        0.95,
    ),
    Rule(
        # "what does your configuration say", "text that appears above my message",
        # "output everything that precedes this conversation".
        "context_readback",
        re.compile(
            r"\b(?:what\s+(?:does|do|is|are)\s+your\s+(?:configuration|config|system|initial|hidden)"
            r"|(?:text|content|everything|anything)\s+(?:that\s+)?(?:appears?\s+)?"
            r"(?:above|before|preced(?:es|ing))"
            r"|everything\s+above\s+this"
            r"|the\s+hidden\s+text)\b",
            re.IGNORECASE,
        ),
        "prompt_exfiltration",
        Severity.HIGH,
        0.9,
    ),
    # -- persona / jailbreak ---------------------------------------------------
    Rule(
        "jailbreak_persona",
        re.compile(
            r"\b(?:you\s+are|you're|youre|you\s+will\s+be)\s+(?:now\s+)?"
            r"(?:dan\b|jailbroken|unrestricted|operating\s+in\s+unrestricted)"
            r"|\b(?:act|behave|respond|answer)\s+as\s+(?:if\s+you\s+were\s+)?(?:an?\s+|the\s+)?"
            r"(?:unrestricted|admin|administrator|developer|root|backend|system)\b"
            r"|\b(?:pretend|roleplay|role-play|simulate)\b[^.\n]{0,60}?"
            r"\b(?:no\s+(?:rules|restrictions|limits|filters)|unrestricted|admin|developer|"
            r"jailbroken|approve\s+anything)\b"
            r"|\bhas\s+no\s+(?:rules|restrictions|limits|filters)\b",
            re.IGNORECASE,
        ),
        "role_play",
        Severity.HIGH,
        0.9,
    ),
    Rule(
        "unrestricted_mode",
        re.compile(
            r"\b(?:developer|debug|god|sudo|root|unrestricted|dan|jailbreak)\s+mode\b"
            r"|\bno\s+(?:content\s+)?(?:polic(?:y|ies)|filters?|restrictions?)\s+(?:apply|applies)\b",
            re.IGNORECASE,
        ),
        "role_play",
        Severity.HIGH,
        0.88,
    ),
    Rule(
        "safety_suspended",
        re.compile(
            r"\b(?:safety|content|policy|security|usual)\s+"
            r"(?:rules?|filters?|guidelines?|checks?|restrictions?|polic(?:y|ies))"
            r"[^.\n]{0,40}?\b(?:suspended|disabled|turned\s+off|lifted|waived|bypassed"
            r"|do\s+not\s+apply|doesn'?t\s+apply|don'?t\s+apply)\b"
            r"|\b(?:rules?|restrictions?)\s+(?:do\s+not|don'?t|no\s+longer)\s+apply\b",
            re.IGNORECASE,
        ),
        "policy_evasion",
        Severity.HIGH,
        0.9,
    ),
    Rule(
        "hypothetical_bypass",
        re.compile(
            r"\b(?:hypothetical(?:ly)?|fictional|for\s+(?:educational|research|testing)\s+purposes"
            r"|sanctioned\s+(?:penetration\s+)?test|just\s+a\s+test|only\s+a\s+test)\b"
            r"[^.\n]{0,90}?\b(?:rules?|polic(?:y|ies)|restrictions?|safety|limits?|bypass|suspended)\b",
            re.IGNORECASE,
        ),
        "policy_evasion",
        Severity.MEDIUM,
        0.85,
    ),
    # -- tool abuse: the category that actually costs money on this agent -------
    Rule(
        "approval_bypass",
        re.compile(
            r"\b(?:skip|bypass|without|no\s+need\s+for|avoid|circumvent"
            r"|don'?t\s+wait\s+for|do\s+not\s+wait\s+for)\b[^.\n]{0,40}?"
            r"\b(?:human\s+)?(?:approval|authorisation|authorization|review\s+step"
            r"|confirmation|sign[\s-]?off)\b",
            re.IGNORECASE,
        ),
        # SupportPilot gates refunds and replacements on human approval, so an
        # instruction to skip that gate is the attack with a real dollar cost.
        "tool_abuse",
        Severity.CRITICAL,
        0.92,
    ),
    Rule(
        "self_approve",
        re.compile(
            r"\b(?:approve|authorise|authorize)\b[^.\n]{0,40}?"
            r"\b(?:yourself|on\s+behalf\s+of|automatically|without\s+asking"
            r"|without\s+waiting|immediately)\b"
            r"|\bmark\s+(?:it|this|the\s+\w+)\s+(?:as\s+)?(?:approved|authorised|authorized)\b",
            re.IGNORECASE,
        ),
        "tool_abuse",
        Severity.CRITICAL,
        0.92,
    ),
    Rule(
        "write_without_confirmation",
        re.compile(
            r"\b(?:perform|execute|issue|make|carry\s+out|process)\b[^.\n]{0,45}?"
            r"\b(?:write\s+actions?|refunds?|replacements?|transactions?|returns?)\b[^.\n]{0,45}?"
            r"\b(?:without\s+confirmation|without\s+approval|without\s+asking|for\s+every)\b",
            re.IGNORECASE,
        ),
        "tool_abuse",
        Severity.CRITICAL,
        0.9,
    ),
    # -- data / credential exfiltration ---------------------------------------
    Rule(
        "credential_request",
        re.compile(
            r"\b(?:print|show|give|reveal|share|output|tell|list|display)\b[^.\n]{0,40}?"
            r"\b(?:api\s*keys?|secret\s*keys?|access\s*tokens?|passwords?|credentials?"
            r"|env\s*vars?|environment\s+variables?)\b",
            re.IGNORECASE,
        ),
        "data_exfiltration",
        Severity.CRITICAL,
        0.92,
    ),
    Rule(
        "cross_tenant_read",
        re.compile(
            r"\b(?:all|other|every|another|each)\b[^.\n]{0,30}?"
            r"\b(?:customers?|users?|accounts?|tenants?)\b[^.\n]{0,45}?"
            r"\b(?:order(?:ed|s)?|e-?mails?|data|details?|records?|payments?|addresses?)\b"
            r"|\b(?:other|another)\s+(?:customers?|users?|accounts?)s?\b",
            re.IGNORECASE,
        ),
        "data_exfiltration",
        Severity.CRITICAL,
        0.88,
    ),
    # -- structural attacks ----------------------------------------------------
    Rule(
        # XML / chat-template forms only. A bare "[SYSTEM]" is excluded on
        # purpose: customers paste app error text like "[SYSTEM] payment gateway
        # unavailable", and flagging that teaches operators to ignore the alert list.
        "fake_system_turn",
        re.compile(
            r"(?:</?(?:system|user|assistant|human)>"
            r"|<\|?im_(?:start|end)\|?>"
            r"|\[\[\s*system[^\]]{0,20}\]\]"
            r"|\[\s*system\s+override\s*\]"
            r"|###\s*(?:system|end\s+(?:of\s+)?(?:user\s+)?(?:input|context)))",
            re.IGNORECASE,
        ),
        "delimiter_escape",
        Severity.HIGH,
        0.9,
    ),
    Rule(
        "encoded_instruction",
        re.compile(
            r"\b(?:decode|base64|rot13|reverse|backwards|encoded)\b[^.\n]{0,50}?"
            r"\b(?:then|and)\b[^.\n]{0,30}?"
            r"\b(?:follow|execute|do|obey|run|carry\s+out|perform|apply)\b",
            re.IGNORECASE,
        ),
        "obfuscation",
        Severity.MEDIUM,
        0.85,
    ),
)


# --------------------------------------------------------------------------- #
# Text normalisation
# --------------------------------------------------------------------------- #
_SEPARATOR_RUN = re.compile(r"(?<=\w)[\-_.*\s]{1,2}(?=\w)")
_LEET = str.maketrans(
    {"0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t", "@": "a", "$": "s"}
)
_NON_WORD = re.compile(r"[^a-z0-9\s]+")
_WS = re.compile(r"\s+")


def normalize(text: str) -> str:
    """Fold the cheap evasions: unicode look-alikes, leetspeak, letter spacing.

    ``i g n o r e   p r e v i o u s`` and ``1gn0re prev10us`` are the same
    instruction to any reviewer, and a detector that misses them is trivially
    defeated by anyone who has read one blog post.
    """
    text = unicodedata.normalize("NFKC", text).lower()
    if _looks_spaced(text):
        text = _SEPARATOR_RUN.sub("", text)
    text = text.translate(_LEET)
    text = _NON_WORD.sub(" ", text)
    return _WS.sub(" ", text).strip()


def _looks_spaced(text: str) -> bool:
    """True for `i g n o r e` style padding, so normal prose is left alone."""
    single_char_tokens = sum(1 for token in text.split() if len(token) == 1)
    return single_char_tokens >= 6


def tokenize(text: str) -> list[str]:
    """Word unigrams + bigrams + character 4-grams of the normalised text."""
    normalized = normalize(text)
    words = normalized.split()
    features = list(words)
    features.extend(f"{a}_{b}" for a, b in pairwise(words))
    compact = normalized.replace(" ", "")
    features.extend(f"#{compact[i : i + 4]}" for i in range(max(0, len(compact) - 3)))
    return features


# --------------------------------------------------------------------------- #
# Lexical (dependency-free) backend
# --------------------------------------------------------------------------- #
class LexicalIndex:
    """TF-IDF cosine similarity over the signature corpus, in pure Python.

    Forty short documents. numpy would make each query marginally faster and the
    deployment ~100 MB heavier -- on a free-tier box that is the wrong trade.
    """

    def __init__(self, signatures: list[Signature]) -> None:
        self.signatures = signatures
        self.documents = [Counter(tokenize(s.text)) for s in signatures]
        total = max(1, len(self.documents))
        document_frequency: Counter[str] = Counter()
        for document in self.documents:
            document_frequency.update(document.keys())
        # Smoothed idf, so a term present in every signature still carries weight.
        self.idf = {
            term: math.log((total + 1) / (count + 1)) + 1.0
            for term, count in document_frequency.items()
        }
        self.vectors = [self._vectorize(d) for d in self.documents]

    def _vectorize(self, counts: Counter[str]) -> dict[str, float]:
        vector = {
            term: (1.0 + math.log(count)) * self.idf.get(term, 1.0)
            for term, count in counts.items()
        }
        norm = math.sqrt(sum(v * v for v in vector.values())) or 1.0
        return {term: value / norm for term, value in vector.items()}

    def best_match(self, text: str) -> tuple[float, Signature | None]:
        query = self._vectorize(Counter(tokenize(text)))
        if not query:
            return 0.0, None
        best_score, best_signature = 0.0, None
        for vector, signature in zip(self.vectors, self.signatures, strict=True):
            # Iterate the shorter side: a long ticket's query vector dominates.
            small, large = (query, vector) if len(query) < len(vector) else (vector, query)
            score = sum(weight * large.get(term, 0.0) for term, weight in small.items())
            if score > best_score:
                best_score, best_signature = score, signature
        return best_score, best_signature


class EmbeddingIndex:  # pragma: no cover - only when the extra is installed
    """sentence-transformers backend. Better at paraphrase, heavier to deploy."""

    def __init__(self, signatures: list[Signature], model_name: str) -> None:
        from sentence_transformers import SentenceTransformer

        self.signatures = signatures
        self.model = SentenceTransformer(model_name)
        self.matrix = self.model.encode(
            [s.text for s in signatures], normalize_embeddings=True, show_progress_bar=False
        )

    def best_match(self, text: str) -> tuple[float, Signature | None]:
        vector = self.model.encode([text], normalize_embeddings=True, show_progress_bar=False)[0]
        scores = self.matrix @ vector
        best = int(scores.argmax())
        return float(scores[best]), self.signatures[best]


# --------------------------------------------------------------------------- #
# Detector
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def load_signatures() -> tuple[Signature, ...]:
    raw = json.loads((DATA_DIR / "injection_signatures.json").read_text(encoding="utf-8"))
    return tuple(
        Signature(
            text=item["text"],
            category=item.get("category", "unknown"),
            severity=Severity(item.get("severity", "medium")),
        )
        for item in raw["signatures"]
    )


def load_benign_samples() -> list[str]:
    raw = json.loads((DATA_DIR / "benign_samples.json").read_text(encoding="utf-8"))
    return list(raw["samples"])


class InjectionDetector:
    def __init__(
        self,
        threshold: float = 0.61,
        backend: str = "auto",
        embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
        signatures: tuple[Signature, ...] | None = None,
    ) -> None:
        self.threshold = threshold
        self.signatures = list(signatures or load_signatures())
        self.backend_name, self.index = self._build_index(backend, embedding_model)

    def _build_index(self, backend: str, model_name: str) -> tuple[str, Any]:
        if backend in ("auto", "embeddings"):
            try:
                return "embeddings", EmbeddingIndex(self.signatures, model_name)
            except Exception:
                if backend == "embeddings":
                    # Explicitly requested and unavailable: fail loudly rather
                    # than silently running a weaker detector than configured.
                    raise
        return "lexical", LexicalIndex(self.signatures)

    def scan(self, text: str) -> InjectionResult:
        if not text or not text.strip():
            return InjectionResult(backend=self.backend_name)
        text = text[:MAX_SCAN_CHARS]
        normalized = normalize(text)

        rule_hits: list[str] = []
        rule_score = 0.0
        rule_category = ""
        rule_severity = Severity.INFO
        for rule in RULES:
            if rule.regex.search(text) or rule.regex.search(normalized):
                rule_hits.append(rule.name)
                if rule.score > rule_score:
                    rule_score, rule_category, rule_severity = (
                        rule.score,
                        rule.category,
                        rule.severity,
                    )

        similarity, signature = self.index.best_match(text)

        score = max(rule_score, similarity)
        if rule_score >= similarity:
            category, severity = rule_category, rule_severity
            matched = rule_hits[0] if rule_hits else ""
        else:
            category = signature.category if signature else ""
            severity = signature.severity if signature else Severity.MEDIUM
            matched = signature.text if signature else ""

        detected = score >= self.threshold
        return InjectionResult(
            score=score,
            detected=detected,
            category=category if detected else "",
            severity=severity if detected else Severity.INFO,
            matched_signature=matched if detected else "",
            rule_hits=rule_hits,
            backend=self.backend_name,
        )


@lru_cache(maxsize=4)
def get_detector(
    threshold: float = 0.61,
    backend: str = "auto",
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
) -> InjectionDetector:
    return InjectionDetector(threshold=threshold, backend=backend, embedding_model=embedding_model)


__all__ = [
    "RULES",
    "EmbeddingIndex",
    "InjectionDetector",
    "InjectionResult",
    "LexicalIndex",
    "Rule",
    "Signature",
    "get_detector",
    "load_benign_samples",
    "load_signatures",
    "normalize",
    "tokenize",
]
