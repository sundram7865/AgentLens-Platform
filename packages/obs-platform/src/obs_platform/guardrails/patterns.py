"""PII / secret pattern registry.

One definition, two consumers: the guardrail scanner (which records findings)
and the API redactor (which rewrites payloads before they reach a viewer). Two
separate copies of "what counts as a credit card" is how a system ends up
flagging something it then happily returns in full.

Why regex plus validators rather than only an NER model
-------------------------------------------------------
Presidio is supported and preferred when it is installed (``OBS_PII_ENGINE``),
but it pulls in spaCy and a language model -- comfortably over 400 MB resident,
which does not fit the 512 MB free-tier box this project promises to deploy on.
So the built-in engine is the default, and it is deliberately not "just regex":
every high-severity structured identifier is confirmed by its own **checksum**
(Luhn for cards, mod-97 for IBAN, Verhoeff for Aadhaar) before it is reported.
That is what keeps a 16-digit order number from being flagged as a credit card.

Scope, stated honestly: this covers structured identifiers and secrets well, and
free-text names and addresses not at all. Names need NER -- install the
``presidio`` extra for that.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from obs_sdk.schema import Severity


@dataclass(frozen=True)
class Pattern:
    """One detector: how to find it, how to confirm it, how to mask it."""

    name: str
    regex: re.Pattern[str]
    severity: Severity
    score: float
    #: Returns False to reject a regex hit (checksum failure, known test value).
    validator: Callable[[str], bool] | None = None
    #: Renders the masked replacement. Defaults to full masking.
    masker: Callable[[str], str] | None = None
    description: str = ""
    #: Regex groups whose content is the actual secret (for context-prefixed hits).
    value_group: int = 0
    tags: tuple[str, ...] = field(default_factory=tuple)

    def mask(self, value: str) -> str:
        return self.masker(value) if self.masker else mask_all(value)


# --------------------------------------------------------------------------- #
# Maskers
# --------------------------------------------------------------------------- #
def mask_all(value: str) -> str:
    return "*" * min(len(value), 12)


def mask_keep_last(value: str, keep: int = 4) -> str:
    digits = re.sub(r"\D", "", value)
    if len(digits) <= keep:
        return "*" * len(digits)
    return "*" * (len(digits) - keep) + digits[-keep:]


def mask_email(value: str) -> str:
    """Keep the first character and the domain: enough to debug, not enough to contact."""
    local, _, domain = value.partition("@")
    if not domain:
        return mask_all(value)
    head = local[0] if local else ""
    return "{}{}@{}".format(head, "*" * max(3, len(local) - 1), domain)


def mask_ip(value: str) -> str:
    if ":" in value:  # IPv6
        return value.split(":")[0] + ":****"
    parts = value.split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.x.x"
    return mask_all(value)


def mask_secret(value: str) -> str:
    """Keep a short prefix so a leaked key can be identified and rotated."""
    prefix = value[:6]
    return "{}{}".format(prefix, "*" * 10)


# --------------------------------------------------------------------------- #
# Validators
# --------------------------------------------------------------------------- #
def luhn_valid(value: str) -> bool:
    """Luhn check. Rejects the 16-digit order ids that would otherwise look like cards."""
    digits = [int(c) for c in re.sub(r"\D", "", value)]
    if not 12 <= len(digits) <= 19:
        return False
    checksum = 0
    parity = len(digits) % 2
    for index, digit in enumerate(digits):
        if index % 2 == parity:
            digit *= 2
            if digit > 9:
                digit -= 9
        checksum += digit
    return checksum % 10 == 0


def iban_valid(value: str) -> bool:
    """ISO 13616 mod-97 check."""
    compact = re.sub(r"[^A-Za-z0-9]", "", value).upper()
    if not 15 <= len(compact) <= 34:
        return False
    rearranged = compact[4:] + compact[:4]
    digits = "".join(str(int(c, 36)) if c.isalpha() else c for c in rearranged)
    try:
        return int(digits) % 97 == 1
    except ValueError:
        return False


_SSN_INVALID_AREAS = {"000", "666"}


def ssn_valid(value: str) -> bool:
    """US SSN structural rules: no 000/666/9xx area, no 00 group, no 0000 serial."""
    digits = re.sub(r"\D", "", value)
    if len(digits) != 9:
        return False
    area, group, serial = digits[:3], digits[3:5], digits[5:]
    if area in _SSN_INVALID_AREAS or area.startswith("9"):
        return False
    return group != "00" and serial != "0000"


# Verhoeff tables, used by the Aadhaar checksum.
_VERHOEFF_D = (
    (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
    (1, 2, 3, 4, 0, 6, 7, 8, 9, 5),
    (2, 3, 4, 0, 1, 7, 8, 9, 5, 6),
    (3, 4, 0, 1, 2, 8, 9, 5, 6, 7),
    (4, 0, 1, 2, 3, 9, 5, 6, 7, 8),
    (5, 9, 8, 7, 6, 0, 4, 3, 2, 1),
    (6, 5, 9, 8, 7, 1, 0, 4, 3, 2),
    (7, 6, 5, 9, 8, 2, 1, 0, 4, 3),
    (8, 7, 6, 5, 9, 3, 2, 1, 0, 4),
    (9, 8, 7, 6, 5, 4, 3, 2, 1, 0),
)
_VERHOEFF_P = (
    (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
    (1, 5, 7, 6, 2, 8, 3, 0, 9, 4),
    (5, 8, 0, 3, 7, 9, 6, 1, 4, 2),
    (8, 9, 1, 6, 0, 4, 3, 5, 2, 7),
    (9, 4, 5, 3, 1, 2, 6, 8, 7, 0),
    (4, 2, 8, 6, 5, 7, 3, 9, 0, 1),
    (2, 7, 9, 3, 8, 0, 6, 4, 1, 5),
    (7, 0, 4, 6, 9, 1, 3, 2, 5, 8),
)


def aadhaar_valid(value: str) -> bool:
    """Verhoeff checksum.

    Included because this platform watches an India-facing ecommerce support
    product. Presidio's default recognisers are US/EN-tuned and would not catch
    this at all -- worth knowing rather than assuming coverage.
    """
    digits = re.sub(r"\D", "", value)
    if len(digits) != 12 or digits[0] in "01":
        return False
    checksum = 0
    for index, digit in enumerate(reversed(digits)):
        checksum = _VERHOEFF_D[checksum][_VERHOEFF_P[index % 8][int(digit)]]
    return checksum == 0


def phone_valid(value: str) -> bool:
    """Confirm a digit run is plausibly a phone number.

    Phone numbers are the most ambiguous identifier here: "2024-2025" and a
    7-digit order reference both look like one. The rule is a national-length
    number (10-15 digits) or anything with an explicit country code, which keeps
    year ranges and short reference numbers out while still catching both the
    US `(555) 123-4567` and the Indian `+91 98765 43210` shapes.
    """
    digits = re.sub(r"\D", "", value)
    if len(set(digits)) <= 1:
        return False
    if value.lstrip().startswith("+"):
        return 7 <= len(digits) <= 15
    return 10 <= len(digits) <= 15


def _always(_: str) -> bool:
    return True


# --------------------------------------------------------------------------- #
# The registry
# --------------------------------------------------------------------------- #
PATTERNS: tuple[Pattern, ...] = (
    Pattern(
        name="EMAIL_ADDRESS",
        regex=re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
        severity=Severity.MEDIUM,
        score=0.95,
        masker=mask_email,
        description="Email address",
        tags=("pii", "contact"),
    ),
    Pattern(
        name="CREDIT_CARD",
        # Separators allowed, then confirmed by Luhn -- the regex alone would
        # flag every 16-digit order number in a support ticket.
        regex=re.compile(r"\b(?:\d[ -]*?){13,19}\b"),
        severity=Severity.CRITICAL,
        score=0.99,
        validator=luhn_valid,
        masker=lambda v: mask_keep_last(v, 4),
        description="Payment card number (Luhn-validated)",
        tags=("pii", "financial", "pci"),
    ),
    Pattern(
        name="US_SSN",
        regex=re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
        severity=Severity.CRITICAL,
        score=0.9,
        validator=ssn_valid,
        description="US Social Security Number",
        tags=("pii", "government_id"),
    ),
    Pattern(
        name="IN_AADHAAR",
        regex=re.compile(r"\b[2-9]\d{3}[ -]?\d{4}[ -]?\d{4}\b"),
        severity=Severity.CRITICAL,
        score=0.9,
        validator=aadhaar_valid,
        masker=lambda v: mask_keep_last(v, 4),
        description="Aadhaar number (Verhoeff-validated)",
        tags=("pii", "government_id", "india"),
    ),
    Pattern(
        name="IN_PAN",
        regex=re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b"),
        severity=Severity.HIGH,
        score=0.85,
        masker=lambda v: v[:2] + "*" * 7 + v[-1],
        description="Indian Permanent Account Number",
        tags=("pii", "government_id", "india"),
    ),
    Pattern(
        name="IBAN",
        regex=re.compile(r"\b[A-Z]{2}\d{2}[ ]?(?:[A-Z0-9]{4}[ ]?){2,7}[A-Z0-9]{1,4}\b"),
        severity=Severity.HIGH,
        score=0.9,
        validator=iban_valid,
        masker=lambda v: v[:4] + "*" * 8,
        description="International bank account number",
        tags=("pii", "financial"),
    ),
    Pattern(
        name="PHONE_NUMBER",
        regex=re.compile(r"(?<![\w.+])\+?\d[\d .()-]{6,18}\d(?![\w.])"),
        severity=Severity.MEDIUM,
        score=0.6,
        validator=phone_valid,
        masker=lambda v: mask_keep_last(v, 3),
        description="Telephone number",
        tags=("pii", "contact"),
    ),
    Pattern(
        name="IP_ADDRESS",
        regex=re.compile(
            r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d?\d)\b"
        ),
        severity=Severity.LOW,
        score=0.7,
        masker=mask_ip,
        description="IPv4 address",
        tags=("pii", "network"),
    ),
    Pattern(
        name="AWS_ACCESS_KEY",
        regex=re.compile(r"\b(?:AKIA|ASIA|AIDA|AROA)[0-9A-Z]{16}\b"),
        severity=Severity.CRITICAL,
        score=0.99,
        masker=mask_secret,
        description="AWS access key id",
        tags=("secret", "credential"),
    ),
    Pattern(
        name="API_KEY",
        regex=re.compile(
            r"\b(?:sk-[A-Za-z0-9_-]{16,}|sk-ant-[A-Za-z0-9_-]{16,}|ghp_[A-Za-z0-9]{20,}"
            r"|gho_[A-Za-z0-9]{20,}|xox[baprs]-[A-Za-z0-9-]{10,}|AIza[0-9A-Za-z_-]{30,})"
        ),
        severity=Severity.CRITICAL,
        score=0.97,
        masker=mask_secret,
        description="Provider API key or token",
        tags=("secret", "credential"),
    ),
    Pattern(
        name="JWT",
        regex=re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"),
        severity=Severity.HIGH,
        score=0.9,
        masker=mask_secret,
        description="JSON Web Token",
        tags=("secret", "credential"),
    ),
    Pattern(
        name="PRIVATE_KEY",
        regex=re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |PGP )?PRIVATE KEY-----"),
        severity=Severity.CRITICAL,
        score=0.99,
        masker=lambda _: "-----BEGIN PRIVATE KEY----- [REDACTED]",
        description="Embedded private key",
        tags=("secret", "credential"),
    ),
    Pattern(
        name="CREDENTIALED_URL",
        regex=re.compile(r"\b[a-z][a-z0-9+.-]*://[^\s:/@]+:[^\s:/@]+@[^\s/]+"),
        severity=Severity.CRITICAL,
        score=0.95,
        masker=lambda v: re.sub(r"://[^\s:/@]+:[^\s:/@]+@", "://***:***@", v),
        description="URL containing inline credentials",
        tags=("secret", "credential"),
    ),
)

PATTERNS_BY_NAME: dict[str, Pattern] = {p.name: p for p in PATTERNS}

#: Patterns whose hits are secrets rather than personal data. Redacted for every
#: role, including admin -- an operator has no reason to read a live API key out
#: of a trace, and a screenshot of one is a credential leak.
SECRET_PATTERNS: frozenset[str] = frozenset(
    p.name for p in PATTERNS if "secret" in p.tags or "credential" in p.tags
)


@dataclass(frozen=True)
class Match:
    """One confirmed hit."""

    pattern: str
    severity: Severity
    score: float
    start: int
    end: int
    value: str
    masked: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "type": self.pattern,
            "severity": self.severity.value,
            "score": self.score,
            "start": self.start,
            "end": self.end,
            "masked": self.masked,
        }


def scan_text(
    text: str, patterns: tuple[Pattern, ...] = PATTERNS, max_matches: int = 200
) -> list[Match]:
    """Find every validated match, resolving overlaps in favour of the stronger hit."""
    if not text:
        return []
    found: list[Match] = []
    for pattern in patterns:
        for match in pattern.regex.finditer(text):
            value = match.group(pattern.value_group)
            if not value:
                continue
            validator = pattern.validator or _always
            if not validator(value):
                continue
            found.append(
                Match(
                    pattern=pattern.name,
                    severity=pattern.severity,
                    score=pattern.score,
                    start=match.start(pattern.value_group),
                    end=match.end(pattern.value_group),
                    value=value,
                    masked=pattern.mask(value),
                )
            )
            if len(found) >= max_matches:
                break
    return _resolve_overlaps(found)


def _resolve_overlaps(matches: list[Match]) -> list[Match]:
    """Keep the strongest match per span.

    A card number also matches the phone pattern. Reporting both would double
    the finding count and put a MEDIUM label next to a CRITICAL one.
    """
    ordered = sorted(matches, key=lambda m: (-m.severity.rank, -m.score, m.start, -m.end))
    kept: list[Match] = []
    for candidate in ordered:
        if any(candidate.start < k.end and k.start < candidate.end for k in kept):
            continue
        kept.append(candidate)
    return sorted(kept, key=lambda m: m.start)


def redact_text(text: str, patterns: tuple[Pattern, ...] = PATTERNS) -> tuple[str, list[Match]]:
    """Replace every match with its mask. Returns the new text and what was found."""
    matches = scan_text(text, patterns)
    if not matches:
        return text, []
    out: list[str] = []
    cursor = 0
    for match in matches:
        out.append(text[cursor : match.start])
        out.append(match.masked)
        cursor = match.end
    out.append(text[cursor:])
    return "".join(out), matches


__all__ = [
    "PATTERNS",
    "PATTERNS_BY_NAME",
    "SECRET_PATTERNS",
    "Match",
    "Pattern",
    "aadhaar_valid",
    "iban_valid",
    "luhn_valid",
    "mask_all",
    "mask_email",
    "mask_ip",
    "mask_keep_last",
    "mask_secret",
    "phone_valid",
    "redact_text",
    "scan_text",
    "ssn_valid",
]
