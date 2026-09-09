"""Server-side redaction.

The rule this module exists to enforce: **redaction happens while the response
body is being built, not in the browser.** Hiding a field with CSS, or filtering
it in a React component, is cosmetic -- `curl` with the same token gets the raw
value, and so does anyone who opens devtools. Here, the bytes that leave the
process are already masked, so there is no bypass to find.

Two levels:

``full``
    For the ``viewer`` role. Masks everything the pattern registry knows about:
    emails, phone numbers, cards, government ids, IPs, secrets.

``secrets only``
    For the ``admin`` role. An admin is trusted with customer PII -- that is the
    point of the role -- but nobody needs to read a live API key out of a trace,
    and a screenshot of one is a credential leak. Secrets stay masked for
    everyone.
"""

from __future__ import annotations

import functools
from typing import Any

from ..guardrails.patterns import PATTERNS, SECRET_PATTERNS, Pattern
from ..guardrails.patterns import redact_text as _redact_text

MAX_DEPTH = 8
MAX_ITEMS = 200

SECRET_ONLY_PATTERNS: tuple[Pattern, ...] = tuple(p for p in PATTERNS if p.name in SECRET_PATTERNS)


class PatternRedactor:
    """Masks known identifiers in any string reachable from a payload."""

    def __init__(self, patterns: tuple[Pattern, ...] = PATTERNS, full: bool = True) -> None:
        self.patterns = patterns
        #: True when this redactor strips PII as well as secrets. Surfaced on the
        #: response so a client can tell a masked value from a real one.
        self.full = full

    def redact_text(self, value: str) -> str:
        if not value:
            return value
        return _redact_text(value, self.patterns)[0]

    def redact_payload(self, value: dict[str, Any]) -> dict[str, Any]:
        result = self._walk(value, 0)
        return result if isinstance(result, dict) else {}

    def _walk(self, value: Any, depth: int) -> Any:
        if depth > MAX_DEPTH:
            # A payload nested deeper than this is not something a human reads;
            # returning a marker beats either recursing forever or leaking it.
            return "[redaction depth limit]"
        if isinstance(value, str):
            return self.redact_text(value)
        if isinstance(value, dict):
            return {str(k): self._walk(v, depth + 1) for k, v in list(value.items())[:MAX_ITEMS]}
        if isinstance(value, (list, tuple)):
            return [self._walk(v, depth + 1) for v in list(value)[:MAX_ITEMS]]
        return value


@functools.lru_cache(maxsize=2)
def get_redactor(full: bool = True) -> PatternRedactor:
    """Cached redactor. Stateless, so one instance per level is enough."""
    return PatternRedactor(PATTERNS if full else SECRET_ONLY_PATTERNS, full=full)


def redactor_for_role(can_view_raw: bool) -> PatternRedactor:
    """Pick the redaction level for a role.

    Note there is no ``None`` branch: every response goes through a redactor.
    Making "no redaction at all" unrepresentable is what stops a future endpoint
    from forgetting to ask for one.
    """
    return get_redactor(full=not can_view_raw)


__all__ = [
    "SECRET_ONLY_PATTERNS",
    "PatternRedactor",
    "get_redactor",
    "redactor_for_role",
]
