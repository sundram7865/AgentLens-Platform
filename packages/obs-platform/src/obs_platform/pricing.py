"""Token pricing.

Cost is computed **here**, in the platform, not in the SDK. That split is
deliberate: a vendor price change should be a config edit on one service, not a
redeploy of SupportPilot.

Everything is integer micro-dollars (USD * 1e6). Summing tens of thousands of
float costs and comparing the total against a budget cap is how a guard rail
drifts by a cent a day until it stops guarding anything.

The table below is a **default**, and it will go stale. Override it without a
code change by setting ``OBS_PRICING_JSON`` to a JSON object of
``{"model-prefix": {"input": <usd per 1M>, "output": <usd per 1M>}}``. Unknown
models cost zero and are counted in ``unknown_models()`` so the gap is visible
on the dashboard rather than silently reported as free.
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass

MICROS_PER_USD = 1_000_000


@dataclass(frozen=True)
class ModelPrice:
    """USD per 1M tokens."""

    input_per_mtok: float
    output_per_mtok: float


# Keys are matched as case-insensitive *prefixes*, longest first, so
# "claude-haiku-4-5" matches an id that carries a provider prefix or suffix.
DEFAULT_PRICES: dict[str, ModelPrice] = {
    # -- Anthropic (verified against the Claude API pricing table, 2026-06) ---
    "claude-fable-5": ModelPrice(10.00, 50.00),
    "claude-mythos-5": ModelPrice(10.00, 50.00),
    "claude-opus-5": ModelPrice(5.00, 25.00),
    "claude-opus-4-8": ModelPrice(5.00, 25.00),
    "claude-opus-4-7": ModelPrice(5.00, 25.00),
    "claude-opus-4-6": ModelPrice(5.00, 25.00),
    "claude-sonnet-5": ModelPrice(2.00, 10.00),
    "claude-sonnet-4-6": ModelPrice(3.00, 15.00),
    "claude-haiku-4-5": ModelPrice(1.00, 5.00),
    # -- Other providers: reasonable defaults, NOT authoritative. Check your
    #    provider's current pricing and override via OBS_PRICING_JSON.
    "gemini-2.0-flash": ModelPrice(0.10, 0.40),
    "gemini-1.5-flash": ModelPrice(0.075, 0.30),
    "gemini-1.5-pro": ModelPrice(1.25, 5.00),
    "gpt-4o-mini": ModelPrice(0.15, 0.60),
    "gpt-4o": ModelPrice(2.50, 10.00),
    "llama-3.3-70b": ModelPrice(0.59, 0.79),
    "llama-3.1-8b": ModelPrice(0.05, 0.08),
    # The mock provider SupportPilot uses with AI_PROVIDER=mock.
    "mock": ModelPrice(0.0, 0.0),
}

_lock = threading.Lock()
_prices: dict[str, ModelPrice] | None = None
_unknown: dict[str, int] = {}


def _load_prices() -> dict[str, ModelPrice]:
    global _prices
    if _prices is not None:
        return _prices
    with _lock:
        if _prices is not None:  # pragma: no cover - double-checked locking
            return _prices
        merged = dict(DEFAULT_PRICES)
        raw = os.environ.get("OBS_PRICING_JSON")
        if raw:
            try:
                for key, value in json.loads(raw).items():
                    merged[key.lower()] = ModelPrice(float(value["input"]), float(value["output"]))
            except Exception as exc:  # pragma: no cover - config error path
                raise ValueError("OBS_PRICING_JSON is not valid pricing JSON") from exc
        _prices = merged
        return _prices


def reset_pricing_cache() -> None:
    global _prices
    with _lock:
        _prices = None
        _unknown.clear()


def lookup(model: str | None) -> ModelPrice | None:
    """Longest-prefix match on a model id, case-insensitively."""
    if not model:
        return None
    needle = model.strip().lower()
    prices = _load_prices()
    best: ModelPrice | None = None
    best_len = -1
    for prefix, price in prices.items():
        if prefix in needle and len(prefix) > best_len:
            best, best_len = price, len(prefix)
    return best


def cost_micros(model: str | None, prompt_tokens: int, completion_tokens: int) -> int:
    """Cost of one call in integer micro-dollars. Unknown models cost 0, loudly."""
    price = lookup(model)
    if price is None:
        if model:
            with _lock:
                _unknown[model] = _unknown.get(model, 0) + 1
        return 0
    # Clamp before multiplying. Usage validation already rejects negatives, but
    # this is the money path: a negative prompt count arriving from anywhere
    # would quietly *subtract* from the cost and, via tenant_usage, from the
    # budget a tenant has spent -- turning the spend cap into something an
    # attacker could unwind.
    prompt_tokens = max(0, prompt_tokens)
    completion_tokens = max(0, completion_tokens)
    # price is USD per 1e6 tokens, and a micro-dollar is USD/1e6, so the two
    # factors of 1e6 cancel: micros == tokens * price_per_mtok.
    micros = prompt_tokens * price.input_per_mtok + completion_tokens * price.output_per_mtok
    return round(micros)


def unknown_models() -> dict[str, int]:
    """Models seen with no price entry, and how often. Surfaced on /health/meta."""
    with _lock:
        return dict(_unknown)


def micros_to_usd(micros: int) -> float:
    return round(micros / MICROS_PER_USD, 6)


def usd_to_micros(usd: float) -> int:
    return round(usd * MICROS_PER_USD)


__all__ = [
    "DEFAULT_PRICES",
    "MICROS_PER_USD",
    "ModelPrice",
    "cost_micros",
    "lookup",
    "micros_to_usd",
    "reset_pricing_cache",
    "unknown_models",
    "usd_to_micros",
]
