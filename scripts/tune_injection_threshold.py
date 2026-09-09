#!/usr/bin/env python
"""Measure the prompt-injection threshold instead of guessing it.

Run:

    python scripts/tune_injection_threshold.py
    python scripts/tune_injection_threshold.py --backend embeddings   # needs the extra

What it evaluates, and why each part matters:

**Held-out attacks** (``injection_holdout.json``) -- paraphrases that are *not*
in the signature corpus. This is the honest recall number. Scoring the detector
against its own corpus returns ~1.0 for every entry and measures nothing.

**Leave-one-out over the corpus** -- each signature scored against an index
built without it. Isolates how much of the recall comes from similarity
generalising versus from the rules.

**Benign support messages** (``benign_samples.json``) -- the false-positive
rate. This set deliberately includes near-misses ("ignore my last message",
"could a manager override the 30 day window", a quoted `<Error 500>`), because a
threshold tuned only against obviously-innocent text looks perfect here and then
flags real tickets in production.

The chosen operating point favours **precision**: a false positive puts a
paying customer's ticket in a review queue and trains the operator to ignore the
alert list, which costs more than the marginal attack this catch rate misses.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [
    str(ROOT / "packages/obs-sdk/src"),
    str(ROOT / "packages/obs-platform/src"),
]

from obs_platform.guardrails.injection import (  # noqa: E402
    DATA_DIR,
    InjectionDetector,
    load_benign_samples,
    load_signatures,
)


@dataclass
class Metrics:
    threshold: float
    true_positives: int
    false_negatives: int
    false_positives: int
    true_negatives: int

    @property
    def recall(self) -> float:
        total = self.true_positives + self.false_negatives
        return self.true_positives / total if total else 0.0

    @property
    def precision(self) -> float:
        predicted = self.true_positives + self.false_positives
        return self.true_positives / predicted if predicted else 1.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if (p + r) else 0.0

    @property
    def false_positive_rate(self) -> float:
        total = self.false_positives + self.true_negatives
        return self.false_positives / total if total else 0.0


def load_holdout() -> list[str]:
    raw = json.loads((DATA_DIR / "injection_holdout.json").read_text(encoding="utf-8"))
    return list(raw["attacks"])


def score_all(detector: InjectionDetector, texts: list[str]) -> list[float]:
    return [detector.scan(text).score for text in texts]


def leave_one_out_scores(backend: str) -> list[float]:
    """Score each signature against a detector that has never seen it."""
    signatures = list(load_signatures())
    scores: list[float] = []
    for index, held_out in enumerate(signatures):
        rest = tuple(s for i, s in enumerate(signatures) if i != index)
        detector = InjectionDetector(threshold=1.1, backend=backend, signatures=rest)
        scores.append(detector.scan(held_out.text).score)
    return scores


def choose_operating_point(sweep: list[Metrics], min_precision: float) -> Metrics:
    """Pick the MIDDLE of the best-scoring plateau, not its edge.

    Argmax-F1 is the obvious choice and the wrong one: when a whole band of
    thresholds scores identically, argmax returns the lowest of them, which sits
    directly against the highest benign score. One slightly spicier support
    ticket then becomes a false positive. Taking the midpoint of the plateau
    maximises the margin to both classes, which is what actually makes the
    setting hold up on traffic the corpus has not seen.
    """
    viable = [m for m in sweep if m.precision >= min_precision] or sweep
    best_f1 = max(m.f1 for m in viable)
    plateau = [m for m in viable if abs(m.f1 - best_f1) < 1e-9]
    # Longest contiguous run at the best score.
    runs: list[list[Metrics]] = [[]]
    for metrics in sorted(plateau, key=lambda m: m.threshold):
        if runs[-1] and round(metrics.threshold - runs[-1][-1].threshold, 4) > 0.011:
            runs.append([])
        runs[-1].append(metrics)
    longest = max(runs, key=len)
    return longest[len(longest) // 2]


def evaluate(positive: list[float], negative: list[float], threshold: float) -> Metrics:
    return Metrics(
        threshold=threshold,
        true_positives=sum(1 for s in positive if s >= threshold),
        false_negatives=sum(1 for s in positive if s < threshold),
        false_positives=sum(1 for s in negative if s >= threshold),
        true_negatives=sum(1 for s in negative if s < threshold),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", default="lexical", choices=["lexical", "embeddings", "auto"])
    parser.add_argument(
        "--min-precision",
        type=float,
        default=1.0,
        help="Operating constraint: reject any threshold below this precision (default 1.0).",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable output")
    args = parser.parse_args()

    # threshold=1.1 means "never fire", so scan() returns the raw score and the
    # sweep below is what decides detection.
    detector = InjectionDetector(threshold=1.1, backend=args.backend)
    holdout = load_holdout()
    benign = load_benign_samples()

    holdout_scores = score_all(detector, holdout)
    benign_scores = score_all(detector, benign)
    loo_scores = leave_one_out_scores(args.backend)

    thresholds = [round(0.30 + 0.01 * i, 2) for i in range(66)]
    sweep = [evaluate(holdout_scores, benign_scores, t) for t in thresholds]

    best = choose_operating_point(sweep, args.min_precision)

    if args.json:
        print(
            json.dumps(
                {
                    "backend": detector.backend_name,
                    "recommended_threshold": best.threshold,
                    "precision": round(best.precision, 4),
                    "recall": round(best.recall, 4),
                    "f1": round(best.f1, 4),
                    "false_positive_rate": round(best.false_positive_rate, 4),
                    "holdout_attacks": len(holdout),
                    "benign_samples": len(benign),
                },
                indent=2,
            )
        )
        return 0

    print(f"backend                : {detector.backend_name}")
    print(f"held-out attacks       : {len(holdout)}")
    print(f"benign support messages: {len(benign)}")
    print(f"corpus signatures      : {len(loo_scores)} (leave-one-out)")
    print()
    print("score distribution")
    print(
        f"  attacks   min={min(holdout_scores):.3f}  median={_median(holdout_scores):.3f}  max={max(holdout_scores):.3f}"
    )
    print(
        f"  benign    min={min(benign_scores):.3f}  median={_median(benign_scores):.3f}  max={max(benign_scores):.3f}"
    )
    print(f"  LOO corpus median={_median(loo_scores):.3f}")
    print()
    print(
        f"{'thresh':>7} {'prec':>7} {'recall':>7} {'f1':>7} {'FPR':>7}  {'TP':>3} {'FN':>3} {'FP':>3}"
    )
    for metrics in sweep:
        if round(metrics.threshold * 100) % 5:
            continue
        marker = " <-- recommended" if metrics.threshold == best.threshold else ""
        print(
            f"{metrics.threshold:>7.2f} {metrics.precision:>7.3f} {metrics.recall:>7.3f} "
            f"{metrics.f1:>7.3f} {metrics.false_positive_rate:>7.3f}  "
            f"{metrics.true_positives:>3} {metrics.false_negatives:>3} "
            f"{metrics.false_positives:>3}{marker}"
        )

    print()
    print(f"RECOMMENDED OBS_INJECTION_THRESHOLD = {best.threshold}")
    print(
        f"  precision {best.precision:.3f} | recall {best.recall:.3f} | "
        f"F1 {best.f1:.3f} | FPR {best.false_positive_rate:.3f}"
    )

    missed = [t for t, s in zip(holdout, holdout_scores, strict=True) if s < best.threshold]
    if missed:
        print("\n  missed at this threshold (known blind spots, documented in docs/TUNING.md):")
        for text in missed:
            print(f"    - {text[:90]}")
    flagged = [t for t, s in zip(benign, benign_scores, strict=True) if s >= best.threshold]
    if flagged:
        print("\n  benign messages flagged (false positives):")
        for text in flagged:
            print(f"    - {text[:90]}")
    return 0


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if not ordered:
        return 0.0
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


if __name__ == "__main__":
    raise SystemExit(main())
