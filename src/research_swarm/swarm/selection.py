"""Seed-aware incumbent comparisons and diverse parent selection."""

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from math import copysign, inf, isfinite, sqrt
from statistics import fmean, variance
from typing import Literal


@dataclass(frozen=True)
class Comparison:
    decision: Literal["replicate", "retain", "promote"]
    gain: float | None
    z_score: float | None


def compare_seeds(
    incumbent: Mapping[int, float],
    challenger: Mapping[int, float],
    *,
    minimize: bool = True,
    min_replicates: int = 2,
    seed_std: float = 0.0,
    threshold: float = 1.96,
) -> Comparison:
    """Compare independent seed means at the same fidelity, metric and panel.

    Each recipe's observations are keyed by distinct evaluation seed.
    Calibration supplies a standard-deviation floor, so two lucky identical
    measurements cannot erase known seed noise.
    """
    if min_replicates < 2 or not isfinite(seed_std) or seed_std < 0:
        raise ValueError("need at least two replicates and a finite nonnegative seed_std")
    if not isfinite(threshold) or threshold < 0:
        raise ValueError("threshold must be finite and nonnegative")
    for scores in (incumbent, challenger):
        if any(not isfinite(score) for score in scores.values()):
            raise ValueError("scores must be finite")
    if min(len(incumbent), len(challenger)) < min_replicates:
        return Comparison("replicate", None, None)
    gain = fmean(challenger.values()) - fmean(incumbent.values())
    if minimize:
        gain = -gain
    error = sqrt(
        sum(
            max(variance(scores.values()), seed_std**2) / len(scores)
            for scores in (incumbent, challenger)
        )
    )
    z_score = gain / error if error else copysign(inf, gain) if gain else 0.0
    return Comparison("promote" if gain > 0 and z_score >= threshold else "retain", gain, z_score)


@dataclass(frozen=True)
class Parent:
    candidate_id: str
    family: str
    score: float
    # Recipe features exclude evaluation seeds and candidate identifiers.
    features: frozenset[str]


def island_parents(
    population: Sequence[Parent],
    count: int,
    *,
    minimize: bool = True,
    per_family: int = 1,
    novelty_weight: float = 0.25,
) -> list[Parent]:
    """Select across families without replacement using rank and Jaccard novelty."""
    if count < 0 or per_family < 1 or not 0 <= novelty_weight <= 1:
        raise ValueError("invalid parent selection limits")
    if any(not isfinite(parent.score) for parent in population):
        raise ValueError("scores must be finite")
    if len({p.candidate_id for p in population}) != len(population):
        raise ValueError("expected one score per candidate")

    ranked = sorted(population, key=lambda p: (p.score if minimize else -p.score, p.candidate_id))
    selected: list[Parent] = []
    used: dict[str, int] = defaultdict(int)

    def priority(item: tuple[int, Parent]) -> tuple[int, float, int]:
        rank, parent = item
        quality = 1 - rank / max(1, len(population) - 1)
        distances = []
        for other in selected:
            union = parent.features | other.features
            distance = 1 - len(parent.features & other.features) / len(union) if union else 0
            distances.append(distance)
        novelty = min(distances, default=1.0)
        priority_score = (1 - novelty_weight) * quality + novelty_weight * novelty
        return -used[parent.family], priority_score, -rank

    remaining = list(enumerate(ranked))
    while remaining and len(selected) < count:
        eligible = [item for item in remaining if used[item[1].family] < per_family]
        if not eligible:
            break
        chosen = max(eligible, key=priority)
        remaining.remove(chosen)
        parent = chosen[1]
        used[parent.family] += 1
        selected.append(parent)
    return selected
