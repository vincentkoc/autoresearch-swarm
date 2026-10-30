"""A fixed-panel paired sign gate with explicit, narrow statistical claims."""

from __future__ import annotations

import math
from fractions import Fraction
from typing import Annotated, Literal

from pydantic import Field, model_validator

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import Direction
from research_swarm.domain.types import VersionedRecord


class FixedPairedPolicy(VersionedRecord):
    version: Literal["paired-sign-v1"] = "paired-sign-v1"
    seeds: tuple[Annotated[int, Field(strict=True, ge=0, le=9_007_199_254_740_991)], ...] = Field(
        min_length=1, max_length=128
    )
    minimum_effect: float = Field(default=0.0, strict=True, ge=0)
    confidence: float = Field(default=0.95, strict=True, gt=0.5, lt=1)

    @model_validator(mode="after")
    def feasible_panel(self) -> FixedPairedPolicy:
        if len(set(self.seeds)) != len(self.seeds) or any(
            type(seed) is not int or not 0 <= seed <= 9_007_199_254_740_991 for seed in self.seeds
        ):
            raise ValueError("Confirmation requires distinct nonnegative integer seeds")
        if Fraction(1, 2 ** len(self.seeds)) > 1 - Fraction(str(self.confidence)):
            raise ValueError("The fixed panel cannot meet this confidence threshold")
        return self


class PairedObservation(VersionedRecord):
    seed: int = Field(strict=True, ge=0, le=9_007_199_254_740_991)
    candidate: float = Field(strict=True)
    comparator: float = Field(strict=True)


class Rational(VersionedRecord):
    """Decimal integer strings preserve exact values beyond JSON's safe integers."""

    numerator: str = Field(pattern=r"^-?(0|[1-9][0-9]*)$", max_length=2048)
    denominator: str = Field(pattern=r"^[1-9][0-9]*$", max_length=2048)

    @classmethod
    def of(cls, value: Fraction) -> Rational:
        return cls(numerator=str(value.numerator), denominator=str(value.denominator))

    def fraction(self) -> Fraction:
        return Fraction(int(self.numerator), int(self.denominator))


class GateResult(VersionedRecord):
    outcome: Literal["promote", "reject", "replicate", "inconclusive"]
    expected_pairs: int
    observed_pairs: int
    wins: int
    ties: int
    missing_seeds: tuple[int, ...]
    alpha: Rational
    p_value: Rational | None = None
    mean_improvement: Rational | None = None
    median_improvement: Rational | None = None
    reason: str

    def finite_mean(self) -> float | None:
        if self.mean_improvement is None:
            return None
        try:
            value = float(self.mean_improvement.fraction())
        except OverflowError:
            return None
        return value if math.isfinite(value) else None


def paired_sign_gate(
    policy: FixedPairedPolicy,
    observations: tuple[PairedObservation, ...],
    direction: Direction,
) -> GateResult:
    """Test P(direction-adjusted improvement > minimum_effect) <= 0.5.

    Requires independent, preselected pairs. No mean confidence interval, optional
    stopping, adaptive panel reuse, or generalized performance claim is implied.
    Eligibility and exposure are enforced by the caller before this pure function.
    """
    policy = FixedPairedPolicy.model_validate(policy)
    observations = tuple(PairedObservation.model_validate(item) for item in observations)
    direction = Direction(direction)
    seeds = {item.seed for item in observations}
    if len(seeds) != len(observations) or not seeds <= set(policy.seeds):
        raise ResearchError(
            ErrorCode.COMPARISON_INELIGIBLE, "Pairs contain duplicate or undeclared seeds"
        )
    differences = [
        (Fraction(str(item.comparator)) - Fraction(str(item.candidate)))
        * (1 if direction == Direction.MINIMIZE else -1)
        for item in observations
    ]
    effect = Fraction(str(policy.minimum_effect))
    alpha = 1 - Fraction(str(policy.confidence))
    wins = sum(value > effect for value in differences)
    missing = tuple(seed for seed in policy.seeds if seed not in seeds)
    fields = {
        "expected_pairs": len(policy.seeds),
        "observed_pairs": len(observations),
        "wins": wins,
        "ties": sum(value == effect for value in differences),
        "missing_seeds": missing,
        "alpha": Rational.of(alpha),
    }
    if missing:
        return GateResult(**fields, outcome="replicate", reason="fixed_panel_incomplete")
    count = len(differences)
    p_value = Fraction(sum(math.comb(count, index) for index in range(wins, count + 1)), 2**count)
    mean = sum(differences, Fraction()) / count
    ordered = sorted(differences)
    median = (ordered[(count - 1) // 2] + ordered[count // 2]) / 2
    outcome: Literal["promote", "reject", "inconclusive"]
    if mean <= effect:
        outcome, reason = "reject", "practical_effect_not_met"
    elif p_value > alpha:
        outcome, reason = "inconclusive", "fixed_panel_threshold_not_met"
    else:
        outcome, reason = "promote", "fixed_panel_threshold_met"
    return GateResult(
        **fields,
        outcome=outcome,
        reason=reason,
        p_value=Rational.of(p_value),
        mean_improvement=Rational.of(mean),
        median_improvement=Rational.of(median),
    )
