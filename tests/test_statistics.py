from __future__ import annotations

import itertools
from fractions import Fraction

import pytest
from pydantic import ValidationError

from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.experiments import Direction
from research_swarm.evaluation.statistics import (
    FixedPairedPolicy,
    PairedObservation,
    paired_sign_gate,
)


def observations(values: tuple[float, ...]) -> tuple[PairedObservation, ...]:
    return tuple(
        PairedObservation(seed=seed, candidate=0.0, comparator=value)
        for seed, value in enumerate(values)
    )


def test_three_unanimous_wins_cannot_establish_95_percent_gate() -> None:
    with pytest.raises(ValidationError, match="cannot meet"):
        FixedPairedPolicy(seeds=(0, 1, 2))
    permissive = FixedPairedPolicy(seeds=(0, 1, 2), confidence=0.8)
    result = paired_sign_gate(permissive, observations((1.0,) * 3), Direction.MINIMIZE)
    assert result.p_value is not None and result.p_value.fraction() == Fraction(1, 8)
    for invalid in ((True, 1, 2, 3, 4), (0, 0, 2, 3, 4), (0.0, 1, 2, 3, 4)):
        with pytest.raises(ValidationError):
            FixedPairedPolicy(seeds=invalid)


def test_exact_null_sign_enumeration_and_direction_invariance() -> None:
    policy = FixedPairedPolicy(seeds=(0, 1, 2, 3, 4))
    passing = 0
    for signs in itertools.product((-1.0, 1.0), repeat=5):
        result = paired_sign_gate(policy, observations(signs), Direction.MINIMIZE)
        inverse = paired_sign_gate(
            policy, observations(tuple(-value for value in signs)), Direction.MAXIMIZE
        )
        assert result == inverse
        passing += result.outcome == "promote"
    assert passing == 1  # 1/32 under independent, equally likely null signs.
    four = paired_sign_gate(policy, observations((1.0,) * 4 + (-1.0,)), Direction.MINIMIZE)
    assert four.p_value is not None and four.p_value.fraction() == Fraction(6, 32)
    assert four.outcome == "inconclusive"


def test_no_op_ties_and_practical_threshold_never_promote() -> None:
    policy = FixedPairedPolicy(seeds=tuple(range(5)), minimum_effect=0.1)
    for value in (0.0, 0.1):
        result = paired_sign_gate(policy, observations((value,) * 5), Direction.MINIMIZE)
        assert result.outcome == "reject" and result.wins == 0
    assert result.ties == 5


def test_mean_safeguard_rejects_large_loss_despite_sign_win() -> None:
    policy = FixedPairedPolicy(seeds=tuple(range(10)))
    result = paired_sign_gate(policy, observations((1.0,) * 9 + (-100.0,)), Direction.MINIMIZE)
    assert result.p_value is not None and result.p_value.fraction() < Fraction(5, 100)
    assert result.outcome == "reject" and result.finite_mean() == -9.1


def test_favorable_prefixes_do_not_compute_significance_or_promote() -> None:
    policy = FixedPairedPolicy(seeds=tuple(range(8)))
    for count in range(8):
        result = paired_sign_gate(policy, observations((1.0,) * count), Direction.MINIMIZE)
        assert result.outcome == "replicate"
        assert result.p_value is None and result.mean_improvement is None
        assert result.missing_seeds == tuple(range(count, 8))


def test_duplicate_and_unknown_seeds_cannot_be_selected() -> None:
    policy = FixedPairedPolicy(seeds=tuple(range(5)))
    with pytest.raises(ResearchError, match="duplicate or undeclared"):
        paired_sign_gate(policy, observations((1.0,)) * 5, Direction.MINIMIZE)
    with pytest.raises(ResearchError, match="duplicate or undeclared"):
        paired_sign_gate(policy, observations((1.0,) * 6), Direction.MINIMIZE)


def test_large_exact_probabilities_and_finite_extremes_remain_serializable() -> None:
    policy = FixedPairedPolicy(seeds=tuple(range(128)))
    pairs = tuple(
        PairedObservation(seed=i, candidate=-1e308, comparator=1e308) for i in policy.seeds
    )
    result = paired_sign_gate(policy, pairs, Direction.MINIMIZE)
    assert result.outcome == "promote" and result.finite_mean() is None
    assert result.p_value is not None and result.p_value.fraction() == Fraction(1, 2**128)
    assert canonical_bytes(result.model_dump(mode="json"))
