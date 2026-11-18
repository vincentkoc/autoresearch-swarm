import math

import pytest

from research_swarm.swarm.schedule import ASHA
from research_swarm.swarm.selection import Parent, compare_seeds, island_parents


@pytest.mark.parametrize("minimize", [True, False])
def test_async_promotions_preserve_trajectory_and_rung_budget(minimize: bool) -> None:
    scheduler = ASHA([1, 3, 9], reduction=2, minimize=minimize)
    sign = 1 if minimize else -1
    scheduler.report("early", 1, 4 * sign)
    scheduler.report("early", 1, 4 * sign)
    assert scheduler.promote() is None
    scheduler.report("slow", 1, 8 * sign)
    assert scheduler.promote() == ("early", 3)
    scheduler.report("early", 3, 7 * sign)
    scheduler.report("late", 1, 1 * sign)
    assert scheduler.promote() is None
    scheduler.report("other", 1, 2 * sign)
    assert scheduler.promote() == ("late", 3)
    scheduler.report("late", 3, 3 * sign)
    assert scheduler.promote() == ("late", 9)
    scheduler.report("late", 9, 2 * sign)
    assert scheduler.promote() is None
    with pytest.raises(ValueError, match="never promoted"):
        scheduler.report("slow", 9, 1)
    with pytest.raises(ValueError, match="conflicting"):
        scheduler.report("early", 1, 0)


def test_replicate_before_promote_with_calibrated_noise() -> None:
    incumbent = {1: 4.0, 2: 4.0}
    lucky_seed = {10: 3.5}
    assert compare_seeds(incumbent, lucky_seed).decision == "replicate"
    challenger = {**lucky_seed, 11: 3.5}
    uncertain = compare_seeds(incumbent, challenger, seed_std=1.0)
    assert uncertain.decision == "retain"
    assert uncertain.z_score == pytest.approx(0.5)
    assert compare_seeds(incumbent, {10: 3, 11: 6}).decision == "retain"
    certain = compare_seeds(incumbent, challenger, seed_std=0.1)
    assert certain.decision == "promote"
    assert certain.z_score == pytest.approx(5.0)
    assert compare_seeds(challenger, incumbent, minimize=False).decision == "promote"
    assert compare_seeds(incumbent, incumbent, threshold=0).decision == "retain"
    with pytest.raises(ValueError, match="finite"):
        compare_seeds({1: math.nan}, challenger)


def test_islands_cap_seed_clones_and_reward_novel_recipes() -> None:
    top = Parent("best", "dense", 10, frozenset({"wide"}))
    clone = Parent("clone", "dense", 9, top.features)
    similar = Parent("similar", "sparse", 8, top.features)
    novel = Parent("novel", "sparse", 7, frozenset({"narrow"}))
    pool = [clone, novel, similar, top]
    assert island_parents(pool, 3, minimize=False, novelty_weight=0.8) == [top, novel]
    assert island_parents(pool, 2, minimize=False, novelty_weight=0) == [top, similar]
    assert island_parents(pool, 4, minimize=False, per_family=2, novelty_weight=0) == [
        top,
        similar,
        clone,
        novel,
    ]
