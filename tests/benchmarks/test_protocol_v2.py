import hashlib
import math

import pytest

from benchmarks.study import open_study, recipe_key, run_campaign
from benchmarks.suite import PANEL_SEEDS, comparison_seeds, get_task, greedy_search


def test_protocols_use_disjoint_member_streams_and_registered_seed_labels(tmp_path):
    first = get_task("program")
    second = get_task("program", protocol="v2")
    for panel in PANEL_SEEDS:
        assert set(first.members(panel)).isdisjoint(second.members(panel))
    search = comparison_seeds("v2")
    assert search == tuple(
        int.from_bytes(hashlib.sha256(f"ars-bench-v2/search-seed/{i}".encode()).digest()[:4], "big")
        for i in range(3)
    )
    assert set(search).isdisjoint(comparison_seeds("v2", calibration=True))
    path = tmp_path / "shared.sqlite"
    open_study(first, path)
    with pytest.raises(ValueError, match="configuration changed"):
        open_study(second, path)


def test_confirmed_score_read_does_not_expose_an_unscored_recipe(tmp_path):
    task = get_task("program", protocol="v2")
    study = open_study(task, tmp_path / "study.sqlite")
    assert study.confirmed_score("never evaluated") is None
    arm = run_campaign(task, study, greedy_search, budget=3, seed=7)
    assert arm.selected is not None and arm.confirmed_score is not None
    counts = study.look_counts()
    assert study.confirmed_score(recipe_key(arm.selected)) == arm.confirmed_score
    assert study.look_counts() == counts


@pytest.mark.parametrize(("name", "recipes"), [("mlp", 1512), ("nanogpt", 1008)])
def test_expanded_training_space_applies_its_extra_dimensions(name, recipes):
    torch = pytest.importorskip("torch")
    torch.set_num_threads(1)
    task = get_task(name, protocol="v2")
    assert math.prod(map(len, task.space.values())) == recipes
    initial = task.initial
    alternative = {key: values[-1] for key, values in task.space.items()}
    assert task.score(initial, seed=7) != task.score(alternative, seed=7)
