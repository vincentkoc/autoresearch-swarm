"""Generated benchmark scores for the programmatic task/study boundary."""

import sqlite3
from dataclasses import replace

import pytest

from benchmarks.study import open_study, run_campaign
from benchmarks.suite import get_task
from research_swarm.swarm.panels import PanelStudy


def generated_task(protocol, offset, calls):
    def score(candidate, seed, role, device, size, fidelity):
        calls.append((role, tuple(sorted(candidate.items())), seed))
        return offset + sum(value != "identity" for value in candidate.values())

    return replace(get_task("program", protocol=protocol), evaluate=score)


def snapshot(path):
    with sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True) as db:
        looks = db.execute("SELECT * FROM panel_looks ORDER BY id").fetchall()
        state = db.execute("SELECT * FROM panel_state ORDER BY key").fetchall()
    return looks, state


def propose(task, history, rng):
    return {**task.initial, "line1": "lower"}


def test_open_study_rejects_another_declared_task_before_replay(tmp_path):
    calls, other_calls = [], []
    task = generated_task("v1", 100, calls)
    other = generated_task("v2", 500, other_calls)
    path = tmp_path / "v1.sqlite"
    study = open_study(task, path)
    first = run_campaign(task, study, propose, budget=2, seed=7)
    assert first.confirmed_score == 101
    before = snapshot(path)
    previous_calls = calls.copy()

    for incompatible in (
        other,
        replace(task, metric="different_metric"),
        replace(task, direction="minimize"),
    ):
        with pytest.raises(ValueError, match="configuration changed"):
            run_campaign(incompatible, study, propose, budget=2, seed=7)
        assert snapshot(path) == before
        assert calls == previous_calls and other_calls == []

    matching = open_study(task, path)
    replay = run_campaign(task, matching, propose, budget=2, seed=7)
    assert replay.decision == first.decision and replay.confirmed_score == 101
    assert [role for role, _, _ in calls[len(previous_calls) :]] == ["search", "search"]

    independent = open_study(other, tmp_path / "v2.sqlite")
    second = run_campaign(other, independent, propose, budget=2, seed=7)
    assert second.confirmed_score == 501
    assert {role for role, _, _ in other_calls} == {
        "calibration_proxy",
        "calibration_target",
        "search",
        "selection",
        "confirmation",
    }


def test_study_keeps_its_own_decision_rules_and_requires_matching_seeds_and_recipes(tmp_path):
    calls = []
    task = generated_task("v1", 100, calls)
    registered = open_study(task, tmp_path / "registered.sqlite")
    config = dict(
        panels=registered.panels,
        variants=registered.variants,
        seeds=registered.seeds,
        revision=registered.revision,
        direction=task.direction,
        evaluate=registered.evaluate,
        min_gain=5,
        look_limits={"confirmation": 1},
    )
    custom = PanelStudy(tmp_path / "custom.sqlite", **config)
    arm = run_campaign(task, custom, propose, budget=2, seed=7)
    assert arm.decision.reason == "selection" and arm.confirmed_score == 100
    previous_calls = calls.copy()
    for key in ("variants", "seeds"):
        changed = tuple(reversed(config[key]))
        wrong = PanelStudy(tmp_path / f"{key}.sqlite", **(config | {key: changed}))
        with pytest.raises(ValueError, match="configuration changed"):
            run_campaign(task, wrong, propose, budget=2, seed=7)
        assert wrong.look_counts() == {} and calls == previous_calls
