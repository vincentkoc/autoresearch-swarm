import json
import math
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace

import pytest

from research_swarm.swarm.panels import PanelSplit, PanelStudy

SPLIT = PanelSplit(
    ("train-a",), ("search-a",), ("select-a",), ("confirm-a",), ("proxy-a",), ("target-a",)
)


def study(tmp_path, evaluate, direction="maximize", **overrides):
    inputs = dict(
        panels=SPLIT,
        variants=("base", "one", "two"),
        seeds=(0, 1),
        evaluate=evaluate,
        direction=direction,
        revision="v1",
        look_limits={"search": 2},
    )
    return PanelStudy(tmp_path / "panels.db", **(inputs | overrides))


def measure(key, role, members, seed):
    assert members == SPLIT.members(role)
    proxy = {"base": 0, "one": 1, "two": 2}.get(key, 3)
    if role == "calibration_target":
        return 5 + 2 * proxy + seed
    return proxy


def test_calibrate_before_access_and_fit_across_variants(tmp_path):
    first = study(tmp_path, measure)
    with pytest.raises(RuntimeError, match="calibrate"):
        first.search("one", 0)
    with pytest.raises(RuntimeError, match="calibrate"):
        first.record_search("one", 0)
    with pytest.raises(RuntimeError, match="calibrate"):
        first.consider("one", "base")
    report = first.calibrate()
    assert report.slope == pytest.approx(2)
    assert report.intercept == pytest.approx(5.5)
    assert report.seed_noise == pytest.approx(math.sqrt(0.5))
    assert report.trials == 12
    assert report.seconds_per_trial > 0
    assert report.seconds_per_trial * report.trials_per_second == pytest.approx(1)
    first.record_search("one", 0)
    reopened = study(tmp_path, measure)
    assert reopened.calibrate() == report
    assert reopened.search("one", 0) == 1
    with pytest.warns(UserWarning, match="2-look budget"):
        reopened.search("one", 0)
    assert reopened.look_counts() == {"calibration_proxy": 6, "calibration_target": 6, "search": 3}
    with pytest.raises(ValueError, match="configuration changed"):
        study(tmp_path, measure, revision="changed-proxy")


@pytest.mark.parametrize(
    "field,ids",
    [
        ("train", ("confirm-a",)),
        ("selection", ("search-a",)),
        ("search", ("a", "a")),
        ("calibration_target", ("select-a",)),
        ("confirmation", ()),
    ],
)
def test_reject_leakage_before_scoring(field, ids):
    with pytest.raises(ValueError):
        replace(SPLIT, **{field: ids})


def test_flat_proxy_has_undefined_slope_and_nonfinite_calibration_blocks_search(tmp_path):
    def flat(key, role, members, seed):
        return 1 if role == "calibration_proxy" else measure(key, role, members, seed)

    assert study(tmp_path / "flat", flat).calibrate().slope is None
    bad = study(tmp_path / "bad", lambda *args: float("nan"))
    with pytest.raises(ValueError, match="non-finite"):
        bad.calibrate()
    with pytest.raises(RuntimeError, match="calibrate"):
        bad.search("two", 0)
    assert bad.look_counts() == {"calibration_proxy": 1}


@pytest.mark.parametrize("direction,sign", [("maximize", 1), ("minimize", -1)])
def test_search_overfit_fails_held_out_promotion(tmp_path, direction, sign):
    def scores(key, role, members, seed):
        if key == "overfit":
            return sign * {"search": 100, "selection": 5, "confirmation": -1}[role]
        return sign * measure(key, role, members, seed)

    protocol = study(tmp_path, scores, direction)
    protocol.calibrate()
    assert protocol.consider("base", None).promoted
    assert protocol.search("overfit", 0) == sign * 100
    rejected = protocol.consider("overfit", "base")
    assert not rejected.promoted
    assert rejected.selection_gain == 5 and rejected.confirmation_gain == -1
    assert protocol.look_counts()["confirmation"] == 2
    reopened = study(tmp_path, scores, direction)
    assert reopened.consider("overfit", "base") == rejected
    assert reopened.look_counts() == protocol.look_counts()


def test_insignificant_selection_gain_spares_confirmation(tmp_path):
    def noisy(key, role, members, seed):
        if role == "selection":
            return 0.3 if key == "one" else 0.0
        return measure(key, role, members, seed)

    protocol = study(tmp_path, noisy)
    protocol.calibrate()
    assert protocol.consider("one", "base").reason == "selection"
    assert "confirmation" not in protocol.look_counts()


def test_racing_decisions_share_one_confirmation_and_result(tmp_path):
    first = study(tmp_path, measure)
    second = study(tmp_path, measure)
    first.calibrate()
    start = threading.Barrier(2)

    def promote(protocol):
        start.wait(timeout=5)
        return protocol.consider("two", "base")

    with ThreadPoolExecutor(2) as executor:
        decisions = list(executor.map(promote, (first, second)))
    assert decisions[0] == decisions[1] and decisions[0].promoted
    assert first.look_counts()["confirmation"] == 2
    assert first.look_counts()["selection"] == 4


def test_failed_confirmation_remains_consumed(tmp_path):
    def fail(key, role, members, seed):
        if role == "confirmation":
            raise TimeoutError
        return measure(key, role, members, seed)

    first = study(tmp_path, fail)
    first.calibrate()
    with pytest.raises(TimeoutError):
        first.consider("two", "base")
    reopened = study(tmp_path, fail)
    with pytest.raises(RuntimeError, match="consumed"):
        reopened.consider("two", "base")
    assert reopened.look_counts()["confirmation"] == 1


def test_benchmark_entry_point_calibrates_shares_holdouts_and_resumes(tmp_path):
    db = tmp_path / "program.db"
    command = [sys.executable, "-m", "benchmarks", "program", "--budget", "5", "--study", str(db)]
    first = json.loads(subprocess.check_output(command))
    second = json.loads(subprocess.check_output(command))
    assert first["calibration"]["trials"] == 18
    assert first["panel_looks"]["search"] == 10
    assert second["panel_looks"]["search"] == 20
    assert first["finalists"] == second["finalists"]
    assert first["panel_looks"]["confirmation"] == second["panel_looks"]["confirmation"]
    with sqlite3.connect(db) as connection:
        before_search = connection.execute("SELECT role FROM panel_looks ORDER BY id LIMIT 18")
        assert {role for (role,) in before_search} == {"calibration_proxy", "calibration_target"}


def test_worker_factory_scores_serialized_content_and_deduplicates_identical_recipes(tmp_path):
    from benchmarks.study import worker_policy
    from benchmarks.suite import get_task

    spec = SimpleNamespace(
        command=(sys.executable, "-m", "benchmarks.trial", "program"),
        task_dir=str(tmp_path),
        campaign_id="test/campaign",
        metric="pass_rate",
        direction="maximize",
    )
    policy = worker_policy(spec)
    baseline = get_task("program").initial
    best = {**baseline, "line1": "lower", "line2": "hyphenate", "line3": "trim-hyphens"}
    reference = json.dumps(
        {"params": baseline, "text": None, "diff": None, "source_revision": None}
    )
    challenger = json.dumps({"params": best, "text": None, "diff": None, "source_revision": None})
    policy.calibrate()
    policy.record_search(challenger, 47)
    assert policy.consider(challenger, reference).promoted
    assert policy.consider(json.dumps({"params": best}, indent=2), reference).promoted
    assert policy.study.look_counts()["confirmation"] == 2
