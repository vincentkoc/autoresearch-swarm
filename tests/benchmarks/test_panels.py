import json
import math
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest

from research_swarm.swarm.panels import Calibration, Decision, PanelSplit, PanelStudy

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


def preload_targets(tmp_path, targets):
    protocol = study(tmp_path, lambda *args: pytest.fail("preloaded observations only"))
    with sqlite3.connect(protocol.path) as db:
        for recipe, scores in zip(protocol.variants, targets, strict=True):
            for seed, score in zip(protocol.seeds, scores, strict=True):
                db.executemany(
                    "INSERT INTO panel_looks(role,candidate,seed,score) VALUES(?,?,?,?)",
                    [
                        ("calibration_proxy", recipe, seed, 1),
                        ("calibration_target", recipe, seed, score),
                    ],
                )
    return protocol


TINY = 2.0**-600
TINY_TARGETS = [(-TINY, TINY), (3 * TINY, 5 * TINY), (7 * TINY, 9 * TINY)]


@pytest.mark.parametrize(
    "targets,noise",
    [
        (TINY_TARGETS, math.sqrt(2) * TINY),
        ([(1, 1), (5, 5), (9, 9)], 0.0),
        ([(-1, 1), (2, 6), (8, 8)], math.sqrt(10 / 3)),
        (
            [(-sys.float_info.max, sys.float_info.max), (0, 0), (1, 1)],
            sys.float_info.max * math.sqrt(2 / 3),
        ),
        (
            [(-TINY, TINY), (sys.float_info.max, sys.float_info.max), (1, 1)],
            math.sqrt(2 / 3) * TINY,
        ),
    ],
    ids=["tiny", "constant-within-recipe", "ordinary", "large", "tiny-with-large-constant"],
)
def test_preloaded_seed_noise_at_each_scale(tmp_path, targets, noise):
    protocol = preload_targets(tmp_path, targets)
    counts = protocol.look_counts()
    fit = protocol.calibrate()
    assert fit.slope is None and fit.intercept is None
    assert fit.seed_noise == pytest.approx(noise, rel=2e-15, abs=0)
    assert protocol.calibrate() == fit and protocol.look_counts() == counts


@pytest.mark.parametrize("legacy", [False, True])
def test_nonfinite_preloaded_target_cannot_authorize_a_decision(tmp_path, legacy):
    protocol = preload_targets(tmp_path, [(1, 2), (0, math.inf), (3, 4)])
    counts = protocol.look_counts()
    if legacy:
        protocol._put("calibration", Calibration(None, None, 0.0, 12, None, None))
    with pytest.raises(ValueError, match="non-finite calibration_target"):
        if legacy:
            protocol.consider("new", "base")
        else:
            protocol.calibrate()
    assert protocol._state(protocol.decision_key("new", "base")) is None
    assert protocol.look_counts() == counts


@pytest.mark.parametrize("varying", [True, False], ids=["stale-zero", "genuine-zero"])
def test_cached_zero_noise_before_new_comparison_preserves_history(tmp_path, varying):
    targets = TINY_TARGETS if varying else [(1, 1), (5, 5), (9, 9)]
    protocol = preload_targets(tmp_path, targets)
    legacy_fit = Calibration(None, None, 0.0, 12, 1, 1)
    prior = Decision(True, "confirmation", TINY, TINY)
    old_key = protocol.decision_key("old", "base")
    protocol._put("calibration", legacy_fit)
    protocol._put(old_key, prior)
    with sqlite3.connect(protocol.path) as db:
        for candidate, score in [("new", TINY), ("base", 0.0)]:
            for seed in protocol.seeds:
                row = db.execute(
                    "INSERT INTO panel_looks(role,candidate,seed,score) VALUES('selection',?,?,?)",
                    (candidate, seed, score),
                )
                slot = json.dumps(["selection", "new", "base", candidate, seed])
                db.execute("INSERT INTO panel_state VALUES(?,?)", (slot, str(row.lastrowid)))
            db.execute(
                "INSERT INTO panel_looks(role,candidate,seed,score) VALUES('confirmation',?,?,?)",
                (candidate, protocol.seeds[0], score),
            )
        before = db.execute("SELECT * FROM panel_looks ORDER BY id").fetchall()
    reopened = study(tmp_path, lambda *args: pytest.fail("unexpected scoring"))
    old_record = reopened._state(old_key)
    assert reopened.consider("old", "base") == prior
    decision = reopened.consider("new", "base")
    expected = Decision(False, "selection", TINY) if varying else prior
    assert decision == expected
    expected_noise = math.sqrt(2) * TINY if varying else 0.0
    assert reopened.calibrate() == replace(legacy_fit, seed_noise=expected_noise)
    assert json.loads(reopened._state("calibration")) == asdict(reopened.calibrate())
    assert reopened._state(old_key) == old_record
    with sqlite3.connect(protocol.path) as db:
        assert db.execute("SELECT * FROM panel_looks ORDER BY id").fetchall() == before


def test_summary_only_zero_noise_keeps_saved_decision(tmp_path):
    protocol = study(tmp_path, lambda *args: pytest.fail("unexpected scoring"))
    fit = Calibration(None, None, 0.0, 12, None, None)
    prior = Decision(True, "confirmation", 1.0, 1.0)
    protocol._put("calibration", fit)
    protocol._put(protocol.decision_key("old", "base"), prior)
    assert protocol.calibrate() == fit and protocol.consider("old", "base") == prior
    with pytest.raises(ValueError, match="lacks target replicates"):
        protocol.consider("new", "base")
    assert protocol.look_counts() == {}


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
        if role == "confirmation" and key == "two":
            raise TimeoutError
        return measure(key, role, members, seed)

    first = study(tmp_path, fail)
    first.calibrate()
    rejected = first.consider("two", "base")
    assert not rejected.promoted and "TimeoutError" in rejected.error
    reopened = study(tmp_path, fail)
    assert reopened.consider("two", "base") == rejected
    assert reopened.look_counts()["confirmation"] == 1
    assert reopened.consider("one", "base").promoted


@pytest.mark.parametrize(
    "interrupted_role,selection_looks", [("selection", 3), ("confirmation", 4)]
)
def test_restart_after_exit_cannot_repeat_a_held_out_look(
    tmp_path, interrupted_role, selection_looks
):
    def crash(key, role, members, seed):
        if key == "two" and role == interrupted_role and (role == "confirmation" or seed == 1):
            raise SystemExit
        return measure(key, role, members, seed)

    interrupted = study(tmp_path, crash)
    interrupted.calibrate()
    with pytest.raises(SystemExit):
        interrupted.consider("two", "base")
    before = interrupted.look_counts()
    assert before["selection"] == selection_looks
    resumed = study(tmp_path, measure)
    refused = resumed.consider("two", "base")
    assert not refused.promoted and "consumed" in refused.error
    assert resumed.look_counts() == before
    assert resumed.consider("one", "base").promoted
    assert resumed.look_counts()["selection"] == selection_looks + 4


def test_resume_finishes_a_scored_comparison_without_more_looks(tmp_path, monkeypatch):
    interrupted = study(tmp_path, measure)
    interrupted.calibrate()

    def exit_before_decision_is_saved(*args):
        raise SystemExit

    monkeypatch.setattr(interrupted, "_put", exit_before_decision_is_saved)
    with pytest.raises(SystemExit):
        interrupted.consider("two", "base")
    before = interrupted.look_counts()
    resumed = study(tmp_path, measure)
    assert resumed.consider("two", "base").promoted
    assert resumed.look_counts() == before


def test_legacy_unfinished_confirmation_skips_selection_on_resume(tmp_path):
    original = study(tmp_path, measure)
    original.calibrate()
    # Prior ledgers identified the confirmation recipe but not the selection pair.
    with sqlite3.connect(original.path) as db:
        db.executemany(
            "INSERT INTO panel_looks(role, candidate, seed, score) VALUES (?, ?, ?, ?)",
            [
                ("selection", key, seed, float(key == "two"))
                for seed in (0, 1)
                for key in ("two", "base")
            ]
            + [("confirmation", "two", 0, None)],
        )
    before = original.look_counts()
    resumed = study(tmp_path, measure)
    assert resumed.consider("two", "base").reason == "evaluation_failed"
    assert resumed.look_counts() == before


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
