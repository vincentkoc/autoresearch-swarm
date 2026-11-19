"""Fresh-process calibration recovery using generated scores and the actual study ledger."""

import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path

import pytest

from research_swarm.swarm.panels import PanelEvaluationError, PanelSplit, PanelStudy


def child(directory, fault):
    def evaluate(candidate, role, members, seed):
        with (directory / "calls.jsonl").open("a") as out:
            out.write(json.dumps([role, candidate, seed]) + "\n")
        if fault == "unscored" and (role, candidate, seed) == ("calibration_proxy", "a", 1):
            os._exit(43)
        value = ("a", "b", "c").index(candidate)
        return value if role == "calibration_proxy" else 2 * value + seed

    study = PanelStudy(
        directory / "panels.sqlite",
        panels=PanelSplit(("t",), ("s",), ("v",), ("c",), ("p",), ("y",)),
        variants=("a", "b", "c"),
        seeds=(0, 1),
        evaluate=evaluate,
        direction="maximize",
        revision="generated",
        look_limits={},
    )
    if fault == "scored":
        measure = study._measure

        def measure_then_exit(role, candidate, seed):
            score = measure(role, candidate, seed)
            if (role, candidate, seed) == ("calibration_target", "a", 0):
                os._exit(43)
            return score

        study._measure = measure_then_exit
    if fault == "before_fit":
        study._put = lambda key, value: os._exit(43)
    try:
        print(json.dumps(asdict(study.calibrate())))
    except PanelEvaluationError as error:
        print(str(error), file=sys.stderr)
        sys.exit(2)


def run(directory, fault="none"):
    directory.mkdir(exist_ok=True)
    return subprocess.run(
        [sys.executable, "-m", __name__, str(directory), fault],
        cwd=Path(__file__).resolve().parents[2],
        text=True,
        capture_output=True,
        timeout=20,
    )


def looks(directory):
    with sqlite3.connect(directory / "panels.sqlite") as db:
        return db.execute(
            "SELECT id,role,candidate,seed,score FROM panel_looks ORDER BY id"
        ).fetchall()


def test_unscored_reservation_stays_spent_after_process_exit(tmp_path):
    assert run(tmp_path, "unscored").returncode == 43
    before = looks(tmp_path)
    assert before == [
        (1, "calibration_proxy", "a", 0, 0),
        (2, "calibration_target", "a", 0, 0),
        (3, "calibration_proxy", "a", 1, None),
    ]
    calls = (tmp_path / "calls.jsonl").read_bytes()
    for _ in range(2):
        replay = run(tmp_path)
        assert replay.returncode == 2 and "calibration_proxy look consumed" in replay.stderr
        assert looks(tmp_path) == before and (tmp_path / "calls.jsonl").read_bytes() == calls
    with sqlite3.connect(tmp_path / "panels.sqlite") as db:
        assert (
            db.execute("SELECT value FROM panel_state WHERE key='calibration'").fetchone() is None
        )


@pytest.mark.parametrize(("fault", "saved_count"), [("scored", 2), ("before_fit", 12)])
def test_scored_reservations_survive_until_the_fit_is_committed(tmp_path, fault, saved_count):
    uninterrupted, interrupted = tmp_path / "uninterrupted", tmp_path / "interrupted"
    baseline = json.loads(run(uninterrupted).stdout)
    assert run(interrupted, fault).returncode == 43
    assert len(looks(interrupted)) == saved_count
    restored = json.loads(run(interrupted).stdout)
    assert baseline["seconds_per_trial"] > 0 and baseline["trials_per_second"] > 0
    assert restored["seconds_per_trial"] is None and restored["trials_per_second"] is None
    assert {k: restored[k] for k in ("slope", "intercept", "seed_noise", "trials")} == {
        k: baseline[k] for k in ("slope", "intercept", "seed_noise", "trials")
    }
    assert looks(interrupted) == looks(uninterrupted)
    assert (interrupted / "calls.jsonl").read_bytes() == (
        uninterrupted / "calls.jsonl"
    ).read_bytes()
    assert json.loads(run(interrupted).stdout) == restored
    assert looks(interrupted) == looks(uninterrupted)


def test_ambiguous_legacy_scores_cannot_be_chosen_for_a_partial_fit(tmp_path):
    assert run(tmp_path, "scored").returncode == 43
    with sqlite3.connect(tmp_path / "panels.sqlite") as db:
        db.execute(
            "INSERT INTO panel_looks(role, candidate, seed, score) "
            "VALUES ('calibration_proxy','a',0,5)"
        )
    before = looks(tmp_path)
    calls = (tmp_path / "calls.jsonl").read_bytes()
    replay = run(tmp_path)
    assert replay.returncode == 2 and "Multiple calibration_proxy looks" in replay.stderr
    assert looks(tmp_path) == before and (tmp_path / "calls.jsonl").read_bytes() == calls


if __name__ == "__main__":
    child(Path(sys.argv[1]), sys.argv[2])
