"""A CLI restart must stop before dispatch when a calibration result was lost."""

import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest

from tests.integration.test_campaign_kpi_attribution import generated_policy
from tests.paths import REPO_ROOT


def interrupted_policy(spec):
    policy = generated_policy(spec)
    evaluate = policy.study.evaluate

    def record_score(key, role, members, seed):
        value = evaluate(key, role, members, seed)
        with (Path(spec.task_dir) / "scored.jsonl").open("a") as ledger:
            print(json.dumps([role, key, seed, value]), file=ledger)
        if (
            role == os.environ.get("TEST_CALIBRATION_CUT")
            and json.loads(key)["x"] == 0
            and seed == 1
        ):
            os._exit(43)
        return value

    policy.study.evaluate = record_score
    return policy


def snapshot(directory):
    panel_path = directory / "generated" / "panels.db"
    with closing(sqlite3.connect(panel_path.as_uri() + "?mode=ro", uri=True)) as db:
        rows = db.execute(
            "SELECT id,role,candidate,seed,score FROM panel_looks ORDER BY id"
        ).fetchall()
        fits = db.execute("SELECT value FROM panel_state WHERE key='calibration'").fetchall()
    queue = directory / "queue.sqlite"
    with closing(sqlite3.connect(queue.as_uri() + "?mode=ro", uri=True)) as db:
        trials = db.execute("SELECT count(*) FROM swarm_trials").fetchone()[0]
    scored = [json.loads(line) for line in (directory / "scored.jsonl").read_text().splitlines()]
    return rows, fits, trials, scored


@pytest.mark.parametrize("role", ["calibration_proxy", "calibration_target"])
def test_cli_restarts_preserve_consumed_calibration_before_dispatch(tmp_path, role):
    (tmp_path / "campaign.yaml").write_text(
        json.dumps(
            {
                "campaign_id": "generated",
                "metric": "score",
                "direction": "maximize",
                "max_trials": 1,
                "max_workers": 1,
                "command": [sys.executable, "-c", "raise AssertionError('unexpected dispatch')"],
                "candidates": [{"candidate_id": "base", "params": {"x": 0}}],
                "panel_policy": f"{__name__}:interrupted_policy",
                "options": {"fixture_slope": 2},
            }
        )
    )

    def run(cut=""):
        return subprocess.run(
            [
                str(Path(sys.executable).with_name("research")),
                "swarm",
                "run",
                "campaign.yaml",
                "--db",
                "queue.sqlite",
            ],
            cwd=tmp_path,
            env={**os.environ, "PYTHONPATH": str(REPO_ROOT), "TEST_CALIBRATION_CUT": cut},
            capture_output=True,
            text=True,
            timeout=20,
        )

    interrupted = run(role)
    assert interrupted.returncode == 43, interrupted.stderr or interrupted.stdout
    before = snapshot(tmp_path)
    rows, fits, trials, scored = before
    assert not fits and trials == 0
    assert len(scored) == len(rows)
    assert [list(row[1:]) for row in rows[:-1]] == scored[:-1]
    assert scored[-1][:3] == list(rows[-1][1:4])
    interrupted_role, recipe, seed = scored[-1][:3]
    assert (interrupted_role, json.loads(recipe), seed) == (role, {"x": 0}, 1)
    assert rows[-1][4] is None and scored[-1][3] is not None
    for _ in range(2):
        replay = run()
        assert replay.returncode == 2, replay.stdout or replay.stderr
        message = json.loads(replay.stderr)["error"]["message"]
        assert role in message and "consumed" in message.lower()
        assert snapshot(tmp_path) == before
