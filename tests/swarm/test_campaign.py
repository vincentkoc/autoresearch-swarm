"""Exercise the installed CLI with eight overlapping CPU bigram processes."""

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import yaml

from research_swarm.swarm.spec import load_spec
from tests.paths import REPO_ROOT

BIGRAM = REPO_ROOT / "examples" / "cpu_bigram"


def test_eight_processes_overlap_one_crashes_and_winner_survives_resume(tmp_path: Path) -> None:
    barrier = tmp_path / "barrier.py"
    barrier.write_text(
        """import os, sys, time
from pathlib import Path

gate, workload = sys.argv[1:]
gate = Path(gate)
(gate / f"{os.getpid()}.ready").touch()
end = time.monotonic() + 20
while len(list(gate.glob("*.ready"))) < 8:
    if time.monotonic() >= end:
        raise TimeoutError("eight child processes did not reach the barrier")
    time.sleep(0.01)
os.execv(sys.executable, [sys.executable, workload])
""",
        encoding="utf-8",
    )
    spec = yaml.safe_load((BIGRAM / "swarm.yaml").read_text())
    spec["command"] = [sys.executable, str(barrier), str(tmp_path), str(BIGRAM / "swarm_trial.py")]
    spec["trial_timeout_seconds"] = 30
    yaml_file = tmp_path / "campaign.yaml"
    yaml_file.write_text(yaml.safe_dump(spec))
    db = tmp_path / "swarm.db"
    args = ["research", "swarm", "run", str(yaml_file), "--db", str(db)]
    # Use the active interpreter's entry point, also when pytest was launched by absolute path.
    env = {**os.environ, "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]}
    run = subprocess.run(args, capture_output=True, text=True, timeout=45, env=env)
    assert run.returncode == 0, run.stderr + run.stdout
    summary = json.loads(run.stdout)
    assert len(list(tmp_path.glob("*.ready"))) == 8
    assert (summary["trials"], summary["attempts"], summary["failed"], summary["lost"]) == (
        8,
        8,
        1,
        0,
    )
    assert len(summary["top"]) == 3
    winner = summary["selected"]
    assert winner["result"]["attempt"]["trial"]["candidate"]["params"]["alpha"] == 0.01
    with sqlite3.connect(db) as conn:
        results = [
            json.loads(row[0]) for row in conn.execute("SELECT observation FROM swarm_attempts")
        ]
        (crash,) = (r for r in results if r["status"] == "failed")
        assert "status 1" in crash["error"] and "Smoothing alpha" in "".join(crash["logs"])
        assert winner["score"] == min(
            r["metrics"]["bits_per_byte"] for r in results if r["status"] == "ok"
        )
        (selected,) = conn.execute("SELECT selected_trial_id FROM swarm_campaigns").fetchone()
        assert selected == winner["result"]["attempt"]["trial"]["trial_id"]
        events = conn.execute("SELECT count(*) FROM swarm_events").fetchone()
    replay = subprocess.run(args, capture_output=True, text=True, timeout=30, env=env, check=True)
    assert json.loads(replay.stdout) == summary
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM swarm_events").fetchone() == events


def test_yaml_rejects_unknown_fields_and_wrong_types(tmp_path: Path) -> None:
    import pytest

    spec = yaml.safe_load((BIGRAM / "swarm.yaml").read_text())
    path = tmp_path / "bad.yaml"
    for change in (
        {"max_trials": True},
        {"max_worker": 9},
        {"max_workers": 0},
        {"search_space": {"x": []}},
        {"candidates": [{"candidate_id": "x", "parms": {}}]},
    ):
        path.write_text(yaml.safe_dump({**spec, **change}))
        with pytest.raises(ValueError):
            load_spec(path)
