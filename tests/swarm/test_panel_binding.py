"""Local restart contracts for the SQLite queue and calibrated panel study."""

import json
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from benchmarks.study import WorkerPanelPolicy, recipe_key
from research_swarm.swarm.interfaces import CampaignSpec
from research_swarm.swarm.store import SQLiteStore
from tests.benchmarks.test_panels import measure, study


def generated_policy(spec):
    offset = int(os.environ["FIXTURE_OFFSET"])
    keys = [recipe_key({"x": x}) for x in ("0", "1", "2")]

    def score(key, role, members, seed):
        return keys.index(key) + (offset if role in ("confirmation", "calibration_target") else 0)

    return WorkerPanelPolicy(
        study(
            Path(os.environ["FIXTURE_STUDY"]), score, variants=keys, revision=f"generated/{offset}"
        )
    )


def test_cli_rejects_crossed_campaign_and_study_identities(tmp_path):
    def run(queue, campaign, panel, offset, operators=("param-mutate",)):
        spec = {
            "campaign_id": campaign,
            "metric": "score",
            "direction": "maximize",
            "max_trials": 2,
            "max_workers": 1,
            "command": [
                sys.executable,
                "-c",
                "import json,os; from pathlib import Path; "
                "trial=json.loads(Path(os.environ['SWARM_TRIAL']).read_text()); "
                "x=trial['candidate']['params']['x']; "
                "Path(os.environ['SWARM_METRICS']).write_text(json.dumps({'score':int(x)}))",
            ],
            "candidates": [{"candidate_id": "base", "params": {"x": "0"}}],
            "search_space": {"x": ["0", "1"]},
            "proposer": "research_swarm.swarm.operators.config:from_spec",
            "options": {"search": {"operators": operators, "replicate": False}},
            "panel_policy": "tests.swarm.test_panel_binding:generated_policy",
        }
        path = tmp_path / "campaign.yaml"
        path.write_text(json.dumps(spec))
        return subprocess.run(
            [
                str(Path(sys.executable).with_name("research")),
                "swarm",
                "run",
                path,
                "--db",
                tmp_path / queue,
            ],
            cwd=tmp_path,
            text=True,
            capture_output=True,
            timeout=20,
            env={
                **os.environ,
                "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
                "FIXTURE_STUDY": str(tmp_path / panel),
                "FIXTURE_OFFSET": str(offset),
            },
        )

    def looks(panel):
        with sqlite3.connect(tmp_path / panel / "panels.db") as db:
            return db.execute("SELECT * FROM panel_looks ORDER BY id").fetchall()

    original = run("a.db", "a", "first", 0)
    other = run("b.db", "b", "second", 50)
    assert json.loads(original.stdout)["confirmed_score"] == 1
    assert json.loads(other.stdout)["confirmed_score"] == 51
    before = looks("first"), looks("second")
    swapped = run("a.db", "a", "second", 50)
    assert swapped.returncode and "study identity changed" in swapped.stdout + swapped.stderr
    reused = run("new.db", "a", "first", 0, ("param-mutate", "crossover"))
    assert reused.returncode and "different campaign spec" in reused.stdout + reused.stderr
    with sqlite3.connect(tmp_path / "new.db") as db:
        assert db.execute("SELECT count(*) FROM swarm_campaigns").fetchone() == (0,)
    assert json.loads(run("a.db", "a", "first", 0).stdout) == json.loads(original.stdout)
    assert (looks("first"), looks("second")) == before


def test_competing_plans_can_only_register_one_spec(tmp_path):
    session = study(tmp_path, measure)
    spec = CampaignSpec(
        campaign_id="race",
        metric="score",
        direction="maximize",
        command=("score",),
        max_trials=1,
        max_workers=1,
    )

    def register(seed):
        store = SQLiteStore(tmp_path / f"{seed}.sqlite")
        try:
            store.register_panel_campaign(replace(spec, seed=seed), session)
        except ValueError as exc:
            assert "different campaign spec" in str(exc)
            return False
        return True

    with ThreadPoolExecutor(2) as workers:
        assert sorted(workers.map(register, (3, 7))) == [False, True]
    assert session.look_counts() == {}


def test_legacy_confirmations_require_the_original_queue_on_first_binding(tmp_path):
    session = study(tmp_path, measure)
    session.calibrate()
    assert session.consider("base", None).promoted
    before = session.look_counts()
    with pytest.raises(ValueError, match="unbound confirmation history"):
        session.bind_campaign("legacy", "spec", None, "new")
    with pytest.warns(UserWarning, match="historical association cannot be verified"):
        binding = session.bind_campaign("legacy", "spec", None, "started")
    assert session.bind_campaign("legacy", "spec", binding, "selected") == binding
    assert session.look_counts() == before


def test_shared_study_keeps_separate_arm_bindings_and_cannot_drop_the_hook(tmp_path):
    spec = CampaignSpec(
        campaign_id="arm-a",
        metric="score",
        direction="maximize",
        command=("score",),
        max_trials=2,
        max_workers=1,
    )
    session = study(tmp_path, measure)
    first = SQLiteStore(tmp_path / "a.sqlite")
    first.register_panel_campaign(spec, session)
    session.calibrate()
    assert session.consider("base", None).promoted
    before = session.look_counts()
    second = SQLiteStore(tmp_path / "b.sqlite")
    second.register_panel_campaign(replace(spec, campaign_id="arm-b", seed=3), session)
    with pytest.raises(ValueError, match="original panel study"):
        first.register_panel_campaign(spec, object())
    first.register_panel_campaign(spec, session)
    assert session.look_counts() == before
