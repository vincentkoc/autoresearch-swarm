"""Campaign reports consume existing panel records without opening any held-out panel."""

import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path

import pytest

from benchmarks.study import WorkerPanelPolicy, recipe_key
from research_swarm.swarm.interfaces import Result
from research_swarm.swarm.panels import Calibration, Decision, PanelSplit, PanelStudy
from research_swarm.swarm.report import capture
from research_swarm.swarm.spec import load_spec
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec, trial


def recipe(n):
    return recipe_key({"x": n})


def replay_policy(campaign):
    def unexpected_evaluation(*args):
        raise AssertionError("a cached panel outcome must not invoke the evaluator")

    return WorkerPanelPolicy(
        PanelStudy(
            Path(campaign.task_dir) / campaign.options["study"],
            panels=PanelSplit(("t",), ("s",), ("v",), ("c",), ("p",), ("y",)),
            variants=[recipe(i) for i in range(3)],
            seeds=[1, 2],
            evaluate=unexpected_evaluation,
            direction="maximize",
            revision="synthetic",
            look_limits={},
        )
    )


def campaign_file(directory, name, panel, candidates):
    path = directory / f"{name}.json"
    path.write_text(
        json.dumps(
            {
                "campaign_id": name,
                "metric": "score",
                "direction": "maximize",
                "command": [
                    sys.executable,
                    "-c",
                    "import json,os; from pathlib import Path; "
                    "trial=json.loads(Path(os.environ['SWARM_TRIAL']).read_text()); "
                    "x=trial['candidate']['params']['x']; "
                    "Path(os.environ['SWARM_METRICS']).write_text(json.dumps({'score':x}))",
                ],
                "max_trials": len(candidates),
                "max_workers": 1,
                "panel_policy": "tests.swarm.test_panel_kpis:replay_policy",
                "options": {"study": panel},
                "candidates": [
                    {"candidate_id": f"trial-{i}", "params": {"x": x}, "text": str(i)}
                    for i, x in enumerate(candidates)
                ],
            }
        )
    )
    return path


def seed_study(study, calibration, decisions):
    # Populate only generated records. Real policy calls must hit these exact cache keys.
    with sqlite3.connect(study.path) as db:
        records = [("calibration", calibration)] + [
            (
                study.decision_key(
                    recipe(challenger), None if incumbent is None else recipe(incumbent)
                ),
                decision,
            )
            for challenger, incumbent, decision in decisions
        ]
        db.executemany(
            "INSERT INTO panel_state VALUES (?,?)",
            [(key, json.dumps(asdict(value))) for key, value in records],
        )
        db.executemany(
            "INSERT INTO panel_looks(role,candidate,seed,score) VALUES ('confirmation',?,1,?)",
            [(recipe(x), score) for x, score in [(0, 1), (1, 2), (2, 1)]],
        )


def look_rows(path, *, held_out=False):
    with sqlite3.connect(path) as db:
        return db.execute(
            "SELECT * FROM panel_looks "
            + ("WHERE role!='search' " if held_out else "")
            + "ORDER BY id"
        ).fetchall()


def test_bound_campaign_reports_replay_only_their_own_comparisons(tmp_path):
    queue = tmp_path / "queue.sqlite"
    first = campaign_file(tmp_path, "first", "shared.sqlite", [0, 1, 2, 2, 3, 4])
    second = campaign_file(tmp_path, "second", "shared.sqlite", [0, 2])
    flat = campaign_file(tmp_path, "flat", "flat.sqlite", [0])
    for path in (first, second, flat):
        campaign = load_spec(path)
        SQLiteStore(queue).register_panel_campaign(campaign, replay_policy(campaign))
    shared = replay_policy(load_spec(first)).study
    other = replay_policy(load_spec(flat)).study
    reference = (0, None, Decision(True, "reference"))
    seed_study(
        shared,
        Calibration(2.5, 1, 0.25, 12, 1, 1),
        [
            reference,
            (1, 0, Decision(True, "confirmation", 1, 1)),
            (2, 1, Decision(False, "confirmation", 1, -1)),
            (3, 1, Decision(False, "selection", -1)),
            (4, 1, Decision(False, "evaluation_failed")),
            (2, 0, Decision(False, "confirmation", 1, 0)),
        ],
    )
    seed_study(other, Calibration(None, None, 0, 12, 1, 1), [reference])
    held_out = [look_rows(study.path, held_out=True) for study in (shared, other)]

    def run(path):
        return subprocess.check_output(
            [str(Path(sys.executable).with_name("research")), "swarm", "run", path, "--db", queue],
            cwd=tmp_path,
            timeout=30,
            env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2])},
        )

    original = run(first)
    for path in (second, flat):
        run(path)
    for campaign, value, passes, decisions, slope, noise in [
        ("first", 50, 1, 2, 2.5, 0.25),
        ("second", 0, 0, 1, 2.5, 0.25),
        ("flat", None, 0, 0, None, 0),
    ]:
        kpis = capture(queue, campaign)["kpis"]
        rate = kpis["confirmation_pass_rate"]
        assert (rate["value"], rate["passes"], rate["decisions"]) == (value, passes, decisions)
        calibrated = kpis["proxy_to_target_slope"]
        assert (calibrated["value"], calibrated["seed_noise"]) == (slope, noise)
        if slope is None:
            assert "constant" in calibrated["detail"]
        expected_gain = {"first": -1, "second": 0, "flat": None}[campaign]
        assert rate["last_confirmation"] == (
            None
            if expected_gain is None
            else {"promoted": False, "confirmation_gain": expected_gain}
        )
    assert [look_rows(study.path, held_out=True) for study in (shared, other)] == held_out
    looks = look_rows(shared.path)
    revision = capture(queue, "first")["campaign_revision"]
    assert run(first) == original
    assert look_rows(shared.path) == looks
    assert capture(queue, "first")["campaign_revision"] == revision


def test_legacy_partial_and_unclassified_panel_evidence(tmp_path):
    queue = tmp_path / "legacy.sqlite"
    store = SQLiteStore(queue)
    campaign = spec()
    store.create_campaign(campaign)
    store.submit(trial(1))
    attempt = store.claim("test", "fixture")
    assert attempt and store.report(Result(attempt=attempt, status="ok", metrics={"loss": 1}))
    for metric in ("confirmation_pass_rate", "proxy_to_target_slope"):
        result = capture(queue, "test")["kpis"][metric]
        assert result["value"] is None and "identity" in result["detail"]

    # Read the earlier queue shape directly, without running a schema migration.
    old = tmp_path / "pre-binding.sqlite"
    with sqlite3.connect(old) as db:
        db.execute("ATTACH DATABASE ? AS source", (str(queue),))
        for table in (
            "swarm_campaigns",
            "swarm_events",
            "swarm_candidates",
            "swarm_attempts",
            "swarm_trials",
            "swarm_projects",
        ):
            columns = [
                row[1]
                for row in db.execute(f"PRAGMA source.table_info({table})")
                if row[1] not in {"panel_binding", "panel_evidence"}
            ]
            db.execute(f"CREATE TABLE {table} AS SELECT {','.join(columns)} FROM source.{table}")
    assert capture(old, "test")["kpis"] == capture(queue, "test")["kpis"]

    class Bound:
        def bind_campaign(self, campaign_id, spec_digest, expected, prior):
            return campaign_id

    for name in ("test", "new"):
        binding = store.register_panel_campaign(spec(campaign_id=name), Bound())
        store.record_panel_outcome(name, binding, Calibration(0, 1, 2, 12, 1, 1))
        store.record_panel_outcome(
            name, binding, Decision(True, "confirmation", 2, 1), comparison="a"
        )
    store.record_panel_outcome("new", "test", Decision(False, "confirmation", 1, 0), comparison="b")
    kpis = capture(queue, "test")["kpis"]
    rate = kpis["confirmation_pass_rate"]
    assert rate["value"] is None and "predate" in rate["detail"]
    assert rate["decisions"] is None and rate["recorded_decisions"] == rate["recorded_passes"] == 1
    assert kpis["proxy_to_target_slope"]["value"] == 0
    assert capture(queue, "new")["kpis"]["confirmation_pass_rate"]["value"] == 100
    store.record_panel_outcome("new", "new", object(), comparison="b")
    rate = capture(queue, "new")["kpis"]["confirmation_pass_rate"]
    assert rate["value"] is None and "classify" in rate["detail"]
    assert rate["passes"] is None and rate["recorded_passes"] == 1


@pytest.mark.parametrize("boolean_gain", [False, True])
def test_boolean_confirmation_gain_leaves_rate_unknown(tmp_path, boolean_gain):
    queue = tmp_path / "queue.sqlite"
    store = SQLiteStore(queue)
    campaign = spec()
    store.create_campaign(campaign)
    binding = "generated-study"
    with sqlite3.connect(queue) as db:
        db.execute(
            "UPDATE swarm_campaigns SET panel_binding=? WHERE campaign_id=?",
            (binding, campaign.campaign_id),
        )
    store.record_panel_outcome(campaign.campaign_id, binding, Calibration(1, 0, 0, 12, 1, 1))
    store.record_panel_outcome(
        campaign.campaign_id, binding, Decision(False, "confirmation", 1, 0), comparison="measured"
    )
    measured = capture(queue, campaign.campaign_id)["kpis"]["confirmation_pass_rate"]
    assert (measured["value"], measured["passes"], measured["decisions"]) == (0, 0, 1)

    store.record_panel_outcome(
        campaign.campaign_id,
        binding,
        Decision(False, "confirmation", 1, boolean_gain),
        comparison="unmeasured",
    )
    rate = capture(queue, campaign.campaign_id)["kpis"]["confirmation_pass_rate"]
    assert rate["value"] is None and rate["passes"] is None and rate["decisions"] is None
    assert (rate["recorded_passes"], rate["recorded_decisions"]) == (0, 1)
    assert rate["last_confirmation"] == measured["last_confirmation"]
