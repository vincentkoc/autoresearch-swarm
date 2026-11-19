"""Reconcile operator reports against generated campaign evidence in SQLite."""

import json
import math
import os
import re
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest

from benchmarks.study import WorkerPanelPolicy, recipe_key
from research_swarm.swarm.interfaces import Result
from research_swarm.swarm.reader import CampaignReader
from research_swarm.swarm.store import SQLiteStore
from tests.benchmarks.test_panels import study
from tests.paths import REPO_ROOT
from tests.swarm.test_store import spec, trial


def generated_policy(spec):
    multiplier = spec.options["fixture_slope"]

    def score(key, role, members, seed):
        x = json.loads(key)["x"]
        if role == "calibration_target":
            return multiplier * x + seed / 10
        if role == "confirmation" and x == 2:
            return -1
        return x

    return WorkerPanelPolicy(
        study(
            Path(spec.task_dir) / spec.campaign_id,
            score,
            variants=[recipe_key({"x": x}) for x in range(3)],
            revision=f"generated/{multiplier}",
            look_limits={},
        )
    )


def cli(tmp_path, *args):
    result = subprocess.run(
        [str(Path(sys.executable).with_name("research")), *args],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def export(tmp_path, queue, campaign):
    output = tmp_path / f"{campaign}.html"
    cli(tmp_path, "report", "view", "--db", queue, "--campaign", campaign, "--output", output)
    return dict(re.findall(r"<dt>([^<]+)</dt><dd><strong>([^<]+)</strong>", output.read_text()))


def run_fixture(tmp_path, queue, campaign, recipes, workers, slope):
    path = tmp_path / f"{campaign}.yaml"
    path.write_text(
        json.dumps(
            {
                "campaign_id": campaign,
                "metric": "score",
                "direction": "maximize",
                "max_trials": len(set(recipes)),
                "max_workers": workers,
                "command": [
                    sys.executable,
                    "-c",
                    "import json, os; from pathlib import Path; "
                    "trial=json.loads(Path(os.environ['SWARM_TRIAL']).read_text()); "
                    "score=trial['candidate']['params']['x']; "
                    "Path(os.environ['SWARM_METRICS']).write_text(json.dumps({'score':score}))",
                ],
                "candidates": [{"candidate_id": str(x), "params": {"x": x}} for x in recipes],
                "panel_policy": f"{__name__}:generated_policy",
                "options": {"fixture_slope": slope},
            }
        )
    )
    summary = json.loads(cli(tmp_path, "swarm", "run", path, "--db", queue))
    assert (summary["trials"], summary["confirmed_score"]) == (len(set(recipes)), 1)
    return path


def guide_evidence(db, campaign):
    guide = (REPO_ROOT / "docs/operator-guide.md").read_text()
    (query,) = re.findall(
        r"^~~~sql\n(SELECT id, kind, proposed_count,.*?);\n~~~$", guide, re.M | re.S
    )
    db.row_factory = sqlite3.Row
    return db.execute(query, {"campaign": campaign}).fetchall()


def test_cli_proposal_and_panel_evidence_is_bound_to_the_campaign(tmp_path):
    queue = tmp_path / "queue.sqlite"
    primary = run_fixture(tmp_path, queue, "primary", [0, 0, 1, 1, 2], 2, 2)
    run_fixture(tmp_path, queue, "neighbor", [0, 1], 1, 4)

    with closing(sqlite3.connect(queue.as_uri() + "?mode=ro", uri=True)) as db:
        for campaign, counts, outcomes, slope in [
            ("primary", (5, 3), [True, False], 2),
            ("neighbor", (2, 2), [True], 4),
        ]:
            rows = guide_evidence(db, campaign)
            batches = [row for row in rows if row["kind"] == "proposal_batch"]
            proposed = sum(row["proposed_count"] for row in batches)
            accepted = sum(row["accepted_count"] for row in batches)
            assert (proposed, accepted) == counts
            evidence = [
                json.loads(row["panel_evidence"]) for row in rows if row["kind"] == "panel_decision"
            ]
            measured = [d for d in evidence if d["reason"] == "confirmation"]
            assert [d["promoted"] for d in measured] == outcomes
            assert [d["confirmation_gain"] > 0 for d in measured] == outcomes
            (calibration,) = [
                json.loads(row["panel_evidence"])
                for row in rows
                if row["kind"] == "panel_calibration"
            ]
            assert calibration["slope"] == slope
            assert all(d["binding"] == calibration["binding"] for d in evidence)
            metrics = CampaignReader(queue).snapshot(campaign)["kpis"]
            proposal = metrics["proposer_acceptance_rate"]
            assert (proposal["proposed"], proposal["accepted"], proposal["rejected"]) == (
                proposed,
                accepted,
                proposed - accepted,
            )
            assert proposal["fraction"] == pytest.approx(accepted / proposed)
            assert proposal["value"] == pytest.approx(100 * accepted / proposed)
            confirm = metrics["confirmation_pass_rate"]
            assert (confirm["passes"], confirm["decisions"]) == (sum(outcomes), len(measured))
            assert confirm["value"] == pytest.approx(100 * sum(outcomes) / len(measured))
            assert confirm["last_confirmation"] == {
                "promoted": outcomes[-1],
                "confirmation_gain": measured[-1]["confirmation_gain"],
            }
            fit = metrics["proxy_to_target_slope"]
            assert fit["value"] == calibration["slope"]
            assert fit["seed_noise"] == calibration["seed_noise"] == pytest.approx(math.sqrt(0.005))
            assert metrics["gpu_utilization"]["value"] is None
            html = export(tmp_path, queue, campaign)
            assert html["Proposer acceptance rate"] == f"{100 * accepted / proposed:g} %"
            assert html["Confirmation pass rate"] == f"{100 * sum(outcomes) / len(measured):g} %"
            assert html["Proxy-to-target slope"] == f"{slope} target / proxy"
            assert html["GPU utilization"] == "Unavailable"

        def event_ids():
            return db.execute("SELECT id, kind FROM swarm_events ORDER BY id").fetchall()

        study_uri = (tmp_path / "primary" / "panels.db").as_uri() + "?mode=ro"
        with closing(sqlite3.connect(study_uri, uri=True)) as panel:
            before = event_ids()
            looks = panel.execute("SELECT * FROM panel_looks ORDER BY id").fetchall()
            cli(tmp_path, "swarm", "run", primary, "--db", queue)
            assert event_ids() == before
            assert panel.execute("SELECT * FROM panel_looks ORDER BY id").fetchall() == looks


def test_offline_report_weights_generated_device_intervals_by_allocated_time(tmp_path):
    queue = tmp_path / "queue.sqlite"
    store = SQLiteStore(queue)
    device = "GPU-00000000-0000-0000-0000-000000000001"
    for campaign, second_busy in [("complete", 0), ("gap", None)]:
        store.create_campaign(
            spec(campaign_id=campaign, resource="gpu", options={"backend": {"device": device}})
        )
        store.submit(trial(1, campaign_id=campaign))
        identity = {"allocation_id": campaign, "device": device, "worker_id": "fixture"}
        store.record_gpu_time(campaign, "gpu_allocation_started", {**identity, "sequence": 0})
        attempt = store.claim(campaign, "fixture")
        assert attempt is not None
        for sequence, (busy, allocated) in enumerate([(0.5, 1), (second_busy, 1.5)], 1):
            store.record_gpu_time(
                campaign,
                "gpu_device_interval",
                {
                    **identity,
                    "sequence": sequence,
                    "busy_device_seconds": busy,
                    "allocated_device_seconds": allocated,
                },
            )
        assert store.report(Result(attempt=attempt, status="ok", metrics={"loss": 2}))
        store.record_gpu_time(campaign, "gpu_allocation_finished", {**identity, "sequence": 3})

    with closing(sqlite3.connect(queue.as_uri() + "?mode=ro", uri=True)) as db:
        for campaign in ("complete", "gap"):
            intervals = [
                json.loads(row["gpu_time"])
                for row in guide_evidence(db, campaign)
                if row["kind"] == "gpu_device_interval"
            ]
            covered = [s for s in intervals if s["busy_device_seconds"] is not None]
            busy = sum(s["busy_device_seconds"] for s in covered)
            allocated = sum(s["allocated_device_seconds"] for s in intervals)
            observed = sum(s["allocated_device_seconds"] for s in covered)
            assert (busy, allocated, observed) == (0.5, 2.5, 2.5 if campaign == "complete" else 1)
            metric = CampaignReader(queue).snapshot(campaign)["kpis"]["gpu_utilization"]
            assert (
                metric["busy_device_seconds"],
                metric["allocated_device_seconds"],
                metric["covered_device_seconds"],
            ) == (busy, allocated, observed)
            report = export(tmp_path, queue, campaign)
            if observed == allocated:
                assert metric["coverage"] == "complete"
                assert metric["value"] == 100 * busy / allocated
                assert report["GPU utilization"] == "20 %"
            else:
                assert metric["coverage"] == "partial" and metric["value"] is None
                assert report["GPU utilization"] == "Unavailable"
