"""Generated local instrument readings. These tests exercise no physical GPU."""

from __future__ import annotations

import json
import math
import sqlite3
import subprocess
import sys
import time
from collections import deque
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from research_swarm.swarm.backends import gpu_time
from research_swarm.swarm.command import run_campaign
from research_swarm.swarm.reader import CampaignReader
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec, trial

GPU_A = "GPU-00000000-0000-0000-0000-000000000001"
GPU_B = "GPU-00000000-0000-0000-0000-000000000002"


def config(*devices):
    return {
        "backend": {"device": devices[0], "sample_gpu_time": True},
        "resources": {"devices_mib": {device: 100 for device in devices}},
    }


def kpi(path, campaign="test"):
    return CampaignReader(path).snapshot(campaign)["kpis"]["gpu_utilization"]


def record(store, allocation, device, sequence, kind, *, campaign="test", **seconds):
    store.record_gpu_time(
        campaign,
        kind,
        {
            "allocation_id": allocation,
            "device": device,
            "worker_id": "local",
            "sequence": sequence,
            **seconds,
        },
    )


def interval(store, allocation, device, sequence, allocated, busy):
    record(
        store,
        allocation,
        device,
        sequence,
        "gpu_device_interval",
        allocated_device_seconds=allocated,
        busy_device_seconds=busy,
    )


def set_sample_windows(monkeypatch, *durations):
    """Deliver scheduled ticks, then take the final sample on the real allocation drain."""
    pending = deque(durations)
    elapsed = 0.0

    class SampleEvent(Event):
        def wait(self, timeout=None):
            nonlocal elapsed
            if len(pending) == 1:
                super().wait()
            elapsed += pending.popleft()
            return not pending

    # Keep worker deadlines and subprocess supervision on the host's real clock.
    monkeypatch.setattr(gpu_time, "time", SimpleNamespace(monotonic=lambda: elapsed))
    monkeypatch.setattr(gpu_time, "Event", SampleEvent)


def test_instrumented_run_packs_retries_and_replays(tmp_path, monkeypatch):
    set_sample_windows(monkeypatch, 1.0, 0.5)
    driver = Mock(wraps=subprocess)
    driver.run.return_value = subprocess.CompletedProcess(
        ["nvidia-smi"], 0, stdout=f"{GPU_A}, 100\n{GPU_B}, 20\n".encode()
    )
    monkeypatch.setattr(gpu_time, "run_command", driver.run)
    workload = """
import json, os
from pathlib import Path
trial = json.loads(Path(os.environ["SWARM_TRIAL"]).read_text())
assert os.environ["CUDA_VISIBLE_DEVICES"] in (
    "GPU-00000000-0000-0000-0000-000000000001", "GPU-00000000-0000-0000-0000-000000000002")
if trial["candidate"]["candidate_id"] == "a" and not Path("retried").exists():
    Path("retried").touch()
    raise SystemExit(2)
Path(os.environ["SWARM_METRICS"]).write_text('{"loss": 1}')
"""
    yaml = tmp_path / "campaign.json"
    yaml.write_text(
        json.dumps(
            {
                "campaign_id": "instrumented",
                "resource": "gpu",
                "evaluation_mode": "smoke",
                "direction": "minimize",
                "metric": "loss",
                "command": [sys.executable, "-c", workload],
                "max_workers": 2,
                "max_trials": 2,
                "candidates": [{"candidate_id": "a"}, {"candidate_id": "b"}],
                "options": config(GPU_A, GPU_B),
            }
        )
    )
    path = tmp_path / "instrumented.sqlite"
    report, code = run_campaign(yaml, path)
    assert code == 0 and (report["trials"], report["attempts"], report["failed"]) == (2, 3, 1)
    snap = CampaignReader(path).snapshot("instrumented")
    item = snap["kpis"]["gpu_utilization"]
    assert item["value"] == pytest.approx(60) and item["coverage"] == "complete"
    with sqlite3.connect(path) as db:
        samples = [
            json.loads(row[0])
            for row in db.execute(
                "SELECT gpu_time FROM swarm_events WHERE kind='gpu_device_interval'"
            )
        ]
        starts = db.execute(
            "SELECT COUNT(*) FROM swarm_events WHERE kind='gpu_allocation_started'"
        ).fetchone()[0]
    assert [call.args[0] for call in driver.run.call_args_list] == [
        ["nvidia-smi", "--query-gpu=uuid,utilization.gpu", "--format=csv,noheader,nounits"]
    ] * 2
    assert len(samples) == 4
    assert item["covered_device_seconds"] == item["allocated_device_seconds"] == 3.0
    assert starts == 2 and {sample["device"] for sample in samples} == {GPU_A, GPU_B}
    assert item["busy_device_seconds"] == pytest.approx(
        sum(s["busy_device_seconds"] for s in samples)
    )
    assert item["covered_device_seconds"] == pytest.approx(
        sum(s["allocated_device_seconds"] for s in samples)
    )
    # A fresh process replay must leave the allocation ledger alone.
    replay = subprocess.run(
        [sys.executable, "-m", "research_swarm", "swarm", "run", str(yaml), "--db", str(path)],
        capture_output=True,
        text=True,
    )
    assert replay.returncode == 0, replay.stderr
    after = CampaignReader(path).snapshot("instrumented")
    assert after["kpis"]["gpu_utilization"] == item
    assert after["campaign_revision"] == snap["campaign_revision"]


@pytest.mark.parametrize(
    ("windows", "expected_busy"),
    [
        ((math.nextafter(1.0, 0.0),), (None,)),
        ((math.nextafter(2.0, math.inf),), (None,)),
        ((1.0, 2.0, 2.125, 0.25), (0.5, 1.0, None, 0.125)),
    ],
)
def test_sampler_coverage_boundaries(tmp_path, monkeypatch, windows, expected_busy):
    set_sample_windows(monkeypatch, *windows)
    monkeypatch.setattr(gpu_time, "_busy_fractions", lambda: {GPU_A: 0.5})
    path = tmp_path / "boundaries.sqlite"
    store = SQLiteStore(path)
    store.create_campaign(spec(resource="gpu", options=config(GPU_A)))
    with gpu_time.sample_allocation(store, "test", "local", (GPU_A,)):
        pass
    with sqlite3.connect(path) as db:
        intervals = db.execute(
            "SELECT json_extract(gpu_time,'$.allocated_device_seconds'),"
            "json_extract(gpu_time,'$.busy_device_seconds') FROM swarm_events "
            "WHERE kind='gpu_device_interval' ORDER BY id"
        ).fetchall()
    assert intervals == list(zip(windows, expected_busy, strict=True))
    item = kpi(path)
    assert item["value"] is None and item["coverage"] == "partial"
    assert item["missing_samples"] == expected_busy.count(None)
    assert item["covered_device_seconds"] == sum(
        seconds for seconds, busy in intervals if busy is not None
    )


def test_weighted_evidence_replay_idle_and_gaps(tmp_path):
    path = tmp_path / "weighted.sqlite"
    store = SQLiteStore(path)
    store.create_campaign(spec(resource="gpu", options=config(GPU_A, GPU_B)))
    store.create_campaign(spec(campaign_id="cpu"))
    store.create_campaign(spec(campaign_id="unknown", resource="gpu"))
    for campaign in ("cpu", "unknown"):
        item = kpi(path, campaign)
        assert item["coverage"] == "unavailable"
        assert (
            item["value"] is item["allocated_device_seconds"] is item["busy_device_seconds"] is None
        )
    record(store, "one", GPU_A, 0, "gpu_allocation_started")
    interval(store, "one", GPU_A, 1, 2, 0)
    record(store, "one", GPU_A, 2, "gpu_allocation_finished")
    assert kpi(path)["value"] is None and "1 missing devices" in kpi(path)["detail"]
    record(store, "two", GPU_B, 0, "gpu_allocation_started")
    interval(store, "two", GPU_B, 1, 6, 4)
    assert kpi(path)["value"] is None and kpi(path)["open_allocations"] == 1
    record(store, "two", GPU_B, 2, "gpu_allocation_finished")
    store = SQLiteStore(path)
    interval(store, "two", GPU_B, 1, 6, 4)
    record(store, "one", GPU_A, 2, "gpu_allocation_finished")
    with pytest.raises(ValueError, match="Conflicting"):
        interval(store, "two", GPU_B, 1, 6, 3)
    item = kpi(path)
    assert (item["value"], item["busy_device_seconds"], item["allocated_device_seconds"]) == (
        50,
        4,
        8,
    )
    assert item["coverage"] == "complete" and item["covered_device_seconds"] == 8

    gap = tmp_path / "gap.sqlite"
    with sqlite3.connect(path) as original, sqlite3.connect(gap) as copy:
        original.backup(copy)
    store = SQLiteStore(gap)
    record(store, "lost", GPU_A, 0, "gpu_allocation_started")
    interval(store, "lost", GPU_A, 1, 3, None)
    record(store, "lost", GPU_A, 2, "gpu_allocation_finished")
    item = kpi(gap)
    assert (item["value"], item["coverage"], item["busy_device_seconds"]) == (None, "partial", 4)
    assert (item["covered_device_seconds"], item["allocated_device_seconds"]) == (8, 11)


def test_overlap_and_uninstrumented_claim_do_not_report_a_percentage(tmp_path):
    path = tmp_path / "overlap.sqlite"
    store = SQLiteStore(path)
    for campaign in ("test", "neighbor"):
        store.create_campaign(spec(campaign_id=campaign, resource="gpu", options=config(GPU_A)))
    record(store, "one", GPU_A, 0, "gpu_allocation_started")
    interval(store, "one", GPU_A, 1, 1, 0)
    record(store, "one", GPU_A, 2, "gpu_allocation_finished")
    assert kpi(path)["value"] == 0
    store.submit(trial(1))
    assert store.claim("test", "external")
    assert kpi(path)["value"] is None and "1 uncovered claims" in kpi(path)["detail"]
    record(store, "two", GPU_A, 0, "gpu_allocation_started")
    record(store, "other", GPU_A, 0, "gpu_allocation_started", campaign="neighbor")
    for campaign, allocation in (("test", "two"), ("neighbor", "other")):
        record(
            store,
            allocation,
            GPU_A,
            1,
            "gpu_device_interval",
            campaign=campaign,
            allocated_device_seconds=1,
            busy_device_seconds=1,
        )
        record(store, allocation, GPU_A, 2, "gpu_allocation_finished", campaign=campaign)
    for campaign in ("test", "neighbor"):
        item = kpi(path, campaign)
        assert item["value"] is item["allocated_device_seconds"] is None
        assert "Overlapping" in item["detail"]


def test_missing_driver_readings_and_legacy_database(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path))
    assert gpu_time._busy_fractions() == {}
    script = tmp_path / "nvidia-smi"
    script.write_text(f"#!/bin/sh\nprintf '%s\\n' '{GPU_A}, N/A' '{GPU_B}, 101'\n")
    script.chmod(0o755)
    assert gpu_time._busy_fractions() == {}
    path = tmp_path / "legacy.sqlite"
    store = SQLiteStore(path)
    store.create_campaign(spec(resource="gpu"))
    with sqlite3.connect(path) as db:
        db.execute("DROP INDEX swarm_gpu_sequence")
        db.execute("ALTER TABLE swarm_events DROP COLUMN gpu_time")
    assert kpi(path)["coverage"] == "unavailable" and kpi(path)["value"] is None


def test_interrupted_sampler_leaves_an_open_allocation_on_restart(tmp_path):
    path = tmp_path / "interrupted.sqlite"
    store = SQLiteStore(path)
    store.create_campaign(spec(resource="gpu", options=config(GPU_A)))
    code = """
import sys, time
from pathlib import Path
from research_swarm.swarm.backends import gpu_time
from research_swarm.swarm.store import SQLiteStore
gpu_time._busy_fractions = lambda: {sys.argv[2]: 0.25}
with gpu_time.sample_allocation(SQLiteStore(Path(sys.argv[1])), "test", "local", (sys.argv[2],)):
    time.sleep(60)
"""
    process = subprocess.Popen([sys.executable, "-c", code, str(path), GPU_A])
    try:
        deadline = time.monotonic() + 15
        while kpi(path)["covered_device_seconds"] is None:
            assert process.poll() is None and time.monotonic() < deadline
            time.sleep(0.05)
    finally:
        process.kill()
        process.wait(timeout=5)
    # The missing tail duration cannot be inferred from the replacement worker.
    partial = kpi(path)
    assert partial["open_allocations"] == 1 and partial["value"] is None
    assert partial["busy_device_seconds"] == pytest.approx(partial["covered_device_seconds"] * 0.25)
    assert kpi(SQLiteStore(path).path) == partial


def test_cpu_workload_cannot_supply_device_time_in_usage_or_objective(tmp_path, monkeypatch):
    def unexpected_probe():
        pytest.fail("CPU campaigns must not query accelerator telemetry")

    monkeypatch.setattr(gpu_time, "_busy_fractions", unexpected_probe)
    script = """
import os
from pathlib import Path
Path(os.environ["SWARM_METRICS"]).write_text('{"loss": 1, "gpu_utilization": 95}')
usage = '{"busy_device_seconds": 9, "allocated_device_seconds": 10}'
Path(os.environ["SWARM_USAGE"]).write_text(usage)
"""
    yaml = tmp_path / "cpu.json"
    yaml.write_text(
        json.dumps(
            {
                "campaign_id": "cpu",
                "resource": "cpu",
                "evaluation_mode": "smoke",
                "direction": "minimize",
                "metric": "loss",
                "command": [sys.executable, "-c", script],
                "max_workers": 1,
                "max_trials": 1,
                "candidates": [{"candidate_id": "a"}],
            }
        )
    )
    path = tmp_path / "cpu.sqlite"
    assert run_campaign(yaml, path)[1] == 0
    item = kpi(path, "cpu")
    assert (item["value"], item["coverage"], item["allocated_device_seconds"]) == (
        None,
        "unavailable",
        None,
    )
