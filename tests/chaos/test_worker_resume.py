"""Crash a calibrated local campaign during its first search attempt and resume it."""

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import psutil
import pytest

from research_swarm.swarm.store import SQLiteStore
from research_swarm.swarm.worker import WorkerLoop
from tests.paths import REPO_ROOT
from tests.swarm.test_local_backend import assert_child_stopped
from tests.swarm.test_panel_binding import generated_policy


def short_lease_worker(spec, store, backend):
    return WorkerLoop(store, backend, "local", heartbeat_seconds=0.05, lost_after_seconds=0.5)


@pytest.mark.skipif(os.name != "posix", reason="worker SIGKILL requires POSIX")
def test_calibrated_worker_kill_resume_and_replay(tmp_path, monkeypatch):
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    monkeypatch.setenv("FIXTURE_OFFSET", "0")
    monkeypatch.setenv("FIXTURE_STUDY", str(tmp_path))
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(REPO_ROOT), os.getenv("PYTHONPATH", "")]))
    task = tmp_path / "score.py"
    task.write_text(
        """import os, time
from pathlib import Path

started = Path(os.environ["SWARM_ATTEMPT_ID"] + ".started")
first = not any(Path.cwd().glob("*.started"))
ready = started.with_suffix(".pending")
ready.write_text(f"{os.getpid()} {os.getppid()}")
ready.rename(started)
if first:
    time.sleep(60)
Path(os.environ["SWARM_METRICS"]).write_text('{"score": 1}')
"""
    )
    spec = tmp_path / "campaign.yaml"
    spec.write_text(
        json.dumps(
            {
                "campaign_id": "recovery",
                "metric": "score",
                "direction": "maximize",
                "command": [sys.executable, str(task)],
                "task_dir": str(tmp_path),
                "max_workers": 1,
                "max_trials": 1,
                "max_attempts": 2,
                "candidates": [{"candidate_id": "challenger", "params": {"x": "1"}}],
                "panel_policy": "tests.swarm.test_panel_binding:generated_policy",
                "worker": "tests.chaos.test_worker_resume:short_lease_worker",
            }
        )
    )
    queue = tmp_path / "queue.sqlite"
    command = [str(Path(sys.executable).with_name("research")), "swarm", "run", spec, "--db", queue]
    with subprocess.Popen(command, cwd=tmp_path) as worker:
        try:
            deadline = time.monotonic() + 15
            while not any(tmp_path.glob("*.started")):
                assert worker.poll() is None, "worker exited before the trial started"
                assert time.monotonic() < deadline, "trial did not start"
                time.sleep(0.02)
            descendants = psutil.Process(worker.pid).children(recursive=True)
            assert descendants
            assert SQLiteStore(queue).queue_depth("recovery") == (0, 1)
            study = generated_policy(None).study
            calibration = {"calibration_proxy": 6, "calibration_target": 6}
            assert study.look_counts() == {**calibration, "search": 1}
            worker.kill()
            assert worker.wait(timeout=5) == -signal.SIGKILL
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.wait(timeout=5)
    for child in descendants:
        asyncio.run(assert_child_stopped(child.pid))

    def resume():
        return json.loads(subprocess.check_output(command, cwd=tmp_path, timeout=20))

    summary = resume()
    store = SQLiteStore(queue)
    [trial] = store.trials("recovery")
    lost, accepted = store.results("recovery")
    assert lost.attempt.trial == accepted.attempt.trial == trial
    assert (lost.status, lost.failure_class, lost.attempt.number) == (
        "lost",
        "heartbeat_expired",
        1,
    )
    assert (accepted.status, accepted.attempt.number, accepted.metrics) == ("ok", 2, {"score": 1})
    assert (summary["trials"], summary["attempts"], summary["lost"]) == (1, 2, 1)
    assert summary["confirmed_score"] == 1
    assert summary["selected"]["result"]["attempt"]["attempt_id"] == accepted.attempt.attempt_id
    assert not store.report(replace(lost, status="ok", metrics={"score": 99}))
    assert not store.report(accepted)
    # Both executions consumed a search look. Only the committed winner opened held-out panels.
    final_looks = {**calibration, "search": 2, "selection": 4, "confirmation": 2}
    assert study.look_counts() == final_looks
    assert resume() == summary
    assert store.trials("recovery") == [trial]
    assert store.results("recovery") == [lost, accepted]
    assert store.queue_depth("recovery") == (0, 0)
    assert study.look_counts() == final_looks
    markers = list(tmp_path.glob("*.started"))
    assert {p.stem for p in markers} == {r.attempt.attempt_id for r in (lost, accepted)}
    for marker in markers:
        for pid in map(int, marker.read_text().split()):
            asyncio.run(assert_child_stopped(pid))
