"""Eight independent resident workers claim and report against one durable campaign."""

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

import psutil
import pytest

from research_swarm.swarm.backends.local import LocalBackend
from research_swarm.swarm.loop import candidate_key
from research_swarm.swarm.store import SQLiteStore
from research_swarm.swarm.worker import WorkerLoop
from tests.benchmarks.test_panels import measure, study
from tests.swarm.test_loop import campaign
from tests.swarm.test_store import trial

WORKERS = 8


def _spec(root):
    return campaign(
        evaluation_mode="calibrated",
        command=(sys.executable, str(root / "task.py")),
        task_dir=str(root),
        max_workers=WORKERS,
        max_trials=WORKERS,
    )


async def _work(root, identity):
    panels = study(root, measure, look_limits={"search": WORKERS})
    queue = SQLiteStore(root / "queue.sqlite")
    worker = WorkerLoop(queue, LocalBackend(1), identity)

    async def reserve_search(attempt):
        panels.record_search(candidate_key(attempt.trial.candidate), attempt.trial.seed)

    assert await worker.run_once(_spec(root), before_execute=reserve_search)


@pytest.mark.skipif(os.name != "posix", reason="LocalBackend uses POSIX process groups")
def test_eight_workers_share_one_store(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    queue = SQLiteStore(tmp_path / "queue.sqlite")
    panels = study(tmp_path, measure, look_limits={"search": WORKERS})
    queue.register_panel_campaign(spec, panels)
    panels.calibrate()
    trials = [trial(i, campaign_id=spec.campaign_id) for i in range(WORKERS)]
    for entry in trials:
        assert queue.submit(entry)

    # No workload can report until all eight real local commands reach the latch.
    (tmp_path / "task.py").write_text(
        "import json, os, time\n"
        "from pathlib import Path\n"
        "Path(f'ready-{os.getpid()}').touch()\n"
        "deadline = time.monotonic() + 60\n"
        "while not Path('release').exists():\n"
        "    if time.monotonic() >= deadline: raise TimeoutError('release')\n"
        "    time.sleep(0.02)\n"
        "Path(os.environ['SWARM_METRICS']).write_text(json.dumps({'score': 1.0}))\n"
    )
    workers = []
    witnesses = []
    module = "tests.chaos.test_eight_workers"
    try:
        for n in range(WORKERS):
            workers.append(subprocess.Popen([sys.executable, "-m", module, str(tmp_path), str(n)]))
        deadline = time.monotonic() + 45
        while len(list(tmp_path.glob("ready-*"))) != WORKERS:
            assert all(worker.poll() is None for worker in workers), "worker exited before latch"
            assert time.monotonic() < deadline, "eight workloads did not reach the latch"
            time.sleep(0.02)
        assert queue.queue_depth(spec.campaign_id) == (0, WORKERS)
        assert queue.results(spec.campaign_id) == []
        expected_looks = {"calibration_proxy": 6, "calibration_target": 6, "search": WORKERS}
        assert panels.look_counts() == expected_looks
        for worker in workers:
            process = psutil.Process(worker.pid)
            witnesses.extend([process, *process.children(recursive=True)])
        workloads = {int(path.name.split("-")[1]) for path in tmp_path.glob("ready-*")}
        assert workloads <= {process.pid for process in witnesses}
    finally:
        (tmp_path / "release").touch()
        for worker in workers:
            try:
                worker.wait(timeout=15)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=5)

    assert all(worker.returncode == 0 for worker in workers)
    _, alive = psutil.wait_procs(witnesses, timeout=5)
    assert not alive
    reopened = SQLiteStore(tmp_path / "queue.sqlite")
    results = reopened.results(spec.campaign_id)
    assert len(results) == WORKERS
    assert {r.attempt.trial.trial_id for r in results} == {t.trial_id for t in trials}
    assert {r.attempt.worker_id for r in results} == {str(i) for i in range(WORKERS)}
    assert all(r.status == "ok" and r.attempt.number == 1 for r in results)
    assert all(not reopened.report(r) for r in results)
    assert reopened.results(spec.campaign_id) == results
    assert reopened.queue_depth(spec.campaign_id) == (0, 0)
    assert panels.look_counts() == expected_looks


if __name__ == "__main__":
    asyncio.run(_work(Path(sys.argv[1]), sys.argv[2]))
