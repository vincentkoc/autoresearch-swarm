"""Observe a real CPU workload across claim, failure, worker restart and retry."""

import asyncio
import sqlite3
import sys
import time
from pathlib import Path

import pytest

from research_swarm.swarm.backends.local import LocalBackend
from research_swarm.swarm.backends.ssh_pool import PoolHost, SSHPoolBackend
from research_swarm.swarm.interfaces import Backend, CampaignSpec, Candidate, Trial
from research_swarm.swarm.store import SQLiteStore
from research_swarm.swarm.worker import WorkerLoop
from tests.swarm.test_ssh_pool import fake_pool_commands


@pytest.mark.parametrize("kind", ["local", "ssh"])
async def test_launch_ack_and_attempt_diagnostics_survive_retry_and_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    program = """
from pathlib import Path
import os, time
if Path('fail-first').exists():
    raise SystemExit(7)
Path('entered').touch()
while not Path('release').exists():
    time.sleep(0.01)
Path(os.environ['SWARM_METRICS']).write_text('{"score":13}')
"""
    backend: Backend
    if kind == "ssh":
        aws, ssh = fake_pool_commands(tmp_path, monkeypatch)
        backend = SSHPoolBackend(
            [
                PoolHost(
                    target="demo-worker",
                    instance_id="pool",
                    region="test-region",
                    task_dir=str(tmp_path),
                    device="cpu",
                )
            ],
            aws=aws,
            ssh=ssh,
        )
        (tmp_path / "drop-next-ssh").touch()
    else:
        backend = LocalBackend(1)
        (tmp_path / "fail-first").touch()
    spec = CampaignSpec(
        campaign_id="demo",
        max_workers=1,
        max_trials=1,
        max_attempts=2,
        metric="score",
        direction="maximize",
        task_dir=str(tmp_path),
        command=(sys.executable, "-c", program),
        trial_timeout_seconds=15,
    )
    db = tmp_path / "queue.sqlite"
    store = SQLiteStore(db)
    store.create_campaign(spec)
    assert store.submit(
        Trial(
            trial_id="one",
            campaign_id="demo",
            seed=0,
            fidelity=1,
            candidate=Candidate(candidate_id="one"),
        )
    )
    assert await WorkerLoop(store, backend, "first").run_once(spec)
    (tmp_path / "fail-first").unlink(missing_ok=True)
    reopened = SQLiteStore(db)
    retry = asyncio.create_task(WorkerLoop(reopened, backend, "second").run_once(spec))
    try:
        async with asyncio.timeout(7):
            while not (tmp_path / "entered").exists():
                await asyncio.sleep(0.01)
        # Read from a separate connection while the retry is still running.
        with sqlite3.connect(db) as reader:
            start_query = """SELECT e.attempt_id, e.at, a.started_at FROM swarm_events e
                             JOIN swarm_attempts a USING(attempt_id)
                             WHERE e.kind='workload_started' ORDER BY e.id"""
            expected_starts = 1 if kind == "ssh" else 2
            deadline = time.monotonic() + 2
            while len(starts := reader.execute(start_query).fetchall()) < expected_starts:
                if time.monotonic() >= deadline:
                    break
                await asyncio.sleep(0.01)
            assert len(reopened.results("demo")) == 1
            if starts:
                reopened.workload_started(starts[-1][0])
    finally:
        (tmp_path / "release").touch()
        assert await retry
    first, second = reopened.results("demo")
    assert first.status == ("lost" if kind == "ssh" else "failed")
    assert second.status == "ok" and second.metrics == {"score": 13}
    assert first.attempt.trial == second.attempt.trial
    assert [r[0] for r in starts] == (
        [second.attempt.attempt_id]
        if kind == "ssh"
        else [first.attempt.attempt_id, second.attempt.attempt_id]
    )
    assert all(ack >= claim for _, ack, claim in starts)
    assert first.failure_class == ("remote_dispatch" if kind == "ssh" else "command_exit")
    assert second.failure_class is None
    assert not reopened.report(second)
    reopened.workload_started(first.attempt.attempt_id)
    with sqlite3.connect(db) as reader:
        assert reader.execute(start_query).fetchall() == starts
        assert reader.execute(
            "SELECT count(*) FROM swarm_events WHERE kind='finished'"
        ).fetchone() == (2,)
