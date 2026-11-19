"""Local CPU workers, generated volumes and an injected managed-job CLI."""

from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
import sys
from collections import Counter
from dataclasses import replace
from pathlib import Path
from threading import Barrier

import pytest

from research_swarm.swarm import data_stage
from research_swarm.swarm.backends.local import LocalBackend
from research_swarm.swarm.command import campaign_backend
from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Trial
from research_swarm.swarm.spec import load_spec
from research_swarm.swarm.store import SQLiteStore
from research_swarm.swarm.worker import WorkerLoop
from tests.swarm.test_backend_qualification import campaign_spec, run_campaign
from tests.swarm.test_skypilot import fake_sky


def trial(name: str) -> Trial:
    return Trial(
        trial_id=name,
        campaign_id="demo",
        candidate=Candidate(candidate_id=name),
        seed=3,
        fidelity=1,
    )


def rows(path: Path) -> list:
    return [json.loads(line) for line in path.read_text().splitlines()]


class BootBackend(LocalBackend):
    def __init__(self, root: Path, location: str, revision: str, rendezvous: Barrier):
        super().__init__(1)
        self.source = root / "source"
        self.volume = root / f"volume-{location}"
        self.nvme = root / f"nvme-{location}"
        self.revision = revision
        self.rendezvous = rendezvous

    def boot(self) -> Path:
        self.rendezvous.wait()
        stage = data_stage.prepare(self.source, self.volume, "demo", self.revision)
        self.rendezvous.wait()
        data = data_stage.hydrate(self.volume, self.nvme, "demo", self.revision)
        assert data_stage.read_stage(data.parent) == stage
        return data

    async def execute(self, spec: CampaignSpec, attempt: Attempt):
        data = await asyncio.to_thread(self.boot)
        return await LocalBackend(1, data_dir=data).execute(spec, attempt)


async def test_concurrent_boot_and_restart_copy_each_revision_once_per_location(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    database = tmp_path / "queue.sqlite"
    spec = replace(load_spec(campaign_spec(tmp_path, "local", {})), max_workers=3, max_trials=9)
    store = SQLiteStore(database)
    store.create_campaign(spec)
    copy_tree = data_stage._hash_tree
    journal = tmp_path / "copies.jsonl"

    def count_copy(source: Path, target: Path | None, threads: int):
        result = copy_tree(source, target, threads)
        if target is not None:  # Cache verification reads existing bytes without copying.
            location = target.relative_to(tmp_path).parts[0]
            with journal.open("a") as log:
                print(json.dumps([location, result[0]]), file=log)
        return result

    monkeypatch.setattr(data_stage, "_hash_tree", count_copy)
    expected: Counter = Counter()
    for wave, (revision, value) in enumerate([("v1", "13"), ("v1", "13"), ("v2", "21")]):
        (source / "points").write_text(value)
        records = [(".", "directory"), ("points", hashlib.sha256(value.encode()).hexdigest())]
        digest = hashlib.sha256(json.dumps(records, separators=(",", ":")).encode()).hexdigest()
        rendezvous = Barrier(3, timeout=10)

        workers = []
        for index, location in enumerate(("west", "west", "east")):
            owner = f"worker-{wave}-{index}"
            assert store.submit(trial(owner))
            workers.append(
                WorkerLoop(
                    SQLiteStore(database),
                    BootBackend(tmp_path, location, revision, rendezvous),
                    owner,
                )
            )
        async with asyncio.timeout(20):
            assert all(await asyncio.gather(*(worker.run_once(spec) for worker in workers)))
        for location in ("west", "east"):
            for kind in ("volume", "nvme"):
                expected[(f"{kind}-{location}", digest)] = 1
        assert Counter(map(tuple, rows(journal))) == expected
        results = store.results("demo")[-3:]
        assert len(results) == 3 and all(
            r.status == "ok" and r.metrics == {"score": int(value)} for r in results
        )
    executions = (tmp_path / "executions").read_text().splitlines()
    assert len(executions) == len(set(executions)) == 9


@pytest.mark.parametrize(
    ("status", "recoveries", "retry_spot", "transport_error"),
    [
        ("RECOVERING", 0, False, False),
        ("RECOVERING", 0, False, True),
        ("RUNNING", 1, False, False),
        ("RUNNING", 0, True, False),
    ],
)
async def test_restart_fences_previous_job_before_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    recoveries: int,
    retry_spot: bool,
    transport_error: bool,
) -> None:
    source, volume, nvme = (tmp_path / name for name in ("source", "volume", "nvme"))
    source.mkdir()
    (source / "points").write_text("13")
    published = data_stage.prepare(source, volume, "demo", "v1")
    sky = fake_sky(tmp_path, monkeypatch, status)
    spec_file = campaign_spec(
        tmp_path,
        "skypilot",
        dict(
            sky=sky,
            state=str(tmp_path),
            placements=["aws/test-region"],
            device="cpu",
            idle_minutes=7,
            poll_seconds=0.01,
            data=dict(
                revision="v1", volume=str(volume), nvme=str(nvme), mount_root=str(tmp_path / "mnt")
            ),
        ),
    )
    spec = load_spec(spec_file)
    db = tmp_path / "queue.sqlite"
    store = SQLiteStore(db)
    store.create_campaign(spec)
    submitted = trial("one")
    assert store.submit(submitted)
    # Stop the original worker after the provider accepts launch but before its first poll.
    program = """
import asyncio, os, sys
from pathlib import Path
from research_swarm.swarm.backends.skypilot import from_spec
from research_swarm.swarm.spec import load_spec
from research_swarm.swarm.store import SQLiteStore
from research_swarm.swarm.worker import WorkerLoop
spec = load_spec(Path(sys.argv[1]))
backend = from_spec(spec)
async def interrupt_observer(job, attempt):
    if sys.argv[3] == "transport-error":
        raise OSError("interrupted observation")
    os._exit(23)
backend._observe = interrupt_observer
asyncio.run(WorkerLoop(SQLiteStore(Path(sys.argv[2])), backend, "original").run_once(spec))
"""
    stopped = subprocess.run(
        [
            sys.executable,
            "-c",
            program,
            str(spec_file),
            str(db),
            "transport-error" if transport_error else "hard-exit",
        ],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert stopped.returncode == (0 if transport_error else 23), stopped.stderr
    assert store.queue_depth("demo") == ((1, 0) if transport_error else (0, 1))
    jobs_file = tmp_path / "jobs.json"
    jobs = json.loads(jobs_file.read_text())
    assert len(jobs) == 1 and jobs[0]["status"] == ("CANCELLED" if transport_error else status)
    if recoveries:
        jobs[0]["recovery_count"] = recoveries
        jobs_file.write_text(json.dumps(jobs))
    worker = WorkerLoop(
        SQLiteStore(db),
        campaign_backend(spec),
        "replacement",
        heartbeat_seconds=0.05,
        lost_after_seconds=0.2,
    )
    async with asyncio.timeout(20):
        while not await worker.run_once(spec):
            await asyncio.sleep(0.025)
    lost, accepted = store.results("demo")
    assert (lost.status, accepted.status) == ("lost", "ok")
    if transport_error:
        assert lost.error == "interrupted observation"
    else:
        assert lost.failure_class == "heartbeat_expired"
    assert lost.attempt.trial == accepted.attempt.trial == submitted
    assert (lost.attempt.number, accepted.attempt.number) == (1, 2)
    assert data_stage.read_stage(nvme / "demo/v1") == published
    assert accepted.metrics == {"score": 13}
    assert not store.report(lost) and not store.report(accepted)
    assert (
        run_campaign(spec_file, db)["selected"]["result"]["attempt"]["attempt_id"]
        == accepted.attempt.attempt_id
    )
    assert store.results("demo") == [lost, accepted]
    assert (tmp_path / "executions").read_text().splitlines() == [accepted.attempt.attempt_id]
    calls = rows(tmp_path / "calls")
    actions = [call[1] for call in calls]
    assert set(actions) == {"launch", "queue", "cancel", "logs"}
    assert [call for call in calls if call[1] == "cancel"] == [["jobs", "cancel", "-y", "101"]]
    assert [action for action in actions if action in {"launch", "cancel"}] == [
        "launch",
        "cancel",
        "launch",
    ]
    tasks = rows(tmp_path / "tasks")
    assert [task["resources"]["any_of"][0]["use_spot"] for task in tasks] == [True, retry_spot]
