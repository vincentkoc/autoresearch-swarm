from __future__ import annotations

import asyncio
import math
import multiprocessing
import signal
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, replace
from multiprocessing.connection import Connection
from pathlib import Path

import pytest

from research_swarm.swarm.backends.local import LocalBackend
from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Result, Trial
from research_swarm.swarm.meter import CostMeter
from research_swarm.swarm.resources import ResourcePool
from research_swarm.swarm.store import SQLiteStore
from research_swarm.swarm.worker import WorkerLoop, from_spec


def _campaign(directory: Path) -> CampaignSpec:
    return CampaignSpec(
        campaign_id="campaign",
        metric="loss",
        direction="minimize",
        max_trials=4,
        max_workers=2,
        command=(sys.executable, "task.py"),
        task_dir=str(directory),
        trial_timeout_seconds=10,
    )


def _trial(spec: CampaignSpec, name: str) -> Trial:
    return Trial(
        trial_id=name,
        campaign_id=spec.campaign_id,
        candidate=Candidate(candidate_id=name),
        fidelity=1.0,
        seed=7,
    )


def test_price_wall_time_and_tokens_independently(tmp_path: Path) -> None:
    spec = replace(
        _campaign(tmp_path),
        trial_timeout_seconds=1800,
        price_per_hour=3.6,
        price_per_million_input_tokens=1.25,
        price_per_million_output_tokens=5,
        estimated_input_tokens=1_000_000,
        estimated_output_tokens=200_000,
    )
    trial = _trial(spec, "one")
    attempt = Attempt(attempt_id="first", trial=trial, worker_id="worker", number=1, started_at=0)
    meter = CostMeter()
    assert meter.estimate(spec, trial) == 4.05
    assert meter.charge(spec, attempt, 90, 80_000, 10_000) == 0.24
    with pytest.raises(ValueError):
        meter.charge(spec, attempt, math.nan, 0, 0)
    with pytest.raises(ValueError):
        meter.charge(spec, attempt, 0, -1, 0)
    with pytest.raises(ValueError):
        meter.estimate(replace(spec, price_per_hour=-1), trial)


class _TokenBackend:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    def free_slots(self, spec: CampaignSpec) -> int:
        return 2

    async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
        self.calls += 1
        self.started.set()
        await self.release.wait()
        return Result(attempt=attempt, status="ok", metrics={"loss": 1.0}, input_tokens=20)


async def test_budget_gates_dispatch_then_settles_measured_usage(tmp_path: Path) -> None:
    spec = replace(
        _campaign(tmp_path),
        max_cost=1.25,
        estimated_input_tokens=100,
        resource="llm",
        price_per_million_input_tokens=10_000,
    )
    db = tmp_path / "queue.db"
    store = SQLiteStore(db)
    store.create_campaign(spec)
    for name in ("one", "two", "three"):
        store.submit(_trial(spec, name))
    pool = ResourcePool(llm=2, tokens_per_minute=200)
    backend = _TokenBackend()
    first = WorkerLoop(store, backend, "one", pool=pool)
    second = WorkerLoop(SQLiteStore(db), backend, "two", pool=pool)
    observed: list[Attempt] = []

    async def observe(attempt: Attempt) -> None:
        observed.append(attempt)

    running = asyncio.create_task(first.run_once(spec, before_execute=observe))
    await asyncio.wait_for(backend.started.wait(), timeout=5)
    try:
        assert not await second.run_once(spec, before_execute=observe)
        assert backend.calls == 1
        assert len(observed) == 1
    finally:
        backend.release.set()
        await running
    assert await second.run_once(spec, before_execute=observe)
    assert [r.cost for r in store.results(spec.campaign_id)] == [0.2, 0.2]
    # A fresh pool has capacity, but the remaining 0.85 cannot cover the estimate.
    assert not await WorkerLoop(store, backend, "three").run_once(spec, before_execute=observe)
    assert backend.calls == 2
    assert observed == [r.attempt for r in store.results(spec.campaign_id)]


async def test_resident_worker_survives_command_failure_and_timeout(tmp_path: Path) -> None:
    (tmp_path / "task.py").write_text(
        "import json, os, sys, time\n"
        "from pathlib import Path\n"
        "name = json.loads(Path(os.environ['SWARM_TRIAL']).read_text())['trial_id']\n"
        "if name == 'oom': sys.exit(137)\n"
        "if name == 'hang': time.sleep(30)\n"
        "Path(os.environ['SWARM_METRICS']).write_text('{\"loss\": 2}')\n"
    )
    spec = replace(_campaign(tmp_path), max_workers=1, max_attempts=1, trial_timeout_seconds=1)
    store = SQLiteStore(tmp_path / "queue.db")
    store.create_campaign(spec)
    for name in ("oom", "hang", "good"):
        store.submit(_trial(spec, name))
    stop = asyncio.Event()
    worker = WorkerLoop(
        store, LocalBackend(1), "resident", heartbeat_seconds=0.1, lost_after_seconds=2
    )
    resident = asyncio.create_task(worker.run(spec, stop=stop, poll_seconds=0.01))
    try:
        async with asyncio.timeout(15):
            while store.outstanding(spec.campaign_id):
                await asyncio.sleep(0.02)
    finally:
        stop.set()
        await resident
    results = {r.attempt.trial.trial_id: r for r in store.results(spec.campaign_id)}
    assert results["oom"].status == results["hang"].status == "failed"
    assert "137" in (results["oom"].error or "")
    assert "timed out" in (results["hang"].error or "").lower()
    assert results["good"].metrics == {"loss": 2}
    assert {r.attempt.worker_id for r in results.values()} == {"resident"}


async def test_remote_startup_is_budgeted_and_has_its_own_time_allowance(tmp_path: Path) -> None:
    class DelayedBackend(_TokenBackend):
        startup_timeout_seconds = 1.0

        async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
            self.calls += 1
            assert spec.trial_timeout_seconds == 0.05
            await asyncio.sleep(0.12)
            return Result(attempt=attempt, status="ok", metrics={"loss": 1.0})

    store = SQLiteStore(tmp_path / "queue.db")
    backend = DelayedBackend()
    worker = WorkerLoop(store, backend, "remote", heartbeat_seconds=0.01, lost_after_seconds=0.5)
    spec = replace(_campaign(tmp_path), price_per_hour=3600, trial_timeout_seconds=0.05, max_cost=1)
    store.create_campaign(spec)
    store.submit(_trial(spec, "denied"))
    assert not await worker.run_once(spec)
    assert backend.calls == 0
    spec = replace(spec, campaign_id="funded", max_cost=2)
    store.create_campaign(spec)
    store.submit(_trial(spec, "admitted"))
    assert await worker.run_once(spec)
    (observation,) = store.results(spec.campaign_id)
    assert observation.status == "ok"
    assert observation.cost >= 0.12


async def test_backend_cancellation_and_invalid_metrics_remain_attempt_data(tmp_path: Path) -> None:
    class InterruptedBackend(_TokenBackend):
        async def execute(self, spec: CampaignSpec, attempt: Attempt) -> Result:
            if attempt.number == 1:
                raise asyncio.CancelledError
            return Result(attempt=attempt, status="ok", metrics={"loss": math.nan}, input_tokens=20)

    store = SQLiteStore(tmp_path / "queue.db")
    spec = replace(_campaign(tmp_path), price_per_million_input_tokens=10_000)
    store.create_campaign(spec)
    store.submit(_trial(spec, "one"))
    worker = WorkerLoop(store, InterruptedBackend(), "resident")
    seen: list[Attempt] = []

    async def observe(attempt: Attempt) -> None:
        seen.append(attempt)

    assert await worker.run_once(spec, before_execute=observe)
    assert await worker.run_once(spec, before_execute=observe)
    lost, failed = store.results(spec.campaign_id)
    assert seen == [lost.attempt, failed.attempt]
    assert lost.status == "lost"
    assert (failed.status, failed.cost) == ("failed", 0.2)


async def test_stale_heartbeat_cancels_execution_without_a_second_report(tmp_path: Path) -> None:
    spec = _campaign(tmp_path)
    db = tmp_path / "queue.db"
    store = SQLiteStore(db)
    store.create_campaign(spec)
    store.submit(_trial(spec, "recover"))
    backend = _TokenBackend()
    worker = WorkerLoop(store, backend, "original", heartbeat_seconds=0.05, lost_after_seconds=1)
    running = asyncio.create_task(worker.run_once(spec))
    await asyncio.wait_for(backend.started.wait(), timeout=5)
    replacement_store = SQLiteStore(db)
    assert replacement_store.requeue_expired(spec.campaign_id, time.time() + 1) == 1
    assert await asyncio.wait_for(running, timeout=3)
    backend.release.set()
    assert await WorkerLoop(replacement_store, backend, "replacement").run_once(spec)
    assert [r.status for r in store.results(spec.campaign_id)] == ["lost", "ok"]


async def test_worker_uses_measured_vram_for_packed_device_dispatch(tmp_path: Path) -> None:
    # This workload reports a synthetic peak. No accelerator runtime is needed.
    (tmp_path / "task.py").write_text(
        "import json, os\n"
        "from pathlib import Path\n"
        "assert os.environ['CUDA_VISIBLE_DEVICES'] == '1'\n"
        "Path(os.environ['SWARM_METRICS']).write_text(json.dumps({'loss': 2}))\n"
        "Path(os.environ['SWARM_USAGE']).write_text(json.dumps({'peak_vram_mib': 20.4}))\n"
    )

    @dataclass(frozen=True, kw_only=True)
    class ConfiguredCampaign(CampaignSpec):
        options: dict[str, object]

    fields = asdict(replace(_campaign(tmp_path), resource="gpu", max_workers=3))
    fields["options"] = {"resources": {"devices_mib": {"cuda:1": 60}, "target": 0.8}}
    spec = ConfiguredCampaign(**fields)
    store = SQLiteStore(tmp_path / "queue.db")
    store.create_campaign(spec)
    for name in ("calibration", "a", "b", "c"):
        store.submit(_trial(spec, name))
    worker = from_spec(spec, store, LocalBackend(3))
    assert await worker.run_once(spec)
    assert sorted(await asyncio.gather(*(worker.run_once(spec) for _ in range(3)))) == [
        False,
        True,
        True,
    ]
    assert await worker.run_once(spec)
    assert [r.peak_vram_mib for r in store.results(spec.campaign_id)] == [20.4] * 4


def _run_doomed_worker(db: Path, spec: CampaignSpec, observed: Connection) -> None:
    # Keep the killed backend's temporary directory inside this experiment.
    tempfile.tempdir = spec.task_dir

    class ObservedStore(SQLiteStore):
        def heartbeat(self, attempt_id: str, *, cost: float = 0.0) -> bool:
            accepted = super().heartbeat(attempt_id, cost=cost)
            if accepted and not observed.closed and (Path(spec.task_dir) / "started").exists():
                observed.send(cost)
                observed.close()
            return accepted

    worker = WorkerLoop(
        ObservedStore(db), LocalBackend(1), "first", heartbeat_seconds=0.05, lost_after_seconds=0.5
    )
    asyncio.run(worker.run_once(spec))


async def test_sigkill_requeues_and_completes_trial_exactly_once(tmp_path: Path) -> None:
    (tmp_path / "task.py").write_text(
        "import os, time\n"
        "from pathlib import Path\n"
        "try:\n"
        "    Path('started').touch(exist_ok=False)\n"
        "except FileExistsError:\n"
        "    pass\n"
        "else:\n"
        "    deadline = time.monotonic() + 20\n"
        "    while not Path('release').exists() and time.monotonic() < deadline:\n"
        "        time.sleep(0.02)\n"
        "    Path('original-finished').touch()\n"
        "Path(os.environ['SWARM_METRICS']).write_text('{\"loss\": 7}')\n"
    )
    spec = replace(_campaign(tmp_path), price_per_hour=3.6)
    db = tmp_path / "queue.db"
    store = SQLiteStore(db)
    store.create_campaign(spec)
    trial = _trial(spec, "survivor")
    assert store.submit(trial)
    context = multiprocessing.get_context("spawn")
    reader, writer = context.Pipe(duplex=False)
    process = context.Process(target=_run_doomed_worker, args=(db, spec, writer))
    process.start()
    writer.close()
    try:
        assert reader.poll(20), "the first command must start and persist a priced heartbeat"
        last_observed_cost = reader.recv()
        assert last_observed_cost > 0
        process.kill()
        process.join(timeout=5)
        assert process.exitcode == -signal.SIGKILL
        replacement = WorkerLoop(
            SQLiteStore(db),
            LocalBackend(1),
            "replacement",
            heartbeat_seconds=0.05,
            lost_after_seconds=0.5,
        )
        async with asyncio.timeout(10):
            while not await replacement.run_once(spec):
                await asyncio.sleep(0.02)
        assert not await replacement.run_once(spec)
        lost, ok = store.results(spec.campaign_id)
        assert (lost.status, ok.status) == ("lost", "ok")
        assert (lost.attempt.number, ok.attempt.number) == (1, 2)
        assert lost.attempt.trial == ok.attempt.trial == trial
        assert lost.cost >= last_observed_cost
        assert ok.metrics == {"loss": 7}
        assert not store.report(replace(lost, status="ok", metrics={"loss": 99}))
        assert not store.report(ok)
        assert list(store.results(spec.campaign_id)) == [lost, ok]
        assert store.outstanding(spec.campaign_id) == 0
    finally:
        reader.close()
        if process.is_alive():
            process.kill()
        process.join(timeout=5)
        (tmp_path / "release").touch()
        if (tmp_path / "started").exists():
            async with asyncio.timeout(5):
                while not (tmp_path / "original-finished").exists():
                    await asyncio.sleep(0.02)
