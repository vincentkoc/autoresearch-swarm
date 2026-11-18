from __future__ import annotations

import asyncio
import sys
from dataclasses import replace
from pathlib import Path

import psutil
import pytest
from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Trial

from research_swarm.swarm.backends.local import LocalBackend


def make_attempt(number: int = 1) -> Attempt:
    trial = Trial(
        trial_id="trial",
        candidate=Candidate(candidate_id="one", params={"x": 3}),
        campaign_id="test",
        seed=11,
        fidelity=1,
    )
    return Attempt(
        attempt_id=str(number), trial=trial, worker_id="worker", number=number, started_at=0
    )


def make_spec(tmp_path: Path, program: str) -> CampaignSpec:
    return CampaignSpec(
        campaign_id="test",
        metric="gain",
        direction="maximize",
        command=(sys.executable, "-c", program),
        task_dir=str(tmp_path),
        max_trials=5,
        max_workers=2,
        trial_timeout_seconds=5,
    )


@pytest.mark.parametrize(
    ("device", "visible", "expected"),
    [("cpu", "", "cpu"), ("mps", "", "mps"), ("cuda:2", "2", "cuda:0")],
)
async def test_trial_and_device_binding(
    tmp_path: Path, device: str, visible: str, expected: str
) -> None:
    program = """
import json, os
from pathlib import Path
trial = json.loads(Path(os.environ['SWARM_TRIAL']).read_text())
print(os.environ['SWARM_DEVICE'], repr(os.environ['CUDA_VISIBLE_DEVICES']))
assert Path.cwd() == Path(os.environ['SWARM_DATA'])
gain = trial['seed'] + trial['candidate']['params']['x']
Path(os.environ['SWARM_METRICS']).write_text(json.dumps({'gain': gain}))
usage = {'input_tokens': 10, 'output_tokens': 6, 'peak_vram_mib': 93.5}
Path(os.environ['SWARM_USAGE']).write_text(json.dumps(usage))
"""
    backend = LocalBackend(3, data_dir=tmp_path).for_device(device)
    result = await backend.execute(make_spec(tmp_path, program), make_attempt())
    assert result.status == "ok" and result.metrics == {"gain": 14}
    assert result.logs == (f"{expected} {visible!r}\n",)
    assert (result.input_tokens, result.output_tokens, result.peak_vram_mib) == (10, 6, 93.5)


@pytest.mark.parametrize(
    "output", ['{"wrong": 1}', '{"gain": "bad"}', '{"gain": NaN}', '{"gain": false}']
)
async def test_invalid_metrics_are_failed_attempts(tmp_path: Path, output: str) -> None:
    spec = make_spec(
        tmp_path, f"import os; open(os.environ['SWARM_METRICS'], 'w').write({output!r})"
    )
    result = await LocalBackend(1).execute(spec, make_attempt())
    assert result.status == "failed" and result.error and not result.metrics


@pytest.mark.parametrize("cancel", [True, False])
async def test_timeout_and_cancel_kill_descendants_and_free_slot(
    tmp_path: Path, cancel: bool
) -> None:
    program = """
import subprocess, sys, time
from pathlib import Path
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
Path('child.pid').write_text(str(child.pid))
time.sleep(60)
"""
    backend = LocalBackend(1)
    spec = replace(make_spec(tmp_path, program), trial_timeout_seconds=5 if cancel else 0.8)
    task = asyncio.create_task(backend.for_device("mps").execute(spec, make_attempt()))
    async with asyncio.timeout(3):
        while not (tmp_path / "child.pid").exists():
            await asyncio.sleep(0.01)
    child_pid = int((tmp_path / "child.pid").read_text())
    assert backend.free_slots(spec) == 0
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        result = await task
        assert result.status == "failed" and "timed out" in (result.error or "")
    async with asyncio.timeout(3):
        while psutil.pid_exists(child_pid):
            try:
                if psutil.Process(child_pid).status() == psutil.STATUS_ZOMBIE:
                    break
            except psutil.NoSuchProcess:
                break
            await asyncio.sleep(0.01)
    assert backend.free_slots(spec) == 1


async def test_crash_does_not_stop_sibling_and_output_is_bounded(tmp_path: Path) -> None:
    backend = LocalBackend(2)
    bad = make_spec(tmp_path, "import sys; print('x' * 64000); sys.exit(9)")
    good = make_spec(
        tmp_path, "import os; open(os.environ['SWARM_METRICS'], 'w').write('{\"gain\": 1}')"
    )
    failed, ok = await asyncio.gather(
        backend.execute(bad, make_attempt()), backend.execute(good, make_attempt(2))
    )
    assert failed.status == "failed" and failed.error == "command exited with status 9"
    assert 0 < len(failed.logs[0]) <= 16384
    assert ok.status == "ok" and backend.free_slots(good) == 2
