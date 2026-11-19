from __future__ import annotations

import asyncio
import sys
from dataclasses import replace
from pathlib import Path

import psutil
import pytest

from research_swarm.swarm.backends.local import LocalBackend
from research_swarm.swarm.data_stage import hydrate, prepare
from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Trial


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


async def assert_child_stopped(pid: int) -> None:
    async with asyncio.timeout(3):
        while psutil.pid_exists(pid):
            try:
                if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                    return
            except psutil.NoSuchProcess:
                return
            await asyncio.sleep(0.01)


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
        tmp_path,
        "import os; open(os.environ['SWARM_USAGE'], 'w').write("
        '\'{"input_tokens": 100, "output_tokens": 7, "peak_vram_mib": 19}\'); '
        f"open(os.environ['SWARM_METRICS'], 'w').write({output!r})",
    )
    result = await LocalBackend(1).execute(spec, make_attempt())
    assert result.status == "failed" and result.error and not result.metrics
    assert (result.input_tokens, result.output_tokens, result.peak_vram_mib) == (100, 7, 19)


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
    await assert_child_stopped(child_pid)
    assert backend.free_slots(spec) == 1


@pytest.mark.parametrize("exit_code", [0, 7])
async def test_leader_exit_finishes_trial_with_inherited_output_pipes(
    tmp_path: Path, exit_code: int
) -> None:
    program = """
import os, subprocess, sys
from pathlib import Path
helper = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
Path('helper.pid').write_text(str(helper.pid))
Path(os.environ['SWARM_METRICS']).write_text('{"gain": 2}')
print('trial finished')
print('last diagnostic', file=sys.stderr)
"""
    spec = make_spec(tmp_path, program + f"\nsys.exit({exit_code})")
    backend = LocalBackend(1)
    result = await backend.execute(spec, make_attempt())
    assert result.status == ("failed" if exit_code else "ok"), result.error
    assert result.error == (f"command exited with status {exit_code}" if exit_code else None)
    assert result.metrics == ({} if exit_code else {"gain": 2})
    assert result.logs == ("trial finished\n", "last diagnostic\n")
    assert backend.free_slots(spec) == 1
    pid = int((tmp_path / "helper.pid").read_text())
    await assert_child_stopped(pid)


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


async def test_local_dispatch_rejects_changed_staged_input(tmp_path: Path) -> None:
    source, volume, nvme = (tmp_path / name for name in ("source", "region", "nvme"))
    source.mkdir()
    (source / "points").write_bytes(b"13")
    prepare(source, volume, "test", "v1")
    data = hydrate(volume, nvme, "test", "v1")
    backend = LocalBackend(1, data_dir=data)
    spec = make_spec(
        tmp_path,
        "import os; from pathlib import Path; Path('started').touch(); "
        "Path(os.environ['SWARM_METRICS']).write_text('{\"gain\":1}')",
    )
    (data / "points").chmod(0o644)
    (data / "points").write_bytes(b"99")
    (data / "points").chmod(0o444)
    result = await backend.execute(spec, make_attempt())
    assert result.status == "failed" and "differs" in (result.error or "")
    assert not (tmp_path / "started").exists() and backend.free_slots(spec) == 1


async def test_ordinary_local_dataset_keeps_its_permissions(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    (tmp_path / "stage.json").write_text('{"owner":"another-tool"}')
    initial_mode = data.stat().st_mode
    spec = make_spec(
        tmp_path,
        "import os; from pathlib import Path; "
        "Path(os.environ['SWARM_METRICS']).write_text('{\"gain\":1}')",
    )
    result = await LocalBackend(1, data_dir=data).execute(spec, make_attempt())
    assert result.status == "ok" and data.stat().st_mode == initial_mode
