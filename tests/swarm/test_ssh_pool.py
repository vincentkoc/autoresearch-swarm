from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import pytest

from research_swarm.swarm.backends.keeper import active
from research_swarm.swarm.backends.ssh_pool import BootData, PoolHost, SSHPoolBackend
from research_swarm.swarm.data_stage import prepare
from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Trial


def executable(directory: Path, name: str, body: str) -> str:
    path = directory / name
    path.write_text(f"#!{sys.executable}\n" + body)
    path.chmod(0o755)
    return str(path)


async def test_pool_resumes_protected_host_and_runs_again_after_failed_trial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FIXTURE", str(tmp_path))
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ["PATH"])
    aws = executable(
        tmp_path,
        "aws",
        """
import json, os, sys
from pathlib import Path
root = Path(os.environ["FIXTURE"])
with (root / "provider.log").open("a") as f:
    print(json.dumps(sys.argv[1:]), file=f)
if sys.argv[2] == "describe-instances":
    print(json.dumps("running" if (root / "up").exists() else "stopped"))
if sys.argv[2] == "start-instances":
    (root / "up").touch()
""",
    )
    ssh = executable(
        tmp_path,
        "ssh",
        """
import asyncio, json, os, sys
from pathlib import Path
from research_swarm.swarm.backends import ssh_pool
root = Path(os.environ["FIXTURE"])
assert sys.argv[-2] == "demo-worker"
assert sys.argv[-1] == "python3 -m research_swarm.swarm.backends.ssh_pool run"
def fake_bind(data, mountpoint):
    mountpoint.mkdir(parents=True, exist_ok=True)
    if not (mountpoint / "points").exists():
        (mountpoint / "points").symlink_to(data / "points")
ssh_pool.remote.mount_readonly = fake_bind
asyncio.run(ssh_pool.run_on_host(json.load(sys.stdin), root / "activity"))
""",
    )
    executable(
        tmp_path,
        "systemctl",
        """
import sys
assert sys.argv[1:] == ["is-active", "--quiet", "research-swarm-keeper.service"]
""",
    )
    (tmp_path / "activity").touch()
    source = tmp_path / "source"
    source.mkdir()
    (source / "points").write_text("13")
    volume, nvme = tmp_path / "region", tmp_path / "nvme"
    prepare(source, volume, "demo", "v1")
    pool = SSHPoolBackend(
        [
            PoolHost(
                target="demo-worker",
                instance_id="synthetic-instance",
                region="test-region",
                task_dir=str(tmp_path),
                device="cpu",
                data=BootData(
                    revision="v1",
                    volume=str(volume),
                    nvme=str(nvme),
                    mount_root=str(tmp_path / "mounted"),
                ),
            )
        ],
        aws=aws,
        ssh=ssh,
    )
    script = """
import json, os
from pathlib import Path
points = int((Path(os.environ["SWARM_DATA"]) / "points").read_text())
Path(os.environ["SWARM_METRICS"]).write_text(json.dumps({"score": points}))
"""
    trial = Trial(
        trial_id="trial",
        campaign_id="demo",
        seed=0,
        fidelity=1,
        candidate=Candidate(candidate_id="one"),
    )
    attempt = Attempt(attempt_id="attempt", trial=trial, worker_id="worker", number=1, started_at=0)
    spec = CampaignSpec(
        campaign_id="demo",
        direction="maximize",
        metric="score",
        command=(sys.executable, "-c", script),
        max_workers=3,
        max_trials=1,
    )
    result = await pool.execute(spec, attempt)
    assert result.status == "ok" and result.metrics == {"score": 13}
    assert (nvme / "demo/v1/data/points").read_text() == "13"
    assert pool.free_slots(spec) == 1

    failed = await pool.execute(replace(spec, command=(sys.executable, "-c", "exit(4)")), attempt)
    assert failed.status == "failed"
    calls = [json.loads(line) for line in (tmp_path / "provider.log").read_text().splitlines()]
    assert [call[1] for call in calls[:5]] == [
        "modify-instance-attribute",
        "modify-instance-attribute",
        "describe-instances",
        "start-instances",
        "wait",
    ]
    assert "--disable-api-termination" in calls[0] and "Value=stop" in calls[1]
    await pool.stop()
    calls = [json.loads(line) for line in (tmp_path / "provider.log").read_text().splitlines()]
    assert [call[1] for call in calls[5:]] == [
        "modify-instance-attribute",
        "modify-instance-attribute",
        "describe-instances",
        "stop-instances",
    ]
    assert pool.free_slots(spec) == 0
    assert (await pool.execute(spec, attempt)).status == "lost"


async def test_protection_failure_blocks_ssh_dispatch(tmp_path: Path) -> None:
    aws = executable(tmp_path, "aws", "raise SystemExit(4)")
    ssh = executable(tmp_path, "ssh", f"open({str(tmp_path / 'unexpected')!r}, 'w').close()")
    pool = SSHPoolBackend(
        [PoolHost(target="pool", instance_id="item", region="test", task_dir=".")], aws=aws, ssh=ssh
    )
    spec = CampaignSpec(
        campaign_id="demo",
        metric="score",
        direction="maximize",
        command=("false",),
        max_workers=1,
        max_trials=1,
    )
    trial = Trial(
        trial_id="trial",
        campaign_id="demo",
        candidate=Candidate(candidate_id="c"),
        seed=0,
        fidelity=1,
    )
    attempt = Attempt(attempt_id="one", trial=trial, worker_id="w", number=1, started_at=0)
    result = await pool.execute(spec, attempt)
    assert result.status == "lost" and not (tmp_path / "unexpected").exists()


def test_detached_keeper_stops_only_after_activity_finished(tmp_path: Path) -> None:
    lockfile = tmp_path / "activity"
    commands = tmp_path / "commands"
    echo = shutil.which("echo", path=os.defpath)
    assert echo is not None
    (tmp_path / "systemctl").symlink_to(echo)
    with commands.open("wb") as output:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "research_swarm.swarm.backends.keeper",
                "--idle-seconds",
                "0.3",
                "--activity",
                str(lockfile),
            ],
            env={**os.environ, "PATH": str(tmp_path)},
            stdout=output,
        )
    try:
        deadline = time.monotonic() + 5
        while not lockfile.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        with active(lockfile):
            time.sleep(0.5)
            assert commands.read_bytes() == b"" and process.poll() is None
        assert process.wait(timeout=5) == 0
        assert commands.read_bytes() == b"poweroff\n"
        with pytest.raises(RuntimeError, match="stopping"), active(lockfile):
            pytest.fail("stopping host admitted work")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
