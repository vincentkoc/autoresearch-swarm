"""Run one campaign through its YAML backend, durable retry queue and scoped GC."""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from research_swarm.swarm.command import campaign_backend
from research_swarm.swarm.data_stage import hydrate, prepare, read_stage
from research_swarm.swarm.interfaces import Candidate, Trial
from research_swarm.swarm.spec import load_spec
from research_swarm.swarm.store import SQLiteStore
from research_swarm.swarm.worker import WorkerLoop
from tests.swarm.test_campaign_gc import fake_gc_aws, gc
from tests.swarm.test_skypilot import fake_sky
from tests.swarm.test_ssh_pool import fake_pool_commands


def run_campaign(spec: Path, db: Path) -> dict[str, Any]:
    run = subprocess.run(
        [sys.executable, "-m", "research_swarm", "swarm", "run", str(spec), "--db", str(db)],
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert run.returncode == 0, run.stderr + run.stdout
    return json.loads(run.stdout)


def campaign_spec(tmp_path: Path, backend: str, options: dict[str, Any]) -> Path:
    (tmp_path / "trial.py").write_text(
        "import errno, json, os\nfrom pathlib import Path\n"
        "assert os.environ['SWARM_DEVICE'] == 'cpu'\n"
        "data = Path(os.environ['SWARM_DATA'])\n"
        "assert (data.stat().st_mode | (data / 'points').stat().st_mode) & 0o222 == 0\n"
        "if os.geteuid() != 0:\n"
        "    try:\n"
        "        (data / 'points').open('ab').close()\n"
        "    except OSError as error:\n"
        "        assert error.errno in (errno.EACCES, errno.EPERM, errno.EROFS)\n"
        "    else:\n"
        "        raise AssertionError('worker opened staged input for writing')\n"
        "points = int((data / 'points').read_text())\n"
        "with Path('executions').open('a') as f: print(os.environ['SWARM_ATTEMPT_ID'], file=f)\n"
        "Path(os.environ['SWARM_METRICS']).write_text(json.dumps({'score': points}))\n"
    )
    spec = tmp_path / "campaign.yaml"
    config = dict(
        campaign_id="demo",
        evaluation_mode="smoke",
        metric="score",
        direction="maximize",
        backend=f"research_swarm.swarm.backends.{backend}:from_spec",
        options={"backend": options},
        command=[sys.executable, "trial.py"],
        task_dir=str(tmp_path),
        search_space={"recipe": [1]},
        max_trials=1,
        max_workers=1,
        max_attempts=2,
    )
    spec.write_text(yaml.safe_dump(config))
    return spec


def ssh_options(
    root: Path, monkeypatch: pytest.MonkeyPatch, data: dict[str, str]
) -> dict[str, Any]:
    aws, ssh = fake_pool_commands(root, monkeypatch)
    host = dict(
        target="demo-worker",
        instance_id="pool",
        region="test-region",
        task_dir=str(root),
        device="cpu",
        data=data,
    )
    return {"hosts": [host], "ssh": ssh, "aws": aws}


def sky_resources(tmp_path: Path) -> set[str]:
    tasks = [json.loads(line) for line in (tmp_path / "tasks").read_text().splitlines()]
    assert [task["resources"]["any_of"][0]["use_spot"] for task in tasks] == [True, False]
    calls = [json.loads(line) for line in (tmp_path / "calls").read_text().splitlines()]
    assert [call for call in calls if call[1] == "cancel"] == [["jobs", "cancel", "-y", "101"]]
    # Provider inventory inherits labels from the actual submitted retry.
    tags = [{"Key": k, "Value": v} for k, v in tasks[1]["resources"]["labels"].items()]
    (tmp_path / "ec2.json").write_text(
        json.dumps(
            {
                "Instances": [
                    {"InstanceId": "owned", "State": {"Name": "running"}, "Tags": tags},
                    {"InstanceId": "pool", "State": {"Name": "running"}},
                ],
                "Volumes": [{"VolumeId": "disk", "State": "available", "Tags": tags}],
            }
        )
    )
    (tmp_path / "allow-delete").touch()
    return {f"sky:{task['name']}" for task in tasks}


@pytest.mark.parametrize("backend", ["local", "ssh_pool", "skypilot"])
def test_backend_campaign_qualification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, backend: str
) -> None:
    source, volume, nvme = (tmp_path / name for name in ("source", "region", "nvme"))
    source.mkdir()
    (source / "points").write_text("13")
    stage = prepare(source, volume, "demo", "v1")
    prepare(source, volume, "demo", "keep")
    prepare(source, volume, "other", "v1")
    (source / "points").write_text("99")

    data = dict(revision="v1", volume=str(volume), nvme=str(nvme), mount_root=str(tmp_path / "mnt"))
    # Missing executables make accidental provider calls from local/SSH GC fail closed.
    sky = aws = str(tmp_path / "unused-provider")
    options: dict[str, Any]
    if backend == "local":
        options = {"device": "cpu", "data_dir": str(hydrate(volume, nvme, "demo", "v1"))}
    elif backend == "ssh_pool":
        (tmp_path / "drop-next-ssh").touch()
        options = ssh_options(tmp_path, monkeypatch, data)
    else:
        sky = fake_sky(tmp_path, monkeypatch, "RECOVERING")
        aws = fake_gc_aws(tmp_path)
        options = dict(
            state=str(tmp_path),
            sky=sky,
            placements=["aws/test-region"],
            device="cpu",
            idle_minutes=7,
            poll_seconds=0.01,
            data=data,
        )

    spec = campaign_spec(tmp_path, backend, options)
    db = tmp_path / "campaign.sqlite"
    summary = run_campaign(spec, db)
    results = SQLiteStore(db).results("demo")
    assert [r.status for r in results] == (["ok"] if backend == "local" else ["lost", "ok"])
    winner = results[-1].attempt
    assert summary["trials"] == 1 and summary["attempts"] == len(results)
    assert summary["selected"]["score"] == 13
    assert summary["selected"]["result"]["attempt"]["attempt_id"] == winner.attempt_id
    if backend != "local":
        lost = results[0].attempt
        assert lost.trial == winner.trial and lost.attempt_id != winner.attempt_id
        assert (lost.number, winner.number) == (1, 2)
    assert read_stage(nvme / "demo/v1") == stage
    assert run_campaign(spec, db) == summary
    assert SQLiteStore(db).results("demo") == results
    assert (tmp_path / "executions").read_text().splitlines() == [winner.attempt_id]

    jobs: set[str] = set()
    cloud: set[str] = set()
    if backend == "skypilot":
        jobs = sky_resources(tmp_path)
        cloud = {"owned", "disk"}
    elif backend == "ssh_pool":
        calls = [json.loads(line) for line in (tmp_path / "provider.log").read_text().splitlines()]
        assert "exited with status 255" in (results[0].error or "")
        assert "--disable-api-termination" in calls[0] and "Value=stop" in calls[1]
        assert [call[1] for call in calls].count("start-instances") == 1
        assert all(call[1] != "terminate-instances" for call in calls)

    args = ("--volume", str(volume), "--keep", "data:keep", "--sky", sky, "--aws", aws)
    code, listed = gc(tmp_path, *args)
    assert code == 0 and set(listed) == jobs | cloud | {"data:keep", "data:v1"}
    assert listed["data:v1"]["action"] == "listed" and read_stage(volume / "demo/v1") == stage
    code, applied = gc(tmp_path, *args, "--apply")
    assert code == 0 and all(applied[k]["action"] == "removed" for k in cloud | {"data:v1"})
    assert all(applied[k]["action"] == "terminal" for k in jobs)
    assert applied["data:keep"]["action"] == "kept" and not (volume / "demo/v1").exists()
    assert all((volume / p / "data/points").read_text() == "13" for p in ("demo/keep", "other/v1"))
    code, replay = gc(tmp_path, *args, "--apply")
    assert code == 0 and set(replay) == jobs | {"data:keep"}
    if backend == "skypilot":
        owned, pool = json.loads((tmp_path / "ec2.json").read_text())["Instances"]
        assert [owned["State"]["Name"], pool["State"]["Name"]] == ["terminated", "running"]


@pytest.mark.parametrize(("kind", "outcome"), [("local", "failed"), ("ssh_pool", "lost")])
async def test_worker_retries_corrupt_cache_once_and_deduplicates_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str, outcome: str
) -> None:
    source, volume, nvme = (tmp_path / name for name in ("source", "region", "nvme"))
    source.mkdir()
    (source / "points").write_bytes(b"13")
    prepare(source, volume, "demo", "v1")
    cache = hydrate(volume, nvme, "demo", "v1")
    options: dict[str, Any] = {"device": "cpu", "data_dir": str(cache)}
    if kind == "ssh_pool":
        options = ssh_options(
            tmp_path,
            monkeypatch,
            dict(
                revision="v1", volume=str(volume), nvme=str(nvme), mount_root=str(tmp_path / "mnt")
            ),
        )
    spec = load_spec(campaign_spec(tmp_path, kind, options))
    backend = campaign_backend(spec)
    db = tmp_path / "queue.sqlite"
    store = SQLiteStore(db)
    store.create_campaign(spec)
    trial = Trial(
        trial_id="one",
        campaign_id="demo",
        candidate=Candidate(candidate_id="one"),
        seed=3,
        fidelity=1,
    )
    assert store.submit(trial)
    worker = WorkerLoop(store, backend, "worker")

    points = cache / "points"
    points.chmod(0o644)
    points.write_bytes(b"99")
    points.chmod(0o444)
    assert await worker.run_once(spec)
    (first,) = store.results("demo")
    assert first.status == outcome and "differs" in (first.error or "")
    assert not (tmp_path / "executions").exists() and backend.free_slots(spec) == 1
    replay = SQLiteStore(db)
    assert not replay.report(first) and replay.queue_depth("demo") == (1, 0)

    # Restore this synthetic cache so the same resident worker can complete its retry.
    points.chmod(0o644)
    points.write_bytes(b"13")
    points.chmod(0o444)
    assert await worker.run_once(spec)
    failed, accepted = store.results("demo")
    assert failed == first and accepted.status == "ok" and accepted.metrics == {"score": 13}
    assert failed.attempt.trial == accepted.attempt.trial == trial
    assert (failed.attempt.number, accepted.attempt.number) == (1, 2)
    assert failed.attempt.attempt_id != accepted.attempt.attempt_id
    assert not replay.report(accepted) and not replay.report(first)
    assert replay.results("demo") == [failed, accepted] and replay.queue_depth("demo") == (0, 0)
    assert not await worker.run_once(spec) and backend.free_slots(spec) == 1
    assert (tmp_path / "executions").read_text().splitlines() == [accepted.attempt.attempt_id]
