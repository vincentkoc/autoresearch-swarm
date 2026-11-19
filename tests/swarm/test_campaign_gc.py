from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from research_swarm.swarm.backends.ec2_gc import EC2, CloudResource
from research_swarm.swarm.backends.jobs import Inventory
from research_swarm.swarm.backends.skypilot import SkyCLI
from research_swarm.swarm.campaign_gc import _collect_job, _stages
from research_swarm.swarm.data_stage import prepare


def gc(state: Path, *args: str) -> tuple[int, dict[str, dict[str, Any]]]:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "research_swarm",
            "--state-dir",
            str(state),
            "campaign",
            "gc",
            "demo",
            *args,
        ],
        capture_output=True,
        env={**os.environ, "FIXTURE": str(state)},
    )
    data = json.loads(result.stdout)
    assert data["campaign"] == "demo", result.stderr
    return result.returncode, {entry["id"]: entry for entry in data["resources"]}


def test_stage_gc_lists_preserves_keep_and_ignores_foreign_roots(tmp_path: Path) -> None:
    source = tmp_path / "input"
    source.mkdir()
    (source / "sample").write_text("v1")
    volume = tmp_path / "volume"
    for campaign, revision in [("demo", "keep"), ("demo", "old"), ("another", "old")]:
        prepare(source, volume, campaign, revision)
    foreign = volume / "demo" / "foreign"
    foreign.mkdir()
    (foreign / "data").mkdir()
    (foreign / "stage.json").write_text('{"owner": "someone-else"}')
    (volume / "demo" / "link").symlink_to(volume / "another" / "old")
    options = ("--volume", str(volume), "--keep", "data:keep")
    code, entries = gc(tmp_path, *options)
    assert code == 0 and set(entries) == {"data:old", "data:keep"}
    assert (volume / "demo/old/data/sample").exists()
    code, entries = gc(tmp_path, *options, "--apply")
    assert code == 0 and entries["data:old"]["action"] == "removed"
    assert entries["data:keep"]["action"] == "kept"
    assert not (volume / "demo/old").exists()
    assert (volume / "another/old/data/sample").exists() and foreign.is_dir()
    assert gc(tmp_path, *options, "--apply")[0] == 0


def test_interrupted_stage_deletion_can_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, volume = tmp_path / "source", tmp_path / "volume"
    source.mkdir()
    prepare(source, volume, "demo", "old")
    with monkeypatch.context() as patch:

        def interrupt(path: Path) -> None:
            raise OSError("interrupted after data removal")

        patch.setattr(Path, "rmdir", interrupt)
        assert _stages("demo", [volume], set(), True)[0].action.startswith("error:")
    assert not (volume / "demo/old/data").exists()
    with pytest.raises(ValueError, match="cleanup"):
        prepare(source, volume, "demo", "old")
    assert gc(tmp_path, "--volume", str(volume), "--apply")[1]["data:old"]["action"] == "removed"
    assert prepare(source, volume, "demo", "old").files == 0


def fake_cli(state: Path, name: str, program: str) -> str:
    exe = state / name
    exe.write_text(
        f"#!{sys.executable}\n"
        + """
import json, os, sys
from pathlib import Path
root = Path(os.environ['FIXTURE'])
args = sys.argv[1:]
with (root / 'calls').open('a') as f:
    print(json.dumps(args), file=f)
"""
        + program
    )
    exe.chmod(0o755)
    return str(exe)


def fake_gc_aws(tmp_path: Path) -> str:
    return fake_cli(
        tmp_path,
        "aws",
        """
path = root / 'ec2.json'
data = json.loads(path.read_text())
assert 'test-region' in args
op = args[1]
if op == 'describe-instances':
    print(json.dumps({'Reservations': [{'Instances': data['Instances']}]}))
elif op == 'describe-volumes':
    vols = data['Volumes']
    if 'Name=volume-id,Values=retagged' in args:
        for v in vols:
            if v['VolumeId'] == 'retagged':
                v['Tags'] = []
        path.write_text(json.dumps(data))
    print(json.dumps({'Volumes': vols}))
elif op == 'terminate-instances':
    assert args[args.index('--instance-ids') + 1] == 'owned'
    data['Instances'][0]['State']['Name'] = 'terminated'
    path.write_text(json.dumps(data))
elif op == 'wait':
    assert args[2:5] == ['instance-terminated', '--instance-ids', 'owned']
elif op == 'delete-volume':
    assert args[args.index('--volume-id') + 1] == 'disk'
    if not (root / 'allow-delete').exists():
        sys.exit(2)
    data['Volumes'] = [v for v in data['Volumes'] if v['VolumeId'] != 'disk']
    path.write_text(json.dumps(data))
else:
    raise AssertionError(args)
""",
    )


def test_gc_fences_exact_job_and_rechecks_tags_before_each_delete(tmp_path: Path) -> None:
    registry = Inventory(tmp_path)
    job, _ = registry.reserve("demo", "t", "a", ["test-region"])
    job.job_id = 12
    registry.save(job)
    other, _ = registry.reserve("other", "t", "a", ["test-region"])
    tags = [{"Key": k, "Value": v} for k, v in job.labels.items()]
    other_tags = [{"Key": k, "Value": v} for k, v in other.labels.items()]
    rows = {
        "Instances": [
            {"InstanceId": "owned", "Tags": tags, "State": {"Name": "running"}},
            {"InstanceId": "borrowed", "State": {"Name": "running"}},
            {"InstanceId": "other", "Tags": other_tags, "State": {"Name": "running"}},
        ],
        "Volumes": [
            {"VolumeId": v, "State": "available", "Tags": tags} for v in ["disk", "retagged"]
        ],
    }
    db = tmp_path / "ec2.json"
    db.write_text(json.dumps(rows))
    (tmp_path / "sky.json").write_text(
        json.dumps(
            [
                {"job_id": 12, "job_name": job.name, "status": "RUNNING"},
                {"job_id": 21, "job_name": other.name, "status": "RUNNING"},
            ]
        )
    )
    sky = fake_cli(
        tmp_path,
        "sky",
        """
path = root / 'sky.json'
jobs = json.loads(path.read_text())
if args == ['jobs', 'queue', '--all', '-o', 'json']:
    print(json.dumps(jobs))
elif args == ['jobs', 'cancel', '-y', '12']:
    jobs[0]['status'] = 'CANCELLED'
    path.write_text(json.dumps(jobs))
else:
    raise AssertionError(args)
""",
    )
    aws = fake_gc_aws(tmp_path)
    args = ("--sky", sky, "--aws", aws)
    code, entries = gc(tmp_path, *args)
    assert code == 0 and set(entries) == {f"sky:{job.name}", "owned", "disk", "retagged"}
    code, entries = gc(tmp_path, *args, "--keep", "disk", "--apply")
    assert code == 0 and all(row["action"] == "kept" for row in entries.values())
    calls = [json.loads(line) for line in (tmp_path / "calls").read_text().splitlines()]
    assert all(call[1] in {"queue", "describe-instances", "describe-volumes"} for call in calls)
    code, entries = gc(tmp_path, *args, "--apply")
    assert code == 3 and entries["owned"]["action"] == "removed"
    assert entries["disk"]["action"].startswith("error:")
    assert entries["retagged"]["action"] == "absent-or-retagged"
    (tmp_path / "allow-delete").touch()
    code, entries = gc(tmp_path, *args, "--apply")
    assert code == 0 and entries["disk"]["action"] == "removed"
    final = json.loads(db.read_bytes())
    assert final["Volumes"][0]["VolumeId"] == "retagged" and len(final["Volumes"]) == 1
    assert [i["State"]["Name"] for i in final["Instances"]] == ["terminated", "running", "running"]
    assert json.loads((tmp_path / "sky.json").read_text())[1]["status"] == "RUNNING"


def test_job_id_collision_leaves_foreign_job_untouched(tmp_path: Path) -> None:
    registry = Inventory(tmp_path)
    job, _ = registry.reserve("demo", "t", "a", [])
    job.job_id = 12
    registry.save(job)
    sky = fake_cli(
        tmp_path,
        "sky",
        """
assert args == ['jobs', 'queue', '--all', '-o', 'json']
print('[{"job_id":12,"job_name":"foreign","status":"RUNNING"}]')
""",
    )
    code, entries = gc(tmp_path, "--sky", sky, "--apply")
    assert code == 3 and entries[f"sky:{job.name}"]["action"] == "unresolved"


async def test_newly_attached_kept_volume_blocks_managed_job_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inventory = Inventory(tmp_path)
    job, _ = inventory.reserve("demo", "trial", "attempt", ["region"])
    row = {"job_name": job.name, "job_id": 3, "status": "RUNNING"}
    disk = CloudResource("volume", "kept-disk", "region", "in-use", job)
    instance = CloudResource("instance", "compute", "region", "running", job, ("kept-disk",))

    async def refreshed(*args: Any) -> list[CloudResource]:
        return [instance, disk]

    monkeypatch.setattr("research_swarm.swarm.campaign_gc._cloud_resources", refreshed)
    entries = await _collect_job(
        job,
        [instance],
        SkyCLI(str(tmp_path / "must-not-execute")),
        EC2("unused"),
        [row],
        {"kept-disk"},
        True,
        inventory,
    )
    assert {e.id for e in entries} == {f"sky:{job.name}", "compute", "kept-disk"}
    assert all(e.action == "kept" for e in entries)
