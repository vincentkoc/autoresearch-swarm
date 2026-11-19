from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

from research_swarm.swarm.backends.skypilot import SkyPilotBackend
from research_swarm.swarm.interfaces import Attempt, CampaignSpec, Candidate, Trial
from research_swarm.swarm.store import SQLiteStore


def fake_sky(root: Path, monkeypatch: pytest.MonkeyPatch, first_status: str) -> str:
    monkeypatch.setenv("SKY_FIXTURE", str(root))
    monkeypatch.setenv("FIRST_STATUS", first_status)
    script = root / "sky"
    script.write_text(
        f"#!{sys.executable}\n"
        + """
import asyncio, contextlib, io, json, os, sys
from pathlib import Path
from research_swarm.swarm.backends import remote
root = Path(os.environ['SKY_FIXTURE'])
args = sys.argv[1:]
assert args[0] == 'jobs'
with (root / 'calls').open('a') as stream:
    print(json.dumps(args), file=stream)
db = root / 'jobs.json'
jobs = json.loads(db.read_text()) if db.exists() else []
action = args[1]
if action == 'launch':
    assert args[2:5] == ['-y', '-o', 'json']
    assert 'jobs.controller.autostop.down=false' in args
    assert 'jobs.controller.autostop.idle_minutes=7' in args
    task = json.loads(Path(args[-1]).read_text())
    identifier = len(jobs) + 101
    with (root / 'tasks').open('a') as stream:
        print(json.dumps(task), file=stream)
    request = json.loads(Path(next(iter(task['file_mounts'].values()))).read_text())
    status = os.environ['FIRST_STATUS'] if not jobs else 'SUCCEEDED'
    logs = io.StringIO()
    if status == 'SUCCEEDED':
        os.chdir(task['workdir'])
        with contextlib.redirect_stdout(logs):
            asyncio.run(remote.run(request))
    jobs.append(dict(job_id=identifier, job_name=task['name'], status=status, recovery_count=0,
                     labels=None, logs='(task) ' + logs.getvalue()))
    print(json.dumps({'job_ids': [identifier]}))
elif action == 'queue':
    assert args[2:] == ['--all', '-o', 'json']
    if (root / 'finish').exists() and jobs[0]['status'] == 'RUNNING':
        jobs[0]['status'] = 'FAILED_SETUP'
    print(json.dumps(jobs))
elif action == 'cancel':
    assert args[2] == '-y'
    job = next(j for j in jobs if j['job_id'] == int(args[3]))
    job['status'] = 'CANCELLED'
elif action == 'logs':
    assert args[2] == '--no-follow'
    print(next(j['logs'] for j in jobs if j['job_id'] == int(args[3])))
else:
    raise ValueError(args)
db.write_text(json.dumps(jobs))
"""
    )
    script.chmod(0o755)
    return str(script)


def campaign(root: Path) -> tuple[CampaignSpec, Trial]:
    command = (
        sys.executable,
        "-c",
        "import json, os; from pathlib import Path; "
        "trial=json.loads(Path(os.environ['SWARM_TRIAL']).read_text()); "
        "Path(os.environ['SWARM_METRICS']).write_text(json.dumps({'score': trial['seed']}))",
    )
    spec = CampaignSpec(
        campaign_id="demo",
        metric="score",
        direction="maximize",
        max_workers=2,
        task_dir=str(root),
        command=command,
        max_trials=1,
        max_attempts=2,
    )
    trial = Trial(
        trial_id="t",
        campaign_id="demo",
        candidate=Candidate(candidate_id="candidate"),
        seed=7,
        fidelity=1,
    )
    return spec, trial


def test_gc_inventory_requires_concrete_aws_regions(tmp_path: Path) -> None:
    for placement in ("aws", "aws/*/us-west-2a", "AWS/us-*/spot", "*/us-west-2"):
        with pytest.raises(ValueError, match="AWS region|named cloud"):
            SkyPilotBackend(state=tmp_path, placements=[placement], max_workers=1)
    backend = SkyPilotBackend(
        state=tmp_path,
        placements=["AWS/us-west-2/*", "aws/us-west-2", "gcp/us-central1"],
        max_workers=1,
    )
    assert backend.aws_regions == ["us-west-2"]
    assert backend.placements == ["aws/us-west-2/*", "aws/us-west-2", "gcp/us-central1"]


@pytest.mark.parametrize("first_status", ["RECOVERING", "FAILED_NO_RESOURCE", "FAILED_CONTROLLER"])
async def test_managed_job_loss_requeues_on_demand_and_preserves_trial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, first_status: str
) -> None:
    sky = fake_sky(tmp_path, monkeypatch, first_status)
    placements = ["aws/us-west-2/us-west-2a", "aws/us-east-1/us-east-1b"]
    backend = SkyPilotBackend(
        state=tmp_path,
        sky=sky,
        placements=placements,
        accelerators="L4:1",
        max_workers=1,
        idle_minutes=7,
        poll_seconds=0.01,
    )
    spec, trial = campaign(tmp_path)
    store = SQLiteStore(tmp_path / "campaign.sqlite")
    store.create_campaign(spec)
    assert store.submit(trial)
    first = store.claim("demo", "worker")
    assert first is not None
    lost = await backend.execute(spec, first)
    assert lost.status == "lost" and store.report(lost)
    retry = store.claim("demo", "worker")
    assert retry is not None and retry.trial == first.trial and retry.attempt_id != first.attempt_id
    ok = await backend.execute(spec, retry)
    assert ok.status == "ok" and ok.metrics == {"score": 7} and store.report(ok)
    assert store.claim("demo", "worker") is None
    assert [r.status for r in store.results("demo")] == ["lost", "ok"]
    tasks = [json.loads(line) for line in (tmp_path / "tasks").read_text().splitlines()]
    assert len(tasks) == 2
    for task, spot in zip(tasks, (True, False), strict=True):
        resources = task["resources"]
        assert resources["any_of"] == [{"infra": p, "use_spot": spot} for p in placements]
        assert resources["labels"]["swarm-job"] == task["name"]
        assert (
            resources["labels"]["swarm-managed"] == "true" and resources["accelerators"] == "L4:1"
        )
        assert resources["job_recovery"]["strategy"] == "EAGER_NEXT_REGION"
    assert (
        tasks[0]["resources"]["labels"]["swarm-campaign"]
        == tasks[1]["resources"]["labels"]["swarm-campaign"]
    )
    calls = [json.loads(line) for line in (tmp_path / "calls").read_text().splitlines()]
    expected_cancel = [["jobs", "cancel", "-y", "101"]] if first_status == "RECOVERING" else []
    assert [call for call in calls if call[1] == "cancel"] == expected_cancel
    assert calls[-1] == ["jobs", "logs", "--no-follow", "102"]
    assert [
        job.job_id for job in sorted(backend.inventory.jobs("demo"), key=lambda j: j.job_id or 0)
    ] == [101, 102]


async def test_reconnect_never_resubmits_same_attempt_and_restores_capacity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sky = fake_sky(tmp_path, monkeypatch, "RUNNING")

    def backend() -> SkyPilotBackend:
        instance = SkyPilotBackend(
            state=tmp_path,
            sky=sys.executable,
            placements=["aws/us-west-2"],
            idle_minutes=7,
            poll_seconds=0.05,
            max_workers=1,
        )
        # Start the generated fake CLI with the existing interpreter.
        instance.cli.argv.append(sky)
        return instance

    owner = backend()
    spec, trial = campaign(tmp_path)
    attempt = Attempt(attempt_id="attempt", trial=trial, worker_id="w", number=1, started_at=0)
    task = asyncio.create_task(owner.execute(spec, attempt))
    async with asyncio.timeout(5):
        while not (tmp_path / "jobs.json").exists():
            await asyncio.sleep(0.01)
    assert owner.free_slots(spec) == 0
    (tmp_path / "finish").touch()
    assert (await task).status == "failed"
    assert owner.free_slots(spec) == 1
    assert (await backend().execute(spec, attempt)).status == "failed"
    assert len((tmp_path / "tasks").read_text().splitlines()) == 1
