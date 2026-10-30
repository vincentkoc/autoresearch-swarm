from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.api.mcp import make_mcp
from research_swarm.client import AsyncResearchClient, LocalTransport, ResearchClient
from research_swarm.control import authority_clock
from research_swarm.control.authority_clock import process_authority_clock
from research_swarm.control.models import TrialData
from research_swarm.control.worker_artifacts import WorkerArtifactRead, WorkerArtifacts
from research_swarm.control.worker_outputs import WorkerOutputIntent
from research_swarm.control.workers import AttemptData, ClaimJob, EnrollWorker, WorkerData
from research_swarm.domain.clocks import ClockChallenge, ClockPolicy
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.workers.clock_estimates import ClockSampler
from research_swarm.workers.clock_signatures import ClockVerifier
from research_swarm.workers.clocks import ClockReading, native_clock
from research_swarm.workers.local import LocalWorker
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_worker_queries import claim, query, read, scoped_worker


class SimulatedClock:
    def __init__(self) -> None:
        self.profile = native_clock().profile
        self.reading = native_clock().sample()

    def sample(self) -> ClockReading:
        return self.reading


@pytest.fixture
def simulated(monkeypatch: pytest.MonkeyPatch) -> SimulatedClock:
    clock = SimulatedClock()
    monkeypatch.setattr(authority_clock, "native_clock", lambda: clock)
    return clock


def timed_worker(pipeline: Pipeline) -> tuple[ResearchClient, WorkerData, str]:
    runtime = pipeline.runtime
    actor = Principal(
        id=new_id(),
        workspace_id=runtime.identity.workspace_id,
        scopes=frozenset({"worker.query", "worker.claim", "worker.inputs", "worker.outputs"}),
    )
    token = runtime.credentials.issue(runtime.principal(), actor).token
    request = EnrollWorker(
        worker_id=new_id(),
        session_id=new_id(),
        principal_id=actor.id,
        name="Synthetic timed worker",
        profiles=pipeline.worker.worker.profiles,
        project_ids=(pipeline.trial["data"]["spec"]["project_id"],),
        clock_mode="bounded-continuous-v1",
    )
    row = pipeline.client.command("worker.enroll", request, idempotency_key=new_id())["result"]
    worker = WorkerData.model_validate(row["data"])
    client = ResearchClient(LocalTransport(runtime, lambda: runtime.credentials.resolve(token)))
    return client, worker, token


def challenge(worker: WorkerData) -> ClockChallenge:
    return ClockChallenge(
        worker_id=worker.binding.worker_id,
        session_id=worker.binding.worker_session_id,
        nonce=new_id(),
    )


def input_request(worker: WorkerData, row: dict[str, Any]) -> WorkerArtifactRead:
    return WorkerArtifactRead(
        worker_id=worker.binding.worker_id,
        session_id=worker.binding.worker_session_id,
        attempt_id=row["id"],
        artifact_id=row["data"]["source_artifact"]["id"],
    )


def test_reopened_runtime_shares_signed_probe_and_atomic_assignment_epoch(
    pipeline: Pipeline,
) -> None:
    client, worker, token = timed_worker(pipeline)
    assert worker.assignment_authority is not None
    sampler = ClockSampler(worker.assignment_authority)
    pending = sampler.begin()
    estimate = pending.complete(client.worker_clock(pending.challenge))
    reopened = LocalRuntime.open(pipeline.runtime.root)
    other = ResearchClient(LocalTransport(reopened, lambda: reopened.credentials.resolve(token)))
    with ThreadPoolExecutor(max_workers=4) as pool:
        replies = list(pool.map(lambda _: other.worker_clock(challenge(worker)), range(4)))
    assert {reply.payload.clock_epoch for reply in replies} == {
        estimate.verified.reply.payload.clock_epoch
    }
    row = claim(other, worker, other.worker_list(query(worker))["items"][0])
    attempt = AttemptData.model_validate(row["data"])
    assert attempt.assignment_time is not None and attempt.signed_assignment is not None
    ClockVerifier(worker.assignment_authority).binding(
        attempt.assignment_time, attempt.signed_assignment
    )
    assert (
        attempt.assignment_time.payload.clock_epoch == estimate.verified.reply.payload.clock_epoch
    )
    assert other.worker_input(input_request(worker, row)) == attempt.source_artifact
    assert client.worker_get(read(worker, "attempt", row["id"])) == row


@pytest.mark.parametrize("fault", ["wall_forward", "wall_backward", "suspend", "counter"])
def test_invalid_clock_cannot_recover_through_runtime_reopen_or_policy_change(
    pipeline: Pipeline,
    simulated: SimulatedClock,
    fault: str,
) -> None:
    client, worker, token = timed_worker(pipeline)
    before = client.worker_clock(challenge(worker))
    old = simulated.reading
    changes = {
        "wall_forward": {"wall_ns": old.wall_ns + 2_000_000_000},
        "wall_backward": {"wall_ns": old.wall_ns - 2_000_000_000},
        "suspend": {
            "continuous_ns": old.continuous_ns + 2_000_000_000,
            "wall_ns": old.wall_ns + 2_000_000_000,
        },
        "counter": {"continuous_ns": old.continuous_ns - 1},
    }
    simulated.reading = replace(old, **changes[fault])
    with pytest.raises(ResearchError) as error:
        client.worker_clock(challenge(worker))
    assert error.value.code == ErrorCode.STALE_FENCE
    simulated.reading = old
    reopened = LocalRuntime.open(pipeline.runtime.root)
    other = ResearchClient(LocalTransport(reopened, lambda: reopened.credentials.resolve(token)))
    with pytest.raises(ResearchError, match="no longer qualified"):
        other.worker_clock(challenge(worker))
    with pytest.raises(ResearchError, match="no longer qualified"):
        claim(other, worker, pipeline.ready())
    with pipeline.runtime.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM budget_reservations").fetchone()[0] == 0
    binding = worker.binding
    with pytest.raises(ResearchError, match="process-frozen"):
        process_authority_clock(
            reopened.database.path,
            binding.workspace_id,
            binding.controller_id,
            binding.control_epoch,
            before.key_id,
            ClockPolicy(maximum_roundtrip_ms=2000),
        )


def test_claim_replay_and_output_recovery_survive_clock_failure(
    pipeline: Pipeline,
    simulated: SimulatedClock,
    tmp_path: Path,
) -> None:
    client, worker, _ = timed_worker(pipeline)
    job = pipeline.ready()
    request = ClaimJob(
        worker_id=worker.binding.worker_id,
        session_id=worker.binding.worker_session_id,
        job_id=job["id"],
    )
    accepted = client.command(
        "worker.claim_job",
        request,
        idempotency_key="timed-claim",
        expected_revision=job["revision"],
    )
    target = input_request(worker, accepted["result"])
    assert client.worker_input(target)
    old = simulated.reading
    simulated.reading = replace(old, wall_ns=old.wall_ns - 2_000_000_000)
    with pytest.raises(ResearchError):
        client.worker_input(target)
    assert (
        client.command(
            "worker.claim_job",
            request,
            idempotency_key="timed-claim",
            expected_revision=job["revision"],
        )
        == accepted
    )
    evidence = tmp_path / "clock-failure.json"
    evidence.write_text('{"observation":"clock unavailable; launch not attempted"}')
    uploaded = client.worker_upload_file(
        WorkerOutputIntent(
            worker_id=target.worker_id,
            session_id=target.session_id,
            attempt_id=target.attempt_id,
            role="journal",
        ),
        evidence,
        idempotency_key="timed-evidence",
    )
    assert uploaded.size_bytes == evidence.stat().st_size


def test_admitted_input_stream_can_finish_after_clock_failure(
    pipeline: Pipeline,
    simulated: SimulatedClock,
) -> None:
    client, worker, token = timed_worker(pipeline)
    row = claim(client, worker, pipeline.ready())
    request = input_request(worker, row)
    runtime = pipeline.runtime
    artifacts = WorkerArtifacts(runtime.database, runtime.artifacts, runtime.service.workers)
    context = CommandContext(principal=runtime.credentials.resolve(token), idempotency_key=new_id())
    old = simulated.reading
    with artifacts.input_reader(context, request) as stream:
        simulated.reading = replace(old, wall_ns=old.wall_ns + 2_000_000_000)
        with pytest.raises(ResearchError):
            artifacts.input_metadata(context, request)
        assert stream.read(2) == b"PK"


def test_clock_is_rechecked_inside_input_pin_transaction(
    pipeline: Pipeline,
    simulated: SimulatedClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, worker, token = timed_worker(pipeline)
    row = claim(client, worker, pipeline.ready())
    request = input_request(worker, row)
    runtime = pipeline.runtime
    artifacts = WorkerArtifacts(runtime.database, runtime.artifacts, runtime.service.workers)
    context = CommandContext(principal=runtime.credentials.resolve(token), idempotency_key=new_id())
    original = runtime.artifacts.pinned_authorized

    @contextmanager
    def changed_before_pin(*args: Any, **kwargs: Any) -> Iterator[Any]:
        old = simulated.reading
        simulated.reading = replace(old, wall_ns=old.wall_ns - 2_000_000_000)
        with original(*args, **kwargs) as record:
            yield record

    monkeypatch.setattr(runtime.artifacts, "pinned_authorized", changed_before_pin)
    with pytest.raises(ResearchError), artifacts.input_reader(context, request):
        pytest.fail("Clock failure admitted a new input stream")
    with runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind='artifact_reference' AND state='retained' "
                "AND json_extract(payload,'$.retention.owner_kind')='artifact_read'"
            ).fetchone()[0]
            == 0
        )


def test_input_expiry_uses_the_issuing_clock_not_raw_utc(
    pipeline: Pipeline,
    simulated: SimulatedClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, worker, _ = timed_worker(pipeline)
    row = claim(client, worker, pipeline.ready())
    request = input_request(worker, row)
    old = simulated.reading
    simulated.reading = replace(
        old,
        continuous_ns=old.continuous_ns + 121_000_000_000,
        awake_ns=old.awake_ns + 121_000_000_000,
        wall_ns=old.wall_ns + 121_000_000_000,
    )
    monkeypatch.setattr(
        "research_swarm.control.worker_artifacts.utc_now", lambda: "2000-01-01T00:00:00.000Z"
    )
    with pytest.raises(ResearchError, match="grant has expired"):
        client.worker_input(request)


def test_counter_regression_between_samples_poisoned_even_above_anchor(
    pipeline: Pipeline,
    simulated: SimulatedClock,
) -> None:
    client, worker, _ = timed_worker(pipeline)
    client.worker_clock(challenge(worker))
    old = simulated.reading
    for elapsed in (2_000_000_000, 1_000_000_000):
        simulated.reading = replace(
            old,
            continuous_ns=old.continuous_ns + elapsed,
            awake_ns=old.awake_ns + elapsed,
            wall_ns=old.wall_ns + elapsed,
        )
        if elapsed == 2_000_000_000:
            client.worker_clock(challenge(worker))
        else:
            with pytest.raises(ResearchError, match="regressed"):
                client.worker_clock(challenge(worker))


def test_timing_evidence_cannot_be_stripped_to_local_mode(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    client, worker, token = timed_worker(pipeline)
    row = claim(client, worker, pipeline.ready())
    request = input_request(worker, row)
    with pipeline.runtime.database.connect() as connection:
        connection.execute(
            "UPDATE records SET payload=json_remove(payload,'$.assignment_time') WHERE id=?",
            (row["id"],),
        )
    with pytest.raises(ResearchError, match="time binding is absent"):
        client.worker_input(request)
    local = LocalWorker(
        client,
        worker,
        root=tmp_path / "worker",
        machine_registry=tmp_path / "machine" / "resources.sqlite",
    )
    attempt = AttemptData.model_validate(row["data"]).model_copy(update={"assignment_time": None})
    with pytest.raises(ResearchError, match="does not yet admit calibrated"):
        local._verify(attempt, TrialData.model_validate(pipeline.trial["data"]))
    missing = WorkerArtifacts(pipeline.runtime.database, pipeline.runtime.artifacts)
    context = CommandContext(
        principal=pipeline.runtime.credentials.resolve(token), idempotency_key=new_id()
    )
    with pytest.raises(ResearchError, match="authority is unavailable"):
        missing.input_metadata(context, request)


async def test_scoped_clock_api_sdk_and_mcp_share_contract(pipeline: Pipeline) -> None:
    client, worker, token = timed_worker(pipeline)
    app = create_app(pipeline.runtime.service, pipeline.runtime.credentials)
    schema = app.openapi()["paths"]["/v1/worker/clock"]["post"]
    assert schema["requestBody"]["content"]["application/json"]["schema"]["required"] == [
        "worker_id",
        "session_id",
        "nonce",
    ]
    assert "200" in schema["responses"]
    with serving(app) as url, ResearchClient.connect(url, token) as remote:
        sample = await AsyncResearchClient(remote).worker_clock(challenge(worker))
        assert sample.payload.binding == worker.binding
        other, other_worker, _ = timed_worker(pipeline)
        with pytest.raises(ResearchError):
            await asyncio.to_thread(remote.worker_clock, challenge(other_worker))
        assert (
            other.worker_clock(challenge(other_worker)).payload.clock_epoch
            == sample.payload.clock_epoch
        )
    tools = await make_mcp(client).list_tools()
    assert "research_worker_clock" in {tool.name for tool in tools}
    legacy, legacy_worker, _ = scoped_worker(pipeline)
    with pytest.raises(ResearchError, match="local UTC"):
        legacy.worker_clock(challenge(legacy_worker))


def test_fresh_process_epoch_cannot_requalify_old_assignment(pipeline: Pipeline) -> None:
    client, worker, token = timed_worker(pipeline)
    reply = client.worker_clock(challenge(worker))
    row = claim(client, worker, pipeline.ready())
    request = input_request(worker, row)
    script = (
        "import json,sys; from pathlib import Path; "
        "from research_swarm.client import ResearchClient,LocalTransport; "
        "from research_swarm.runtime import LocalRuntime; "
        "from research_swarm.domain.clocks import ClockChallenge; "
        "from research_swarm.control.worker_artifacts import WorkerArtifactRead; "
        "from research_swarm.domain.errors import ResearchError; "
        "p=json.load(sys.stdin); r=LocalRuntime.open(Path(p['root'])); "
        "c=ResearchClient(LocalTransport(r,lambda:r.credentials.resolve(p['token']))); "
        "s=c.worker_clock(ClockChallenge.model_validate(p['challenge'])); "
        "print(s.payload.clock_epoch);\n"
        "try: c.worker_input(WorkerArtifactRead.model_validate(p['input']))\n"
        "except ResearchError as e: print(e.code.value)\n"
        "else: raise AssertionError('Old epoch acquired new inputs')\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        input=json.dumps(
            {
                "root": str(pipeline.runtime.root),
                "token": token,
                "challenge": challenge(worker).model_dump(mode="json"),
                "input": request.model_dump(mode="json"),
            }
        ),
        capture_output=True,
        text=True,
        check=True,
        timeout=15,
    )
    epoch, error = completed.stdout.splitlines()
    assert epoch != reply.payload.clock_epoch and error == ErrorCode.STALE_FENCE.value
    assert client.worker_input(request)


def test_inherited_process_cannot_reset_authority_clock(tmp_path: Path) -> None:
    # Run fork in an isolated single-threaded interpreter, never in the test server.
    script = (
        "import os,sys; from pathlib import Path; "
        "from research_swarm.control.authority_clock import process_authority_clock; "
        "from research_swarm.domain.errors import ResearchError; "
        "args=(Path(sys.argv[1]),'workspace','controller',1,'key'); "
        "c=process_authority_clock(*args); c.observe();\n"
        "if not hasattr(os,'fork'): sys.exit(77)\n"
        "pid=os.fork()\n"
        "if pid == 0:\n"
        " try: process_authority_clock(*args).observe()\n"
        " except ResearchError: os._exit(0)\n"
        " os._exit(1)\n"
        "_,status=os.waitpid(pid,0); c.observe(); sys.exit(os.waitstatus_to_exitcode(status))\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path / "clock.sqlite")],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if completed.returncode == 77:
        pytest.skip("Native fork is unavailable")
    assert completed.returncode == 0, completed.stderr
