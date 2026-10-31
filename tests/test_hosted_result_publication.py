"""Native CPU execution against the adapter; SQLite/mock R2 is not workerd proof."""

import asyncio
from datetime import datetime
from types import SimpleNamespace

import pytest

from adapters.cloudflare.research_router import HostedPublicationConfiguration
from research_swarm.control.native_time_witnesses import NativeWitnessSource
from research_swarm.control.time_witnesses import WitnessSigner
from research_swarm.control.worker_outputs import WorkerOutputIntent
from research_swarm.control.workers import AttemptData, WorkerData
from research_swarm.domain.artifacts import ArtifactRecord
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import SignedTimeWitness
from research_swarm.domain.types import new_id
from research_swarm.state.database import encode
from research_swarm.state.private_files import private_state_directory
from research_swarm.storage.async_r2 import ObjectBytes
from research_swarm.storage.r2_objects import generation_key
from research_swarm.workers.clock_signatures import ClockVerifier
from research_swarm.workers.contracts import NativeCommand
from research_swarm.workers.journal_records import JournalState
from research_swarm.workers.local import LocalWorker
from tests.test_hosted_artifacts import current
from tests.test_hosted_http import send
from tests.test_hosted_research import admit, enroll_and_claim, sdk
from tests.test_hosted_research import research as research
from tests.test_hosted_router import counts
from tests.test_hosted_router import hosted as hosted
from tests.test_hosted_worker_inputs import inputs as inputs
from tests.test_hosted_worker_outputs import outputs as outputs
from tests.test_manifest import project as project
from tests.test_time_witnesses import SEED

TRAIN = """import json, os, time
from pathlib import Path
context = json.loads(Path(os.environ["RESEARCH_CONTEXT"]).read_text())
started = time.perf_counter()
time.sleep(context["time_budget_ms"] / 1000)
report = {
    "schema_version": "1.0",
    "context_digest": context["context_digest"],
    "observed_seed": context["seed"],
    "phase_timings_ms": {"active_training": int((time.perf_counter() - started) * 1000)},
}
Path(context["report_path"]).write_text(json.dumps(report))
print("synthetic training complete")
"""


@pytest.fixture
async def publication(research, project, tmp_path, request):
    h, loop = research, asyncio.get_running_loop()
    mode = getattr(request, "param", "closed")
    native = NativeWitnessSource(h.config.authority, new_id(), SEED, policy=h.config.source.policy)
    stale_context, changed = h.registry.discontinuity()
    h.registry.write(
        stale_context,
        lambda tx: h.registry.providers.fence(
            tx, changed.evidence, name="fixture.operation", payload={}
        ),
    )
    snapshot = h.registry.current()
    context = h.registry.context(revision=snapshot.revision)
    binding, waiting = h.registry.providers.prepare(
        context, snapshot, "fixture.native-source", {}, source=native.source
    )
    proof = waiting.complete(
        canonical_bytes(native.observe(waiting.request).model_dump(mode="json"))
    )
    h.registry.write(
        context,
        lambda tx: h.registry.providers.requalify(
            tx, binding, proof, name="fixture.native-source", payload={}
        ),
    )
    h.config = HostedPublicationConfiguration(
        **h.config.model_dump(exclude={"profile", "source"}), source=native.source
    )
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (h.config.digest, encode(h.config.model_dump(mode="json"))),
        )
    )

    async def acquire(request):
        h.witness.requests.append(request)
        if h.witness.failure is not None:
            raise h.witness.failure
        return canonical_bytes(native.observe(request).model_dump(mode="json"))

    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=acquire)
    credential = h.credentials.issue(
        h.principal,
        h.worker_credential.principal.model_copy(
            update={
                "scopes": {
                    "worker.query",
                    "worker.claim",
                    "worker.inputs",
                    "worker.outputs",
                    "worker.publish",
                }
            }
        ),
    )
    (project / "train.py").write_text("raise SystemExit(1)\n" if mode == "failed" else TRAIN)
    manifest = project / "research.toml"
    manifest.write_text(manifest.read_text().replace("timeout_ms = 1000", "timeout_ms = 10000"))
    captured = {}

    def execute():
        with sdk(h, loop) as operator, sdk(h, loop, token=credential.token) as client:
            admitted = admit(operator, project)
            claimed, _ = enroll_and_claim(
                operator,
                admitted,
                credential.principal.id,
                worker_client=client,
                project_ids=[admitted.project["id"]],
            )
            worker = WorkerData.model_validate(
                operator.get("worker", claimed["result"]["data"]["dispatch"]["worker_id"])["data"]
            )
            runner = LocalWorker(
                client,
                worker,
                root=tmp_path / "worker",
                machine_registry=tmp_path / "machine" / "resources.sqlite",
            )
            if mode == "not_started":
                attempt = AttemptData.model_validate(claimed["result"]["data"])
                grant = ClockVerifier(worker.assignment_authority).binding(
                    attempt.assignment_time, attempt.signed_assignment
                )
                stage = next(
                    stage
                    for stage in attempt.signed_assignment.payload.spec.stages
                    if stage.id == attempt.dispatch.stage_id
                )
                runner.journal.receive(
                    grant,
                    NativeCommand(argv=stage.argv, cwd=str(tmp_path), timeout_ms=stage.timeout_ms),
                    require_preparation=True,
                )
                runner.journal.request_local_cancel(attempt.dispatch.attempt_id)
                with private_state_directory(runner.root / "work" / attempt.dispatch.attempt_id):
                    pass
                assert (
                    runner.journal.settle_unstarted(attempt.dispatch.attempt_id).state
                    == JournalState.NOT_STARTED
                )
            original = client.command

            def capture(operation, payload, **kwargs):
                if operation == "worker.publish_attempt":
                    captured.update(payload=payload.model_dump(mode="json"), **kwargs)
                    raise ResearchError(
                        ErrorCode.DELIVERY_UNCERTAIN, "Fixture pauses before acceptance"
                    )
                return original(operation, payload, **kwargs)

            client.command = capture
            with pytest.raises(ResearchError) as held:
                runner.execute(claimed["result"]["id"])
            assert held.value.code == ErrorCode.DELIVERY_UNCERTAIN
            entry = runner.journal.get(claimed["result"]["id"])
            if mode == "not_started":
                assert entry.state == JournalState.NOT_STARTED
                assert entry.native is None and entry.outcome is None
            else:
                assert entry.state == JournalState.CLOSED
                assert entry.native is not None and entry.outcome is not None
                assert entry.outcome.released_at is not None and entry.outcome.closure.scope_closed
            if mode == "closed":
                assert captured["payload"]["report_artifact"] is not None, captured["payload"][
                    "finding"
                ]
            return admitted, claimed, runner, entry

    admitted, claimed, runner, entry = await asyncio.to_thread(execute)
    return SimpleNamespace(
        h=h,
        credential=credential,
        admitted=admitted,
        claimed=claimed,
        runner=runner,
        entry=entry,
        request=captured,
        acquire=acquire,
    )


async def publish(case):
    return await case.h.router.command(
        case.credential.token,
        "worker.publish_attempt",
        case.request["payload"],
        idempotency_key=case.request["idempotency_key"],
    )


async def test_native_cpu_result_settles_once_without_scientific_receipt(publication):
    case, h = publication, publication.h
    before = current(h, "attempt", case.claimed["result"]["id"])
    assert before["data"]["publication_status"] == "pending"
    accepted = await publish(case)
    assert accepted["result"]["state"] == "succeeded"
    assert accepted["result"]["data"]["publication_status"] == "accepted"
    result = current(h, "run_result", before["id"])
    assert result["data"]["status"] == "succeeded"
    assert (
        h.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind IN "
                "('evaluation_receipt','promotion','artifact_read_session')"
            ).one()[0]
        )
        == 0
    )
    assert (
        current(h, "trial", case.admitted.trial["result"]["id"])["data"]["evaluation_status"]
        == "pending"
    )
    before_replay = counts(h), len(h.transport.calls), len(h.witness.requests)
    assert await publish(case) == accepted
    assert (counts(h), len(h.transport.calls), len(h.witness.requests)) == before_replay


async def test_publication_replays_after_reopen_with_source_and_storage_unavailable(publication):
    case, h = publication, publication.h
    accepted = await publish(case)
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=case.acquire)
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture source unavailable")
    h.transport.hide_reads = True
    before = counts(h), len(h.transport.calls), len(h.witness.requests)
    assert await publish(case) == accepted
    assert (counts(h), len(h.transport.calls), len(h.witness.requests)) == before


@pytest.mark.parametrize("publication", ["not_started", "failed"], indirect=True)
async def test_non_success_dispositions_preserve_native_accounting(publication):
    case, h = publication, publication.h
    accepted = await publish(case)
    not_started = case.entry.state == JournalState.NOT_STARTED
    assert accepted["result"]["state"] == ("not_started" if not_started else "failed")
    assert (
        h.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='evaluation_receipt'"
            ).one()[0]
        )
        == 0
    )
    if not_started:
        assert (
            h.store.read(
                lambda sql: sql.execute(
                    "SELECT COUNT(*) FROM records WHERE kind='run_result'"
                ).one()[0]
            )
            == 0
        )


@pytest.mark.parametrize("role", ["journal", "report", "stdout"])
async def test_every_evidence_generation_is_verified_before_acceptance(publication, role):
    case, h = publication, publication.h
    payload = case.request["payload"]
    reference = payload["logs"]["stdout"] if role == "stdout" else payload[role + "_artifact"]
    record = ArtifactRecord.model_validate(current(h, "artifact", reference["id"])["data"])
    blob = h.router.service.artifacts._blob(record)
    key = generation_key(h.config.storage.prefix, record.workspace_id, blob)
    saved = h.transport.objects[key]
    h.transport.objects[key] = ObjectBytes(b"corrupted evidence", saved.metadata)
    before = counts(h), len(h.witness.requests)
    with pytest.raises(ResearchError):
        await publish(case)
    assert (counts(h), len(h.witness.requests)) == before
    assert (
        current(h, "attempt", case.claimed["result"]["id"])["data"]["publication_status"]
        == "pending"
    )


async def test_missing_role_receipt_fails_before_any_byte_access(publication):
    case, h = publication, publication.h
    case.request["payload"]["journal_artifact"] = case.request["payload"]["report_artifact"]
    before = counts(h), len(h.transport.calls), len(h.witness.requests)
    with pytest.raises(ResearchError, match="receipt is absent"):
        await publish(case)
    assert (counts(h), len(h.transport.calls), len(h.witness.requests)) == before


@pytest.mark.parametrize("fault", ["credential", "worker", "catalog", "cancel"])
async def test_publication_rechecks_after_the_fresh_witness(publication, monkeypatch, fault):
    case, h = publication, publication.h
    original = h.router.acquire

    async def acquire(request):
        response = await original(request)
        if fault == "credential":
            h.credentials.revoke(h.principal, case.credential.id)
        elif fault == "worker":
            h.store.write(
                lambda sql: sql.execute(
                    "UPDATE records SET state='revoked' WHERE kind='worker' AND id=?",
                    (case.entry.dispatch.worker_id,),
                )
            )
        elif fault == "catalog":
            identifier = case.request["payload"]["journal_artifact"]["id"]
            row = current(h, "artifact", identifier)
            row["data"]["storage_generation"] = new_id()
            h.store.write(
                lambda sql: sql.execute(
                    "UPDATE records SET payload=? WHERE kind='artifact' AND id=?",
                    (encode(row["data"]), identifier),
                )
            )
        else:
            asyncio.current_task().cancel()
        return response

    monkeypatch.setattr(h.router, "acquire", acquire)
    task = asyncio.create_task(publish(case))
    with pytest.raises((ResearchError, asyncio.CancelledError)):
        await task
    assert (
        current(h, "attempt", case.claimed["result"]["id"])["data"]["publication_status"]
        == "pending"
    )
    assert (
        h.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM budget_settlements").one()[0])
        == 0
    )


async def test_acceptance_expiry_after_reads_commits_conservative_fence(publication, monkeypatch):
    case, h = publication, publication.h
    original = h.router.acquire
    lease = current(h, "acceptance_lease", case.entry.dispatch.attempt_id)
    expiry = datetime.fromisoformat(lease["data"]["expires_at"].replace("Z", "+00:00"))
    expired_ms = int(expiry.timestamp() * 1000) + 1

    async def expire(request):
        observed = SignedTimeWitness.model_validate_json(await original(request)).payload
        future = observed.model_copy(update={"authority_time_ms": expired_ms})
        return canonical_bytes(
            WitnessSigner(h.config.source, SEED).sign(future).model_dump(mode="json")
        )

    monkeypatch.setattr(h.router, "acquire", expire)
    with pytest.raises(ResearchError) as expired:
        await publish(case)
    assert expired.value.code == ErrorCode.ACCEPTANCE_EXPIRED
    attempt = current(h, "attempt", case.claimed["result"]["id"])
    assert (
        attempt["state"] == "execution_unknown"
        and attempt["data"]["publication_status"] == "pending"
    )
    assert current(h, "acceptance_lease", attempt["id"])["state"] == "expired"
    assert (
        h.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind IN ('run_result','evaluation_receipt')"
            ).one()[0]
        )
        == 0
    )
    assert (
        h.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM budget_settlements").one()[0])
        == 0
    )


@pytest.mark.parametrize("change", ["schema", "seed", "grant"])
async def test_uploaded_bytes_do_not_bypass_observation_validation(publication, tmp_path, change):
    case, h, loop = publication, publication.h, asyncio.get_running_loop()
    role = "report" if change == "seed" else "journal"
    if change == "schema":
        body = b"{}"
    elif change == "grant":
        altered = case.entry.model_copy(
            update={
                "command": case.entry.command.model_copy(
                    update={"timeout_ms": case.entry.command.timeout_ms + 1}
                )
            }
        )
        body = canonical_bytes(altered.model_dump(mode="json"))
    else:
        from research_swarm.domain.canonical import strict_json_loads

        ref = case.request["payload"]["report_artifact"]
        record = ArtifactRecord.model_validate(current(h, "artifact", ref["id"])["data"])
        value = strict_json_loads(
            await h.objects.read(record.workspace_id, h.router.service.artifacts._blob(record))
        )
        value["observed_seed"] += 1
        body = canonical_bytes(value)
    path = tmp_path / "altered.json"
    path.write_bytes(body)
    intent = WorkerOutputIntent(
        worker_id=case.entry.dispatch.worker_id,
        session_id=case.entry.dispatch.worker_session_id,
        attempt_id=case.entry.dispatch.attempt_id,
        role=role,
    )

    def upload():
        with sdk(h, loop, token=case.credential.token) as worker:
            return worker.worker_upload_file(intent, path, idempotency_key="altered")

    altered = await asyncio.to_thread(upload)
    case.request["payload"][role + "_artifact"] = altered.model_dump(mode="json")
    with pytest.raises(ResearchError):
        await publish(case)
    assert (
        current(h, "attempt", case.entry.dispatch.attempt_id)["data"]["publication_status"]
        == "pending"
    )
    assert (
        h.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM budget_settlements").one()[0])
        == 0
    )


async def test_publication_artifact_count_is_bounded_before_io(publication):
    case, h = publication, publication.h
    case.request["payload"]["outputs"] = {
        "output_" + str(index): case.request["payload"]["journal_artifact"] for index in range(33)
    }
    before = counts(h), len(h.transport.calls), len(h.witness.requests)
    with pytest.raises(ResearchError) as refused:
        await publish(case)
    assert refused.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert (counts(h), len(h.transport.calls), len(h.witness.requests)) == before


async def test_output_profile_does_not_implicitly_enable_result_publication(outputs):
    consume = []
    response = await send(
        outputs.h,
        "POST",
        "/v1/commands/worker.publish_attempt",
        body=b"unused",
        token=outputs.credential.token,
        consume=consume,
    )
    assert response.status == 501 and consume == []
