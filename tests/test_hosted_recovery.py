"""Real signed original journals through SQLite/mock R2; not workerd qualification."""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import httpx
import pytest

from adapters.cloudflare.checkpoint_closure import reconcile_target_attempt
from adapters.cloudflare.hosted_http import handle
from adapters.cloudflare.recovery_router import ExistingRecoveryRouter
from research_swarm.client import ResearchClient
from research_swarm.control.auth import CredentialStore
from research_swarm.control.observations import PublishAttempt
from research_swarm.control.worker_artifacts import WorkerAttemptTarget
from research_swarm.control.worker_outputs import WorkerOutputIntent, WorkerUploadTarget
from research_swarm.control.worker_records import AttemptData, WorkerData
from research_swarm.domain.canonical import canonical_bytes, content_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database, Transaction
from research_swarm.storage.async_r2 import BoundedR2Objects
from research_swarm.workers.clock_signatures import ClockVerifier
from research_swarm.workers.contracts import NativeCommand
from research_swarm.workers.journal import WorkerJournal
from research_swarm.workers.journal_records import JournalState
from tests.test_async_r2 import Transport
from tests.test_checkpoint_admission import admit
from tests.test_checkpoint_admission import elected as elected
from tests.test_checkpoint_capture import sealed as sealed
from tests.test_checkpoint_import import recovery as recovery
from tests.test_checkpoint_import import snapshot
from tests.test_checkpoint_request import imported as imported
from tests.test_checkpoint_target import configured as configured
from tests.test_cloudflare_deployment import deployment as deployment
from tests.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.test_hosted_research import enroll_and_claim, sdk
from tests.test_hosted_router import FixtureStore, Gate
from tests.test_manifest import project as project
from tests.test_production_delivery import production as production


@pytest.fixture
async def source(production, tmp_path):
    e, loop = production, asyncio.get_running_loop()
    credentials = CredentialStore.open_existing(Database.from_store(e.store))
    principal = e.manifest.credentials[0].principal
    e.original = credentials.issue(
        principal,
        principal.model_copy(
            update={
                "id": new_id(),
                "scopes": {"worker.query", "worker.claim", "worker.outputs", "worker.publish"},
            }
        ),
    )

    def claim():
        with sdk(e, loop) as operator, sdk(e, loop, token=e.original.token) as worker:
            claimed, _ = enroll_and_claim(
                operator,
                e.admitted,
                e.original.principal.id,
                worker_client=worker,
                project_ids=[e.admitted.project["id"]],
            )
            attempt = AttemptData.model_validate(claimed["result"]["data"])
            data = WorkerData.model_validate(
                operator.get("worker", attempt.dispatch.worker_id)["data"]
            )
            return claimed, attempt, data

    e.claimed, attempt, e.worker = await asyncio.to_thread(claim)
    grant = ClockVerifier(e.worker.assignment_authority).binding(
        attempt.assignment_time, attempt.signed_assignment
    )
    stage = next(
        stage
        for stage in attempt.signed_assignment.payload.spec.stages
        if stage.id == attempt.dispatch.stage_id
    )
    journal = WorkerJournal(tmp_path / "original-journal")
    journal.receive(
        grant,
        NativeCommand(argv=stage.argv, cwd=str(tmp_path), timeout_ms=stage.timeout_ms),
        require_preparation=True,
    )
    journal.request_local_cancel(attempt.dispatch.attempt_id)
    e.entry = journal.settle_unstarted(attempt.dispatch.attempt_id)
    assert (
        e.entry.state == JournalState.NOT_STARTED
        and e.entry.native is None
        and e.entry.outcome is None
    )
    e.journal_path = tmp_path / "original-journal.json"
    e.journal_path.write_bytes(canonical_bytes(e.entry.model_dump(mode="json")))

    def metadata(sql):
        sql.execute(
            "CREATE TABLE research_schema (singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            "version INTEGER NOT NULL)"
        )
        sql.execute("INSERT INTO research_schema VALUES (1,3)")

    e.store.write(metadata)
    return e


@pytest.fixture
async def receiver(elected):
    e = elected
    await admit(e)
    e.objects = BoundedR2Objects(
        e.manifest.configuration.storage, Transport(e.manifest.configuration.storage)
    )
    e.router = await reopen(e)
    e.target_request = WorkerAttemptTarget(
        worker_id=e.source.worker.binding.worker_id,
        session_id=e.source.worker.binding.worker_session_id,
        attempt_id=e.source.entry.dispatch.attempt_id,
    )
    e.token = e.source.original.token
    return e


async def reopen(e):
    return await ExistingRecoveryRouter.open(
        Gate(),
        lambda: e.target,
        e.manifest,
        e.source.secrets,
        target_json=json.dumps(e.target_config),
        crypto=e.source.crypto,
        objects=e.objects,
    )


def recovery_client(e, loop):
    def route(request):
        async def deliver():
            async def read(limit):
                assert len(request.content) <= limit
                return request.content

            return await handle(
                e.router,
                request.method,
                str(request.url),
                dict(request.headers),
                read,
                policy=e.router.policy,
            )

        result = asyncio.run_coroutine_threadsafe(deliver(), loop).result(timeout=10)
        return httpx.Response(result.status, content=result.body, headers=dict(result.headers))

    client = ResearchClient.connect("https://target.example", e.token)
    client.transport.http.close()
    client.transport.http = httpx.Client(
        base_url="https://target.example",
        transport=httpx.MockTransport(route),
        headers={"Authorization": "Bearer " + e.token},
    )
    return client


async def upload(e, *, key="journal"):
    loop = asyncio.get_running_loop()

    def transfer():
        with recovery_client(e, loop) as client:
            intent = WorkerOutputIntent(**e.target_request.model_dump(), role="journal")
            reference = client.worker_upload_file(
                intent, e.source.journal_path, idempotency_key=key
            )
            request = PublishAttempt(
                attempt_id=e.target_request.attempt_id, journal_artifact=reference
            )
            reply = client.command(
                "worker.submit_evidence", request, idempotency_key=key + "-submit"
            )
            return reference, reply

    return await asyncio.to_thread(transfer)


async def begin(e, body):
    result = await e.router.command(
        e.token,
        "worker.begin_upload",
        {
            **e.target_request.model_dump(mode="json"),
            "role": "journal",
            "content": {"digest": content_digest(body), "size_bytes": len(body)},
        },
        idempotency_key=new_id(),
    )
    return WorkerUploadTarget(**e.target_request.model_dump(), upload_id=result["result"]["id"])


async def pending(e, body):
    target = await begin(e, body)
    await e.router.worker_upload_part(e.token, target.model_dump(mode="json"), 0, body)
    return target


async def finish(e, target, *, key=None):
    return await e.router.command(
        e.token,
        "worker.finalize_upload",
        target.model_dump(mode="json"),
        idempotency_key=key or new_id(),
    )


def closure_request(e, submission, *, revision=None):
    if revision is None:
        revision = e.target.read(
            lambda sql: sql.execute(
                "SELECT revision FROM records WHERE kind='attempt' AND id=?",
                (e.target_request.attempt_id,),
            ).one()[0]
        )
    return {
        "request_id": new_id(),
        "expected_revision": revision,
        "payload": {
            "attempt_id": e.target_request.attempt_id,
            "submission_id": submission["id"],
            "expected_controller_id": e.manifest.configuration.authority.controller_id,
            "expected_control_epoch": e.manifest.configuration.authority.control_epoch,
            "reason": "Original native evidence proves operational closure",
        },
    }


async def close_attempt(e, request, *, credential=None, store=None):
    return strict_json_loads(
        await reconcile_target_attempt(
            Gate(),
            lambda: e.target if store is None else store,
            e.manifest,
            e.source.secrets,
            identity=e.target.identity,
            credential=e.source.secrets.bootstrap if credential is None else credential,
            target_json=json.dumps(e.target_config),
            request_json=json.dumps(request),
            crypto=e.source.crypto,
            objects=e.objects,
        )
    )


async def test_private_closure_settles_not_started_once_and_keeps_recovery_gate(receiver):
    e = receiver
    _, submitted = await upload(e)
    before, calls = snapshot(e.target), len(e.source.witness.requests)
    request = closure_request(e, submitted["result"])
    closed = await close_attempt(e, request)
    assert closed["disposition"] == "accepted"
    result = closed["result"]
    assert result["disposition"] == result["attempt"]["state"] == "closed_unaccepted"
    assert result["closure"]["data"]["not_started"] is True
    assert set(result["closure"]["data"]["consumed"].values()) == {"0"}
    after = snapshot(e.target)
    assert after["budget_settlements"] != before["budget_settlements"]
    reservations = set(result["attempt"]["data"]["dispatch"]["reservation_ids"])
    assert (
        reservations
        and {row["id"] for row in after["budget_reservations"] if row["state"] == "settled"}
        == reservations
    )
    assert all(
        row["state"] == "recovering" for row in after["records"] if row["kind"] == "workspace_gate"
    )
    assert not [
        row
        for row in after["records"]
        if row["kind"] in {"run_result", "evaluation_receipt", "promotion"}
    ]
    assert await close_attempt(e, request) == closed
    assert snapshot(e.target) == after
    fresh = closure_request(e, submitted["result"], revision=result["attempt"]["revision"])
    assert (await close_attempt(e, fresh))["result"] == result
    assert snapshot(e.target)["budget_settlements"] == after["budget_settlements"]
    assert len(e.source.witness.requests) == calls


@pytest.mark.parametrize("state", [JournalState.RECEIVED, JournalState.UNKNOWN])
async def test_unfinished_journal_never_releases_recovery_budget(receiver, state):
    e = receiver
    e.source.journal_path.write_bytes(
        canonical_bytes(e.source.entry.model_copy(update={"state": state}).model_dump(mode="json"))
    )
    _, submitted = await upload(e)
    before = snapshot(e.target)
    with pytest.raises(ResearchError) as refused:
        await close_attempt(e, closure_request(e, submitted["result"]))
    assert refused.value.code in {ErrorCode.EXECUTION_UNKNOWN, ErrorCode.INVALID_TRANSITION}
    after = snapshot(e.target)
    for table in ("budget_reservations", "budget_settlements", "budget_entries", "records"):
        assert after[table] == before[table]


@pytest.mark.parametrize("damage", ["bootstrap", "admission", "revision", "bytes"])
async def test_private_closure_refuses_missing_authority_or_evidence(receiver, damage):
    e = receiver
    _, submitted = await upload(e)
    request = closure_request(e, submitted["result"])
    if damage == "admission":
        e.target.write(
            lambda sql: sql.execute("DELETE FROM records WHERE kind='logical_recovery_election'")
        )
    elif damage == "revision":
        request["expected_revision"] += 1
    elif damage == "bytes":
        e.objects.transport.objects.clear()
    before, calls = snapshot(e.target), list(e.objects.transport.calls)
    with pytest.raises(ResearchError):
        await close_attempt(e, request, credential="wrong" if damage == "bootstrap" else None)
    assert snapshot(e.target) == before
    if damage != "bytes":
        assert e.objects.transport.calls == calls


@pytest.mark.parametrize("damage", ["admission", "submission"])
async def test_closure_rechecks_awaited_evidence_before_accounting(receiver, monkeypatch, damage):
    e = receiver
    _, submitted = await upload(e)
    request = closure_request(e, submitted["result"])
    original = e.objects.transport.get
    changed = []

    async def read(*args, **kwargs):
        body = await original(*args, **kwargs)
        if damage == "admission":
            e.target.write(
                lambda sql: sql.execute(
                    "DELETE FROM records WHERE kind='logical_recovery_election'"
                )
            )
        else:
            e.target.write(
                lambda sql: sql.execute(
                    "UPDATE records SET state='changed' WHERE kind='attempt_submission'"
                )
            )
        changed.append(snapshot(e.target))
        return body

    monkeypatch.setattr(e.objects.transport, "get", read)
    with pytest.raises(ResearchError):
        await close_attempt(e, request)
    assert changed
    after = snapshot(e.target)
    for table in ("budget_reservations", "budget_settlements", "budget_entries", "records"):
        assert after[table] == changed[-1][table]


async def test_independent_closure_race_has_one_settlement_and_receipt(receiver):
    e = receiver
    _, submitted = await upload(e)
    request = closure_request(e, submitted["result"])
    barrier = Barrier(2)

    def race(_):
        store = FixtureStore(Database(e.target.delegate.path).store)
        store.identity = e.target.identity
        barrier.wait(timeout=10)
        return asyncio.run(close_attempt(e, request, store=store))

    def run():
        with ThreadPoolExecutor(max_workers=2) as pool:
            return list(pool.map(race, (0, 1)))

    results = await asyncio.to_thread(run)
    assert results[0] == results[1]
    after = snapshot(e.target)
    assert len([row for row in after["records"] if row["kind"] == "attempt_closure"]) == 1
    assert len(after["budget_settlements"]) == len(e.source.entry.dispatch.reservation_ids)


async def test_closure_rollback_keeps_budget_and_evidence_together(receiver, monkeypatch):
    e = receiver
    _, submitted = await upload(e)
    request = closure_request(e, submitted["result"])
    before = snapshot(e.target)
    original = Transaction.event

    def broken(tx, kind, *args, **kwargs):
        original(tx, kind, *args, **kwargs)
        if kind == "attempt.closure_reconciled":
            raise RuntimeError("Synthetic failure after final closure event")

    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "event", broken)
        with pytest.raises(RuntimeError, match="Synthetic failure"):
            await close_attempt(e, request)
    assert snapshot(e.target) == before
    assert (await close_attempt(e, request))["disposition"] == "accepted"


async def test_lost_closure_ack_replays_but_changed_admission_still_refuses(receiver, monkeypatch):
    e = receiver
    _, submitted = await upload(e)
    request = closure_request(e, submitted["result"])
    original = Database.execute

    def lost(database, context, operation, *args, **kwargs):
        result = original(database, context, operation, *args, **kwargs)
        if operation == "attempt.reconcile_closure":
            raise ValueError("Synthetic lost committed closure acknowledgment")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(Database, "execute", lost)
        with pytest.raises(ValueError, match="Synthetic lost"):
            await close_attempt(e, request)
    before = snapshot(e.target)
    assert (await close_attempt(e, request))["disposition"] == "accepted"
    assert snapshot(e.target) == before
    e.target.write(
        lambda sql: sql.execute("DELETE FROM records WHERE kind='logical_recovery_election'")
    )
    changed = snapshot(e.target)
    with pytest.raises(ResearchError):
        await close_attempt(e, request)
    assert snapshot(e.target) == changed


async def test_original_scoped_sdk_uploads_and_quarantines_journal_without_clock(receiver):
    e = receiver
    before, witness_calls = snapshot(e.target), len(e.source.witness.requests)
    e.source.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic offline")
    assert e.router.service.workers is None
    reference, reply = await upload(e)
    assert reply["disposition"] == "accepted" and reply["result"]["kind"] == "attempt_submission"
    assert reply["result"]["data"]["original_binding"] == e.source.worker.binding.model_dump(
        mode="json"
    )
    after = snapshot(e.target)
    for table in ("budgets", "budget_reservations", "budget_settlements", "budget_entries"):
        assert after[table] == before[table]
    for kind in ("attempt", "trial", "evaluation", "promotion", "job", "workspace_gate"):
        assert [row for row in after["records"] if row["kind"] == kind] == [
            row for row in before["records"] if row["kind"] == kind
        ]
    assert len(e.source.witness.requests) == witness_calls
    e.router = await reopen(e)
    assert await upload(e) == (reference, reply)
    assert snapshot(e.target) == after


async def test_history_and_capabilities_are_available_to_original_worker_only(receiver):
    e = receiver
    payload = {
        "worker_id": e.target_request.worker_id,
        "session_id": e.target_request.session_id,
        "kind": "historical_attempt",
        "id": e.target_request.attempt_id,
    }
    assert (await e.router.query(e.token, "worker.get", payload))[
        "id"
    ] == e.target_request.attempt_id
    session = {**payload, "kind": "worker_session", "id": e.target_request.session_id}
    assert (await e.router.query(e.token, "worker.get", session))[
        "data"
    ] == e.source.worker.model_dump(mode="json")
    with pytest.raises(ResearchError):
        await e.router.query(e.token, "worker.get", {**payload, "kind": "attempt"})
    with pytest.raises(ResearchError):
        await e.router.query(e.source.token, "worker.get", payload)
    loop = asyncio.get_running_loop()

    def capabilities():
        with recovery_client(e, loop) as client:
            return client.capabilities()

    caps = await asyncio.to_thread(capabilities)
    assert caps["execution"] == caps["clock"] == "unavailable"
    assert "worker.claim_job" not in caps["commands"]
    assert "request_schema" in caps["commands"]["worker.submit_evidence"]


async def test_lost_committed_submission_reply_replays_without_a_second_event(
    receiver, monkeypatch
):
    e = receiver
    original = e.router.command

    async def lost(token, operation, payload, **kwargs):
        result = await original(token, operation, payload, **kwargs)
        if operation == "worker.submit_evidence":
            raise ValueError("Private lost receipt")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(e.router, "command", lost)
        with pytest.raises(ResearchError) as uncertain:
            await upload(e)
        assert uncertain.value.code == ErrorCode.DELIVERY_UNCERTAIN
        assert "Private" not in str(uncertain.value)
    before = snapshot(e.target)
    e.router = await reopen(e)
    assert (await upload(e))[1]["disposition"] == "accepted"
    assert snapshot(e.target) == before


@pytest.mark.parametrize("damage", ["revoked", "expires", "admission"])
async def test_cached_submission_still_requires_current_identity_and_admission(receiver, damage):
    e = receiver
    reference, _ = await upload(e)
    if damage == "admission":
        e.target.write(
            lambda sql: sql.execute("DELETE FROM records WHERE kind='logical_recovery_election'")
        )
    else:
        statement = (
            "revoked=1" if damage == "revoked" else "expires_at='2999-01-01T00:00:00.000000Z'"
        )
        e.target.write(
            lambda sql: sql.execute(
                "UPDATE credentials SET " + statement + " WHERE id=?", (e.source.original.id,)
            )
        )
    before, calls = snapshot(e.target), list(e.objects.transport.calls)
    with pytest.raises(ResearchError):
        await e.router.command(
            e.token,
            "worker.submit_evidence",
            PublishAttempt(
                attempt_id=e.target_request.attempt_id, journal_artifact=reference
            ).model_dump(mode="json"),
            idempotency_key="journal-submit",
        )
    assert snapshot(e.target) == before and e.objects.transport.calls == calls


@pytest.mark.parametrize("damage", ["invalid_json", "different_assignment"])
async def test_finalization_verifies_journal_and_original_assignment(receiver, damage):
    e = receiver
    body = (
        b"{}"
        if damage == "invalid_json"
        else canonical_bytes(
            e.source.entry.model_copy(
                update={
                    "dispatch": e.source.entry.dispatch.model_copy(update={"attempt_id": new_id()})
                }
            ).model_dump(mode="json")
        )
    )
    target = await pending(e, body)
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        await finish(e, target)
    after = snapshot(e.target)
    assert after["records"] == before["records"]
    assert after["budget_entries"] == before["budget_entries"]


@pytest.mark.parametrize("damage", ["revoked", "admission", "catalog"])
async def test_finalize_rechecks_authority_after_r2_read(receiver, monkeypatch, damage):
    e = receiver
    target = await pending(e, e.source.journal_path.read_bytes())
    original = e.objects.transport.get
    changed = []

    async def read(*args, **kwargs):
        result = await original(*args, **kwargs)
        if damage == "revoked":
            e.target.write(
                lambda sql: sql.execute(
                    "UPDATE credentials SET revoked=1 WHERE id=?", (e.source.original.id,)
                )
            )
        elif damage == "admission":
            e.target.write(
                lambda sql: sql.execute(
                    "DELETE FROM records WHERE kind='logical_recovery_election'"
                )
            )
        else:
            e.target.write(
                lambda sql: sql.execute(
                    "UPDATE records SET revision=revision+1 WHERE kind='upload' AND id=?",
                    (target.upload_id,),
                )
            )
        changed.append(snapshot(e.target))
        return result

    monkeypatch.setattr(e.objects.transport, "get", read)
    with pytest.raises(ResearchError):
        await finish(e, target)
    assert changed and snapshot(e.target) == changed[-1]


async def test_duplicate_journal_cleanup_preserves_canonical_generation(receiver):
    e = receiver
    body = e.source.journal_path.read_bytes()
    original, duplicate = await pending(e, body), await pending(e, body)
    first = await finish(e, original)
    before = snapshot(e.target)
    second = await finish(e, duplicate)
    assert first["result"]["artifact"] == second["result"]["artifact"]
    await e.router.command(
        e.token,
        "worker.cleanup_upload",
        duplicate.model_dump(mode="json"),
        idempotency_key="cleanup",
    )
    after = snapshot(e.target)
    assert [r for r in after["records"] if r["kind"] == "artifact"] == [
        r for r in before["records"] if r["kind"] == "artifact"
    ]
    assert len([r for r in after["records"] if r["kind"] == "worker_upload"]) == 2
    assert any(
        value.metadata.get("research-state") == "retired"
        for value in e.objects.transport.objects.values()
    )


async def test_missing_canonical_bytes_are_not_repaired_by_duplicate_upload(receiver):
    e = receiver
    body = e.source.journal_path.read_bytes()
    original, target = await pending(e, body), await pending(e, body)
    reference = (await finish(e, original))["result"]["artifact"]
    # Keep the duplicate generation present while removing only canonical bytes.
    duplicate_bytes = dict(e.objects.transport.objects)
    e.objects.transport.objects.clear()
    e.objects.transport.objects.update(
        {key: value for key, value in duplicate_bytes.items() if target.upload_id in key}
    )
    before = snapshot(e.target)
    with pytest.raises(ResearchError) as refused:
        await finish(e, target)
    assert refused.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    assert snapshot(e.target) == before
    assert reference["id"] != target.upload_id


@pytest.mark.parametrize("role,size", [("stdout", 2), ("report", 2), ("journal", 1024**2 + 1)])
async def test_nonjournal_or_oversized_upload_is_refused_before_r2(receiver, role, size):
    e = receiver
    before = snapshot(e.target)
    with pytest.raises(ResearchError) as refused:
        await e.router.command(
            e.token,
            "worker.begin_upload",
            {
                **e.target_request.model_dump(mode="json"),
                "role": role,
                "content": {"digest": content_digest(b"{}"), "size_bytes": size},
            },
            idempotency_key=new_id(),
        )
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert snapshot(e.target) == before and e.objects.transport.calls == []


@pytest.mark.parametrize(
    "operation",
    [
        "worker.claim_job",
        "worker.publish_attempt",
        "attempt.reconcile_closure",
        "trial.submit",
        "orchestration.prepare_turn",
    ],
)
async def test_recovery_http_refuses_execution_and_operator_commands_before_body(
    receiver, operation
):
    e, consumed = receiver, []

    async def read(limit):
        consumed.append(limit)
        return b"{}"

    result = await handle(
        e.router,
        "POST",
        "/v1/commands/" + operation,
        {"authorization": "Bearer " + e.token, "idempotency-key": "refused"},
        read,
        policy=e.router.policy,
    )
    assert result.status == 501 and consumed == []
    assert strict_json_loads(result.body)["error"]["code"] == "CAPABILITY_UNAVAILABLE"
