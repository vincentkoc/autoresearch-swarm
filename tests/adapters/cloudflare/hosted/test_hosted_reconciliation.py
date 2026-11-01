"""Active authority reconciles real native CPU evidence; this is not workerd proof."""

import asyncio
from datetime import datetime

import pytest

from adapters.cloudflare.hosted_http import capabilities
from adapters.cloudflare.research_router import HostedReconciliationConfiguration
from research_swarm.control.time_witnesses import WitnessSigner
from research_swarm.control.worker_outputs import WorkerOutputIntent
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import SignedTimeWitness
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction, encode
from research_swarm.workers.journal_records import JournalState
from tests.adapters.cloudflare.hosted.test_hosted_artifacts import current
from tests.adapters.cloudflare.hosted.test_hosted_research import sdk
from tests.adapters.cloudflare.hosted.test_hosted_result_publication import hosted as hosted
from tests.adapters.cloudflare.hosted.test_hosted_result_publication import inputs as inputs
from tests.adapters.cloudflare.hosted.test_hosted_result_publication import outputs as outputs
from tests.adapters.cloudflare.hosted.test_hosted_result_publication import project as project
from tests.adapters.cloudflare.hosted.test_hosted_result_publication import (
    publication as publication,
)
from tests.adapters.cloudflare.hosted.test_hosted_result_publication import publish
from tests.adapters.cloudflare.hosted.test_hosted_result_publication import research as research
from tests.adapters.cloudflare.hosted.test_hosted_router import counts
from tests.workers.test_time_witnesses import SEED


@pytest.fixture
async def reconciliation(publication):
    case, h = publication, publication.h
    h.config = HostedReconciliationConfiguration(**h.config.model_dump(exclude={"profile"}))
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (h.config.digest, encode(h.config.model_dump(mode="json"))),
        )
    )
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=case.acquire)
    return case


async def submit(case, *, key="late-evidence"):
    return await case.h.router.command(
        case.credential.token,
        "worker.submit_evidence",
        case.request["payload"],
        idempotency_key=key,
    )


def reconciliation_request(case, submission):
    h = case.h
    return {
        "attempt_id": case.entry.dispatch.attempt_id,
        "submission_id": submission["result"]["id"],
        "expected_controller_id": h.config.authority.controller_id,
        "expected_control_epoch": h.config.authority.control_epoch,
        "reason": "Reconcile the original closed native attempt",
    }


async def reconcile(case, request, *, key="late-acceptance", revision=None, token=None):
    h = case.h
    if revision is None:
        revision = current(h, "attempt", request["attempt_id"])["revision"]
    return await h.router.command(
        token or h.operator.token,
        "attempt.reconcile",
        request,
        idempotency_key=key,
        expected_revision=revision,
    )


def scientific_counts(h):
    return h.store.read(
        lambda sql: (
            sql.execute("SELECT COUNT(*) FROM budget_settlements").one()[0],
            sql.execute("SELECT COUNT(*) FROM records WHERE kind='run_result'").one()[0],
            sql.execute("SELECT COUNT(*) FROM records WHERE kind='evaluation_receipt'").one()[0],
        )
    )


async def expire_publication(case, monkeypatch):
    h = case.h
    lease = current(h, "acceptance_lease", case.entry.dispatch.attempt_id)
    expires = datetime.fromisoformat(lease["data"]["expires_at"].replace("Z", "+00:00"))
    original = h.router.acquire

    async def expired(request):
        observed = SignedTimeWitness.model_validate_json(await original(request)).payload
        future = observed.model_copy(
            update={"authority_time_ms": int(expires.timestamp() * 1000) + 1}
        )
        return canonical_bytes(
            WitnessSigner(h.config.source, SEED).sign(future).model_dump(mode="json")
        )

    monkeypatch.setattr(h.router, "acquire", expired)
    with pytest.raises(ResearchError) as error:
        await publish(case)
    assert error.value.code == ErrorCode.ACCEPTANCE_EXPIRED
    return current(h, "acceptance_lease", case.entry.dispatch.attempt_id)


async def test_expired_original_native_result_is_quarantined_then_settled_once(
    reconciliation, monkeypatch
):
    case, h = reconciliation, reconciliation.h
    original = current(h, "attempt", case.entry.dispatch.attempt_id)["data"]
    lease = await expire_publication(case, monkeypatch)
    assert lease["state"] == "expired"
    submission = await submit(case)
    assert submission["result"]["state"] == "quarantined"
    assert scientific_counts(h) == (0, 0, 0)
    pending = current(h, "attempt", case.entry.dispatch.attempt_id)
    assert pending["state"] == "execution_unknown"
    request = reconciliation_request(case, submission)
    receipt = await reconcile(case, request)
    assert receipt["result"]["disposition"] == "reconciled"
    accepted = receipt["result"]["attempt"]
    assert accepted["state"] == "succeeded"
    assert accepted["data"]["publication_status"] == "accepted"
    for field in ("dispatch", "signed_assignment", "assignment_time"):
        assert accepted["data"][field] == original[field]
    evidence = receipt["result"]["reconciliation"]
    assert evidence["data"]["acceptance_lease"] == lease["data"]
    assert evidence["data"]["acceptance_disposition"] == "expired"
    assert scientific_counts(h) == (1, 1, 0)
    before = counts(h), scientific_counts(h)
    replay = await reconcile(case, request, key="another-intent")
    assert replay["result"]["disposition"] == "already_accepted"
    assert scientific_counts(h) == before[1]
    settled = current(h, "acceptance_lease", case.entry.dispatch.attempt_id)
    assert settled["state"] == "settled"
    assert {key: value for key, value in settled["data"].items() if key != "disposition"} == {
        key: value for key, value in lease["data"].items() if key != "disposition"
    }
    queried = await h.router.query(
        h.operator.token, "get", {"kind": "attempt_reconciliation", "id": evidence["id"]}
    )
    assert queried == evidence


async def test_submission_and_acceptance_replay_offline_after_reopen(reconciliation):
    case, h = reconciliation, reconciliation.h
    submission = await submit(case)
    request = reconciliation_request(case, submission)
    revision = current(h, "attempt", request["attempt_id"])["revision"]
    accepted = await reconcile(case, request, revision=revision)
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=case.acquire)
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture offline")
    h.transport.hide_reads = True
    before = counts(h), len(h.transport.calls), len(h.witness.requests)
    assert await submit(case) == submission
    assert await reconcile(case, request, revision=revision) == accepted
    assert (counts(h), len(h.transport.calls), len(h.witness.requests)) == before


async def test_reconciliation_scope_does_not_require_global_artifact_read(reconciliation):
    case, h = reconciliation, reconciliation.h
    submission = await submit(case)
    request = reconciliation_request(case, submission)
    with pytest.raises(ResearchError) as denied:
        await reconcile(case, request, token=case.credential.token)
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    operator = h.credentials.issue(
        h.principal,
        h.principal.model_copy(update={"id": new_id(), "scopes": {"attempt.reconcile"}}),
    )
    accepted = await reconcile(case, request, token=operator.token)
    assert accepted["result"]["disposition"] == "reconciled"
    assert scientific_counts(h) == (1, 1, 0)


@pytest.mark.parametrize("operation", ["submit", "reconcile"])
@pytest.mark.parametrize("fault", ["credential", "catalog", "revision", "cancel"])
async def test_original_evidence_is_rechecked_after_witness(
    reconciliation, monkeypatch, operation, fault
):
    case, h = reconciliation, reconciliation.h
    request = reconciliation_request(case, await submit(case)) if operation == "reconcile" else None
    original = h.router.acquire

    async def acquire(witness):
        response = await original(witness)
        if fault == "credential":
            credential = case.credential if operation == "submit" else h.operator
            h.credentials.revoke(h.principal, credential.id)
        elif fault == "catalog":
            identifier = case.request["payload"]["journal_artifact"]["id"]
            record = current(h, "artifact", identifier)
            record["data"]["storage_generation"] = new_id()
            h.store.write(
                lambda sql: sql.execute(
                    "UPDATE records SET payload=? WHERE kind='artifact' AND id=?",
                    (encode(record["data"]), identifier),
                )
            )
        elif fault == "revision":
            if operation == "submit":
                h.store.write(
                    lambda sql: sql.execute(
                        "UPDATE records SET state='revoked' WHERE kind='worker' AND id=?",
                        (case.entry.dispatch.worker_id,),
                    )
                )
            else:
                h.store.write(
                    lambda sql: sql.execute(
                        "UPDATE records SET revision=revision+1 WHERE kind='attempt' AND id=?",
                        (case.entry.dispatch.attempt_id,),
                    )
                )
        else:
            asyncio.current_task().cancel()
        return response

    monkeypatch.setattr(h.router, "acquire", acquire)
    task = asyncio.create_task(submit(case) if request is None else reconcile(case, request))
    with pytest.raises((ResearchError, asyncio.CancelledError)):
        await task
    assert scientific_counts(h) == (0, 0, 0)


async def test_reconciliation_event_failure_rolls_back_acceptance_and_settlement(
    reconciliation, monkeypatch
):
    case, h = reconciliation, reconciliation.h
    request = reconciliation_request(case, await submit(case))
    before = counts(h)
    original = Transaction.event

    def fail(transaction, name, *args, **kwargs):
        if name == "attempt.reconciled":
            raise RuntimeError("Fixture interrupted commit")
        return original(transaction, name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="interrupted"):
            await reconcile(case, request)
    assert counts(h) == before
    assert scientific_counts(h) == (0, 0, 0)
    assert (await reconcile(case, request))["result"]["disposition"] == "reconciled"


@pytest.mark.parametrize("fault", ["schema", "unknown", "grant"])
async def test_quarantine_keeps_unacceptable_evidence_without_accepting_it(
    reconciliation, tmp_path, fault
):
    case, h, loop = reconciliation, reconciliation.h, asyncio.get_running_loop()
    path = tmp_path / "malformed.json"
    if fault == "schema":
        body = b"{}"
    elif fault == "unknown":
        body = canonical_bytes(
            case.entry.model_copy(update={"state": JournalState.UNKNOWN}).model_dump(mode="json")
        )
    else:
        entry = case.entry.model_copy(
            update={
                "command": case.entry.command.model_copy(
                    update={"timeout_ms": case.entry.command.timeout_ms + 1}
                )
            }
        )
        body = canonical_bytes(entry.model_dump(mode="json"))
    path.write_bytes(body)
    intent = WorkerOutputIntent(
        worker_id=case.entry.dispatch.worker_id,
        session_id=case.entry.dispatch.worker_session_id,
        attempt_id=case.entry.dispatch.attempt_id,
        role="journal",
    )

    def upload():
        with sdk(h, loop, token=case.credential.token) as worker:
            return worker.worker_upload_file(intent, path, idempotency_key="malformed")

    altered = await asyncio.to_thread(upload)
    case.request["payload"]["journal_artifact"] = altered.model_dump(mode="json")
    submission = await submit(case)
    assert submission["result"]["state"] == "quarantined"
    before = len(h.witness.requests)
    with pytest.raises(ResearchError) as invalid:
        await reconcile(case, reconciliation_request(case, submission))
    assert (
        invalid.value.code
        == {
            "schema": ErrorCode.PROJECT_CONTRACT_INVALID,
            "unknown": ErrorCode.EXECUTION_UNKNOWN,
            "grant": ErrorCode.STALE_FENCE,
        }[fault]
    )
    assert len(h.witness.requests) == before + (fault != "schema")
    assert scientific_counts(h) == (0, 0, 0)


async def test_witness_outage_has_no_submission_and_same_intent_can_retry(reconciliation):
    case, h = reconciliation, reconciliation.h
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Fixture unavailable")
    before = counts(h)
    with pytest.raises(ResearchError) as offline:
        await submit(case)
    assert offline.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    assert counts(h) == before
    h.witness.failure = None
    submission = await submit(case)
    assert submission["result"]["state"] == "quarantined"
    assert scientific_counts(h) == (0, 0, 0)


async def test_competing_reconciliation_intents_share_one_settlement(reconciliation):
    case, h = reconciliation, reconciliation.h
    request = reconciliation_request(case, await submit(case))
    revision = current(h, "attempt", request["attempt_id"])["revision"]
    outcomes = await asyncio.gather(
        reconcile(case, request, key="first", revision=revision),
        reconcile(case, request, key="second", revision=revision),
        return_exceptions=True,
    )
    assert sum(isinstance(value, dict) for value in outcomes) == 1
    refused = next(value for value in outcomes if isinstance(value, ResearchError))
    assert refused.code == ErrorCode.REVISION_CONFLICT
    assert scientific_counts(h) == (1, 1, 0)


async def test_older_profile_keeps_its_explicit_reconciliation_refusal(publication):
    case, h = publication, publication.h
    before = counts(h), len(h.transport.calls), len(h.witness.requests)
    with pytest.raises(ResearchError) as unavailable:
        await submit(case)
    assert unavailable.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert "worker.submit_evidence" not in capabilities(h.config)["commands"]
    assert (counts(h), len(h.transport.calls), len(h.witness.requests)) == before
