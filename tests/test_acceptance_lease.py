from __future__ import annotations

import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.acceptance import after
from research_swarm.control.acceptance_models import ExpireAcceptance, LeaseRenewal
from research_swarm.control.queries import RecordQuery
from research_swarm.control.worker_lifecycle import RequestAttemptCancellation, WorkerHeartbeat
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.controls import ControlAcknowledgment
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Database
from research_swarm.workers.journal import JournalState
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_reconciliation import ClosedAttempt, reconcile, submit
from tests.test_reconciliation import closed as closed


def lease(pipeline: Pipeline, attempt_id: str) -> dict[str, Any]:
    return pipeline.client.get("acceptance_lease", attempt_id)


def expire(pipeline: Pipeline, row: dict[str, Any], key: str | None = None) -> dict[str, Any]:
    return pipeline.client.command(
        "attempt.expire_acceptance",
        ExpireAcceptance(attempt_id=row["id"]),
        idempotency_key=key or new_id(),
        expected_revision=row["revision"],
    )


def clock(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch, row: dict[str, Any], now: str
) -> None:
    monkeypatch.setattr(
        pipeline.runtime.service.acceptance,
        "time",
        lambda worker: (
            now,
            row["data"]["clock_epoch"],
            row["data"]["clock_policy_digest"],
        ),
    )


def heartbeat(pipeline: Pipeline, row: dict[str, Any], *, sequence: int = 1) -> WorkerHeartbeat:
    return WorkerHeartbeat(
        binding=pipeline.worker.worker.binding,
        driver_instance_id=new_id(),
        sequence=sequence,
        lease_renewals=(
            LeaseRenewal(
                attempt_id=row["id"],
                dispatch_digest=row["data"]["dispatch_digest"],
                expected_lease_revision=row["revision"],
            ),
        ),
    )


def test_claim_lease_is_distinct_from_immutable_native_dispatch_and_renews_exactly_once(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    original = lease(pipeline, attempt["id"])
    assert original["data"]["issued_at"] < attempt["data"]["dispatch"]["expires_at"]
    assert attempt["data"]["dispatch"]["hard_deadline_at"] < original["data"]["expires_at"]
    assert original["data"]["expires_at"] < original["data"]["renew_until"]
    before = pipeline.client.budget(pipeline.campaign["id"])
    clock(pipeline, monkeypatch, original, after(original["data"]["expires_at"], -1000))
    request = heartbeat(pipeline, original)
    ack = pipeline.worker.client.command("worker.heartbeat", request, idempotency_key="renew")
    renewed = ack["result"]["lease_renewals"][0]
    assert renewed["disposition"] == "renewed"
    assert renewed["lease"]["revision"] == original["revision"] + 1
    assert renewed["lease"]["data"]["expires_at"] > original["data"]["expires_at"]
    assert (
        pipeline.worker.client.command("worker.heartbeat", request, idempotency_key="renew") == ack
    )
    assert lease(pipeline, attempt["id"]) == renewed["lease"]
    assert pipeline.client.get("attempt", attempt["id"]) == attempt
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    stale = pipeline.worker.client.command(
        "worker.heartbeat", heartbeat(pipeline, original, sequence=2), idempotency_key="stale"
    )
    assert stale["result"]["lease_renewals"][0]["disposition"] == "conflict"


def test_exact_expiry_preserves_cancellation_acknowledgments_and_cannot_revive(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    original = lease(pipeline, attempt["id"])
    cancellation = pipeline.client.command(
        "attempt.request_cancel",
        RequestAttemptCancellation(attempt_id=attempt["id"], reason="Exact owned scope"),
        idempotency_key="cancel",
        expected_revision=attempt["revision"],
    )["result"]["control"]["data"]
    request = heartbeat(pipeline, original).model_copy(
        update={
            "acknowledgments": (
                ControlAcknowledgment(
                    attempt_id=attempt["id"],
                    control_sequence=cancellation["control_sequence"],
                    dispatch_digest=cancellation["dispatch_digest"],
                ),
            )
        }
    )
    clock(pipeline, monkeypatch, original, original["data"]["expires_at"])
    result = pipeline.worker.client.command(
        "worker.heartbeat", request, idempotency_key="late-heartbeat"
    )["result"]
    assert result["lease_renewals"][0]["disposition"] == "expired"
    assert pipeline.client.get("attempt_control", attempt["id"])["state"] == "acknowledged"
    assert pipeline.client.get("attempt", attempt["id"])["state"] == "execution_unknown"
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert all(item["reserved"] == "0" and item["consumed"] == "0" for item in accounts)
    assert all(int(item["uncertain"]) > 0 for item in accounts)
    expired = lease(pipeline, attempt["id"])
    clock(pipeline, monkeypatch, original, original["data"]["issued_at"])
    replay = pipeline.worker.client.command(
        "worker.heartbeat", heartbeat(pipeline, expired, sequence=2), idempotency_key="no-revive"
    )
    assert replay["result"]["lease_renewals"][0]["disposition"] == "expired"
    assert lease(pipeline, attempt["id"]) == expired


@pytest.mark.parametrize("reason", ["backward", "epoch", "policy"])
def test_clock_discontinuity_requires_reconciliation(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
    reason: str,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    original = lease(pipeline, attempt["id"])
    now, epoch, policy = original["data"]["issued_at"], None, None
    if reason == "backward":
        now = after(now, -1)
    elif reason == "epoch":
        epoch = new_id()
    else:
        policy = "sha256:" + "0" * 64
    monkeypatch.setattr(
        pipeline.runtime.service.acceptance, "time", lambda worker: (now, epoch, policy)
    )
    result = expire(pipeline, original)["result"]
    assert result["state"] == "unqualified"
    assert pipeline.client.get("attempt", attempt["id"])["state"] == "execution_unknown"


def test_publication_checks_expiry_before_sweep_and_quarantines_original_closure(
    pipeline: Pipeline,
    closed: ClosedAttempt,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt_id = closed.publication.attempt_id
    original = lease(pipeline, attempt_id)
    clock(pipeline, monkeypatch, original, original["data"]["expires_at"])
    with pytest.raises(ResearchError) as rejected:
        closed.native.execute(attempt_id)
    assert rejected.value.code == ErrorCode.ACCEPTANCE_EXPIRED
    assert lease(pipeline, attempt_id)["state"] == "expired"
    submissions = pipeline.client.list(RecordQuery(kind="attempt_submission"))["items"]
    assert len(submissions) == 1
    submission = pipeline.client.get("attempt_submission", submissions[0]["id"])
    assert submission["data"]["publication"] == closed.publication.model_dump(mode="json")
    assert not pipeline.client.list(RecordQuery(kind="run_result"))["items"]
    reopened = LocalRuntime.open(pipeline.runtime.root)
    with ResearchClient(LocalTransport(reopened)) as operator:
        accepted = reconcile(pipeline, submission, client=operator)
    assert accepted["attempt"]["state"] == "succeeded"
    receipt = accepted["reconciliation"]["data"]
    assert receipt["acceptance_disposition"] == "expired"
    assert receipt["acceptance_lease"]["dispatch_digest"] == original["data"]["dispatch_digest"]
    assert lease(pipeline, attempt_id)["state"] == "settled"
    budget = pipeline.client.budget(pipeline.campaign["id"])
    assert reconcile(pipeline, submission)["disposition"] == "already_accepted"
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget
    assert closed.native.journal.get(attempt_id).state == JournalState.CLOSED


@pytest.mark.parametrize("pipeline", [{"source_finish_delay": 2.0}], indirect=True)
def test_lease_expiry_during_real_native_execution_keeps_original_custody(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = pipeline.worker
    attempt = native.claim(pipeline.ready())
    original = lease(pipeline, attempt["id"])
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(native.execute, attempt["id"])
        deadline = time.monotonic() + 10
        while True:
            try:
                running = native.journal.get(attempt["id"])
            except ResearchError as error:
                assert error.code == ErrorCode.NOT_FOUND
            else:
                if running.state == JournalState.RUNNING:
                    break
            assert not future.done() and time.monotonic() < deadline
            time.sleep(0.01)
        clock(pipeline, monkeypatch, original, original["data"]["expires_at"])
        acknowledgment = expire(pipeline, original, "expire-live")
        assert acknowledgment["result"]["state"] == "expired"
        assert not future.done()
        assert native.resources.availability() == {"cpu": 0}
        held = native.journal.get(attempt["id"])
        assert held.native == running.native and not held.cancel_requested
        assert pipeline.client.get("attempt", attempt["id"])["state"] == "execution_unknown"
        assert all(
            int(item["uncertain"]) > 0
            for item in pipeline.client.budget(pipeline.campaign["id"])["accounts"]
        )
        with pytest.raises(ResearchError) as late:
            future.result(timeout=15)
        assert late.value.code == ErrorCode.ACCEPTANCE_EXPIRED
    ended = native.journal.get(attempt["id"])
    assert ended.state == JournalState.CLOSED and ended.native == running.native
    assert native.resources.availability() == {"cpu": 1}
    submission_row = pipeline.client.list(RecordQuery(kind="attempt_submission"))["items"][0]
    submission = pipeline.client.get("attempt_submission", submission_row["id"])
    assert reconcile(pipeline, submission)["attempt"]["state"] == "succeeded"
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 1
    assert expire(pipeline, original, "expire-live") == acknowledgment
    accounts = pipeline.client.budget(pipeline.campaign["id"])["accounts"]
    assert all(item["reserved"] == item["uncertain"] == "0" for item in accounts)
    assert next(item for item in accounts if item["dimension"] == "attempts")["consumed"] == "1"


def test_two_connections_cannot_both_renew_the_same_lease_revision(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    original = lease(pipeline, attempt["id"])
    request = heartbeat(pipeline, original)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(
                pipeline.worker.client.command,
                "worker.heartbeat",
                request,
                idempotency_key=new_id(),
            )
            for _ in range(2)
        ]
        replies, failures = [], []
        for future in futures:
            try:
                replies.append(future.result())
            except ResearchError as error:
                failures.append(error.code)
    assert len(replies) == 1 and failures == [ErrorCode.REVISION_CONFLICT]
    assert lease(pipeline, attempt["id"])["revision"] == original["revision"] + 1


def test_v2_pending_attempts_are_unqualified_without_rewriting_signed_records(
    pipeline: Pipeline,
    closed: ClosedAttempt,
) -> None:
    attempt_id = closed.publication.attempt_id
    attempt = pipeline.client.get("attempt", attempt_id)
    with pipeline.runtime.database.connect() as connection:
        connection.execute("DELETE FROM records WHERE kind='acceptance_lease'")
        connection.execute("DELETE FROM records WHERE kind='worker_session'")
        connection.execute("DROP INDEX acceptance_leases")
        connection.execute("PRAGMA user_version=2")
    reopened = LocalRuntime.open(pipeline.runtime.root)
    assert pipeline.client.get("attempt", attempt_id) == attempt
    with reopened.database.connect() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
    expired = pipeline.client.command(
        "attempt.expire_acceptance",
        ExpireAcceptance(attempt_id=attempt_id),
        idempotency_key="legacy-expire",
        expected_revision=0,
    )["result"]
    assert expired["state"] == "unqualified"
    assert pipeline.client.get("attempt", attempt_id)["state"] == "execution_unknown"
    with pytest.raises(ResearchError) as unqualified:
        closed.client.command(
            "worker.publish_attempt", closed.publication, idempotency_key="legacy-publish"
        )
    assert unqualified.value.code == ErrorCode.ACCEPTANCE_EXPIRED
    submission = submit(closed)
    assert lease(pipeline, attempt_id)["data"]["policy_version"] == "legacy-unqualified-v1"
    assert lease(pipeline, attempt_id)["data"]["issued_at"] is None
    assert reconcile(pipeline, submission)["attempt"]["state"] == "succeeded"
    assert (
        pipeline.client.get("attempt", attempt_id)["data"]["dispatch"]
        == attempt["data"]["dispatch"]
    )


def test_old_heartbeat_wire_identity_omits_empty_renewals(pipeline: Pipeline) -> None:
    fields = dict(
        binding=pipeline.worker.worker.binding.model_dump(mode="json"),
        driver_instance_id=new_id(),
        sequence=1,
        acknowledgments=[],
        schema_version="1.0",
    )
    assert canonical_bytes(
        WorkerHeartbeat.model_validate(fields).model_dump(mode="json")
    ) == canonical_bytes(fields)


def test_renewal_has_a_fixed_final_cap_and_cannot_extend_native_deadlines(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    original = lease(pipeline, attempt["id"])
    row = original
    for sequence in range(1, 8):
        clock(pipeline, monkeypatch, row, after(row["data"]["expires_at"], -1))
        result = pipeline.worker.client.command(
            "worker.heartbeat",
            heartbeat(pipeline, row, sequence=sequence),
            idempotency_key=new_id(),
        )["result"]["lease_renewals"][0]
        assert result["disposition"] == "renewed"
        row = result["lease"]
        assert row["data"]["expires_at"] <= original["data"]["renew_until"]
    assert row["data"]["expires_at"] == original["data"]["renew_until"]
    assert pipeline.client.get("attempt", attempt["id"]) == attempt
    clock(pipeline, monkeypatch, row, row["data"]["renew_until"])
    result = pipeline.worker.client.command(
        "worker.heartbeat", heartbeat(pipeline, row, sequence=8), idempotency_key=new_id()
    )["result"]["lease_renewals"][0]
    assert result["disposition"] == "expired"


def test_accepted_publication_acknowledgment_survives_clock_and_lease_changes(
    pipeline: Pipeline,
    closed: ClosedAttempt,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt_id = closed.publication.attempt_id
    original = lease(pipeline, attempt_id)
    accepted = closed.client.command(
        "worker.publish_attempt", closed.publication, idempotency_key="accepted-before-expiry"
    )
    before = pipeline.client.budget(pipeline.campaign["id"])
    clock(pipeline, monkeypatch, original, after(original["data"]["renew_until"], 1))
    assert (
        closed.client.command(
            "worker.publish_attempt", closed.publication, idempotency_key="accepted-before-expiry"
        )
        == accepted
    )
    assert (
        closed.client.command(
            "worker.publish_attempt", closed.publication, idempotency_key="accepted-again"
        )["result"]
        == accepted["result"]
    )
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    assert lease(pipeline, attempt_id)["state"] == "settled"


def test_scoped_http_worker_reads_only_its_own_lease(pipeline: Pipeline) -> None:
    from research_swarm.api.http import create_app
    from tests.test_http_delivery import serving
    from tests.test_worker_queries import claim, read, scoped_worker

    client, worker, token = scoped_worker(pipeline)
    attempt = claim(client, worker, pipeline.ready())
    _, other, other_token = scoped_worker(pipeline)
    with serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url:
        with ResearchClient.connect(url, token) as remote:
            row = remote.worker_get(read(worker, "acceptance_lease", attempt["id"]))
            assert row == lease(pipeline, attempt["id"])
        with ResearchClient.connect(url, other_token) as foreign:
            with pytest.raises(ResearchError) as forbidden:
                foreign.worker_get(read(other, "acceptance_lease", attempt["id"]))
            assert forbidden.value.code == ErrorCode.AUTHORIZATION_DENIED


@pytest.mark.parametrize("discontinuity", ["expiry", "backward"])
def test_direct_publication_latches_observed_rejection_across_clock_recovery(
    pipeline: Pipeline,
    closed: ClosedAttempt,
    monkeypatch: pytest.MonkeyPatch,
    discontinuity: str,
) -> None:
    attempt_id = closed.publication.attempt_id
    row = lease(pipeline, attempt_id)
    rejected_at = (
        row["data"]["expires_at"]
        if discontinuity == "expiry"
        else after(row["data"]["issued_at"], -1)
    )
    clock(pipeline, monkeypatch, row, rejected_at)
    with pytest.raises(ResearchError) as first:
        closed.client.command(
            "worker.publish_attempt", closed.publication, idempotency_key="direct-rejection"
        )
    assert first.value.code == ErrorCode.ACCEPTANCE_EXPIRED
    assert lease(pipeline, attempt_id)["state"] == (
        "expired" if discontinuity == "expiry" else "unqualified"
    )
    assert not pipeline.client.list(RecordQuery(kind="attempt_submission"))["items"]
    clock(pipeline, monkeypatch, row, after(row["data"]["issued_at"], 1))
    with pytest.raises(ResearchError) as again:
        closed.client.command(
            "worker.publish_attempt",
            closed.publication,
            idempotency_key="new-key-after-clock-recovery",
        )
    assert again.value.code == ErrorCode.ACCEPTANCE_EXPIRED
    assert not pipeline.client.list(RecordQuery(kind="run_result"))["items"]
    submission = submit(closed)
    assert reconcile(pipeline, submission)["attempt"]["state"] == "succeeded"


def test_rejection_and_lease_fence_roll_back_together_if_accounting_cannot_commit(
    pipeline: Pipeline,
    closed: ClosedAttempt,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from research_swarm.state.ledger import BudgetLedger

    attempt_id = closed.publication.attempt_id
    original = lease(pipeline, attempt_id)
    before = pipeline.client.budget(pipeline.campaign["id"])
    clock(pipeline, monkeypatch, original, original["data"]["expires_at"])
    record_uncertainty = BudgetLedger.mark_uncertain

    def fail(transaction: Any, identifier: str) -> Any:
        record_uncertainty(transaction, identifier)
        raise OSError("Synthetic atomic fence failure")

    with monkeypatch.context() as patch:
        patch.setattr(BudgetLedger, "mark_uncertain", fail)
        with pytest.raises(OSError, match="atomic fence"):
            closed.client.command(
                "worker.publish_attempt", closed.publication, idempotency_key="atomic-rejection"
            )
    assert lease(pipeline, attempt_id) == original
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    with pipeline.runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM commands WHERE idempotency_key=?", ("atomic-rejection",)
            ).fetchone()[0]
            == 0
        )
    with pytest.raises(ResearchError) as rejected:
        closed.client.command(
            "worker.publish_attempt", closed.publication, idempotency_key="atomic-rejection"
        )
    assert rejected.value.code == ErrorCode.ACCEPTANCE_EXPIRED
    assert lease(pipeline, attempt_id)["state"] == "expired"


def test_clock_failure_between_renewal_checks_is_latched_without_a_third_observation(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = pipeline.worker.claim(pipeline.ready())
    row = lease(pipeline, attempt["id"])
    ticks = iter((after(row["data"]["issued_at"], 1), after(row["data"]["issued_at"], -1)))
    monkeypatch.setattr(
        pipeline.runtime.service.acceptance, "time", lambda worker: (next(ticks), None, None)
    )
    reply = pipeline.worker.client.command(
        "worker.heartbeat", heartbeat(pipeline, row), idempotency_key="clock-between-checks"
    )
    assert reply["result"]["lease_renewals"][0]["disposition"] == "unqualified"
    assert lease(pipeline, attempt["id"])["state"] == "unqualified"


def test_newer_schema_is_rejected_before_mutation(tmp_path: Path) -> None:
    path = tmp_path / "newer.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version=99")
    with pytest.raises(ResearchError) as unknown:
        Database(path)
    assert unknown.value.code == ErrorCode.SCHEMA_UNSUPPORTED
