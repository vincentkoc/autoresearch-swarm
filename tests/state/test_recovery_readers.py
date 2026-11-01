"""Recovery reader cleanup needs actual owner closure and an unchanged pin set."""

import subprocess
import sys

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import ResearchClient
from research_swarm.domain.artifacts import ReaderSessionTarget
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.reader_sessions import OpaqueReaderOwnership
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_http_delivery import serving
from tests.state.test_recovery_operations import recovery as recovery
from tests.state.test_recovery_operations import upload

READER = """
import os, sys
from contextlib import ExitStack
from pathlib import Path
from research_swarm.runtime import LocalRuntime
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.control.observations import PublishAttempt
from research_swarm.control.reconciliation import ReconcileAttempt
from research_swarm.control.recovery_operations import recovery_context
from research_swarm.state.database import Transaction
runtime = LocalRuntime.open(Path(sys.argv[1]))
principal = Principal.model_validate_json(sys.argv[2])
request = PublishAttempt.model_validate_json(sys.argv[3])
worker_context = CommandContext(principal=principal, idempotency_key=new_id())
worker, attempt = runtime.database.store.read(
    lambda sql: runtime.service.reconciliation._owner(Transaction(sql, worker_context), request))
worker_context = recovery_context(
    runtime.database, worker_context, 'worker.submit_evidence', request.model_dump(mode='json'),
    attempt_id=request.attempt_id, artifact_id=request.journal_artifact.id)
operator = CommandContext(principal=runtime.principal(), idempotency_key=new_id())
closure = ReconcileAttempt(
    attempt_id=request.attempt_id, submission_id=sys.argv[4],
    expected_controller_id=runtime.identity.controller_id,
    expected_control_epoch=attempt.dispatch.control_epoch+1, reason='Fixture original journal read')
operator = recovery_context(
    runtime.database, operator, 'attempt.reconcile_closure', closure.model_dump(mode='json'),
    attempt_id=request.attempt_id, artifact_id=request.journal_artifact.id)
with ExitStack() as stack:
    stack.enter_context(runtime.service.results.pin_evidence(
        worker_context, request, attempt, worker, scope='worker.publish'))
    stack.enter_context(runtime.service.results.pin_evidence(
        operator, request, attempt, worker, scope='attempt.reconcile'))
    print(runtime.artifacts.reader_session_id, flush=True)
    if sys.argv[5] == 'hold':
        sys.stdin.readline()
    os._exit(0)
"""


def reader_command(recovery, mode="exit"):
    from research_swarm.control.observations import PublishAttempt

    reference, submission = upload(
        recovery, recovery.native.journal.get(recovery.target.attempt_id)
    )
    request = PublishAttempt(attempt_id=recovery.target.attempt_id, journal_artifact=reference)
    principal = recovery.runtime.credentials.resolve(recovery.token)
    return [
        sys.executable,
        "-c",
        READER,
        str(recovery.runtime.root),
        principal.model_dump_json(),
        request.model_dump_json(),
        submission["id"],
        mode,
    ]


@pytest.fixture
def crashed(recovery):
    result = subprocess.run(
        reader_command(recovery), check=True, capture_output=True, text=True, timeout=20
    )
    return result.stdout.strip()


def refs(recovery, session):
    with recovery.runtime.database.connect() as sql:
        return [
            dict(row)
            for row in sql.execute(
                "SELECT id,state,revision,payload FROM records WHERE kind='artifact_reference' "
                "AND json_extract(payload,'$.reader_session_id')=? ORDER BY id",
                (session,),
            )
        ]


def context(recovery):
    return CommandContext(principal=recovery.runtime.principal(), idempotency_key=new_id())


def test_http_operator_closes_dead_reader_with_multiple_pin_actors(recovery, crashed):
    before = refs(recovery, crashed)
    assert len(before) == 2 and all(item["state"] == "retained" for item in before)
    budget = recovery.operator.budget(recovery.campaign_id)
    from research_swarm.runtime import read_private

    token = read_private(recovery.runtime.root, "operator.token", limit=256).decode()
    with (
        serving(create_app(recovery.runtime.service, recovery.runtime.credentials)) as url,
        ResearchClient.connect(url, token) as client,
    ):
        reply = client.command(
            "artifact.reconcile_reader_session",
            ReaderSessionTarget(session_id=crashed),
            idempotency_key="dead-reader",
        )
        assert reply["result"]["state"] == "closed"
        assert (
            client.command(
                "artifact.reconcile_reader_session",
                ReaderSessionTarget(session_id=crashed),
                idempotency_key="dead-reader",
            )
            == reply
        )
    assert all(item["state"] == "released" for item in refs(recovery, crashed))
    assert recovery.operator.budget(recovery.campaign_id) == budget
    assert (
        recovery.operator.get("workspace_gate", recovery.runtime.identity.workspace_id)["state"]
        == "recovering"
    )


def test_live_owner_refuses_then_actual_process_exit_allows_cleanup(recovery):
    child = subprocess.Popen(
        reader_command(recovery, "hold"),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        session = child.stdout.readline().strip()
        assert session and child.poll() is None
        before = refs(recovery, session)
        with pytest.raises(ResearchError, match="still alive"):
            recovery.runtime.artifacts.reconcile_reader_session(context(recovery), session)
        assert refs(recovery, session) == before
    finally:
        child.communicate("finish\n", timeout=15)
    assert child.returncode == 0
    recovery.runtime.artifacts.reconcile_reader_session(context(recovery), session)
    assert all(row["state"] == "released" for row in refs(recovery, session))


def test_cleanup_rolls_back_all_pins_if_second_release_fails(recovery, crashed, monkeypatch):
    before = refs(recovery, crashed)
    original = ArtifactCatalog.release
    calls = 0

    def fail(transaction, reference_id, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("Fixture second pin release interrupted")
        return original(transaction, reference_id, **kwargs)

    command = context(recovery)
    with monkeypatch.context() as patch:
        patch.setattr(ArtifactCatalog, "release", fail)
        with pytest.raises(OSError, match="second pin"):
            recovery.runtime.artifacts.reconcile_reader_session(command, crashed)
    assert refs(recovery, crashed) == before
    assert recovery.operator.get("artifact_read_session", crashed)["state"] == "active"
    reply = recovery.runtime.artifacts.reconcile_reader_session(command, crashed)
    assert reply["result"]["state"] == "closed"


@pytest.mark.parametrize("changed", ["owner", "reference", "added_reference"])
def test_reader_proof_rechecks_exact_session_and_reference_set(
    recovery, crashed, monkeypatch, changed
):
    original = recovery.runtime.database.execute

    def execute(ctx, operation, *args, **kwargs):
        if operation == "artifact.reconcile_reader_session":
            with recovery.runtime.database.connect() as sql:
                if changed == "owner":
                    sql.execute(
                        "UPDATE records SET revision=revision+1 "
                        "WHERE kind='artifact_read_session' AND id=?",
                        (crashed,),
                    )
                elif changed == "reference":
                    sql.execute(
                        "UPDATE records SET revision=revision+1 WHERE kind='artifact_reference' "
                        "AND json_extract(payload,'$.reader_session_id')=?",
                        (crashed,),
                    )
                else:
                    sql.execute(
                        "INSERT INTO records SELECT workspace_id,kind,?,revision,state,campaign_id,"
                        "parent_id,payload,created_at,updated_at FROM records "
                        "WHERE kind='artifact_reference' "
                        "AND json_extract(payload,'$.reader_session_id')=? LIMIT 1",
                        (new_id(), crashed),
                    )
        return original(ctx, operation, *args, **kwargs)

    monkeypatch.setattr(recovery.runtime.database, "execute", execute)
    with pytest.raises(ResearchError) as refused:
        recovery.runtime.artifacts.reconcile_reader_session(context(recovery), crashed)
    assert refused.value.code == ErrorCode.STALE_FENCE
    assert all(row["state"] == "retained" for row in refs(recovery, crashed))


def test_committed_cleanup_replays_without_native_proof_and_bare_command_cannot_bypass(
    recovery, crashed
):
    command = context(recovery)
    with pytest.raises(ResearchError):
        recovery.runtime.database.execute(
            command,
            "artifact.reconcile_reader_session",
            {"session_id": crashed},
            "artifact.admin",
            lambda transaction: {"bypass": True},
        )
    reply = recovery.runtime.artifacts.reconcile_reader_session(command, crashed)
    recovery.runtime.artifacts.reader_ownership = OpaqueReaderOwnership()
    assert recovery.runtime.artifacts.reconcile_reader_session(command, crashed) == reply
    assert (
        recovery.runtime.artifacts.reconcile_reader_session(context(recovery), crashed)["result"][
            "state"
        ]
        == "closed"
    )


@pytest.mark.parametrize("proof", ["opaque", "foreign_host"])
def test_unqualified_owner_proof_does_not_release_recovery_pins(recovery, crashed, proof):
    from research_swarm.domain.canonical import strict_json_loads

    if proof == "opaque":
        recovery.runtime.artifacts.reader_ownership = OpaqueReaderOwnership()
    else:
        row = recovery.operator.get("artifact_read_session", crashed)
        identity = strict_json_loads(row["data"]["identity"])
        identity["host"] = "foreign-fixture-host"
        from research_swarm.domain.canonical import canonical_bytes

        with recovery.runtime.database.connect() as sql:
            sql.execute(
                "UPDATE records SET payload=json_set(payload,'$.identity',?) "
                "WHERE kind='artifact_read_session' AND id=?",
                (canonical_bytes(identity).decode(), crashed),
            )
    before = refs(recovery, crashed)
    with pytest.raises(ResearchError) as refused:
        recovery.runtime.artifacts.reconcile_reader_session(context(recovery), crashed)
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert refs(recovery, crashed) == before
