"""Portable observation parsing and role selection retain native evidence authority."""

import pytest

from research_swarm.control.observations import PublishAttempt
from research_swarm.control.results import MAX_OBSERVATION_BYTES, decode_observation_bytes
from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_worker_inputs import revoke_worker
from tests.test_worker_outputs import intent, output_client


def reference(content):
    return ArtifactRef(id=new_id(), digest=content_digest(content), size_bytes=len(content))


def test_observation_decoder_matches_canonical_object_without_storage():
    value = {"schema_version": "1.0", "finding": "synthetic evidence", "metrics": [1.25]}
    content = canonical_bytes(value)
    assert decode_observation_bytes(reference(content), content) == value


@pytest.mark.parametrize("fault", ["digest", "short", "long", "declared_limit", "actual_limit"])
def test_decoder_refuses_unbound_or_oversized_observation(fault):
    content = b'{"result":1}'
    ref = reference(content)
    if fault == "digest":
        content = b'{"result":2}'
    elif fault == "short":
        content = content[:-1]
    elif fault == "long":
        content += b" "
    elif fault == "declared_limit":
        ref = ref.model_copy(update={"size_bytes": MAX_OBSERVATION_BYTES + 1})
    else:
        content = b" " * (MAX_OBSERVATION_BYTES + 1)
        ref = reference(content)
    with pytest.raises(ResearchError) as invalid:
        decode_observation_bytes(ref, content)
    assert invalid.value.code == ErrorCode.ARTIFACT_UNVERIFIED


@pytest.mark.parametrize("content", [b"{} trailing", b'{"a":1,"a":2}', b'{"n":NaN}', b"\xff"])
def test_observation_decoder_preserves_strict_json(content):
    with pytest.raises(ValueError):
        decode_observation_bytes(reference(content), content)


@pytest.mark.parametrize("content", [b"[]", b"null", b"42", b'"string"'])
def test_observation_decoder_requires_object(content):
    with pytest.raises(ResearchError) as invalid:
        decode_observation_bytes(reference(content), content)
    assert invalid.value.code == ErrorCode.PROJECT_CONTRACT_INVALID


def test_selection_checks_every_role_before_coalescing_canonical_bytes(pipeline, tmp_path):
    client, _, credential, target = output_client(pipeline)
    path = tmp_path / "observation.json"
    path.write_bytes(b"{}")
    journal = client.worker_upload_file(intent(target, role="journal"), path, idempotency_key="j")
    request = PublishAttempt(
        attempt_id=target.attempt_id, journal_artifact=journal, report_artifact=journal
    )
    results = pipeline.runtime.service.results
    context = CommandContext(principal=credential.principal, idempotency_key=new_id())

    def select(sql):
        transaction = Transaction(sql, context)
        attempt, worker = results._publication_owner_in(transaction, request)
        return results._evidence_records_in(
            transaction, request, attempt, worker, scope="worker.publish"
        )

    with pytest.raises(ResearchError, match="receipt is absent"):
        pipeline.runtime.database.store.read(select)
    report = client.worker_upload_file(intent(target, role="report"), path, idempotency_key="r")
    assert report == journal
    selected = pipeline.runtime.database.store.read(select)
    assert list(selected) == [journal.id]
    assert selected[journal.id].reference == journal


def test_legacy_evidence_read_and_reconciliation_scopes_stay_distinct(pipeline, tmp_path):
    client, _, credential, target = output_client(pipeline)
    path = tmp_path / "legacy.json"
    path.write_bytes(b"{}")
    journal = client.worker_upload_file(intent(target, role="journal"), path, idempotency_key="j")
    request = PublishAttempt(attempt_id=target.attempt_id, journal_artifact=journal)
    results = pipeline.runtime.service.results
    context = CommandContext(principal=credential.principal, idempotency_key=new_id())
    attempt, worker = pipeline.runtime.database.store.read(
        lambda sql: results._publication_owner_in(Transaction(sql, context), request)
    )
    legacy = worker.model_copy(update={"project_ids": None})

    def select(scope, grants):
        caller = context.model_copy(
            update={"principal": context.principal.model_copy(update={"scopes": frozenset(grants)})}
        )
        return pipeline.runtime.database.store.read(
            lambda sql: results._evidence_records_in(
                Transaction(sql, caller), request, attempt, legacy, scope=scope
            )
        )

    with pytest.raises(ResearchError) as denied:
        select("worker.publish", {"worker.publish"})
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert select("worker.publish", {"worker.publish", "read"})[journal.id].reference == journal
    assert select("attempt.reconcile", {"attempt.reconcile"})[journal.id].reference == journal
    with pytest.raises(ResearchError):
        select("attempt.reconcile", {"read"})


def test_current_publication_owner_cannot_be_replaced_by_historical_evidence_authority(
    pipeline, tmp_path
):
    client, worker, credential, target = output_client(pipeline)
    path = tmp_path / "journal.json"
    path.write_bytes(b"{}")
    journal = client.worker_upload_file(intent(target, role="journal"), path, idempotency_key="j")
    request = PublishAttempt(attempt_id=target.attempt_id, journal_artifact=journal)
    results = pipeline.runtime.service.results
    context = CommandContext(principal=credential.principal, idempotency_key=new_id())
    revoke_worker(pipeline, worker)
    with pytest.raises(ResearchError) as refused:
        pipeline.runtime.database.store.read(
            lambda sql: results._publication_owner_in(Transaction(sql, context), request)
        )
    assert refused.value.code == ErrorCode.STALE_FENCE
