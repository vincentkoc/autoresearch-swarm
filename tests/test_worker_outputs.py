from __future__ import annotations

import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.auth import IssuedCredential
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.results import PublishAttempt
from research_swarm.control.worker_artifacts import WorkerAttemptTarget
from research_swarm.control.worker_outputs import (
    BeginWorkerUpload,
    WorkerOutputIntent,
    WorkerUploadTarget,
)
from research_swarm.control.workers import WorkerData
from research_swarm.domain.artifacts import StoredBlob, UploadSpec
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.state.artifacts import BeginUpload
from research_swarm.workers.local import LocalWorker
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_worker_inputs import assigned_input, revoke_worker
from tests.test_worker_queries import query


def output_client(
    pipeline: Pipeline,
) -> tuple[ResearchClient, WorkerData, IssuedCredential, WorkerAttemptTarget]:
    _, worker, prior, target = assigned_input(pipeline)
    runtime = pipeline.runtime
    issued = runtime.credentials.issue(
        runtime.principal(),
        prior.principal.model_copy(
            update={"scopes": prior.principal.scopes | {"worker.outputs", "worker.publish"}}
        ),
    )
    client = ResearchClient(
        LocalTransport(runtime, lambda: runtime.credentials.resolve(issued.token))
    )
    return (
        client,
        worker,
        issued,
        WorkerAttemptTarget(
            worker_id=target.worker_id, session_id=target.session_id, attempt_id=target.attempt_id
        ),
    )


def intent(target: WorkerAttemptTarget, **kwargs: Any) -> WorkerOutputIntent:
    return WorkerOutputIntent(**target.model_dump(), **kwargs)


def begin(
    client: ResearchClient, target: WorkerAttemptTarget, content: bytes
) -> WorkerUploadTarget:
    request = BeginWorkerUpload(
        **intent(target, role="journal").model_dump(),
        content=BeginUpload(digest=content_digest(content), size_bytes=len(content)),
    )
    row = client.command("worker.begin_upload", request, idempotency_key=new_id())["result"]
    return WorkerUploadTarget(**target.model_dump(), upload_id=row["id"])


def upload_state(client: ResearchClient, target: WorkerUploadTarget) -> dict[str, Any]:
    return client.transport.query("worker.output", target.model_dump(mode="json"))


def test_worker_object_limit_rejects_without_upload_or_quota_mutation(pipeline: Pipeline) -> None:
    client, _, _, target = output_client(pipeline)
    pipeline.runtime.artifacts.max_object_bytes = 2
    with pipeline.runtime.database.connect() as connection:
        before = connection.execute(
            "SELECT kind,id,payload FROM records ORDER BY kind,id"
        ).fetchall()
    with pytest.raises(ResearchError) as rejected:
        begin(client, target, b"oversized")
    assert rejected.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    with pipeline.runtime.database.connect() as connection:
        after = connection.execute(
            "SELECT kind,id,payload FROM records ORDER BY kind,id"
        ).fetchall()
    assert after == before
    accepted = begin(client, target, b"ok")
    assert upload_state(client, accepted)["state"] == "pending"


@pytest.mark.parametrize("mismatch", ["reference", "generation"])
def test_finalization_rejects_a_verified_blob_from_another_upload(
    pipeline: Pipeline, mismatch: str
) -> None:
    client, _, credential, target = output_client(pipeline)
    request = begin(client, target, b"ordinary evidence")
    client.transport.worker_append(request, 0, b"ordinary evidence")
    runtime = pipeline.runtime
    spec = UploadSpec.model_validate(upload_state(client, request)["data"])
    blob = runtime.artifacts.store.finalize(spec)
    foreign = (
        blob.model_copy(update={"generation": new_id()})
        if mismatch == "generation"
        else (
            blob.model_copy(
                update={"reference": blob.reference.model_copy(update={"size_bytes": 1})}
            )
        )
    )
    context = CommandContext(principal=credential.principal, idempotency_key="foreign-blob")
    before = runtime.database.store.read(
        lambda sql: sql.execute(
            "SELECT COUNT(*) FROM records WHERE kind IN ('artifact','artifact_reference')"
        ).one()[0]
    )
    with pytest.raises(ResearchError) as refused:
        runtime.database.execute(
            context,
            "worker.finalize_upload",
            request.model_dump(mode="json"),
            "worker.outputs",
            lambda transaction: runtime.service.worker_outputs._finalize_in(
                transaction,
                request,
                foreign,
            ),
        )
    assert refused.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    assert upload_state(client, request)["state"] == "pending"
    assert (
        runtime.database.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind IN ('artifact','artifact_reference')"
            ).one()[0]
        )
        == before
    )
    accepted = client.command("worker.finalize_upload", request, idempotency_key="correct-blob")
    assert accepted["result"]["artifact"] == blob.reference.model_dump(mode="json")


def test_restricted_http_worker_executes_and_retains_attempt_evidence(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    _, worker_data, credential, target = output_client(pipeline)
    app = create_app(pipeline.runtime.service, pipeline.runtime.credentials)
    with serving(app) as url, ResearchClient.connect(url, credential.token) as remote:
        superseded = tmp_path / "superseded.json"
        superseded.write_bytes(b'{"observation":"superseded"}')
        for key in ("old-observation", "duplicate-old-observation"):
            remote.worker_upload_file(
                intent(target, role="journal"), superseded, idempotency_key=key
            )
        worker = LocalWorker(
            remote,
            worker_data,
            root=tmp_path / "scoped-worker",
            machine_registry=tmp_path / "scoped-machine" / "resources.sqlite",
        )
        result = worker.execute(target.attempt_id)
        assert result["state"] == "succeeded"
        assert worker.execute(target.attempt_id) == result
        evaluation_job = pipeline.ready()
        authorization = pipeline.client.command(
            "evaluation.authorize",
            AuthorizeEvaluation(
                job_id=evaluation_job["id"], evaluator_id=pipeline.runtime.principal().id
            ),
            idempotency_key="authorize-scoped-evaluation",
            expected_revision=evaluation_job["revision"],
        )["result"]
        next_job = remote.worker_list(query(worker_data))["items"][0]
        second = worker.claim(next_job)
        assert worker.execute(second["id"])["state"] == "succeeded"
        receipt = pipeline.client.command(
            "evaluation.complete",
            CompleteEvaluation(request_id=authorization["id"], attempt_id=second["id"]),
            idempotency_key="complete-scoped-evaluation",
        )["result"]
        assert receipt["state"] == "valid"
        with pytest.raises(ResearchError):
            remote.get("trial", pipeline.trial["id"])
    with pipeline.runtime.database.connect() as connection:
        receipts = connection.execute(
            "SELECT id,payload FROM records WHERE kind='worker_upload' AND state='finalized'"
        ).fetchall()
        assert len(receipts) >= 8
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind='artifact_reference' AND state='retained' "
                "AND json_extract(payload,'$.retention.owner_kind')='worker_upload'"
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind='artifact_reference' AND state='retained' "
                "AND json_extract(payload,'$.retention.owner_kind')='attempt'"
            ).fetchone()[0]
            >= 8
        )


def test_canonical_dedup_keeps_separate_role_and_actor_receipts(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    client, _, credential, target = output_client(pipeline)
    path = tmp_path / "empty"
    path.write_bytes(b"")
    original = pipeline.client.upload_file(
        path, idempotency_key="original", media_type="text/plain"
    )
    for role in ("stdout", "stderr"):
        output = client.worker_upload_file(intent(target, role=role), path, idempotency_key=role)
        assert output == original
        assert (
            client.worker_upload_file(intent(target, role=role), path, idempotency_key=role)
            == output
        )
    artifact = pipeline.runtime.artifacts.get(
        CommandContext(principal=pipeline.runtime.principal(), idempotency_key=new_id()),
        original.id,
    )
    assert artifact.published_by != credential.principal.id
    with pipeline.runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind='worker_upload' AND state='finalized'"
            ).fetchone()[0]
            == 2
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind='upload' AND state='cleanup'"
            ).fetchone()[0]
            == 0
        )
    with pytest.raises(ResearchError):
        client.worker_upload_file(
            intent(target, role="output", output_name="undeclared"),
            path,
            idempotency_key="bad-output",
        )


def test_expired_launch_grant_allows_evidence_but_not_another_attempt(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _, _, target = output_client(pipeline)
    monkeypatch.setattr(
        "research_swarm.control.worker_artifacts.utc_now", lambda: "2999-01-01T00:00:00.000Z"
    )
    upload = begin(client, target, b"closure")
    client.transport.worker_append(upload, 0, b"closure")
    client.transport.worker_append(upload, 0, b"closure")
    with pytest.raises(ResearchError):
        client.transport.worker_append(
            upload.model_copy(update={"attempt_id": new_id()}), 0, b"closure"
        )
    result = client.command("worker.finalize_upload", upload, idempotency_key="finish")
    assert result["disposition"] == "accepted"
    assert upload_state(client, upload)["state"] == "finalized"


def test_finalization_ack_survives_cleanup_failure_and_revocation(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, worker, _, target = output_client(pipeline)
    upload = begin(client, target, b"")
    outputs = pipeline.runtime.service.worker_outputs

    def fail_cleanup(*args: Any, **kwargs: Any) -> None:
        revoke_worker(pipeline, worker)
        raise OSError("Synthetic cleanup failure")

    monkeypatch.setattr(outputs, "_cleanup", fail_cleanup)
    accepted = client.command("worker.finalize_upload", upload, idempotency_key="finalize")
    assert accepted["disposition"] == "accepted"
    assert client.command("worker.finalize_upload", upload, idempotency_key="finalize") == accepted
    with pytest.raises(ResearchError) as stale:
        client.command("worker.finalize_upload", upload, idempotency_key="new-key")
    assert stale.value.code == ErrorCode.STALE_FENCE


def test_failed_duplicate_cleanup_is_explicitly_resumable(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _, _, target = output_client(pipeline)
    path = tmp_path / "empty"
    path.write_bytes(b"")
    canonical = pipeline.client.upload_file(path, idempotency_key="first")
    upload = begin(client, target, b"")
    store = pipeline.runtime.artifacts.store
    delete = store.delete

    def unavailable(*args: Any) -> None:
        raise OSError("Synthetic storage interruption")

    monkeypatch.setattr(store, "delete", unavailable)
    accepted = client.command("worker.finalize_upload", upload, idempotency_key="finalize")
    assert ArtifactRef.model_validate(accepted["result"]["artifact"]) == canonical
    assert upload_state(client, upload)["state"] == "cleanup"
    monkeypatch.setattr(store, "delete", delete)
    cleaned = client.command("worker.cleanup_upload", upload, idempotency_key="cleanup")
    assert client.command("worker.cleanup_upload", upload, idempotency_key="cleanup") == cleaned
    assert upload_state(client, upload)["state"] == "finalized"
    assert client.command("worker.finalize_upload", upload, idempotency_key="finalize") == accepted


@pytest.mark.parametrize("scoped", [False, True])
def test_abort_waits_for_admitted_append_and_cannot_leave_unaccounted_bytes(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
    scoped: bool,
) -> None:
    client, _, _, target = output_client(pipeline)
    upload = begin(client, target, b"data")
    artifacts = pipeline.runtime.artifacts
    reached, release, abort_entered = threading.Event(), threading.Event(), threading.Event()
    original_begin, original_guard = artifacts.store.begin, artifacts.store.upload_guard

    def paused_begin(spec: UploadSpec) -> int:
        reached.set()
        assert release.wait(5)
        return original_begin(spec)

    @contextmanager
    def observed_guard(workspace: str, identifier: str) -> Iterator[None]:
        if reached.is_set():
            abort_entered.set()
        with original_guard(workspace, identifier):
            yield

    monkeypatch.setattr(artifacts.store, "begin", paused_begin)
    monkeypatch.setattr(artifacts.store, "upload_guard", observed_guard)
    context = CommandContext(principal=pipeline.runtime.principal(), idempotency_key="abort")
    with ThreadPoolExecutor(max_workers=2) as pool:
        append = pool.submit(client.transport.worker_append, upload, 0, b"data")
        try:
            assert reached.wait(5)
            abort = (
                pool.submit(client.command, "worker.abort_upload", upload, idempotency_key="abort")
                if scoped
                else pool.submit(artifacts.abort_upload, context, upload.upload_id)
            )
            assert abort_entered.wait(5)
            assert not abort.done()
        finally:
            release.set()
        append.result(timeout=5)
        abort.result(timeout=5)
    state = upload_state(client, upload)
    assert state["state"] == "aborted"
    assert artifacts.store.progress(UploadSpec.model_validate(state["data"])).received_bytes == 0
    with pytest.raises(ResearchError):
        client.transport.worker_append(upload, 0, b"data")


def test_scoped_worker_aborts_partial_upload_without_generic_artifact_authority(
    pipeline: Pipeline,
) -> None:
    client, _, credential, target = output_client(pipeline)
    assert "artifact.upload" not in credential.principal.scopes
    upload = begin(client, target, b"partial journal")
    client.transport.worker_append(upload, 0, b"partial")
    before = pipeline.client.budget(pipeline.campaign["id"])
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        ResearchClient.connect(url, credential.token) as remote,
    ):
        result = remote.command("worker.abort_upload", upload, idempotency_key="abort-own")
        assert remote.command("worker.abort_upload", upload, idempotency_key="abort-own") == result
        assert remote.worker_output(upload)["state"] == "aborted"
    context = CommandContext(principal=pipeline.runtime.principal(), idempotency_key="inspect")
    assert (
        pipeline.runtime.database.read_record(context, "worker_upload", upload.upload_id)["state"]
        == "aborted"
    )
    assert pipeline.runtime.artifacts.occupancy(context)["staging_reserved_bytes"] == 0
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    with pytest.raises(ResearchError):
        client.transport.worker_append(upload, 0, b"partial")


def test_worker_abort_failure_keeps_quota_and_exact_retry_finishes_cleanup(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _, _, target = output_client(pipeline)
    upload = begin(client, target, b"incomplete")
    client.transport.worker_append(upload, 0, b"inc")
    artifacts = pipeline.runtime.artifacts
    context = CommandContext(principal=pipeline.runtime.principal(), idempotency_key="inspect")
    before = pipeline.client.budget(pipeline.campaign["id"])
    delete = artifacts.store.delete
    calls = []

    def unavailable(*args: Any) -> None:
        calls.append(args)
        raise OSError("Fixture byte cleanup interruption")

    with monkeypatch.context() as patch:
        patch.setattr(artifacts.store, "delete", unavailable)
        with pytest.raises(OSError, match="interruption"):
            client.command("worker.abort_upload", upload, idempotency_key="abort")
    assert upload_state(client, upload)["state"] == "aborting"
    assert artifacts.occupancy(context)["staging_reserved_bytes"] == len(b"incomplete")
    assert pipeline.client.budget(pipeline.campaign["id"]) == before

    def finish(*args: Any) -> None:
        calls.append(args)
        delete(*args)

    monkeypatch.setattr(artifacts.store, "delete", finish)
    result = client.command("worker.abort_upload", upload, idempotency_key="abort")
    assert client.command("worker.abort_upload", upload, idempotency_key="abort") == result
    assert len(calls) == 2
    assert upload_state(client, upload)["state"] == "aborted"
    assert artifacts.occupancy(context)["staging_reserved_bytes"] == 0
    assert (
        pipeline.runtime.database.read_record(context, "worker_upload", upload.upload_id)["state"]
        == "aborted"
    )


def test_materialized_but_unpublished_worker_generation_remains_abortable(
    pipeline: Pipeline,
) -> None:
    client, _, _, target = output_client(pipeline)
    upload = begin(client, target, b"invalid journal")
    client.transport.worker_append(upload, 0, b"invalid journal")
    state = upload_state(client, upload)
    spec = UploadSpec.model_validate(state["data"])
    store = pipeline.runtime.artifacts.store
    with store.upload_guard(spec.workspace_id, spec.id):
        blob = store.finalize(spec)
    with store.reader(spec.workspace_id, blob) as stream:
        assert stream.read() == b"invalid journal"
    client.command("worker.abort_upload", upload, idempotency_key="abort-unpublished")
    with pytest.raises((ResearchError, OSError)):
        store.verify(spec.workspace_id, StoredBlob(reference=spec.reference(), generation=spec.id))
    assert upload_state(client, upload)["state"] == "aborted"


@pytest.mark.parametrize("other", ["owner", "attempt", "session"])
def test_worker_abort_refuses_other_provenance_before_mutation(
    pipeline: Pipeline,
    other: str,
) -> None:
    client, _, _, target = output_client(pipeline)
    upload = begin(client, target, b"owned")
    context = CommandContext(principal=pipeline.runtime.principal(), idempotency_key="inspect")
    before = pipeline.runtime.database.snapshot(context)
    caller = pipeline.client if other == "owner" else client
    request = upload if other == "owner" else upload.model_copy(update={other + "_id": new_id()})
    with pytest.raises(ResearchError):
        caller.command("worker.abort_upload", request, idempotency_key="wrong-owner")
    assert pipeline.runtime.database.snapshot(context) == before


@pytest.mark.parametrize("deduplicated", [False, True])
def test_worker_cannot_abort_published_or_deduplicated_output(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    deduplicated: bool,
) -> None:
    client, _, _, target = output_client(pipeline)
    path = tmp_path / "published"
    path.write_bytes(b"retained evidence")
    if deduplicated:
        pipeline.client.upload_file(path, idempotency_key="canonical")
    upload = begin(client, target, path.read_bytes())
    client.transport.worker_append(upload, 0, path.read_bytes())

    def interrupted(*args: Any, **kwargs: Any) -> None:
        raise OSError("Fixture leaves duplicate cleanup pending")

    with monkeypatch.context() as patch:
        if deduplicated:
            patch.setattr(pipeline.runtime.service.worker_outputs, "_cleanup", interrupted)
        result = client.command("worker.finalize_upload", upload, idempotency_key="publish")
    with pytest.raises(ResearchError, match="Published worker output"):
        client.command("worker.abort_upload", upload, idempotency_key="abort-published")
    reference = ArtifactRef.model_validate(result["result"]["artifact"])
    context = CommandContext(principal=pipeline.runtime.principal(), idempotency_key="inspect")
    with pipeline.runtime.artifacts.reader(context, reference) as stream:
        assert stream.read() == path.read_bytes()
    if deduplicated:
        client.command("worker.cleanup_upload", upload, idempotency_key="valid-cleanup")


def test_worker_can_abort_its_unpublished_upload_after_attempt_settles(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    client, worker_data, _, target = output_client(pipeline)
    upload = begin(client, target, b"unused observation")
    worker = LocalWorker(
        client,
        worker_data,
        root=tmp_path / "scoped-worker",
        machine_registry=tmp_path / "scoped-machine" / "resources.sqlite",
    )
    assert worker.execute(target.attempt_id)["state"] == "succeeded"
    before = pipeline.client.budget(pipeline.campaign["id"])
    resources = worker.resources.availability()
    client.command("worker.abort_upload", upload, idempotency_key="after-settlement")
    assert upload_state(client, upload)["state"] == "aborted"
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    assert worker.resources.availability() == resources


def test_completed_worker_abort_replays_after_enrollment_revocation(pipeline: Pipeline) -> None:
    client, worker, _, target = output_client(pipeline)
    upload = begin(client, target, b"unused")
    accepted = client.command("worker.abort_upload", upload, idempotency_key="abort")
    revoke_worker(pipeline, worker)
    assert client.command("worker.abort_upload", upload, idempotency_key="abort") == accepted
    with pytest.raises(ResearchError) as denied:
        client.command("worker.abort_upload", upload, idempotency_key="new-abort")
    assert denied.value.code == ErrorCode.STALE_FENCE


def test_publication_rejects_unproven_or_wrong_role_artifact_before_reading(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _, _, target = output_client(pipeline)
    path = tmp_path / "observation"
    path.write_bytes(b"{}")
    unproven = pipeline.client.upload_file(path, idempotency_key="unproven")
    wrong_role = client.worker_upload_file(
        intent(target, role="report"), path, idempotency_key="report"
    )
    assert wrong_role == unproven

    def no_read(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Unproven evidence reached storage")

    monkeypatch.setattr(pipeline.runtime.artifacts.store, "reader", no_read)
    with pytest.raises(ResearchError, match="receipt is absent"):
        client.command(
            "worker.publish_attempt",
            PublishAttempt(attempt_id=target.attempt_id, journal_artifact=unproven),
            idempotency_key="unproven-publication",
        )


@pytest.mark.parametrize("pipeline", [True], indirect=True)
def test_another_current_worker_cannot_use_an_upload_receipt(pipeline: Pipeline) -> None:
    client, _, _, first = output_client(pipeline)
    other, _, _, second = output_client(pipeline)
    upload = begin(client, first, b"owned")
    substituted = WorkerUploadTarget(**second.model_dump(), upload_id=upload.upload_id)
    with pytest.raises(ResearchError) as denied:
        other.transport.worker_append(substituted, 0, b"owned")
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    with pytest.raises(ResearchError):
        upload_state(other, upload)
    assert upload_state(client, upload)["transfer"]["received_bytes"] == 0


def test_lost_publication_ack_replays_after_revocation_without_native_relaunch(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, worker_data, credential, target = output_client(pipeline)
    app = create_app(pipeline.runtime.service, pipeline.runtime.credentials)
    with serving(app) as url, ResearchClient.connect(url, credential.token) as remote:
        original = remote.command
        saved: dict[str, Any] = {}

        def lost_ack(operation: str, request: Any, **kwargs: Any) -> Any:
            result = original(operation, request, **kwargs)
            if operation == "worker.publish_attempt":
                saved.update(request=request, kwargs=kwargs, result=result)
                raise ResearchError(
                    ErrorCode.DELIVERY_UNCERTAIN, "Synthetic post-commit disconnect"
                )
            return result

        monkeypatch.setattr(remote, "command", lost_ack)
        worker = LocalWorker(
            remote,
            worker_data,
            root=tmp_path / "lost-ack-worker",
            machine_registry=tmp_path / "lost-ack-machine" / "resources.sqlite",
        )
        with pytest.raises(ResearchError) as uncertain:
            worker.execute(target.attempt_id)
        assert uncertain.value.code == ErrorCode.DELIVERY_UNCERTAIN
        journal = worker.journal.get(target.attempt_id)
        assert journal.outcome is not None and journal.outcome.closure.scope_closed
        revoke_worker(pipeline, worker_data)
        replay = original("worker.publish_attempt", saved["request"], **saved["kwargs"])
        assert replay == saved["result"]
        assert worker.journal.get(target.attempt_id) == journal
        with pytest.raises(ResearchError):
            original("worker.publish_attempt", saved["request"], idempotency_key="fresh-key")


@pytest.mark.parametrize("operation", ["worker.finalize_upload", "worker.cleanup_upload"])
def test_waiting_same_key_command_replays_after_revocation(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    client, worker, _, target = output_client(pipeline)
    upload = begin(client, target, b"")
    if operation == "worker.cleanup_upload":
        client.command("worker.finalize_upload", upload, idempotency_key="prepared")
    database = pipeline.runtime.database
    replay, execute = database.replay, database.execute
    barrier, mutex = threading.Barrier(2), threading.Lock()
    seen: set[int] = set()

    def both_miss(context: Any, name: str, *args: Any) -> Any:
        result = replay(context, name, *args)
        if name == operation and result is None:
            with mutex:
                first = threading.get_ident() not in seen
                seen.add(threading.get_ident())
            if first:
                barrier.wait(timeout=5)
        return result

    def revoke_after_commit(context: Any, name: str, *args: Any) -> Any:
        response = execute(context, name, *args)
        if name == operation:
            revoke_worker(pipeline, worker)
        return response

    monkeypatch.setattr(database, "replay", both_miss)
    monkeypatch.setattr(database, "execute", revoke_after_commit)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(client.command, operation, upload, idempotency_key="concurrent")
            for _ in range(2)
        ]
        results = [future.result(timeout=10) for future in futures]
    assert results[0] == results[1]
    assert results[0]["disposition"] == "accepted"


def test_unowned_upload_ids_do_not_create_storage_locks(pipeline: Pipeline) -> None:
    client, _, _, target = output_client(pipeline)
    unknown = WorkerUploadTarget(**target.model_dump(), upload_id=new_id())
    root = pipeline.runtime.artifacts.store.root
    before = set(root.rglob("*"))
    with pytest.raises(ResearchError):
        client.transport.worker_append(unknown, 0, b"data")
    for operation in ("worker.finalize_upload", "worker.cleanup_upload"):
        with pytest.raises(ResearchError):
            client.command(operation, unknown, idempotency_key=new_id())
    assert set(root.rglob("*")) == before
