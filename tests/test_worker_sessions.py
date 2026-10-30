from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import ResearchClient
from research_swarm.control.queries import RecordQuery
from research_swarm.control.results import PublishAttempt
from research_swarm.control.worker_artifacts import WorkerArtifactRead
from research_swarm.control.worker_sessions import archive_session
from research_swarm.control.workers import EnrollWorker, WorkerData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.workers.local import LocalWorker
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_worker_lifecycle import observer
from tests.test_worker_outputs import begin, output_client
from tests.test_worker_queries import query, read, scoped_worker


def advance_epoch(pipeline: Pipeline) -> None:
    with pipeline.runtime.database.connect() as connection:
        connection.execute(
            "UPDATE workspaces SET control_epoch=control_epoch+1 WHERE id=?",
            (pipeline.runtime.identity.workspace_id,),
        )


def test_enrollment_retains_exact_session_and_rejects_reused_session_identity(
    pipeline: Pipeline,
) -> None:
    client, worker, _ = scoped_worker(pipeline)
    recorded = client.worker_get(read(worker, "worker_session", worker.binding.worker_session_id))
    assert recorded["state"] == "immutable" and recorded["revision"] == 1
    assert WorkerData.model_validate(recorded["data"]) == worker
    before = pipeline.client.list(RecordQuery(kind="worker"))["items"]
    with pytest.raises(ResearchError) as conflict:
        pipeline.client.command(
            "worker.enroll",
            EnrollWorker(
                worker_id=new_id(),
                session_id=worker.binding.worker_session_id,
                principal_id=worker.principal_id,
                name="Conflicting enrollment",
                profiles=worker.profiles,
            ),
            idempotency_key=new_id(),
        )
    assert conflict.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert pipeline.client.list(RecordQuery(kind="worker"))["items"] == before
    assert (
        client.worker_get(read(worker, "worker_session", worker.binding.worker_session_id))
        == recorded
    )


def test_legacy_session_reads_do_not_write_and_explicit_archive_is_idempotent(
    pipeline: Pipeline,
) -> None:
    client, worker, _ = scoped_worker(pipeline)
    with pipeline.runtime.database.connect() as connection:
        connection.execute(
            "DELETE FROM records WHERE kind='worker_session' AND id=?",
            (worker.binding.worker_session_id,),
        )
    advance_epoch(pipeline)
    row = client.worker_get(read(worker, "worker_session", worker.binding.worker_session_id))
    assert row["revision"] == 0 and WorkerData.model_validate(row["data"]) == worker
    with pipeline.runtime.database.connect() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE kind='worker_session' AND id=?",
                (worker.binding.worker_session_id,),
            ).fetchone()[0]
            == 0
        )

    def archive() -> dict[str, Any]:
        return pipeline.runtime.database.execute(
            CommandContext(principal=pipeline.runtime.principal(), idempotency_key=new_id()),
            "fixture.archive",
            worker.model_dump(mode="json"),
            "worker.manage",
            lambda transaction: archive_session(transaction, worker),
        )["result"]

    first = archive()
    assert archive() == first and first["revision"] == 1
    assert (
        client.worker_get(read(worker, "worker_session", worker.binding.worker_session_id)) == first
    )


def test_old_session_can_finish_evidence_transport_but_not_get_inputs_or_publish_results(
    pipeline: Pipeline,
) -> None:
    client, worker, _, target = output_client(pipeline)
    attempt = pipeline.client.get("attempt", target.attempt_id)
    upload = begin(client, target, b"historical evidence")
    budget = pipeline.client.budget(pipeline.campaign["id"])
    advance_epoch(pipeline)
    assert client.worker_get(read(worker, "historical_attempt", target.attempt_id)) == attempt
    assert (
        client.worker_list(query(worker, "historical_pending"))["items"][0]["id"]
        == target.attempt_id
    )
    client.transport.worker_append(upload, 0, b"historical evidence")
    accepted = client.command(
        "worker.finalize_upload", upload, idempotency_key="historical-finalize"
    )
    assert accepted["disposition"] == "accepted"
    for kind in ("pending", "offers", "controls"):
        with pytest.raises(ResearchError) as fenced:
            client.worker_list(query(worker, kind))
        assert fenced.value.code == ErrorCode.STALE_FENCE
    with pytest.raises(ResearchError) as input_denied:
        client.worker_input(
            WorkerArtifactRead(
                **target.model_dump(), artifact_id=attempt["data"]["source_artifact"]["id"]
            )
        )
    assert input_denied.value.code == ErrorCode.STALE_FENCE
    artifact = client.transport.query("worker.output", upload.model_dump(mode="json"))["artifact"]
    with pytest.raises(ResearchError) as result_denied:
        client.command(
            "worker.publish_attempt",
            PublishAttempt(attempt_id=target.attempt_id, journal_artifact=artifact),
            idempotency_key="stale-scientific-result",
        )
    assert result_denied.value.code == ErrorCode.STALE_FENCE
    assert pipeline.client.get("attempt", target.attempt_id) == attempt
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget


def test_historical_session_does_not_cross_actor_session_or_workspace(pipeline: Pipeline) -> None:
    client, worker, _, target = output_client(pipeline)
    other, other_worker, _ = scoped_worker(pipeline)
    advance_epoch(pipeline)
    for request in (
        read(worker, "worker_session", worker.binding.worker_session_id),
        read(worker, "historical_attempt", target.attempt_id),
    ):
        with pytest.raises(ResearchError) as denied:
            other.worker_get(request)
        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    with pytest.raises(ResearchError):
        client.worker_get(read(worker, "worker_session", other_worker.binding.worker_session_id))
    with pytest.raises(ResearchError):
        client.worker_get(read(worker, "historical_attempt", new_id()))
    with pytest.raises(ResearchError):
        client.worker_list(query(other_worker, "historical_pending"))


def test_native_closure_upload_survives_epoch_change_without_accepting_scientific_result(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, worker, credential, target = output_client(pipeline)
    app = create_app(pipeline.runtime.service, pipeline.runtime.credentials)

    with serving(app) as url, ResearchClient.connect(url, credential.token) as remote:
        native = LocalWorker(
            remote,
            worker,
            root=tmp_path / "worker",
            machine_registry=tmp_path / "machine" / "resources.sqlite",
        )
        command = remote.transport.command
        changed = False

        def fence_before_upload(operation: str, *args: Any, **kwargs: Any) -> Any:
            nonlocal changed
            if operation == "worker.begin_upload" and not changed:
                changed = True
                advance_epoch(pipeline)
            return command(operation, *args, **kwargs)

        monkeypatch.setattr(remote.transport, "command", fence_before_upload)
        with pytest.raises(ResearchError) as stale:
            native.execute(target.attempt_id)
        assert stale.value.code == ErrorCode.STALE_FENCE and changed
        entry = native.journal.get(target.attempt_id)
        assert entry.outcome is not None and entry.outcome.closure.scope_closed
        assert entry.outcome.exit_code == 0
        assert native.resources.availability() == {"cpu": 1}
        historical = remote.worker_get(read(worker, "historical_attempt", target.attempt_id))
        assert historical["data"]["publication_status"] == "pending"
        with pipeline.runtime.database.connect() as connection:
            assert (
                connection.execute(
                    "SELECT COUNT(*) FROM records WHERE kind='worker_upload' "
                    "AND parent_id=? AND state='finalized'",
                    (target.attempt_id,),
                ).fetchone()[0]
                >= 4
            )
            assert (
                connection.execute(
                    "SELECT COUNT(*) FROM records WHERE kind='run_result' AND id=?",
                    (target.attempt_id,),
                ).fetchone()[0]
                == 0
            )
        assert client.worker_list(query(worker, "historical_pending"))["items"]


def test_explicit_credential_and_session_revocation_still_deny_historical_evidence(
    pipeline: Pipeline,
) -> None:
    client, worker, credential, target = output_client(pipeline)
    upload = begin(client, target, b"")
    with pipeline.runtime.database.connect() as connection:
        connection.execute(
            "UPDATE records SET state='revoked' WHERE kind='worker' AND id=?",
            (worker.binding.worker_id,),
        )
    with pytest.raises(ResearchError) as revoked:
        client.worker_get(read(worker, "worker_session", worker.binding.worker_session_id))
    assert revoked.value.code == ErrorCode.STALE_FENCE
    with pipeline.runtime.database.connect() as connection:
        connection.execute(
            "UPDATE records SET state='fenced' WHERE kind='worker' AND id=?",
            (worker.binding.worker_id,),
        )
    assert client.worker_get(read(worker, "historical_attempt", target.attempt_id))
    pipeline.runtime.credentials.revoke(pipeline.runtime.principal(), credential.id)
    with pytest.raises(ResearchError) as denied:
        client.command("worker.finalize_upload", upload, idempotency_key=new_id())
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED


@pytest.mark.parametrize("pipeline", [True], indirect=True)
def test_historical_pending_pages_keep_original_binding_and_cursor_scope(
    pipeline: Pipeline,
) -> None:

    client, _ = observer(pipeline)
    worker = pipeline.worker.worker
    jobs = pipeline.client.list(RecordQuery(kind="job", state="ready"))["items"]
    attempts = {pipeline.worker.claim(job)["id"] for job in jobs}
    assert len(attempts) == 2
    advance_epoch(pipeline)
    first = client.worker_list(query(worker, "historical_pending", limit=1))
    assert first["next_cursor"] is not None
    second = client.worker_list(
        query(worker, "historical_pending", limit=1, cursor=first["next_cursor"])
    )
    assert {first["items"][0]["id"], second["items"][0]["id"]} == attempts
    assert second["next_cursor"] is None
    other, other_worker, _ = scoped_worker(pipeline)
    with pytest.raises(ResearchError, match="scope differs"):
        other.worker_list(query(other_worker, "historical_pending", cursor=first["next_cursor"]))


def test_current_row_replacement_does_not_transfer_old_session_evidence_ownership(
    pipeline: Pipeline,
) -> None:
    client, worker, _, target = output_client(pipeline)
    other, replacement, _ = scoped_worker(pipeline)
    binding = replacement.binding.model_copy(update={"worker_id": worker.binding.worker_id})
    assert replacement.assignment_authority is not None
    data = replacement.model_copy(
        update={
            "binding": binding,
            "assignment_authority": replacement.assignment_authority.model_copy(
                update={"binding": binding}
            ),
        }
    )
    # Simulate the future replacement transaction; no public fencing API is exposed in this slice.
    with pipeline.runtime.database.connect() as connection:
        connection.execute(
            "UPDATE records SET payload=?,revision=revision+1 WHERE kind='worker' AND id=?",
            (data.model_dump_json(), worker.binding.worker_id),
        )
    assert (
        client.worker_get(read(worker, "historical_attempt", target.attempt_id))["id"]
        == target.attempt_id
    )
    assert client.worker_get(read(worker, "worker_session", worker.binding.worker_session_id))[
        "data"
    ] == worker.model_dump(mode="json")
    with pytest.raises(ResearchError):
        other.worker_get(read(worker, "historical_attempt", target.attempt_id))
    with pytest.raises(ResearchError):
        client.worker_list(query(worker))


def test_unavailable_archived_session_cannot_fall_back_to_current_row(pipeline: Pipeline) -> None:
    client, worker, _ = scoped_worker(pipeline)
    with pipeline.runtime.database.connect() as connection:
        connection.execute(
            "UPDATE records SET state='revoked' WHERE kind='worker_session' AND id=?",
            (worker.binding.worker_session_id,),
        )
    with pytest.raises(ResearchError) as unavailable:
        client.worker_get(read(worker, "worker_session", worker.binding.worker_session_id))
    assert unavailable.value.code == ErrorCode.STALE_FENCE
