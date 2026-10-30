from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import HTTPTransport, LocalTransport, ResearchClient
from research_swarm.control.auth import IssuedCredential
from research_swarm.control.worker_artifacts import WorkerArtifactRead, WorkerArtifacts
from research_swarm.control.workers import WorkerData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_worker_queries import claim, query, scoped_worker


def assigned_input(
    pipeline: Pipeline,
) -> tuple[ResearchClient, WorkerData, IssuedCredential, WorkerArtifactRead]:
    original, worker, token = scoped_worker(pipeline)
    runtime = pipeline.runtime
    principal = runtime.credentials.resolve(token)
    credential = runtime.credentials.issue(
        runtime.principal(),
        principal.model_copy(
            update={
                "scopes": principal.scopes | {"worker.inputs"},
            }
        ),
    )
    client = ResearchClient(
        LocalTransport(runtime, lambda: runtime.credentials.resolve(credential.token))
    )
    attempt = claim(original, worker, original.worker_list(query(worker))["items"][0])
    request = WorkerArtifactRead(
        worker_id=worker.binding.worker_id,
        session_id=worker.binding.worker_session_id,
        attempt_id=attempt["id"],
        artifact_id=attempt["data"]["source_artifact"]["id"],
    )
    return client, worker, credential, request


def context(credential: IssuedCredential) -> CommandContext:
    return CommandContext(principal=credential.principal, idempotency_key=new_id())


def revoke_worker(pipeline: Pipeline, worker: WorkerData) -> None:
    with pipeline.runtime.database.connect() as connection:
        connection.execute(
            "UPDATE records SET state='revoked' WHERE kind='worker' AND id=?",
            (worker.binding.worker_id,),
        )


def active_read_pins(pipeline: Pipeline, credential: IssuedCredential) -> int:
    with pipeline.runtime.database.connect() as connection:
        return connection.execute(
            "SELECT COUNT(*) FROM records WHERE kind='artifact_reference' AND state='retained' "
            "AND json_extract(payload,'$.actor_id')=? "
            "AND json_extract(payload,'$.retention.owner_kind')='artifact_read'",
            (credential.principal.id,),
        ).fetchone()[0]


def test_restricted_http_and_local_downloads_fetch_only_assigned_bytes(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    local, _, credential, request = assigned_input(pipeline)
    expected = local.worker_download(request, tmp_path / "local.zip")
    assert local.worker_input(request) == expected
    with pytest.raises(ResearchError):
        local.download(request.artifact_id, tmp_path / "generic.zip")
    _, other_worker, other_token = scoped_worker(pipeline)
    other_principal = pipeline.runtime.credentials.resolve(other_token)
    other_credential = pipeline.runtime.credentials.issue(
        pipeline.runtime.principal(),
        other_principal.model_copy(
            update={
                "scopes": other_principal.scopes | {"worker.inputs"},
            }
        ),
    )
    other = ResearchClient(
        LocalTransport(
            pipeline.runtime, lambda: pipeline.runtime.credentials.resolve(other_credential.token)
        )
    )
    with pytest.raises(ResearchError, match="another authority"):
        other.worker_download(
            request.model_copy(
                update={
                    "worker_id": other_worker.binding.worker_id,
                    "session_id": other_worker.binding.worker_session_id,
                }
            ),
            tmp_path / "other.zip",
        )
    extra_path = tmp_path / "unassigned.txt"
    extra_path.write_text("Synthetic unassigned evidence", encoding="utf-8")
    extra = pipeline.client.upload_file(extra_path, idempotency_key="unassigned")
    with pytest.raises(ResearchError, match="not an assigned input"):
        local.worker_download(
            request.model_copy(update={"artifact_id": extra.id}), tmp_path / "denied"
        )
    app = create_app(pipeline.runtime.service, pipeline.runtime.credentials)
    with serving(app) as url, ResearchClient.connect(url, credential.token) as remote:
        assert remote.worker_input(request) == expected
        assert remote.worker_download(request, tmp_path / "remote.zip") == expected
        assert (tmp_path / "remote.zip").read_bytes() == (tmp_path / "local.zip").read_bytes()
        with pytest.raises(FileExistsError):
            remote.worker_download(request, tmp_path / "remote.zip")
    assert active_read_pins(pipeline, credential) == 0


def test_authority_is_rechecked_inside_pin_transaction(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, worker, credential, request = assigned_input(pipeline)
    artifacts = WorkerArtifacts(pipeline.runtime.database, pipeline.runtime.artifacts)
    original = pipeline.runtime.artifacts.pinned_authorized

    @contextmanager
    def revoked_before_pin(*args: Any, **kwargs: Any) -> Iterator[Any]:
        revoke_worker(pipeline, worker)
        with original(*args, **kwargs) as record:
            yield record

    monkeypatch.setattr(pipeline.runtime.artifacts, "pinned_authorized", revoked_before_pin)
    with (
        pytest.raises(ResearchError) as stale,
        artifacts.input_reader(context(credential), request),
    ):
        pytest.fail("Revoked worker obtained bytes")
    assert stale.value.code == ErrorCode.STALE_FENCE
    assert active_read_pins(pipeline, credential) == 0


def test_admitted_reader_releases_exact_pin_after_enrollment_revocation(pipeline: Pipeline) -> None:
    _, worker, credential, request = assigned_input(pipeline)
    artifacts = WorkerArtifacts(pipeline.runtime.database, pipeline.runtime.artifacts)
    with artifacts.input_reader(context(credential), request) as stream:
        assert active_read_pins(pipeline, credential) == 1
        revoke_worker(pipeline, worker)
        assert stream.read(2) == b"PK"
        with pytest.raises(ResearchError) as denied:
            artifacts.input_metadata(context(credential), request)
        assert denied.value.code == ErrorCode.STALE_FENCE
    assert active_read_pins(pipeline, credential) == 0


def test_token_revoked_between_metadata_and_content_cannot_create_download(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, credential, request = assigned_input(pipeline)
    app = create_app(pipeline.runtime.service, pipeline.runtime.credentials)
    with serving(app) as url, ResearchClient.connect(url, credential.token) as remote:
        transport = remote.transport
        assert isinstance(transport, HTTPTransport)
        original = transport._request

        def revoke_after_metadata(*args: Any, **kwargs: Any) -> Any:
            response = original(*args, **kwargs)
            pipeline.runtime.credentials.revoke(pipeline.runtime.principal(), credential.id)
            return response

        monkeypatch.setattr(transport, "_request", revoke_after_metadata)
        with pytest.raises(ResearchError):
            remote.worker_download(request, tmp_path / "denied.zip")
    assert not (tmp_path / "denied.zip").exists()
    assert active_read_pins(pipeline, credential) == 0


def test_expired_input_grant_does_not_create_read_pin(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, credential, request = assigned_input(pipeline)
    artifacts = WorkerArtifacts(pipeline.runtime.database, pipeline.runtime.artifacts)
    monkeypatch.setattr(
        "research_swarm.control.worker_artifacts.utc_now", lambda: "2999-01-01T00:00:00.000Z"
    )
    with pytest.raises(ResearchError) as expired:
        artifacts.input_metadata(context(credential), request)
    assert expired.value.code == ErrorCode.STALE_FENCE
    assert active_read_pins(pipeline, credential) == 0
