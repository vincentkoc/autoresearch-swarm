from __future__ import annotations

import hashlib
import subprocess
import sys
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import pytest

from research_swarm.domain.artifacts import (
    ArtifactRecord,
    ArtifactRetention,
    StoredBlob,
    UploadSpec,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.state.artifacts import ArtifactCatalog, ArtifactService, BeginUpload
from research_swarm.state.database import Database
from research_swarm.storage.filesystem import FilesystemBlobStore


def digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


@pytest.fixture
def store(tmp_path: Path) -> FilesystemBlobStore:
    return FilesystemBlobStore(tmp_path / "artifacts")


@pytest.fixture
def service(database: Database, store: FilesystemBlobStore) -> ArtifactService:
    return ArtifactService(database, store, storage_limit_bytes=100)


def upload_spec(principal: Principal, content: bytes) -> UploadSpec:
    return UploadSpec(
        id=new_id(),
        workspace_id=principal.workspace_id,
        actor_id=principal.id,
        digest=digest(content),
        size_bytes=len(content),
    )


def stage(service: ArtifactService, context: Callable[..., CommandContext], content: bytes) -> str:
    response = service.begin_upload(
        context(), BeginUpload(digest=digest(content), size_bytes=len(content))
    )
    identifier: str = response["result"]["id"]
    service.upload_part(context(), identifier, 0, content)
    return identifier


def publish(
    service: ArtifactService, context: Callable[..., CommandContext], content: bytes
) -> ArtifactRecord:
    identifier = stage(service, context, content)
    return ArtifactRecord.model_validate(
        service.finalize_upload(context(), identifier)["result"]["data"]
    )


def test_chunk_replay_partial_crash_and_conflicting_bytes(
    store: FilesystemBlobStore, principal: Principal
) -> None:
    upload = upload_spec(principal, b"abcdef")
    assert store.begin(upload) == 0
    assert store.append(upload, 0, b"abc") == 3
    # The interrupted full chunk is retried; the durable matching prefix is retained.
    assert store.append(upload, 0, b"abcdef") == 6
    assert store.append(upload, 0, b"abcdef") == 6
    with pytest.raises(ResearchError) as error:
        store.append(upload, 1, b"ZZ")
    assert error.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    blob = store.finalize(upload)
    assert store.finalize(upload) == blob
    store.verify(principal.workspace_id, blob)


def test_wrong_digest_short_upload_and_oversize_are_not_published(
    store: FilesystemBlobStore, principal: Principal
) -> None:
    upload = upload_spec(principal, b"abc")
    store.begin(upload)
    store.append(upload, 0, b"a")
    with pytest.raises(ResearchError, match="size mismatch"):
        store.finalize(upload)
    with pytest.raises(ResearchError, match="declared size"):
        store.append(upload, 1, b"xxxx")
    store.append(upload, 1, b"zz")
    with pytest.raises(ResearchError, match="size mismatch"):
        store.finalize(upload)


def test_download_never_overwrites_destination(
    store: FilesystemBlobStore, principal: Principal, tmp_path: Path
) -> None:
    blob = store.put(principal.workspace_id, principal.id, b"original")
    destination = tmp_path / "output"
    store.fetch(principal.workspace_id, blob, destination)
    assert destination.read_bytes() == b"original"
    with pytest.raises(FileExistsError):
        store.fetch(principal.workspace_id, blob, destination)
    assert destination.read_bytes() == b"original"


def test_generation_fences_old_deletion(store: FilesystemBlobStore, principal: Principal) -> None:
    older = store.put(principal.workspace_id, principal.id, b"content")
    newer = store.put(principal.workspace_id, principal.id, b"content")
    assert older.reference == newer.reference
    assert older.generation != newer.generation
    store.delete(principal.workspace_id, older)
    store.delete(principal.workspace_id, older)
    store.verify(principal.workspace_id, newer)


def test_blob_does_not_grant_catalog_access(
    service: ArtifactService,
    store: FilesystemBlobStore,
    principal: Principal,
    context: Callable[..., CommandContext],
) -> None:
    blob = store.put(principal.workspace_id, principal.id, b"uncatalogued")
    with pytest.raises(ResearchError) as error:
        service.get(context(), blob.reference.id)
    assert error.value.code == ErrorCode.NOT_FOUND
    assert not service.database.snapshot(context())["records"]


def test_upload_owner_and_workspace_are_enforced(
    service: ArtifactService, principal: Principal, context: Callable[..., CommandContext]
) -> None:
    upload = stage(service, context, b"abc")
    stranger = CommandContext(
        principal=Principal(
            id=new_id(), workspace_id=principal.workspace_id, scopes={"read", "artifact.upload"}
        ),
        idempotency_key=new_id(),
    )
    with pytest.raises(ResearchError) as error:
        service.upload_part(stranger, upload, 0, b"abc")
    assert error.value.code == ErrorCode.AUTHORIZATION_DENIED
    foreign = stranger.model_copy(
        update={"principal": stranger.principal.model_copy(update={"workspace_id": new_id()})}
    )
    with pytest.raises(ResearchError) as error:
        service.finalize_upload(foreign, upload)
    assert error.value.code == ErrorCode.NOT_FOUND


def test_upload_survives_commit_failure(
    service: ArtifactService,
    context: Callable[..., CommandContext],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identifier = stage(service, context, b"abc")
    execute = service.database.execute

    def interrupted(*args: Any, **kwargs: Any) -> Any:
        if args[1] == "artifact.finalize_upload":
            raise RuntimeError("controller interrupted after blob publication")
        return execute(*args, **kwargs)

    monkeypatch.setattr(service.database, "execute", interrupted)
    with pytest.raises(RuntimeError):
        service.finalize_upload(context(), identifier)
    assert service.database.read_record(context(), "upload", identifier)["state"] == "pending"
    assert not [
        r for r in service.database.snapshot(context())["records"] if r["kind"] == "artifact"
    ]
    monkeypatch.setattr(service.database, "execute", execute)
    restarted = ArtifactService(Database(service.database.path), service.store)
    response = restarted.finalize_upload(context("finalize-retry"), identifier)
    assert response == restarted.finalize_upload(context("finalize-retry"), identifier)
    assert restarted.occupancy(context())["retained_bytes"] == 3
    assert restarted.occupancy(context())["staging_reserved_bytes"] == 0


def test_corrupt_blob_fails_fetch_and_releases_temporary_pin(
    service: ArtifactService,
    store: FilesystemBlobStore,
    context: Callable[..., CommandContext],
    tmp_path: Path,
) -> None:
    record = publish(service, context, b"abc")
    blob = StoredBlob(reference=record.reference, generation=record.storage_generation)
    store._object(record.workspace_id, blob).write_bytes(b"zzz")
    with pytest.raises(ResearchError) as error:
        service.fetch(context(), record.reference, tmp_path / "download")
    assert error.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    assert not (tmp_path / "download").exists()
    refs = [
        row
        for row in service.database.snapshot(context())["records"]
        if row["kind"] == "artifact_reference"
    ]
    assert refs and all(row["state"] == "released" for row in refs)


def test_retention_lives_beyond_upload_and_blocks_deletion(
    service: ArtifactService, context: Callable[..., CommandContext]
) -> None:
    record = publish(service, context, b"abc")
    reference = service.retain(
        context(),
        ArtifactRetention(
            artifact_id=record.reference.id,
            owner_kind="trial",
            owner_id=new_id(),
            purpose="replay",
        ),
    )["result"]
    with pytest.raises(ResearchError, match="retained references"):
        service.delete(context(revision=1), record.reference.id, "cleanup")
    with pytest.raises(ResearchError, match="owner's retention lifecycle"):
        service.release(context(), reference["id"])
    retention = ArtifactRetention.model_validate(reference["data"]["retention"])
    service.database.execute(
        context(),
        "test.owner_retention_finished",
        {},
        "artifact.release",
        lambda transaction: ArtifactCatalog.release(
            transaction,
            reference["id"],
            owner_release=(retention.owner_kind, retention.owner_id),
        ),
    )
    service.delete(context(revision=1), record.reference.id, "completed and unreferenced")
    deleted = service.get(context(), record.reference.id)
    assert deleted.state == "deleted"
    assert deleted.reference.digest == record.reference.digest
    assert deleted.deletion_reason == "completed and unreferenced"
    assert service.occupancy(context())["retained_bytes"] == 0


def test_delete_failure_preserves_occupancy_and_rejects_new_references(
    service: ArtifactService,
    context: Callable[..., CommandContext],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = publish(service, context, b"abc")
    delete = service.store.delete

    def fail(*args: Any) -> None:
        raise OSError("storage offline")

    monkeypatch.setattr(service.store, "delete", fail)
    with pytest.raises(OSError):
        service.delete(context(revision=1), record.reference.id, "cleanup")
    assert service.get(context(), record.reference.id).state == "deleting"
    assert service.occupancy(context())["retained_bytes"] == 3
    with pytest.raises(ResearchError, match="unavailable"):
        service.retain(
            context(),
            ArtifactRetention(
                artifact_id=record.reference.id,
                owner_kind="trial",
                owner_id=new_id(),
                purpose="replay",
            ),
        )
    monkeypatch.setattr(service.store, "delete", delete)
    service.reconcile_deletion(context(), record.reference.id)
    assert service.get(context(), record.reference.id).state == "deleted"
    assert service.occupancy(context())["retained_bytes"] == 0


def test_old_delete_replay_cannot_remove_republished_content(
    service: ArtifactService, context: Callable[..., CommandContext], tmp_path: Path
) -> None:
    record = publish(service, context, b"abc")
    command = context("delete-original", revision=1)
    first = service.delete(command, record.reference.id, "cleanup")
    replacement = publish(service, context, b"abc")
    assert replacement.storage_generation != record.storage_generation
    assert service.delete(command, record.reference.id, "cleanup") == first
    service.fetch(context(), replacement.reference, tmp_path / "restored")
    assert (tmp_path / "restored").read_bytes() == b"abc"


def test_quota_covers_staging_abandonment_and_retained_content(
    service: ArtifactService, context: Callable[..., CommandContext]
) -> None:
    service.storage_limit_bytes = 5
    identifier = stage(service, context, b"12345")
    with pytest.raises(ResearchError, match="capacity"):
        service.begin_upload(context(), BeginUpload(digest=digest(b"x"), size_bytes=1))
    service.abort_upload(context(), identifier)
    assert service.occupancy(context())["staging_reserved_bytes"] == 0
    record = publish(service, context, b"12345")
    assert record.state == "verified"
    with pytest.raises(ResearchError, match="capacity"):
        service.begin_upload(context(), BeginUpload(digest=digest(b"x"), size_bytes=1))


def test_independent_connections_serialize_retain_and_delete(
    service: ArtifactService, context: Callable[..., CommandContext]
) -> None:
    record = publish(service, context, b"abc")

    def compete(retain: bool) -> str:
        independent = ArtifactService(Database(service.database.path), service.store)
        try:
            if retain:
                independent.retain(
                    context(),
                    ArtifactRetention(
                        artifact_id=record.reference.id,
                        owner_kind="trial",
                        owner_id=new_id(),
                        purpose="replay",
                    ),
                )
            else:
                independent.delete(context(revision=1), record.reference.id, "cleanup")
            return "retained" if retain else "deleted"
        except ResearchError:
            return "rejected"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(compete, [True, False]))
    assert results.count("rejected") == 1
    if "retained" in results:
        assert service.get(context(), record.reference.id).state == "verified"


def test_concurrent_same_content_uploads_share_one_catalog_object(
    service: ArtifactService, context: Callable[..., CommandContext], tmp_path: Path
) -> None:
    uploads = [stage(service, context, b"abc") for _ in range(2)]

    def finalize(identifier: str) -> ArtifactRecord:
        independent = ArtifactService(Database(service.database.path), service.store)
        return ArtifactRecord.model_validate(
            independent.finalize_upload(context(), identifier)["result"]["data"]
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        records = list(executor.map(finalize, uploads))
    assert records[0] == records[1]
    assert service.occupancy(context())["retained_bytes"] == 3
    assert service.occupancy(context())["staging_reserved_bytes"] == 0
    service.fetch(context(), records[0].reference, tmp_path / "deduplicated")
    assert (tmp_path / "deduplicated").read_bytes() == b"abc"


def _append_in_process(root: str, upload: dict[str, Any]) -> int:
    return FilesystemBlobStore(Path(root)).append(UploadSpec.model_validate(upload), 0, b"abcdef")


def test_native_locks_coordinate_separate_processes(
    store: FilesystemBlobStore, principal: Principal
) -> None:
    upload = upload_spec(principal, b"abcdef")
    store.begin(upload)
    with ProcessPoolExecutor(max_workers=2, mp_context=get_context("spawn")) as executor:
        futures = [
            executor.submit(_append_in_process, str(store.root), upload.model_dump(mode="json"))
            for _ in range(2)
        ]
        assert [future.result(timeout=30) for future in futures] == [6, 6]
    store.verify(principal.workspace_id, store.finalize(upload))


def test_symlink_storage_directory_is_rejected(
    store: FilesystemBlobStore, principal: Principal, tmp_path: Path
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    try:
        (store.root / principal.workspace_id).symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("Symlink creation requires platform privileges")
    with pytest.raises(ResearchError, match="storage directory"):
        store.begin(upload_spec(principal, b"abc"))
    assert not list(outside.iterdir())


def test_begin_retry_after_publication_does_not_allocate_another_copy(
    store: FilesystemBlobStore, principal: Principal
) -> None:
    upload = upload_spec(principal, b"abc")
    store.begin(upload)
    store.append(upload, 0, b"abc")
    blob = store.finalize(upload)
    assert store.begin(upload) == 3
    assert not store._staged(upload).exists()
    store.verify(principal.workspace_id, blob)


def test_dedup_keeps_healthy_upload_when_existing_generation_is_corrupt(
    service: ArtifactService, store: FilesystemBlobStore, context: Callable[..., CommandContext]
) -> None:
    record = publish(service, context, b"abc")
    old_blob = StoredBlob(reference=record.reference, generation=record.storage_generation)
    store._object(record.workspace_id, old_blob).write_bytes(b"zzz")
    upload_id = stage(service, context, b"abc")
    with pytest.raises(ResearchError, match="mismatch"):
        service.finalize_upload(context(), upload_id)
    good_blob = StoredBlob(reference=record.reference, generation=upload_id)
    store.verify(record.workspace_id, good_blob)
    assert service.occupancy(context())["staging_reserved_bytes"] == 3


def test_deletion_commit_failure_reconciles_without_refunding_early(
    service: ArtifactService,
    context: Callable[..., CommandContext],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = publish(service, context, b"abc")
    execute = service.database.execute

    def interrupted(*args: Any, **kwargs: Any) -> Any:
        if args[1] == "artifact.finish_deletion":
            raise RuntimeError("interrupted after physical deletion")
        return execute(*args, **kwargs)

    monkeypatch.setattr(service.database, "execute", interrupted)
    with pytest.raises(RuntimeError):
        service.delete(context(revision=1), record.reference.id, "cleanup")
    assert service.occupancy(context())["retained_bytes"] == 3
    monkeypatch.setattr(service.database, "execute", execute)
    service.reconcile_deletion(context(), record.reference.id)
    assert service.occupancy(context())["retained_bytes"] == 0


def test_live_reader_session_cannot_be_reconciled(
    service: ArtifactService, context: Callable[..., CommandContext]
) -> None:
    record = publish(service, context, b"abc")
    with (
        service.pinned(context(), record.reference),
        pytest.raises(ResearchError, match="still alive"),
    ):
        service.reconcile_reader_session(context(), service.reader_session_id)


def test_crashed_reader_session_can_be_reconciled(
    service: ArtifactService,
    store: FilesystemBlobStore,
    context: Callable[..., CommandContext],
    principal: Principal,
) -> None:
    record = publish(service, context, b"abc")
    program = """
import os, sys
from pathlib import Path
from research_swarm.domain.identity import Principal, CommandContext
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.state.artifacts import ArtifactService
from research_swarm.state.database import Database
from research_swarm.storage.filesystem import FilesystemBlobStore
principal = Principal.model_validate_json(sys.argv[3])
context = CommandContext(principal=principal, idempotency_key='crash-reader')
service = ArtifactService(Database(Path(sys.argv[1])), FilesystemBlobStore(Path(sys.argv[2])))
with service.pinned(context, ArtifactRef.model_validate_json(sys.argv[4])):
    print(service.reader_session_id, flush=True)
    os._exit(0)
"""
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            program,
            str(service.database.path),
            str(store.root),
            principal.model_dump_json(),
            record.reference.model_dump_json(),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    session_id = completed.stdout.strip()
    with pytest.raises(ResearchError, match="retained references"):
        service.delete(context(revision=1), record.reference.id, "cleanup")
    service.reconcile_reader_session(context(), session_id)
    service.delete(context(revision=1), record.reference.id, "cleanup")
    assert service.get(context(), record.reference.id).state == "deleted"
