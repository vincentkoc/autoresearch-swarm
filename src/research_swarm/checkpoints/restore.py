"""Restore verified bytes into an inactive epoch without cloning execution custody."""

from __future__ import annotations

import re
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from filelock import FileLock
from pydantic import Field

from research_swarm.checkpoints.backup import VerifiedBackup, verify_backup
from research_swarm.checkpoints.files import CHUNK_BYTES, copy_private, measure_file, private_reader
from research_swarm.checkpoints.preservation import verify_preservation
from research_swarm.control.worker_records import WorkerData
from research_swarm.control.worker_sessions import archive_session
from research_swarm.control.worker_turnover import WorkerFence
from research_swarm.domain.artifacts import UploadSpec
from research_swarm.domain.backup import BackupFile, RestoreReceipt, WorkspaceSeal
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import ID, Digest, VersionedRecord, utc_now
from research_swarm.state.database import Database, Transaction
from research_swarm.state.private_files import (
    private_state_directory,
    read_private,
    restore_admission_path,
    sync_directory,
    write_private,
)
from research_swarm.state.sql import SqlSession, next_counter
from research_swarm.storage.filesystem import FilesystemBlobStore


class RestoreIntent(VersionedRecord):
    restore_id: ID
    manifest_digest: Digest
    destination: str


class RestoreCustody(VersionedRecord):
    intent: RestoreIntent
    private_key: str = Field(pattern=r"^[0-9a-f]{64}$", repr=False)

    @property
    def public_key(self) -> str:
        return (
            Ed25519PrivateKey.from_private_bytes(bytes.fromhex(self.private_key))
            .public_key()
            .public_bytes_raw()
            .hex()
        )


STAGED_FILES = (
    "recovery.pending",
    "restore-custody.json",
    "restore.json",
    "identity.json",
    "operator.token",
    "dispatch.key",
    "assignment.key",
    "research.sqlite",
)


def _clean_staging(destination: Path) -> None:
    names = "|".join(re.escape(name) for name in STAGED_FILES)
    pattern = re.compile(
        "(?:" + names + r")\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.pending"
    )
    for path in destination.iterdir():
        if pattern.fullmatch(path.name):
            with private_reader(destination, path.name):
                pass
            path.unlink()
    sync_directory(destination)


def _materialize_files(backup: VerifiedBackup, destination: Path) -> None:
    for item in backup.manifest.private_files:
        expected = BackupFile(digest=item.digest, size_bytes=item.size_bytes)
        target = destination / item.name
        if target.exists() or target.is_symlink():
            if measure_file(destination, item.name, limit=16_384) != expected:
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Restore private file differs")
        else:
            with private_reader(backup.root, "private/" + item.name) as source:
                copy_private(source, target, expected)
    database = destination / "research.sqlite"
    if not database.exists() and not database.is_symlink():
        if any(
            (destination / name).exists() for name in ("research.sqlite-wal", "research.sqlite-shm")
        ):
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Restore database is absent from native state"
            )
        with private_reader(backup.root, "research.sqlite") as source:
            copy_private(source, database, backup.manifest.database)
    # Opening a prior interrupted database is allowed only after its original
    # exclusive intent is verified. SQLite then recovers its own transaction.
    with private_reader(destination, database.name):
        pass


def _materialize_artifacts(backup: VerifiedBackup, destination: Path) -> FilesystemBlobStore:
    store = FilesystemBlobStore(destination / "artifacts")
    for blob in backup.manifest.artifacts:
        upload = UploadSpec(
            id=blob.generation,
            workspace_id=backup.identity.workspace_id,
            actor_id=backup.identity.operator_id,
            digest=blob.reference.digest,
            size_bytes=blob.reference.size_bytes,
            media_type=blob.reference.media_type,
        )
        with store.upload_guard(upload.workspace_id, upload.id):
            offset = store.begin(upload)
            with private_reader(backup.root, "blobs/" + blob.generation) as source:
                source.seek(offset)
                while content := source.read(CHUNK_BYTES):
                    offset = store.append(upload, offset, content)
            if store.finalize(upload) != blob:
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Restored generation differs")
    return store


def _transform(
    database: Database, backup: VerifiedBackup, intent: RestoreIntent, custody: RestoreCustody
) -> RestoreReceipt:
    manifest = backup.manifest
    context = CommandContext(
        principal=Principal(
            id=backup.identity.operator_id,
            workspace_id=backup.identity.workspace_id,
            scopes=frozenset({"*"}),
        ),
        idempotency_key="native-restore:" + intent.restore_id,
    )

    def restore(connection: SqlSession) -> RestoreReceipt:
        transaction = Transaction(connection, context, observe_waits=False)
        existing = connection.execute(
            "SELECT id FROM records WHERE kind='restoration' AND id=?", (intent.restore_id,)
        ).fetchone()
        if existing is not None:
            receipt = RestoreReceipt.model_validate(
                transaction.get("restoration", existing["id"])["data"]
            )
            if (
                receipt.restore_id != intent.restore_id
                or receipt.manifest_digest != manifest.digest
                or receipt.target_public_key != custody.public_key
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Restoration identity differs")
            return receipt
        authority = connection.execute("SELECT * FROM workspaces").fetchall()
        if len(authority) != 1 or (
            authority[0]["id"],
            authority[0]["controller_id"],
            authority[0]["control_epoch"],
        ) != (manifest.seal.workspace_id, manifest.seal.controller_id, manifest.seal.control_epoch):
            raise ResearchError(ErrorCode.STALE_FENCE, "Restore source authority differs")
        gate = transaction.get("workspace_gate", manifest.seal.workspace_id)
        if (
            gate["state"] != "sealed"
            or gate["revision"] != manifest.seal_revision
            or WorkspaceSeal.model_validate(gate["data"]) != manifest.seal
            or authority[0]["next_sequence"] - 1 != manifest.snapshot_evidence_through
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Restore source cut differs")
        epoch = next_counter(manifest.seal.control_epoch)
        for worker_id in manifest.seal.worker_ids:
            row = transaction.get("worker", worker_id)
            worker = WorkerData.model_validate(row["data"])
            archive_session(transaction, worker)
            if row["state"] == "fenced":
                fence = transaction.get("worker_fence", worker.binding.worker_session_id)
                if WorkerFence.model_validate(fence["data"]).binding != worker.binding:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Original worker fence differs")
                continue
            if row["state"] != "active":
                raise ResearchError(ErrorCode.STALE_FENCE, "Original worker state is unsupported")
            fence = transaction.insert(
                "worker_fence",
                worker.binding.worker_session_id,
                WorkerFence(
                    binding=worker.binding,
                    worker_revision=row["revision"],
                    controller_id=manifest.seal.controller_id,
                    control_epoch=epoch,
                    operator_id=context.principal.id,
                    fenced_at=utc_now(),
                    reason="Inactive restore " + intent.restore_id,
                ).model_dump(mode="json"),
                state="immutable",
                parent_id=worker_id,
            )
            updated = transaction.update(
                "worker",
                worker_id,
                expected_revision=row["revision"],
                data=row["data"],
                state="fenced",
            )
            transaction.event("worker.fence_recorded", fence, {"worker_id": worker_id})
            transaction.event("worker.fenced", updated, {"fence_id": fence["id"]})
        connection.execute(
            "UPDATE workspaces SET control_epoch=? WHERE id=?", (epoch, manifest.seal.workspace_id)
        )
        transaction.update(
            "workspace_gate",
            manifest.seal.workspace_id,
            expected_revision=gate["revision"],
            data=gate["data"],
            state="restoring",
        )
        receipt = RestoreReceipt(
            restore_id=intent.restore_id,
            backup_id=manifest.seal.backup_id,
            manifest_digest=manifest.digest,
            workspace_id=manifest.seal.workspace_id,
            controller_id=manifest.seal.controller_id,
            source_custody_id=manifest.source_custody_id,
            source_public_key=manifest.source_public_key,
            target_public_key=custody.public_key,
            source_control_epoch=manifest.seal.control_epoch,
            target_control_epoch=epoch,
            restored_at=utc_now(),
            unresolved_attempt_ids=manifest.seal.unresolved_attempt_ids,
            worker_ids=manifest.seal.worker_ids,
        )
        row = transaction.insert(
            "restoration", receipt.restore_id, receipt.model_dump(mode="json"), state="inactive"
        )
        transaction.event("workspace.restored_inactive", row, {"backup_id": receipt.backup_id})
        return receipt

    return database.store.write(restore)


def _verify_restored(database: Database, receipt: RestoreReceipt) -> None:
    def verify(connection: SqlSession) -> None:
        stored = connection.execute(
            "SELECT state,payload FROM records WHERE kind='restoration' AND id=?",
            (receipt.restore_id,),
        ).fetchone()
        if (
            stored is None
            or stored["state"] != "inactive"
            or RestoreReceipt.model_validate_json(stored["payload"]) != receipt
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Restoration receipt differs")
        authority = connection.execute("SELECT * FROM workspaces").fetchall()
        if len(authority) != 1 or (
            authority[0]["id"],
            authority[0]["controller_id"],
            authority[0]["control_epoch"],
        ) != (receipt.workspace_id, receipt.controller_id, receipt.target_control_epoch):
            raise ResearchError(ErrorCode.STALE_FENCE, "Restored authority differs")
        gate = connection.execute(
            "SELECT state FROM records WHERE kind='workspace_gate' AND id=?",
            (receipt.workspace_id,),
        ).one()
        if gate["state"] != "restoring":
            raise ResearchError(ErrorCode.STALE_FENCE, "Restored authority is not inactive")
        workers = connection.execute(
            "SELECT id,state FROM records WHERE kind='worker' ORDER BY id"
        ).fetchall()
        if tuple(row["id"] for row in workers) != receipt.worker_ids or any(
            row["state"] != "fenced" for row in workers
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Restored workers are not fenced")

    database.store.read(verify)


def _verify_receipt(
    backup: VerifiedBackup, receipt: RestoreReceipt, custody: RestoreCustody
) -> None:
    seal = backup.manifest.seal
    if (
        receipt.restore_id != custody.intent.restore_id
        or receipt.manifest_digest != backup.manifest.digest
        or receipt.backup_id != seal.backup_id
        or receipt.workspace_id != seal.workspace_id
        or receipt.controller_id != seal.controller_id
        or receipt.source_custody_id != backup.manifest.source_custody_id
        or receipt.source_public_key not in (None, backup.manifest.source_public_key)
        or receipt.target_public_key != custody.public_key
        or receipt.source_control_epoch != seal.control_epoch
        or receipt.target_control_epoch != next_counter(seal.control_epoch)
        or receipt.worker_ids != seal.worker_ids
        or receipt.unresolved_attempt_ids != seal.unresolved_attempt_ids
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Restore receipt does not match its source")


def _verify_completed(
    backup: VerifiedBackup, destination: Path, intent: RestoreIntent
) -> RestoreReceipt:
    receipt = RestoreReceipt.model_validate(
        strict_json_loads(read_private(destination, "restore.json", limit=1024**2))
    )
    custody = RestoreCustody.model_validate(
        strict_json_loads(read_private(destination, "restore-custody.json", limit=16_384))
    )
    if custody.intent != intent:
        raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Completed restore intent differs")
    _verify_receipt(backup, receipt, custody)
    for item in backup.manifest.private_files:
        if measure_file(destination, item.name, limit=16_384) != BackupFile(
            digest=item.digest, size_bytes=item.size_bytes
        ):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Restored private file differs")
    with private_reader(destination, "research.sqlite"):
        pass
    database = Database(destination / "research.sqlite")
    _verify_restored(database, receipt)
    verify_preservation(backup, database, intent.restore_id)
    if not (destination / "artifacts").is_dir():
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Restored artifact store is absent")
    store = FilesystemBlobStore(destination / "artifacts")
    for blob in backup.manifest.artifacts:
        store.verify(receipt.workspace_id, blob)
    return receipt


def restore_inactive(
    bundle: Path, destination: Path, *, restore_id: str, expected_digest: str | None = None
) -> RestoreReceipt:
    backup = verify_backup(bundle, expected_digest=expected_digest)
    destination = destination.expanduser().resolve()
    if destination == backup.root.resolve() or backup.root.resolve() in destination.parents:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Restore must be outside its bundle"
        )
    if not destination.parent.is_dir():
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Restore parent must already exist")
    intent = RestoreIntent(
        restore_id=restore_id, manifest_digest=backup.manifest.digest, destination=str(destination)
    )
    admission = restore_admission_path(destination)
    lock_path = admission.with_suffix(".lock")
    with FileLock(lock_path):
        existing = destination.exists() or destination.is_symlink()
        barrier = admission.exists() or admission.is_symlink()
        if barrier:
            original = RestoreIntent.model_validate(
                strict_json_loads(read_private(admission.parent, admission.name, limit=16_384))
            )
            if original != intent:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Pending restore identity differs"
                )
        if not existing or barrier:
            if not barrier:
                write_private(admission, canonical_bytes(intent.model_dump(mode="json")))
            try:
                with private_state_directory(destination, exclusive=not existing):
                    _clean_staging(destination)
                    if any(
                        path.name not in {"recovery.pending", "initialize.lock"}
                        for path in destination.iterdir()
                    ):
                        raise ResearchError(
                            ErrorCode.STALE_FENCE, "Restore root publication differs"
                        )
                    pending = destination / "recovery.pending"
                    if pending.exists() or pending.is_symlink():
                        original = RestoreIntent.model_validate(
                            strict_json_loads(read_private(destination, pending.name, limit=16_384))
                        )
                        if original != intent:
                            raise ResearchError(
                                ErrorCode.STALE_FENCE, "Restore root intent differs"
                            )
                    else:
                        write_private(pending, canonical_bytes(intent.model_dump(mode="json")))
            except FileExistsError:
                admission.unlink()
                sync_directory(admission.parent)
                raise
            sync_directory(destination.parent)
            admission.unlink()
            sync_directory(admission.parent)
        with private_state_directory(destination):
            pending = destination / "recovery.pending"
            if pending.exists() or pending.is_symlink():
                original = RestoreIntent.model_validate(
                    strict_json_loads(read_private(destination, pending.name, limit=16_384))
                )
                if original != intent:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "Pending restore identity differs"
                    )
                _clean_staging(destination)
            else:
                return _verify_completed(backup, destination, intent)
            permitted = {
                "recovery.pending",
                "restore-custody.json",
                "restore.json",
                "identity.json",
                "operator.token",
                "dispatch.key",
                "assignment.key",
                "research.sqlite",
                "research.sqlite-wal",
                "research.sqlite-shm",
                "artifacts",
                "initialize.lock",
            }
            if any(path.name not in permitted for path in destination.iterdir()):
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Pending restore contains unrelated state"
                )
            custody_path = destination / "restore-custody.json"
            if not custody_path.exists() and not custody_path.is_symlink():
                if (destination / "research.sqlite").exists():
                    raise ResearchError(ErrorCode.STALE_FENCE, "Restore custody is missing")
                custody = RestoreCustody(
                    intent=intent,
                    private_key=Ed25519PrivateKey.generate().private_bytes_raw().hex(),
                )
                write_private(custody_path, canonical_bytes(custody.model_dump(mode="json")))
            custody = RestoreCustody.model_validate(
                strict_json_loads(read_private(destination, custody_path.name, limit=16_384))
            )
            if custody.intent != intent:
                raise ResearchError(ErrorCode.STALE_FENCE, "Restore custody intent differs")
            _materialize_files(backup, destination)
            store = _materialize_artifacts(backup, destination)
            database = Database(destination / "research.sqlite")
            verify_preservation(backup, database, intent.restore_id)
            receipt = _transform(database, backup, intent, custody)
            _verify_receipt(backup, receipt, custody)
            _verify_restored(database, receipt)
            verify_preservation(backup, database, intent.restore_id)
            for blob in backup.manifest.artifacts:
                store.verify(receipt.workspace_id, blob)
            write_private(
                destination / "restore.json", canonical_bytes(receipt.model_dump(mode="json"))
            )
            pending.unlink()
            sync_directory(destination)
            return receipt
