"""Verified sealed SQLite/filesystem bundles; export grants no restore authority."""

from __future__ import annotations

import os
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from filelock import FileLock
from pydantic import Field

from research_swarm.checkpoints.cut import SnapshotInventory, inspect_sealed_cut
from research_swarm.checkpoints.files import (
    DATABASE_LIMIT,
    copy_private,
    measure,
    measure_file,
    private_reader,
)
from research_swarm.control.auth import CredentialStore
from research_swarm.domain.assignments import signing_key_id
from research_swarm.domain.backup import (
    BackupCustodyPin,
    BackupFile,
    BackupManifest,
    BackupPrivateFile,
    RegisterBackupCustody,
    SignedBackup,
    SignedRestoreClaim,
    WorkspaceSeal,
)
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import ID, Revision, VersionedRecord, new_id, utc_now
from research_swarm.projects.native_files import SourceReader
from research_swarm.runtime import LocalRuntime, RuntimeIdentity
from research_swarm.state.private_files import (
    private_state_directory,
    read_private,
    sync_directory,
    write_private,
)
from research_swarm.state.schema import SCHEMA_VERSION
from research_swarm.state.sqlite import SQLiteSession
from research_swarm.storage.config import FilesystemConfig

PRIVATE_NAMES = ("assignment.key", "dispatch.key", "identity.json", "operator.token")
MANIFEST_LIMIT = 8 * 1024**2


class SourceCustody(VersionedRecord):
    id: ID
    workspace_id: ID
    controller_id: ID
    control_epoch: Revision | None = Field(default=None, exclude_if=lambda value: value is None)
    private_key: str = Field(pattern=r"^[0-9a-f]{64}$", repr=False)
    claim: SignedRestoreClaim | None = Field(default=None, exclude_if=lambda value: value is None)

    @property
    def key(self) -> Ed25519PrivateKey:
        return Ed25519PrivateKey.from_private_bytes(bytes.fromhex(self.private_key))


@dataclass(frozen=True)
class VerifiedBackup:
    root: Path
    manifest: BackupManifest
    identity: RuntimeIdentity
    custody_pin: BackupCustodyPin | None = None


def source_custody(
    runtime: LocalRuntime, *, create: bool = False, context: CommandContext | None = None
) -> SourceCustody:
    def registered() -> tuple[int, BackupCustodyPin | None]:
        with runtime.database.connect() as connection:
            authority = connection.execute(
                "SELECT control_epoch FROM workspaces WHERE id=?", (runtime.identity.workspace_id,)
            ).one()
            pins = connection.execute(
                "SELECT id,state,payload FROM records WHERE workspace_id=? "
                "AND kind='backup_custody' "
                "AND json_extract(payload,'$.control_epoch')=?",
                (runtime.identity.workspace_id, authority["control_epoch"]),
            ).fetchall()
        if len(pins) > 1 or (pins and pins[0]["state"] != "immutable"):
            raise ResearchError(ErrorCode.STALE_FENCE, "Source custody registration differs")
        pin = BackupCustodyPin.model_validate_json(pins[0]["payload"]) if pins else None
        if pin is not None and pin.custody_id != pins[0]["id"]:
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Source custody registration identity differs"
            )
        return authority["control_epoch"], pin

    epoch, pin = registered()
    if create:
        if context is None:
            raise ValueError("Custody creation requires an explicit operator context")
        context.principal.require("*")
    elif pin is None:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE,
            "Source custody is not registered; re-export its checkpoint",
        )
    path = runtime.root / "backup-custody.json"
    if not path.exists() and not path.is_symlink():
        if pin is not None or not create:
            raise ResearchError(ErrorCode.STALE_FENCE, "Original source custody file is missing")
        custody = SourceCustody(
            id=new_id(),
            workspace_id=runtime.identity.workspace_id,
            controller_id=runtime.identity.controller_id,
            control_epoch=epoch,
            private_key=Ed25519PrivateKey.generate().private_bytes_raw().hex(),
        )
        write_private(path, canonical_bytes(custody.model_dump(mode="json")))
    custody = SourceCustody.model_validate(
        strict_json_loads(read_private(runtime.root, path.name, limit=16_384))
    )
    if (custody.workspace_id, custody.controller_id) != (
        runtime.identity.workspace_id,
        runtime.identity.controller_id,
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Source custody identity differs")
    if custody.control_epoch is None and pin is None and create:
        custody = custody.model_copy(update={"control_epoch": epoch})
        write_private(path, canonical_bytes(custody.model_dump(mode="json")))
    expected = BackupCustodyPin(
        custody_id=custody.id,
        workspace_id=custody.workspace_id,
        controller_id=custody.controller_id,
        control_epoch=epoch,
        public_key=custody.key.public_key().public_bytes_raw().hex(),
    )
    if custody.control_epoch != epoch or (pin is not None and pin != expected):
        raise ResearchError(ErrorCode.STALE_FENCE, "Original source custody does not match its pin")
    if pin is None:
        assert context is not None
        gate = runtime.database.read_record(
            context, "workspace_gate", runtime.identity.workspace_id
        )
        seal = WorkspaceSeal.model_validate(gate["data"])
        runtime.service.register_backup_custody(
            CommandContext(
                principal=context.principal, idempotency_key="backup-custody:" + custody.id
            ),
            RegisterBackupCustody(backup_id=seal.backup_id, pin=expected),
        )
        if registered() != (epoch, expected):
            raise ResearchError(ErrorCode.STALE_FENCE, "Source custody pin was not retained")
    return custody


def _identity(root: Path) -> RuntimeIdentity:
    identity = RuntimeIdentity.model_validate(
        strict_json_loads(read_private(root / "private", "identity.json", limit=16_384))
    )
    if not isinstance(identity.storage, FilesystemConfig) or identity.initialized_at is None:
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Backup requires initialized files")
    dispatch = read_private(root / "private", "dispatch.key", limit=32)
    assignment = read_private(root / "private", "assignment.key", limit=32)
    if len(dispatch) != 32 or len(assignment) != 32:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup key length differs")
    key_id = signing_key_id(
        Ed25519PrivateKey.from_private_bytes(assignment).public_key().public_bytes_raw().hex()
    )
    if key_id != identity.assignment_key_id:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup assignment key differs")
    return identity


def snapshot_inventory(root: Path, identity: RuntimeIdentity) -> SnapshotInventory:
    """Inspect a completed immutable SQLite snapshot without initialization or migrations."""
    path = root / "research.sqlite"
    with private_reader(root, path.name):
        connection = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA trusted_schema=OFF")
        if connection.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Backup schema is unsupported")
        if connection.execute("PRAGMA integrity_check").fetchall()[0][0] != "ok":
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup database is inconsistent")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup foreign keys are invalid")
        inventory = inspect_sealed_cut(
            SQLiteSession(connection),
            workspace_id=identity.workspace_id,
            controller_id=identity.controller_id,
            storage_limit_bytes=identity.storage_limit_bytes,
        )
        token = read_private(root / "private", "operator.token", limit=256).decode("ascii").strip()
        credential = connection.execute(
            "SELECT * FROM credentials WHERE token_digest=?", (CredentialStore._digest(token),)
        ).fetchone()
        if credential is None or credential["revoked"] or credential["expires_at"] is not None:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Backup operator is unavailable")
        principal = Principal.model_validate_json(credential["principal"])
        if principal != Principal(
            id=identity.operator_id, workspace_id=identity.workspace_id, scopes=frozenset({"*"})
        ):
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Backup operator identity differs")
        return inventory
    except sqlite3.DatabaseError as error:
        raise ResearchError(
            ErrorCode.ARTIFACT_UNVERIFIED, "Backup database cannot be read"
        ) from error
    finally:
        connection.close()


def verify_backup(root: Path, *, expected_digest: str | None = None) -> VerifiedBackup:
    root = root.expanduser().absolute()
    signed = SignedBackup.model_validate(
        strict_json_loads(read_private(root, "manifest.json", limit=MANIFEST_LIMIT))
    )
    manifest = signed.manifest
    if expected_digest is not None and manifest.digest != expected_digest:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup manifest identity differs")
    try:
        Ed25519PublicKey.from_public_bytes(bytes.fromhex(manifest.source_public_key)).verify(
            bytes.fromhex(signed.signature), manifest.digest.encode("ascii")
        )
    except InvalidSignature as error:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup signature differs") from error
    expected = {"manifest.json", "research.sqlite"}
    expected.update("private/" + item.name for item in manifest.private_files)
    expected.update("blobs/" + item.generation for item in manifest.artifacts)
    actual = set()
    for parent, directories, names in os.walk(root, followlinks=False):
        for name in directories:
            path = Path(parent) / name
            if path.is_symlink() or path.relative_to(root).as_posix() not in {"private", "blobs"}:
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Unexpected backup directory")
        for name in names:
            actual.add((Path(parent) / name).relative_to(root).as_posix())
    if actual != expected:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup file inventory differs")
    if measure_file(root, "research.sqlite", limit=DATABASE_LIMIT) != manifest.database:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup database digest differs")
    for item in manifest.private_files:
        measured = measure_file(root, "private/" + item.name, limit=16_384)
        if measured != BackupFile(digest=item.digest, size_bytes=item.size_bytes):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup private file differs")
    identity = _identity(root)
    inventory = snapshot_inventory(root, identity)
    if (
        inventory.seal != manifest.seal
        or inventory.seal_revision != manifest.seal_revision
        or inventory.seal_digest != manifest.seal_digest
        or inventory.evidence_through != manifest.snapshot_evidence_through
        or inventory.artifacts != manifest.artifacts
        or manifest.database_schema != SCHEMA_VERSION
        or (
            inventory.custody_pin is not None
            and (inventory.custody_pin.custody_id, inventory.custody_pin.public_key)
            != (manifest.source_custody_id, manifest.source_public_key)
        )
    ):
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup snapshot inventory differs")
    for blob in manifest.artifacts:
        expected_file = BackupFile(
            digest=blob.reference.digest, size_bytes=blob.reference.size_bytes
        )
        if (
            measure_file(root, "blobs/" + blob.generation, limit=expected_file.size_bytes)
            != expected_file
        ):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup artifact differs")
    return VerifiedBackup(root, manifest, identity, inventory.custody_pin)


def export_backup(
    runtime: LocalRuntime, context: CommandContext, destination: Path, *, backup_id: str
) -> BackupManifest:
    # The bundle contains the root operator credential and signing keys. A
    # service-management scope alone must never grant that additional authority.
    context.principal.require("*")
    if context.principal.workspace_id != runtime.identity.workspace_id:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Backup workspace differs")
    if not isinstance(runtime.identity.storage, FilesystemConfig):
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Backup requires filesystem storage")
    destination = destination.expanduser().resolve()
    source_root = runtime.root.resolve()
    if destination == source_root or source_root in destination.parents:
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Backup must be outside runtime")
    if not destination.parent.is_dir():
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Backup destination parent must already exist"
        )
    with FileLock(runtime.root / "backup.lock"):
        row = runtime.database.read_record(context, "workspace_gate", runtime.identity.workspace_id)
        seal = WorkspaceSeal.model_validate(row["data"])
        if row["state"] != "sealed" or seal.backup_id != backup_id:
            raise ResearchError(ErrorCode.STALE_FENCE, "Backup seal differs")
        custody = source_custody(runtime, create=True, context=context)
        public = custody.key.public_key().public_bytes_raw().hex()
        if custody.claim is not None:
            from research_swarm.checkpoints.claims import verify_claim

            verify_claim(custody.claim, source_public_key=public)
        if destination.exists() or destination.is_symlink():
            existing = verify_backup(destination).manifest
            if existing.seal != seal or (
                existing.source_custody_id,
                existing.source_public_key,
            ) != (custody.id, public):
                raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Backup destination differs")
            if (
                custody.claim is not None
                and existing.digest != custody.claim.claim.request.manifest_digest
            ):
                raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Another manifest was elected")
            return existing
        if custody.claim is not None:
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Source already elected a target; copy the original bundle"
            )
        staged = destination.parent / (".research-backup-" + new_id())
        with private_state_directory(staged, exclusive=True):
            pass
        try:
            with (
                private_state_directory(staged),
                private_state_directory(staged / "private"),
                private_state_directory(staged / "blobs"),
            ):
                for name in PRIVATE_NAMES:
                    content = read_private(runtime.root, name, limit=16_384)
                    write_private(staged / "private" / name, content)
                runtime.database.backup(staged / "sqlite.pending")
                with SourceReader(staged) as reader, reader.open("sqlite.pending") as source:
                    fingerprint = measure(source, limit=DATABASE_LIMIT)
                    source.seek(0)
                    copy_private(source, staged / "research.sqlite", fingerprint)
                (staged / "sqlite.pending").unlink()
                identity = _identity(staged)
                inventory = snapshot_inventory(staged, identity)
                if inventory.seal != seal:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Backup cut changed")
                for blob in inventory.artifacts:
                    with runtime.artifacts.store.reader(identity.workspace_id, blob) as source:
                        copy_private(
                            source,
                            staged / "blobs" / blob.generation,
                            BackupFile(
                                digest=blob.reference.digest, size_bytes=blob.reference.size_bytes
                            ),
                        )
                manifest = BackupManifest(
                    source_custody_id=custody.id,
                    source_public_key=public,
                    seal=inventory.seal,
                    seal_revision=inventory.seal_revision,
                    seal_digest=inventory.seal_digest,
                    snapshot_evidence_through=inventory.evidence_through,
                    database_schema=SCHEMA_VERSION,
                    database=measure_file(staged, "research.sqlite", limit=DATABASE_LIMIT),
                    private_files=tuple(
                        BackupPrivateFile(
                            name=name,
                            **measure_file(staged, "private/" + name, limit=16_384).model_dump(
                                exclude={"schema_version"}
                            ),
                        )
                        for name in PRIVATE_NAMES
                    ),
                    artifacts=inventory.artifacts,
                    exported_at=utc_now(),
                )
                envelope = SignedBackup(
                    manifest=manifest,
                    signature=custody.key.sign(manifest.digest.encode("ascii")).hex(),
                )
                write_private(
                    staged / "manifest.json", canonical_bytes(envelope.model_dump(mode="json"))
                )
                verify_backup(staged, expected_digest=manifest.digest)
            # The source lock serializes this profile's exporters. Never replace an
            # existing destination, including a directory created while copying.
            with private_state_directory(destination, exclusive=True):
                for name in ("private", "blobs", "research.sqlite", "manifest.json"):
                    os.rename(staged / name, destination / name)
                    sync_directory(destination)
                sync_directory(destination.parent)
            return manifest
        finally:
            shutil.rmtree(staged)
