"""Workspace sealing identities; a seal never proves native process closure."""

from typing import Literal

from pydantic import Field, model_validator

from research_swarm.domain.artifacts import StoredBlob
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.types import ID, ByteCount, Digest, Revision, Timestamp, VersionedRecord


class SealWorkspace(VersionedRecord):
    backup_id: ID
    expected_controller_id: ID
    expected_control_epoch: Revision
    reason: str = Field(min_length=1, max_length=2048)


class WorkspaceSeal(VersionedRecord):
    backup_id: ID
    workspace_id: ID
    controller_id: ID
    control_epoch: Revision
    operator_id: ID
    reason: str
    sealed_at: Timestamp
    evidence_through: Revision
    unresolved_attempt_ids: tuple[ID, ...] = Field(max_length=10_000)
    worker_ids: tuple[ID, ...] = Field(max_length=10_000)


class BackupFile(VersionedRecord):
    digest: Digest
    size_bytes: ByteCount


class BackupCustodyPin(VersionedRecord):
    custody_id: ID
    workspace_id: ID
    controller_id: ID
    control_epoch: Revision
    public_key: str = Field(pattern=r"^[0-9a-f]{64}$")


class RegisterBackupCustody(VersionedRecord):
    backup_id: ID
    pin: BackupCustodyPin


class BackupPrivateFile(BackupFile):
    name: Literal["identity.json", "operator.token", "dispatch.key", "assignment.key"]


class BackupManifest(VersionedRecord):
    profile: Literal["sealed-native-v1"] = "sealed-native-v1"
    source_custody_id: ID
    source_public_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    seal: WorkspaceSeal
    seal_revision: Revision
    seal_digest: Digest
    snapshot_evidence_through: Revision
    database_schema: Revision
    database: BackupFile
    private_files: tuple[BackupPrivateFile, ...] = Field(min_length=4, max_length=4)
    artifacts: tuple[StoredBlob, ...] = Field(max_length=10_000)
    exported_at: Timestamp
    activation_requires_source_custody: Literal[True] = True

    @model_validator(mode="after")
    def unique_inventory(self) -> "BackupManifest":
        if len({item.name for item in self.private_files}) != 4:
            raise ValueError("Backup private-file inventory is incomplete")
        if len({item.reference.id for item in self.artifacts}) != len(self.artifacts):
            raise ValueError("Duplicate backup artifact")
        if len({item.generation for item in self.artifacts}) != len(self.artifacts):
            raise ValueError("Duplicate backup generation")
        return self

    @property
    def digest(self) -> str:
        return record_digest("research.backup.v1", self.model_dump(mode="json"))


class SignedBackup(VersionedRecord):
    manifest: BackupManifest
    signature: str = Field(pattern=r"^[0-9a-f]{128}$")


class RestoreReceipt(VersionedRecord):
    restore_id: ID
    backup_id: ID
    manifest_digest: Digest
    workspace_id: ID
    controller_id: ID
    source_custody_id: ID
    source_public_key: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$", exclude_if=lambda value: value is None
    )
    target_public_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_control_epoch: Revision
    target_control_epoch: Revision
    restored_at: Timestamp
    unresolved_attempt_ids: tuple[ID, ...] = Field(max_length=10_000)
    worker_ids: tuple[ID, ...] = Field(max_length=10_000)


class RestoreClaimRequest(VersionedRecord):
    backup_id: ID
    manifest_digest: Digest
    workspace_id: ID
    controller_id: ID
    source_custody_id: ID
    source_control_epoch: Revision
    restore_id: ID
    target_public_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    target_control_epoch: Revision

    @property
    def digest(self) -> str:
        return record_digest("research.restore-claim-request.v1", self.model_dump(mode="json"))


class SignedRestoreRequest(VersionedRecord):
    request: RestoreClaimRequest
    signature: str = Field(pattern=r"^[0-9a-f]{128}$")


class RestoreClaim(VersionedRecord):
    claim_id: ID
    request: RestoreClaimRequest
    source_public_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    claimed_at: Timestamp

    @property
    def digest(self) -> str:
        return record_digest("research.restore-claim.v1", self.model_dump(mode="json"))


class SignedRestoreClaim(VersionedRecord):
    claim: RestoreClaim
    signature: str = Field(pattern=r"^[0-9a-f]{128}$")
