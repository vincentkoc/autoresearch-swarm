"""Workspace sealing identities; a seal never proves native process closure."""

from pydantic import Field

from research_swarm.domain.types import ID, Revision, Timestamp, VersionedRecord


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
