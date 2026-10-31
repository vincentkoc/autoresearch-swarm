"""Read-only sealed checkpoint inventory through the shared SQL port."""

from dataclasses import dataclass

from research_swarm.domain.artifacts import ArtifactRecord, StoredBlob, UploadSpec
from research_swarm.domain.backup import BackupCustodyPin, WorkspaceSeal
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.byte_operations import require_closed_byte_operations_in
from research_swarm.state.schema_contracts import TABLES, quoted
from research_swarm.state.sql import SqlSession


@dataclass(frozen=True)
class SnapshotInventory:
    seal: WorkspaceSeal
    seal_revision: int
    seal_digest: str
    evidence_through: int
    artifacts: tuple[StoredBlob, ...]
    custody_pin: BackupCustodyPin | None


def inspect_sealed_cut(
    connection: SqlSession,
    *,
    workspace_id: str,
    controller_id: str,
    storage_limit_bytes: int,
    expected_backup_id: str | None = None,
    require_custody: bool = False,
    require_seal_history: bool = False,
) -> SnapshotInventory:
    authorities = connection.execute("SELECT * FROM workspaces LIMIT 2").fetchall()
    if len(authorities) != 1 or (authorities[0]["id"], authorities[0]["controller_id"]) != (
        workspace_id,
        controller_id,
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Backup workspace differs")
    authority = authorities[0]
    for table in (*[name for name, _, _ in TABLES if name != "workspaces"], "credentials"):
        if (
            connection.execute(
                f"SELECT 1 FROM {quoted(table)} WHERE workspace_id IS NOT ? LIMIT 1",
                (workspace_id,),
            ).fetchone()
            is not None
        ):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup has foreign workspace rows")
    gate = connection.execute(
        "SELECT * FROM records WHERE kind='workspace_gate' AND id=?", (workspace_id,)
    ).fetchone()
    if gate is None or gate["state"] != "sealed":
        raise ResearchError(ErrorCode.STALE_FENCE, "Backup source is not sealed")
    seal = WorkspaceSeal.model_validate_json(gate["payload"])
    if expected_backup_id is not None and seal.backup_id != expected_backup_id:
        raise ResearchError(ErrorCode.STALE_FENCE, "Backup seal identifier differs")
    if (seal.workspace_id, seal.controller_id, seal.control_epoch) != (
        workspace_id,
        controller_id,
        authority["control_epoch"],
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Backup seal identity differs")
    historical = connection.execute(
        "SELECT state,payload FROM records WHERE workspace_id=? AND kind='workspace_seal' AND id=?",
        (workspace_id, seal.backup_id),
    ).fetchone()
    if (
        (historical is None and require_seal_history)
        or (
            historical is not None
            and (
                historical["state"] != "immutable"
                or WorkspaceSeal.model_validate_json(historical["payload"]) != seal
            )
        )
        or authority["next_sequence"] - 1 <= seal.evidence_through
    ):
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup immutable seal differs")
    workers = tuple(
        row[0]
        for row in connection.execute(
            "SELECT id FROM records WHERE kind='worker' ORDER BY id LIMIT 10001"
        )
    )
    unresolved = tuple(
        row[0]
        for row in connection.execute(
            "SELECT id FROM records WHERE kind='attempt' "
            "AND COALESCE(json_extract(payload,'$.publication_status'),'pending') "
            "NOT IN ('accepted','closed_unaccepted') ORDER BY id LIMIT 10001"
        )
    )
    if workers != seal.worker_ids or unresolved != seal.unresolved_attempt_ids:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup execution inventory differs")
    require_closed_byte_operations_in(connection, workspace_id)
    rows = connection.execute(
        "SELECT id,payload FROM records WHERE kind='artifact' AND state='verified' "
        "ORDER BY id LIMIT 10001"
    ).fetchall()
    if len(rows) > 10_000:
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Backup inventory exceeds bound")
    inventory = []
    for row in rows:
        artifact = ArtifactRecord.model_validate_json(row["payload"])
        upload = UploadSpec(
            id=artifact.storage_generation,
            workspace_id=workspace_id,
            actor_id=artifact.published_by,
            digest=artifact.reference.digest,
            size_bytes=artifact.reference.size_bytes,
            media_type=artifact.reference.media_type,
        )
        if (
            artifact.workspace_id != workspace_id
            or artifact.state != "verified"
            or artifact.reference.id != row["id"]
            or upload.reference() != artifact.reference
        ):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup generation differs")
        inventory.append(StoredBlob(reference=artifact.reference, generation=upload.id))
    if sum(item.reference.size_bytes for item in inventory) > storage_limit_bytes:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup exceeds recorded storage")
    missing = connection.execute(
        "SELECT 1 FROM records r LEFT JOIN records a ON a.workspace_id=r.workspace_id "
        "AND a.kind='artifact' AND a.id=r.parent_id AND a.state='verified' "
        "WHERE r.kind='artifact_reference' AND r.state='retained' AND "
        "(a.id IS NULL OR json_extract(r.payload,'$.retention.artifact_id') "
        "IS NOT r.parent_id) "
        "LIMIT 1"
    ).fetchone()
    if missing is not None:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup retained evidence is absent")
    pins = connection.execute(
        "SELECT id,state,payload FROM records WHERE kind='backup_custody' "
        "AND json_extract(payload,'$.control_epoch')=? LIMIT 2",
        (seal.control_epoch,),
    ).fetchall()
    if len(pins) > 1 or (pins and pins[0]["state"] != "immutable"):
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup custody pins differ")
    pin = BackupCustodyPin.model_validate_json(pins[0]["payload"]) if pins else None
    if require_custody and pin is None:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup custody pin is absent")
    if pin is not None and (
        pin.custody_id != pins[0]["id"]
        or pin.workspace_id != seal.workspace_id
        or pin.controller_id != seal.controller_id
        or pin.control_epoch != seal.control_epoch
    ):
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup custody identity differs")
    return SnapshotInventory(
        seal,
        gate["revision"],
        record_digest("research.workspace-seal.v1", seal.model_dump(mode="json")),
        authority["next_sequence"] - 1,
        tuple(inventory),
        pin,
    )
