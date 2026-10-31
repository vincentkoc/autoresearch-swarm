"""Inactive restoration of an already verified cut inside its owner's transaction.

This internal primitive admits no import, signature, source election or activation.
Its caller verifies custody and source bytes before entering the transaction.
"""

from dataclasses import dataclass

from research_swarm.control.worker_records import WorkerData
from research_swarm.control.worker_sessions import archive_session
from research_swarm.control.worker_turnover import WorkerFence
from research_swarm.domain.backup import RestoreReceipt, WorkspaceSeal
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.state.database import Transaction
from research_swarm.state.sql import SqlSession, next_counter


@dataclass(frozen=True)
class RestorationCut:
    seal: WorkspaceSeal
    seal_revision: int
    snapshot_evidence_through: int
    manifest_digest: str
    source_custody_id: str
    source_public_key: str


def restore_inactive_in(
    connection: SqlSession,
    context: CommandContext,
    source: RestorationCut,
    *,
    restore_id: str,
    target_public_key: str,
    restored_at: str | None = None,
) -> RestoreReceipt:
    transaction = Transaction(connection, context, observe_waits=False)
    existing = connection.execute(
        "SELECT id FROM records WHERE kind='restoration' AND id=?", (restore_id,)
    ).fetchone()
    if existing is not None:
        receipt = RestoreReceipt.model_validate(
            transaction.get("restoration", existing["id"])["data"]
        )
        if (
            receipt.restore_id != restore_id
            or receipt.manifest_digest != source.manifest_digest
            or receipt.target_public_key != target_public_key
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Restoration identity differs")
        return receipt
    authority = connection.execute("SELECT * FROM workspaces").fetchall()
    if len(authority) != 1 or (
        authority[0]["id"],
        authority[0]["controller_id"],
        authority[0]["control_epoch"],
    ) != (source.seal.workspace_id, source.seal.controller_id, source.seal.control_epoch):
        raise ResearchError(ErrorCode.STALE_FENCE, "Restore source authority differs")
    gate = transaction.get("workspace_gate", source.seal.workspace_id)
    if (
        gate["state"] != "sealed"
        or gate["revision"] != source.seal_revision
        or WorkspaceSeal.model_validate(gate["data"]) != source.seal
        or authority[0]["next_sequence"] - 1 != source.snapshot_evidence_through
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Restore source cut differs")
    epoch = next_counter(source.seal.control_epoch)
    for worker_id in source.seal.worker_ids:
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
                controller_id=source.seal.controller_id,
                control_epoch=epoch,
                operator_id=context.principal.id,
                fenced_at=restored_at if restored_at is not None else utc_now(),
                reason="Inactive restore " + restore_id,
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
        "UPDATE workspaces SET control_epoch=? WHERE id=?", (epoch, source.seal.workspace_id)
    )
    transaction.update(
        "workspace_gate",
        source.seal.workspace_id,
        expected_revision=gate["revision"],
        data=gate["data"],
        state="restoring",
    )
    receipt = RestoreReceipt(
        restore_id=restore_id,
        backup_id=source.seal.backup_id,
        manifest_digest=source.manifest_digest,
        workspace_id=source.seal.workspace_id,
        controller_id=source.seal.controller_id,
        source_custody_id=source.source_custody_id,
        source_public_key=source.source_public_key,
        target_public_key=target_public_key,
        source_control_epoch=source.seal.control_epoch,
        target_control_epoch=epoch,
        restored_at=restored_at if restored_at is not None else utc_now(),
        unresolved_attempt_ids=source.seal.unresolved_attempt_ids,
        worker_ids=source.seal.worker_ids,
    )
    row = transaction.insert(
        "restoration", receipt.restore_id, receipt.model_dump(mode="json"), state="inactive"
    )
    transaction.event("workspace.restored_inactive", row, {"backup_id": receipt.backup_id})
    return receipt
