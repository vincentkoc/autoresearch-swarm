"""A bounded recovery cut; queued research is preserved, native custody must be closed."""

from dataclasses import dataclass

from research_swarm.control.activation_accounting import require_settled_execution
from research_swarm.domain.artifacts import ArtifactRecord, StoredBlob, UploadSpec
from research_swarm.domain.backup import RecoveryAdmission, RestoreReceipt
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import (
    DriveClosure,
    DriveReceipt,
    DriveTurn,
    OrchestrationBinding,
)
from research_swarm.state.database import JsonObject, Transaction
from research_swarm.state.drive_closures import target_acknowledgment

MAX_VIEW_ENTRIES = 100_000
MAX_VIEW_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class ActivationView:
    digest: str
    artifacts: tuple[StoredBlob, ...]
    evidence_through: int

    @property
    def artifact_digest(self) -> str:
        return record_digest(
            "research.activation-artifacts.v1",
            {"artifacts": [item.model_dump(mode="json") for item in self.artifacts]},
        )


def require_closed_byte_operations(transaction: Transaction) -> None:
    transient = transaction.connection.execute(
        "SELECT kind,id,state FROM records WHERE workspace_id=? AND ("
        "(kind='upload' AND state NOT IN ('finalized','aborted')) OR "
        "(kind='worker_upload' AND state NOT IN ('finalized','aborted')) OR "
        "(kind='artifact' AND state='deleting') OR "
        "(kind='artifact_reference' AND state='retained' "
        "AND (json_extract(payload,'$.retention.owner_kind')='artifact_read' "
        "OR json_extract(payload,'$.reader_session_id') IS NOT NULL))) LIMIT 1",
        (transaction.workspace_id,),
    ).fetchone()
    if transient is not None:
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE,
            "Artifact operations must close before changing workspace authority",
            details={"kind": transient["kind"], "id": transient["id"]},
        )
    inconsistent = transaction.connection.execute(
        "SELECT w.id FROM records w LEFT JOIN records u ON u.workspace_id=w.workspace_id "
        "AND u.kind='upload' AND u.id=w.id WHERE w.workspace_id=? AND w.kind='worker_upload' "
        "AND (u.id IS NULL OR w.state!=u.state "
        "OR json_extract(w.payload,'$.upload_id') IS NOT w.id) LIMIT 1",
        (transaction.workspace_id,),
    ).fetchone()
    if inconsistent is not None:
        raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Worker upload closure differs")


def _turns(transaction: Transaction, admission: RecoveryAdmission) -> list[JsonObject]:
    for identifier in admission.orchestration_campaign_ids:
        row = transaction.get("orchestration_binding", identifier)
        binding = OrchestrationBinding.model_validate(row["data"])
        if row["state"] != "fenced" or binding.pending_turn_id is not None:
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Original driver is not closed")
    acknowledgments = []
    for item in transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND kind='drive_turn' ORDER BY id",
        (transaction.workspace_id,),
    ):
        row = transaction.get("drive_turn", item["id"])
        turn = DriveTurn.model_validate(row["data"])
        response = target_acknowledgment(transaction, turn)
        if row["state"] == "settled":
            evidence = transaction.get("drive_receipt", row["id"])
            receipt = DriveReceipt.model_validate(evidence["data"])
            valid = (
                evidence["state"] == "immutable"
                and response is not None
                and receipt.turn_id == row["id"]
                and receipt.target_request_digest == turn.action.request_digest
                and receipt.target_command_id == response["command_id"]
                and receipt.disposition == response["disposition"]
                and receipt.target_response_digest
                == record_digest("research.drive-response-v1", response)
            )
        elif row["state"] == "closed_unacknowledged":
            evidence = transaction.get("drive_closure", row["id"])
            closure = DriveClosure.model_validate(evidence["data"])
            valid = (
                evidence["state"] == "immutable"
                and response is None
                and closure.turn_id == row["id"]
                and closure.epoch == turn.epoch
                and closure.target_request_digest == turn.action.request_digest
            )
        else:
            valid = False
        if not valid:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE,
                "Original saved turn lacks its terminal acknowledgment or closure",
                details={"turn_id": row["id"]},
            )
        acknowledgments.append({"turn_id": row["id"], "response": response})
    return acknowledgments


def inspect_activation(transaction: Transaction, receipt: RestoreReceipt) -> ActivationView:
    """Read complete row identities so unlogged changes also invalidate native verification."""
    entries: list[JsonObject] = []
    size = 0
    for table, order in (
        ("records", "kind,id"),
        ("budgets", "campaign_id,dimension"),
        ("budget_reservations", "id"),
        ("budget_settlements", "settlement_id"),
        ("budget_entries", "id"),
        ("credentials", "id"),
    ):
        for sql_row in transaction.connection.execute(
            f"SELECT * FROM {table} WHERE workspace_id=? ORDER BY {order} LIMIT 100001",
            (transaction.workspace_id,),
        ):
            names = sql_row.keys()
            value = {key: sql_row[key] for key in names}
            size += len(canonical_bytes(value))
            if len(entries) >= MAX_VIEW_ENTRIES or size > MAX_VIEW_BYTES:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Activation exceeds the native view bound"
                )
            entries.append(
                {"table": table, "digest": record_digest("research.activation-row.v1", value)}
            )
    authority = transaction.connection.execute(
        "SELECT controller_id,control_epoch,next_sequence FROM workspaces WHERE id=?",
        (transaction.workspace_id,),
    ).one()
    if (authority["controller_id"], authority["control_epoch"]) != (
        receipt.controller_id,
        receipt.target_control_epoch,
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Activation authority differs")
    admission = RecoveryAdmission.model_validate(
        transaction.get("recovery_admission", receipt.restore_id)["data"]
    )
    require_settled_execution(transaction, receipt)
    require_closed_byte_operations(transaction)
    for identifier in admission.paused_campaign_ids:
        if transaction.get("campaign", identifier)["state"] not in {"pausing", "paused"}:
            raise ResearchError(ErrorCode.STALE_FENCE, "Recovery campaign pause differs")
    acknowledgments = _turns(transaction, admission)
    inventory: list[StoredBlob] = []
    for item in transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND kind='artifact' AND state='verified' "
        "ORDER BY id LIMIT 10001",
        (transaction.workspace_id,),
    ):
        row = transaction.get("artifact", item["id"])
        artifact = ArtifactRecord.model_validate(row["data"])
        upload = UploadSpec(
            id=artifact.storage_generation,
            workspace_id=transaction.workspace_id,
            actor_id=artifact.published_by,
            digest=artifact.reference.digest,
            size_bytes=artifact.reference.size_bytes,
            media_type=artifact.reference.media_type,
        )
        if (
            len(inventory) >= 10_000
            or artifact.workspace_id != transaction.workspace_id
            or artifact.state != "verified"
            or artifact.reference.id != row["id"]
            or upload.reference() != artifact.reference
        ):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Activation generation differs")
        inventory.append(StoredBlob(reference=artifact.reference, generation=upload.id))
    missing = transaction.connection.execute(
        "SELECT r.id FROM records r LEFT JOIN records a ON a.workspace_id=r.workspace_id "
        "AND a.kind='artifact' AND a.id=r.parent_id AND a.state='verified' "
        "WHERE r.workspace_id=? AND r.kind='artifact_reference' AND r.state='retained' AND "
        "(a.id IS NULL OR json_extract(r.payload,'$.retention.artifact_id') IS NOT r.parent_id) "
        "LIMIT 1",
        (transaction.workspace_id,),
    ).fetchone()
    if missing is not None:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Retained recovery evidence is absent")
    return ActivationView(
        record_digest(
            "research.activation-view.v1",
            {
                "entries": entries,
                "acknowledgments": acknowledgments,
                "authority": {
                    key: authority[key]
                    for key in ("controller_id", "control_epoch", "next_sequence")
                },
            },
        ),
        tuple(inventory),
        authority["next_sequence"] - 1,
    )
