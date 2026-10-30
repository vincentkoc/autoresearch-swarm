"""Seal research mutations at one transaction cut before native backup export."""

from research_swarm.domain.backup import (
    BackupCustodyPin,
    RegisterBackupCustody,
    SealWorkspace,
    WorkspaceSeal,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.state.byte_operations import require_closed_byte_operations
from research_swarm.state.database import Database, JsonObject, Transaction


def _retain_seal(transaction: Transaction, seal: WorkspaceSeal) -> None:
    existing = transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND kind='workspace_seal' AND id=?",
        (transaction.workspace_id, seal.backup_id),
    ).fetchone()
    if existing is not None:
        row = transaction.get("workspace_seal", seal.backup_id)
        if row["state"] != "immutable" or WorkspaceSeal.model_validate(row["data"]) != seal:
            raise ResearchError(ErrorCode.STALE_FENCE, "Immutable seal history differs")
        return
    transaction.insert(
        "workspace_seal", seal.backup_id, seal.model_dump(mode="json"), state="immutable"
    )


class BackupControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def register_custody(
        self, context: CommandContext, request: RegisterBackupCustody
    ) -> JsonObject:
        def register(transaction: Transaction) -> JsonObject:
            row = transaction.get("workspace_gate", transaction.workspace_id)
            seal = WorkspaceSeal.model_validate(row["data"])
            authority = transaction.connection.execute(
                "SELECT controller_id,control_epoch FROM workspaces WHERE id=?",
                (transaction.workspace_id,),
            ).one()
            pin = request.pin
            if (
                row["state"] != "sealed"
                or request.backup_id != seal.backup_id
                or pin.workspace_id != transaction.workspace_id
                or (pin.controller_id, pin.control_epoch)
                != (authority["controller_id"], authority["control_epoch"])
                or pin.control_epoch != seal.control_epoch
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Backup custody cut differs")
            existing = transaction.connection.execute(
                "SELECT id FROM records WHERE workspace_id=? AND kind='backup_custody' "
                "AND json_extract(payload,'$.control_epoch')=?",
                (transaction.workspace_id, pin.control_epoch),
            ).fetchall()
            if existing:
                previous = transaction.get("backup_custody", existing[0]["id"])
                if (
                    len(existing) != 1
                    or previous["state"] != "immutable"
                    or BackupCustodyPin.model_validate(previous["data"]) != pin
                ):
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "Backup custody is already pinned"
                    )
                return previous
            registered = transaction.insert(
                "backup_custody", pin.custody_id, pin.model_dump(mode="json"), state="immutable"
            )
            transaction.observe_waits = False
            transaction.event(
                "workspace.backup_custody_registered", registered, {"backup_id": request.backup_id}
            )
            return registered

        return self.database.execute(
            context,
            "workspace.register_backup_custody",
            request.model_dump(mode="json"),
            "*",
            register,
        )

    def seal(self, context: CommandContext, request: SealWorkspace) -> JsonObject:
        def seal(transaction: Transaction) -> JsonObject:
            authority = transaction.connection.execute(
                "SELECT controller_id,control_epoch,next_sequence FROM workspaces WHERE id=?",
                (transaction.workspace_id,),
            ).one()
            if (authority["controller_id"], authority["control_epoch"]) != (
                request.expected_controller_id,
                request.expected_control_epoch,
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Workspace authority changed")
            existing = transaction.connection.execute(
                "SELECT id FROM records WHERE workspace_id=? AND kind='workspace_gate'",
                (transaction.workspace_id,),
            ).fetchone()
            if existing is not None:
                row = transaction.get("workspace_gate", existing["id"])
                previous = WorkspaceSeal.model_validate(row["data"])
                if context.expected_revision != row["revision"]:
                    raise ResearchError(
                        ErrorCode.REVISION_CONFLICT, "Workspace seal revision differs"
                    )
                if row["state"] == "sealed":
                    if previous.backup_id != request.backup_id or previous.reason != request.reason:
                        raise ResearchError(
                            ErrorCode.IDEMPOTENCY_CONFLICT, "Workspace is already sealed"
                        )
                    return row
                if row["state"] != "active":
                    raise ResearchError(ErrorCode.STALE_FENCE, "Workspace cannot be sealed")
                if request.backup_id == previous.backup_id:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "A new seal requires a new backup ID"
                    )
                _retain_seal(transaction, previous)
            elif context.expected_revision != 0:
                raise ResearchError(
                    ErrorCode.REVISION_CONFLICT, "An active workspace has revision zero"
                )
            if (
                transaction.connection.execute(
                    "SELECT 1 FROM records WHERE workspace_id=? AND kind='workspace_seal' AND id=?",
                    (transaction.workspace_id, request.backup_id),
                ).fetchone()
                is not None
            ):
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Backup ID already identifies an earlier seal"
                )
            require_closed_byte_operations(transaction)
            unresolved = transaction.connection.execute(
                "SELECT id FROM records WHERE workspace_id=? AND kind='attempt' "
                "AND COALESCE(json_extract(payload,'$.publication_status'),'pending') "
                "NOT IN ('accepted','closed_unaccepted') ORDER BY id LIMIT 10001",
                (transaction.workspace_id,),
            ).fetchall()
            workers = transaction.connection.execute(
                "SELECT id FROM records WHERE workspace_id=? AND kind='worker' "
                "ORDER BY id LIMIT 10001",
                (transaction.workspace_id,),
            ).fetchall()
            if max(len(unresolved), len(workers)) > 10_000:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "Workspace exceeds backup profile bounds"
                )
            data = WorkspaceSeal(
                backup_id=request.backup_id,
                workspace_id=transaction.workspace_id,
                controller_id=authority["controller_id"],
                control_epoch=authority["control_epoch"],
                operator_id=context.principal.id,
                reason=request.reason,
                sealed_at=utc_now(),
                evidence_through=authority["next_sequence"] - 1,
                unresolved_attempt_ids=tuple(item["id"] for item in unresolved),
                worker_ids=tuple(item["id"] for item in workers),
            )
            _retain_seal(transaction, data)
            if existing is None:
                row = transaction.insert(
                    "workspace_gate",
                    transaction.workspace_id,
                    data.model_dump(mode="json"),
                    state="sealed",
                )
            else:
                row = transaction.update(
                    "workspace_gate",
                    transaction.workspace_id,
                    expected_revision=row["revision"],
                    data=data.model_dump(mode="json"),
                    state="sealed",
                )
            transaction.event("workspace.sealed", row, {"backup_id": request.backup_id})
            return row

        return self.database.execute(
            context, "workspace.seal", request.model_dump(mode="json"), "service.manage", seal
        )
