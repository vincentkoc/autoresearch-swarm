"""Explicit server-side fencing and session replacement preserve original custody."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from research_swarm.control.worker_records import WorkerData
from research_swarm.control.worker_sessions import archive_session
from research_swarm.control.worker_state import (
    WorkerAdmission,
    WorkerPresence,
    worker_state,
    write_worker_state,
)
from research_swarm.control.workers import authority
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ID, Revision, Timestamp, VersionedRecord, utc_now
from research_swarm.domain.workers import WorkerBinding
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.workers.signatures import AssignmentSigner


class FenceWorker(VersionedRecord):
    binding: WorkerBinding
    expected_controller_id: ID
    expected_control_epoch: Revision
    reason: str = Field(min_length=1, max_length=2048)


class ReplaceWorkerSession(FenceWorker):
    fence_id: ID
    session_id: ID
    principal_id: ID


class WorkerFence(VersionedRecord):
    binding: WorkerBinding
    worker_revision: Revision
    controller_id: ID
    control_epoch: Revision
    operator_id: ID
    fenced_at: Timestamp
    reason: str


class WorkerReplacement(VersionedRecord):
    fence_id: ID
    previous_binding: WorkerBinding
    binding: WorkerBinding
    operator_id: ID
    replaced_at: Timestamp
    reason: str
    previous_admission: WorkerAdmission
    previous_admission_revision: Revision
    previous_admission_state: Literal["active", "draining"]
    previous_presence: WorkerPresence
    previous_presence_revision: Revision
    previous_presence_state: Literal["unobserved", "observed"]


class WorkerTurnover:
    def __init__(self, database: Database, assignment_key: bytes | None) -> None:
        self.database, self._assignment_key = database, assignment_key

    @staticmethod
    def _current(transaction: Transaction, request: FenceWorker) -> tuple[JsonObject, WorkerData]:
        controller, epoch = authority(transaction)
        if (request.expected_controller_id, request.expected_control_epoch) != (controller, epoch):
            raise ResearchError(ErrorCode.STALE_FENCE, "Operator controller authority changed")
        row = transaction.get("worker", request.binding.worker_id)
        if transaction.context.expected_revision != row["revision"]:
            raise ResearchError(ErrorCode.REVISION_CONFLICT, "Worker revision changed")
        worker = WorkerData.model_validate(row["data"])
        if worker.binding != request.binding:
            raise ResearchError(ErrorCode.STALE_FENCE, "Original worker binding differs")
        return row, worker

    def fence(self, context: CommandContext, request: FenceWorker) -> JsonObject:
        def change(transaction: Transaction) -> JsonObject:
            row, worker = self._current(transaction, request)
            if row["state"] == "fenced":
                receipt = transaction.get("worker_fence", worker.binding.worker_session_id)
                if (
                    receipt["state"] != "immutable"
                    or WorkerFence.model_validate(receipt["data"]).binding != worker.binding
                ):
                    raise ResearchError(ErrorCode.STALE_FENCE, "Original fence receipt differs")
                return {"worker": row, "fence": receipt}
            if row["state"] != "active":
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Worker cannot be fenced")
            archived = archive_session(transaction, worker)
            if archived["state"] != "immutable":
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Original session archive is unavailable"
                )
            receipt = transaction.insert(
                "worker_fence",
                worker.binding.worker_session_id,
                WorkerFence(
                    binding=worker.binding,
                    worker_revision=row["revision"],
                    controller_id=request.expected_controller_id,
                    control_epoch=request.expected_control_epoch,
                    operator_id=context.principal.id,
                    fenced_at=utc_now(),
                    reason=request.reason,
                ).model_dump(mode="json"),
                state="immutable",
                parent_id=worker.binding.worker_id,
            )
            updated = transaction.update(
                "worker",
                row["id"],
                expected_revision=row["revision"],
                data=worker.model_dump(mode="json"),
                state="fenced",
            )
            transaction.event("worker.fence_recorded", receipt, {"worker_id": row["id"]})
            transaction.event("worker.fenced", updated, {"fence_id": receipt["id"]})
            return {"worker": updated, "fence": receipt}

        return self.database.execute(
            context, "worker.fence", request.model_dump(mode="json"), "worker.manage", change
        )

    def replace(self, context: CommandContext, request: ReplaceWorkerSession) -> JsonObject:
        def change(transaction: Transaction) -> JsonObject:
            row, old = self._current(transaction, request)
            if row["state"] != "fenced":
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Replacement requires an explicit fence"
                )
            fence = transaction.get("worker_fence", request.fence_id)
            proof = WorkerFence.model_validate(fence["data"])
            if (
                fence["state"] != "immutable"
                or proof.binding != old.binding
                or proof.worker_revision + 1 != row["revision"]
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Replacement fence differs")
            # Reusing a principal would let its old credentials claim the public new session ID.
            used = transaction.connection.execute(
                "SELECT 1 FROM records WHERE workspace_id=? AND kind='worker_session' "
                "AND (id=? OR json_extract(payload,'$.principal_id')=?) LIMIT 1",
                (transaction.workspace_id, request.session_id, request.principal_id),
            ).fetchone()
            current_principal = transaction.connection.execute(
                "SELECT 1 FROM records WHERE workspace_id=? AND kind='worker' "
                "AND (json_extract(payload,'$.principal_id')=? OR "
                "json_extract(payload,'$.binding.worker_session_id')=?) LIMIT 1",
                (transaction.workspace_id, request.principal_id, request.session_id),
            ).fetchone()
            if used is not None or current_principal is not None:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Replacement identity was already used"
                )
            if old.assignment_authority is not None and self._assignment_key is None:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Replacement signing authority is unavailable"
                )
            archived = archive_session(transaction, old)
            if archived["state"] != "immutable":
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Original session archive is unavailable"
                )
            binding = old.binding.model_copy(
                update={
                    "worker_session_id": request.session_id,
                    "controller_id": request.expected_controller_id,
                    "control_epoch": request.expected_control_epoch,
                }
            )
            replacement = WorkerData.model_validate(
                old.model_copy(
                    update={
                        "binding": binding,
                        "principal_id": request.principal_id,
                        "enrolled_at": utc_now(),
                        "assignment_authority": AssignmentSigner(
                            binding, self._assignment_key
                        ).authority
                        if self._assignment_key is not None
                        else None,
                    }
                ).model_dump(mode="json")
            )
            admission = worker_state(transaction, old.binding, "worker_admission")
            presence = worker_state(transaction, old.binding, "worker_presence")
            receipt = transaction.insert(
                "worker_replacement",
                request.session_id,
                WorkerReplacement(
                    fence_id=request.fence_id,
                    previous_binding=old.binding,
                    binding=binding,
                    operator_id=context.principal.id,
                    replaced_at=utc_now(),
                    reason=request.reason,
                    previous_admission=WorkerAdmission.model_validate(admission["data"]),
                    previous_admission_revision=admission["revision"],
                    previous_admission_state=admission["state"],
                    previous_presence=WorkerPresence.model_validate(presence["data"]),
                    previous_presence_revision=presence["revision"],
                    previous_presence_state=presence["state"],
                ).model_dump(mode="json"),
                state="immutable",
                parent_id=row["id"],
            )
            updated = transaction.update(
                "worker",
                row["id"],
                expected_revision=row["revision"],
                data=replacement.model_dump(mode="json"),
                state="active",
            )
            archive_session(transaction, replacement)
            fresh_admission = write_worker_state(
                transaction,
                admission,
                WorkerAdmission(
                    binding=binding,
                    reason="Replacement awaits explicit activation",
                    actor_id=context.principal.id,
                    changed_at=utc_now(),
                ),
                "draining",
            )
            fresh_presence = write_worker_state(
                transaction,
                presence,
                WorkerPresence(binding=binding),
                "unobserved",
            )
            transaction.event("worker.replacement_recorded", receipt, {"worker_id": row["id"]})
            transaction.event("worker.session_replaced", updated, {"replacement_id": receipt["id"]})
            transaction.event(
                "worker.admission_changed", fresh_admission, {"admission": "draining"}
            )
            transaction.event(
                "worker.presence_reset", fresh_presence, {"replacement_id": receipt["id"]}
            )
            return {
                "worker": updated,
                "replacement": receipt,
                "admission": fresh_admission,
                "presence": fresh_presence,
            }

        return self.database.execute(
            context,
            "worker.replace_session",
            request.model_dump(mode="json"),
            "worker.manage",
            change,
        )
