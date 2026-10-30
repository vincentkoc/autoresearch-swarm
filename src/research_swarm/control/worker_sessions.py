"""Historical evidence ownership grants no current execution or input authority."""

from __future__ import annotations

from research_swarm.control.worker_records import AttemptData, WorkerData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.workers import WorkerBinding
from research_swarm.state.database import JsonObject, Transaction


def archive_session(transaction: Transaction, worker: WorkerData) -> JsonObject:
    """Persist the exact enrollment before any future fencing or replacement."""
    if worker.binding.workspace_id != transaction.workspace_id:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Session workspace differs")
    identifier = worker.binding.worker_session_id
    try:
        row = transaction.get("worker_session", identifier)
    except ResearchError as error:
        if error.code != ErrorCode.NOT_FOUND:
            raise
    else:
        if WorkerData.model_validate(row["data"]) != worker:
            raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Worker session identity changed")
        return row
    row = transaction.insert(
        "worker_session",
        identifier,
        worker.model_dump(mode="json"),
        state="immutable",
        parent_id=worker.binding.worker_id,
    )
    transaction.event("worker.session_recorded", row, {"worker_id": worker.binding.worker_id})
    return row


def historical_session(transaction: Transaction, worker_id: str, session_id: str) -> JsonObject:
    try:
        row = transaction.get("worker_session", session_id)
    except ResearchError as error:
        if error.code != ErrorCode.NOT_FOUND:
            raise
        # Legacy enrollments remain readable without a read-side migration. A
        # replacement must archive them before changing the current worker row.
        try:
            current = transaction.get("worker", worker_id)
        except ResearchError as missing:
            if missing.code != ErrorCode.NOT_FOUND:
                raise
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Worker session is not owned"
            ) from None
        row = {
            "id": session_id,
            "kind": "worker_session",
            "revision": 0,
            "state": "immutable",
            "campaign_id": None,
            "parent_id": worker_id,
            "data": current["data"],
        }
    worker = WorkerData.model_validate(row["data"])
    if (
        worker.principal_id != transaction.context.principal.id
        or worker.binding.workspace_id != transaction.workspace_id
        or worker.binding.worker_id != worker_id
        or worker.binding.worker_session_id != session_id
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Worker session is not owned")
    if row["state"] != "immutable":
        raise ResearchError(ErrorCode.STALE_FENCE, "Historical evidence authority is unavailable")
    try:
        current = transaction.get("worker", worker_id)
    except ResearchError as error:
        if error.code != ErrorCode.NOT_FOUND:
            raise
    else:
        if WorkerData.model_validate(current["data"]).binding == worker.binding and current[
            "state"
        ] not in {"active", "fenced"}:
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Worker session evidence authority is revoked"
            )
    return row


def historical_attempt_owner(
    transaction: Transaction,
    worker_id: str,
    session_id: str,
    attempt_id: str,
    *,
    require_signed: bool = True,
) -> tuple[WorkerData, AttemptData]:
    worker = WorkerData.model_validate(
        historical_session(transaction, worker_id, session_id)["data"]
    )
    try:
        attempt = AttemptData.model_validate(transaction.get("attempt", attempt_id)["data"])
    except ResearchError as error:
        if error.code != ErrorCode.NOT_FOUND:
            raise
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Attempt is not assigned to session"
        ) from None
    binding = WorkerBinding.model_validate(
        {key: getattr(attempt.dispatch, key) for key in WorkerBinding.model_fields}
    )
    if binding != worker.binding:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Attempt is not assigned to session")
    signed = attempt.signed_assignment
    if signed is None:
        if require_signed or worker.project_ids is not None:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Scoped evidence requires a signed assignment"
            )
    elif (
        signed.payload.dispatch != attempt.dispatch
        or worker.project_ids is not None
        and signed.payload.spec.project_id not in worker.project_ids
    ):
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Historical assignment differs from enrollment"
        )
    return worker, attempt
