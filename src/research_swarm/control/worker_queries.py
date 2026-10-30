"""Bounded worker views expose eligible offers and the caller's own assignments."""

from __future__ import annotations

import base64
import binascii
from typing import Literal

from pydantic import Field, ValidationError

from research_swarm.control.acceptance import lease_row
from research_swarm.control.models import CampaignData, JobData, ProjectRevisionData, TrialData
from research_swarm.control.queries import RecordCursor, summarize
from research_swarm.control.worker_sessions import historical_attempt_owner, historical_session
from research_swarm.control.worker_state import admission_open, worker_state
from research_swarm.control.workers import AttemptData, WorkerData, worker_authority
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ID, VersionedRecord
from research_swarm.state.database import Database, JsonObject, Transaction


class WorkerQuery(VersionedRecord):
    worker_id: ID
    session_id: ID
    kind: Literal["pending", "offers", "controls", "historical_pending"]
    limit: int = Field(default=25, strict=True, ge=1, le=100)
    cursor: str | None = Field(default=None, max_length=2048)


class WorkerRead(VersionedRecord):
    worker_id: ID
    session_id: ID
    kind: Literal[
        "worker",
        "attempt",
        "acceptance_lease",
        "trial",
        "worker_admission",
        "worker_presence",
        "worker_session",
        "worker_replacement",
        "historical_attempt",
    ]
    id: ID


def eligible_offer(transaction: Transaction, worker: WorkerData, row: JsonObject) -> bool:
    if not admission_open(transaction, worker.binding):
        return False
    job = JobData.model_validate(row["data"])
    trial_row = transaction.get("trial", job.trial_id)
    trial = TrialData.model_validate(trial_row["data"])
    if (
        worker.environment_contracts is not None
        and trial.spec.environment_digest not in worker.environment_contracts
    ):
        return False
    if worker.project_ids is not None and trial.spec.project_id not in worker.project_ids:
        return False
    campaign_row = transaction.get("campaign", row["campaign_id"])
    if campaign_row["state"] != "running" and not (
        campaign_row["state"] == "pausing" and trial_row["state"] == "running"
    ):
        return False
    if trial.execution_outcome != "pending":
        return False
    campaign = CampaignData.model_validate(campaign_row["data"])
    project = ProjectRevisionData.model_validate(
        transaction.get("project_revision", campaign.contract.project_revision_id)["data"]
    )
    if project.resolved.manifest.evaluation.measurement_trust == "protected":
        return False
    if job.stage.kind == "evaluate" and job.evaluation_request_id is None:
        return False
    profile = next(
        (p for p in worker.profiles if p.name == job.stage.resources.engine_profile), None
    )
    if profile is None:
        return False
    try:
        profile.require(job.stage.resources, project.resolved.manifest.environment.network_policy)
    except ResearchError as error:
        if error.code != ErrorCode.CAPABILITY_UNAVAILABLE:
            raise
        return False
    return True


class WorkerQueries:
    def __init__(self, database: Database) -> None:
        self.database = database

    def get(self, context: CommandContext, request: WorkerRead) -> JsonObject:
        context.principal.require("worker.query")
        request = WorkerRead.model_validate(request)
        with self.database.connect() as connection:
            connection.execute("BEGIN")
            transaction = Transaction(connection, context)
            if request.kind == "worker_session":
                if request.id != request.session_id:
                    raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Session identity differs")
                return historical_session(transaction, request.worker_id, request.session_id)
            if request.kind == "historical_attempt":
                historical_attempt_owner(
                    transaction,
                    request.worker_id,
                    request.session_id,
                    request.id,
                    require_signed=False,
                )
                return transaction.get("attempt", request.id)
            worker = worker_authority(transaction, request.worker_id, request.session_id)
            if request.kind == "worker_replacement":
                if request.id != request.session_id:
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED, "Replacement session differs"
                    )
                row = transaction.get("worker_replacement", request.id)
                if row["state"] != "immutable" or row["data"][
                    "binding"
                ] != worker.binding.model_dump(mode="json"):
                    raise ResearchError(ErrorCode.STALE_FENCE, "Replacement receipt differs")
                return row
            if request.kind in {"worker", "worker_admission", "worker_presence"}:
                if request.id != request.worker_id:
                    raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Worker identity differs")
                if request.kind == "worker_admission" or request.kind == "worker_presence":
                    return worker_state(transaction, worker.binding, request.kind)
            elif request.kind in {"attempt", "acceptance_lease"}:
                try:
                    row = transaction.get("attempt", request.id)
                except ResearchError as error:
                    if error.code != ErrorCode.NOT_FOUND:
                        raise
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED, "Record is not assigned to worker"
                    ) from error
                dispatch = AttemptData.model_validate(row["data"]).dispatch
                if (
                    dispatch.worker_id != request.worker_id
                    or dispatch.worker_session_id != request.session_id
                    or dispatch.controller_id != worker.binding.controller_id
                    or dispatch.control_epoch != worker.binding.control_epoch
                ):
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED, "Record is not assigned to worker"
                    )
                return (
                    lease_row(transaction, request.id)
                    if request.kind == "acceptance_lease"
                    else row
                )
            else:
                owned = connection.execute(
                    "SELECT id FROM records WHERE workspace_id=? AND kind='attempt' "
                    "AND json_extract(payload,'$.dispatch.worker_id')=? "
                    "AND json_extract(payload,'$.dispatch.worker_session_id')=? "
                    "AND json_extract(payload,'$.dispatch.control_epoch')=? "
                    "AND json_extract(payload,'$.dispatch.controller_id')=? "
                    "AND json_extract(payload,'$.dispatch.trial_id')=? LIMIT 1",
                    (
                        transaction.workspace_id,
                        request.worker_id,
                        request.session_id,
                        worker.binding.control_epoch,
                        worker.binding.controller_id,
                        request.id,
                    ),
                ).fetchone()
                if owned is None:
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED, "Record is not assigned to worker"
                    )
            return transaction.get(request.kind, request.id)

    def list(self, context: CommandContext, request: WorkerQuery) -> JsonObject:
        context.principal.require("worker.query")
        request = WorkerQuery.model_validate(request)
        with self.database.connect() as connection:
            connection.execute("BEGIN")
            transaction = Transaction(connection, context)
            worker = (
                WorkerData.model_validate(
                    historical_session(transaction, request.worker_id, request.session_id)["data"]
                )
                if request.kind == "historical_pending"
                else worker_authority(transaction, request.worker_id, request.session_id)
            )
            scope = record_digest(
                "research.worker-page",
                {
                    "binding": worker.binding.model_dump(mode="json"),
                    "kind": request.kind,
                    "actor_id": context.principal.id,
                },
            )
            if request.cursor is None:
                through = connection.execute(
                    "SELECT COALESCE(MAX(rowid),0) FROM records WHERE workspace_id=?",
                    (transaction.workspace_id,),
                ).fetchone()[0]
                cursor = RecordCursor(scope_digest=scope, after=0, through=through)
            else:
                try:
                    cursor = RecordCursor.model_validate(
                        strict_json_loads(
                            base64.b64decode(request.cursor, altchars=b"-_", validate=True)
                        )
                    )
                except (ValueError, binascii.Error, ValidationError) as error:
                    raise ResearchError(
                        ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid worker cursor"
                    ) from error
                if cursor.scope_digest != scope or cursor.after > cursor.through:
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED, "Worker cursor scope differs"
                    )
            if request.kind == "offers" and not admission_open(transaction, worker.binding):
                return {"items": [], "next_cursor": None, "scanned": 0}
            kind = {
                "pending": "attempt",
                "historical_pending": "attempt",
                "offers": "job",
                "controls": "attempt_control",
            }[request.kind]
            if request.kind == "offers":
                predicate = "AND state='ready'"
            else:
                source = (
                    "dispatch" if request.kind in {"pending", "historical_pending"} else "binding"
                )
                predicate = (
                    f"AND json_extract(payload,'$.{source}.worker_id')=? "
                    f"AND json_extract(payload,'$.{source}.worker_session_id')=? "
                    f"AND json_extract(payload,'$.{source}.control_epoch')=? "
                    f"AND json_extract(payload,'$.{source}.controller_id')=? "
                )
                predicate += (
                    "AND json_extract(payload,'$.publication_status')='pending'"
                    if request.kind in {"pending", "historical_pending"}
                    else "AND state='requested'"
                )
            parameters = (
                [
                    request.worker_id,
                    request.session_id,
                    worker.binding.control_epoch,
                    worker.binding.controller_id,
                ]
                if request.kind != "offers"
                else []
            )
            rows = connection.execute(
                "SELECT rowid,id FROM records WHERE workspace_id=? AND kind=? "
                "AND rowid>? AND rowid<=? " + predicate + " ORDER BY rowid LIMIT ?",
                (
                    transaction.workspace_id,
                    kind,
                    cursor.after,
                    cursor.through,
                    *parameters,
                    request.limit + 1,
                ),
            ).fetchall()
            more, scanned = len(rows) > request.limit, rows[: request.limit]
            items = []
            for item in scanned:
                row = transaction.get(kind, item["id"])
                if request.kind == "offers" and not eligible_offer(transaction, worker, row):
                    continue
                if request.kind == "pending":
                    attempt = AttemptData.model_validate(row["data"])
                    if attempt.dispatch.controller_id != worker.binding.controller_id:
                        continue
                items.append(summarize(row))
            next_cursor = (
                base64.urlsafe_b64encode(
                    canonical_bytes(
                        cursor.model_copy(update={"after": scanned[-1]["rowid"]}).model_dump(
                            mode="json"
                        )
                    )
                ).decode("ascii")
                if more
                else None
            )
            return {"items": items, "next_cursor": next_cursor, "scanned": len(scanned)}
