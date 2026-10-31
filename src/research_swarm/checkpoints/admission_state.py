"""Shared SQL recovery fencing; signing, custody and admission guards remain with callers."""

from research_swarm.control.campaigns import request_drain_pause, settle_pause
from research_swarm.control.orchestration import fence_binding
from research_swarm.domain.backup import RecoveryAdmission
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import JsonObject, Transaction
from research_swarm.state.sql import SqlSession


def research_ids(connection: SqlSession, workspace: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    bindings = connection.execute(
        "SELECT id,state FROM records WHERE workspace_id=? AND kind='orchestration_binding' "
        "ORDER BY id LIMIT 10001",
        (workspace,),
    ).fetchall()
    campaigns = connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND kind='campaign' "
        "AND state IN ('running','pausing') ORDER BY id LIMIT 10001",
        (workspace,),
    ).fetchall()
    if max(len(bindings), len(campaigns)) > 10_000:
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE, "Recovery exceeds native profile bounds"
        )
    if any(row["state"] not in {"active", "fenced"} for row in bindings):
        raise ResearchError(ErrorCode.STALE_FENCE, "Unknown orchestration recovery state")
    return tuple(row["id"] for row in bindings), tuple(row["id"] for row in campaigns)


def admit_in(transaction: Transaction, admission: RecoveryAdmission) -> JsonObject:
    transaction.observe_waits = False
    request = admission.signed_claim.claim.request
    for identifier in admission.orchestration_campaign_ids:
        row = transaction.get("orchestration_binding", identifier)
        if row["state"] == "active":
            fence_binding(transaction, row, reason="Native recovery " + request.restore_id)
    # Owner records and pending turns stay recorded. Fencing prevents their old
    # principals from becoming ordinary actors or executing after later activation.
    for identifier in admission.paused_campaign_ids:
        row = transaction.get("campaign", identifier)
        if row["state"] == "running":
            request_drain_pause(transaction, row)
        else:
            settle_pause(transaction, identifier)
    recorded = transaction.insert(
        "recovery_admission",
        request.restore_id,
        admission.model_dump(mode="json"),
        state="immutable",
    )
    gate = transaction.get("workspace_gate", request.workspace_id)
    transaction.update(
        "workspace_gate",
        request.workspace_id,
        expected_revision=gate["revision"],
        state="recovering",
        data=gate["data"],
    )
    transaction.event("workspace.recovery_admitted", recorded, {"backup_id": request.backup_id})
    return recorded


def reject_admission(transaction: Transaction, error: ResearchError) -> None:
    # A failed transition must leave the pristine import retryable. Do not
    # append a rejection acknowledgment that its next preservation check rejects.
    raise error
