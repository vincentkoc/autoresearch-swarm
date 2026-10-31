"""Recovery route issuance authorizes one exact original-worker destination."""

from collections.abc import Mapping
from typing import Any

from research_swarm.domain.backup import RecoveryAdmission
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.recovery_routes import RecoveryRouteRequest
from research_swarm.state.recovery_guard import recovery_record, require_recovery_authority
from research_swarm.state.sql import SqlSession, StoreIdentity

OPERATION = "worker.issue_recovery_route"


def require_recovery_route(
    sql: SqlSession,
    context: CommandContext,
    authority: StoreIdentity,
    operation: str,
    payload: Mapping[str, Any],
    request_digest: str,
) -> None:
    guard = context._recovery_route
    if (
        guard is None
        or operation != OPERATION
        or guard.request_digest != request_digest
        or payload.get("restore_id") != guard.restore_id
        or context._recovery_guard is not None
        or context._recovery_operation is not None
        or context._recovery_reader is not None
        or context._recovery_turn is not None
        or context._recovery_activation is not None
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery route proof is required")
    receipt = require_recovery_authority(sql, context, authority, guard)
    admitted = RecoveryAdmission.model_validate(
        recovery_record(sql, receipt.workspace_id, "recovery_admission", receipt.restore_id)[2]
    )
    request = RecoveryRouteRequest.model_validate(payload.get("request"))
    from research_swarm.control.recovery_route_issuance import route_from_snapshot

    route = route_from_snapshot(
        sql,
        context,
        request,
        receipt,
        admitted,
        nonexpiring_credential=guard.nonexpiring_credential,
    )
    if route.route_id != guard.route_id or route.digest != guard.route_digest:
        raise ResearchError(ErrorCode.STALE_FENCE, "Recovery route original custody changed")
