"""Narrow preparation effects while an elected target remains non-operational."""

from collections.abc import Mapping
from typing import Any

from research_swarm.domain.backup import RecoveryAdmission
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.recovery_runtime import RecoveryRuntimePlan
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.state.database import _request_digest
from research_swarm.state.recovery_guard import recovery_record, require_recovery_authority
from research_swarm.state.sql import SqlSession, StoreIdentity

PLAN_KIND = "recovery_runtime_plan"
OPERATIONS = frozenset(
    {
        "credential.install",
        "time_provider.register",
        "time_provider.requalify",
        "time_provider.fence",
    }
)


def require_recovery_runtime(
    sql: SqlSession,
    context: CommandContext,
    authority: StoreIdentity,
    operation: str,
    payload: Mapping[str, Any],
    request_digest: str,
) -> None:
    guard = context._recovery_runtime
    if (
        guard is None
        or operation not in OPERATIONS
        or guard.operation != operation
        or guard.request_digest != request_digest
        or request_digest != _request_digest(context, operation, payload)
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Runtime preparation proof differs")
    receipt = require_recovery_authority(sql, context, authority, guard)
    workspace = context.principal.workspace_id
    admitted = RecoveryAdmission.model_validate(
        recovery_record(sql, workspace, "recovery_admission", guard.restore_id)[2]
    )
    row = sql.execute(
        "SELECT payload FROM records WHERE workspace_id=? AND kind=? AND id=? "
        "AND state='immutable' AND revision=1 AND typeof(payload)='text' "
        "AND length(CAST(payload AS BLOB))<=1048576",
        (workspace, PLAN_KIND, guard.restore_id),
    ).fetchone()
    if row is None or admitted.operator_id != context.principal.id:
        raise ResearchError(ErrorCode.STALE_FENCE, "Runtime preparation owner or plan changed")
    context.principal.require("auth.manage")
    plan = RecoveryRuntimePlan.model_validate_json(row[0])
    if (
        plan.digest != guard.plan_digest
        or plan.restore_id != receipt.restore_id
        or (plan.authority.workspace_id, plan.authority.controller_id, plan.authority.control_epoch)
        != (receipt.workspace_id, receipt.controller_id, receipt.target_control_epoch)
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Runtime preparation plan changed")
    if operation == "credential.install":
        member = next((item for item in plan.credentials if item.id == payload.get("id")), None)
        if member is None or dict(payload) != {
            "id": member.id,
            "principal": member.principal.model_dump(mode="json"),
            "token_digest": member.token_digest,
        }:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Credential is outside runtime plan"
            )
        if (
            sql.execute(
                "SELECT 1 FROM credentials WHERE id=? OR token_digest=? LIMIT 1",
                (member.id, member.token_digest),
            ).fetchone()
            is not None
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Recovery credentials must be fresh")
    elif operation == "time_provider.register":
        if context.expected_revision != 0 or dict(payload) != {
            "authority": plan.authority.model_dump(mode="json"),
            "source": plan.source.model_dump(mode="json"),
        }:
            raise ResearchError(ErrorCode.STALE_FENCE, "Runtime provider registration differs")
    else:
        # TimeProviders independently verifies the exact current snapshot, witness
        # signature, nonce, request preimage and unretired source before writing.
        expected = payload.get("expected", {})
        data = expected.get("data", {}) if isinstance(expected, Mapping) else {}
        scope = data.get("scope", {}) if isinstance(data, Mapping) else {}
        if (
            not isinstance(data, Mapping)
            or not isinstance(scope, Mapping)
            or {key: scope.get(key) for key in WitnessAuthorityScope.model_fields}
            != plan.authority.model_dump(mode="json")
            or data.get("source") != plan.source.model_dump(mode="json")
            or payload.get("source") != plan.source.model_dump(mode="json")
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Runtime provider qualification differs")
