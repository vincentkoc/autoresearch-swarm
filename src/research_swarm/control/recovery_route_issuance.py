"""Build a route from one original-custody snapshot; signing stays with its owner."""

from research_swarm.control.worker_records import AttemptData, WorkerData
from research_swarm.domain.backup import RecoveryAdmission, RestoreReceipt
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.endpoints import controller_url
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.recovery_routes import RecoveryRouteRequest, WorkerRecoveryRoute
from research_swarm.state.sql import SqlSession


def normalize_route_request(request: RecoveryRouteRequest) -> RecoveryRouteRequest:
    request = RecoveryRouteRequest.model_validate(request)
    return request.model_copy(
        update={
            "source_controller_url": controller_url(request.source_controller_url),
            "destination_url": controller_url(request.destination_url),
        }
    )


def route_from_snapshot(
    sql: SqlSession,
    context: CommandContext,
    request: RecoveryRouteRequest,
    receipt: RestoreReceipt,
    admission: RecoveryAdmission,
) -> WorkerRecoveryRoute:
    """The caller must verify current admission and its signing custody separately."""
    context.principal.require("*")
    request = normalize_route_request(request)
    workspace = context.principal.workspace_id
    if workspace != receipt.workspace_id:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery route workspace differs")
    owner = sql.execute(
        "SELECT 1 FROM records WHERE workspace_id=? AND kind='orchestration_owner' AND id=?",
        (workspace, context.principal.id),
    ).fetchone()
    if owner is not None:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Drivers cannot issue recovery routes")

    def payload(kind: str, identifier: str, *, immutable: bool = False) -> str:
        row = sql.execute(
            "SELECT state,revision,payload FROM records WHERE workspace_id=? AND kind=? AND id=? "
            "AND typeof(payload)='text' AND length(CAST(payload AS BLOB))<=1048576",
            (workspace, kind, identifier),
        ).fetchone()
        if row is None or (immutable and (row["state"], row["revision"]) != ("immutable", 1)):
            raise ResearchError(ErrorCode.STALE_FENCE, "Original recovery record differs")
        return str(row["payload"])

    attempt = AttemptData.model_validate_json(payload("attempt", request.attempt_id))
    worker = WorkerData.model_validate_json(
        payload("worker_session", attempt.dispatch.worker_session_id, immutable=True)
    )
    if (
        attempt.dispatch.attempt_id != request.attempt_id
        or any(
            getattr(attempt.dispatch, key) != getattr(worker.binding, key)
            for key in type(worker.binding).model_fields
        )
        or request.attempt_id not in receipt.unresolved_attempt_ids
        or worker.binding.worker_id not in receipt.worker_ids
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Recovery route attempt session differs")
    credential = sql.execute(
        "SELECT workspace_id,principal,revoked FROM credentials WHERE id=? "
        "AND typeof(principal)='text' AND length(CAST(principal AS BLOB))<=16384",
        (request.credential_id,),
    ).fetchone()
    if credential is None or credential["revoked"] or credential["workspace_id"] != workspace:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Original recovery credential is unavailable"
        )
    principal = Principal.model_validate_json(credential["principal"])
    if principal.id != worker.principal_id or principal.workspace_id != workspace:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery credential owner differs")
    for scope in ("worker.query", "worker.outputs", "worker.publish"):
        principal.require(scope)
    return WorkerRecoveryRoute(
        **request.model_dump(),
        binding=worker.binding,
        worker_data_digest=record_digest(
            "research.recovery-worker.v1", worker.model_dump(mode="json")
        ),
        principal_id=worker.principal_id,
        restoration=receipt,
        admission=admission,
    )
