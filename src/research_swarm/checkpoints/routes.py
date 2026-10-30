"""Export a credential-free destination from the elected target's native custody."""

from filelock import FileLock

from research_swarm.checkpoints.admission import verify_admitted
from research_swarm.checkpoints.restore import RestoreCustody
from research_swarm.control.recovery_routes import verify_recovery_route
from research_swarm.control.worker_records import AttemptData, WorkerData
from research_swarm.crypto import sign
from research_swarm.domain.backup import BackupCustodyPin, RecoveryAdmission, RestoreReceipt
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.endpoints import controller_url
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.recovery_routes import (
    RecoveryRouteRequest,
    SignedWorkerRecoveryRoute,
    WorkerRecoveryRoute,
)
from research_swarm.runtime import LocalRuntime, private_state_directory
from research_swarm.state.private_files import read_private, restore_admission_path, write_private

MAX_ROUTE_BYTES = 4 * 1024 * 1024


def issue_recovery_route(
    runtime: LocalRuntime, context: CommandContext, request: RecoveryRouteRequest
) -> SignedWorkerRecoveryRoute:
    context.principal.require("*")
    if context.principal.workspace_id != runtime.identity.workspace_id:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery route workspace differs")
    request = RecoveryRouteRequest.model_validate(request)
    request = request.model_copy(
        update={
            "source_controller_url": controller_url(request.source_controller_url),
            "destination_url": controller_url(request.destination_url),
        }
    )
    with FileLock(restore_admission_path(runtime.root).with_suffix(".lock")):
        receipt = RestoreReceipt.model_validate(
            strict_json_loads(read_private(runtime.root, "restore.json", limit=MAX_ROUTE_BYTES))
        )
        if not verify_admitted(runtime.database, receipt):
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Recovery route requires admitted target custody"
            )
        custody = RestoreCustody.model_validate(
            strict_json_loads(read_private(runtime.root, "restore-custody.json", limit=16_384))
        )
        if (
            custody.public_key != receipt.target_public_key
            or custody.intent.restore_id != receipt.restore_id
            or custody.intent.manifest_digest != receipt.manifest_digest
            or custody.intent.destination != str(runtime.root.resolve())
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Recovery route target custody differs")
        attempt = AttemptData.model_validate(
            runtime.database.read_record(context, "attempt", request.attempt_id)["data"]
        )
        worker = WorkerData.model_validate(
            runtime.database.read_record(
                context, "worker_session", attempt.dispatch.worker_session_id
            )["data"]
        )
        if any(
            getattr(attempt.dispatch, key) != getattr(worker.binding, key)
            for key in type(worker.binding).model_fields
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Recovery route attempt session differs")
        with runtime.database.connect() as connection:
            credential = connection.execute(
                "SELECT workspace_id,principal,revoked FROM credentials WHERE id=?",
                (request.credential_id,),
            ).fetchone()
            owner = connection.execute(
                "SELECT 1 FROM records WHERE workspace_id=? "
                "AND kind='orchestration_owner' AND id=?",
                (context.principal.workspace_id, context.principal.id),
            ).fetchone()
        if owner is not None:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Drivers cannot issue recovery routes"
            )
        if (
            credential is None
            or credential["revoked"]
            or credential["workspace_id"] != receipt.workspace_id
        ):
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Original recovery credential is unavailable"
            )
        principal = Principal.model_validate_json(credential["principal"])
        if principal.id != worker.principal_id or principal.workspace_id != receipt.workspace_id:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery credential owner differs")
        for scope in ("worker.query", "worker.outputs", "worker.publish"):
            principal.require(scope)
        admitted = RecoveryAdmission.model_validate(
            runtime.database.read_record(context, "recovery_admission", receipt.restore_id)["data"]
        )
        pin = BackupCustodyPin.model_validate(
            runtime.database.read_record(context, "backup_custody", receipt.source_custody_id)[
                "data"
            ]
        )
        route = WorkerRecoveryRoute(
            **request.model_dump(),
            binding=worker.binding,
            worker_data_digest=record_digest(
                "research.recovery-worker.v1", worker.model_dump(mode="json")
            ),
            principal_id=worker.principal_id,
            restoration=receipt,
            admission=admitted,
        )
        signed = SignedWorkerRecoveryRoute(
            route=route,
            target_signature=sign(
                runtime.service.crypto_provider,
                bytes.fromhex(custody.private_key),
                route.digest.encode("ascii"),
            ).hex(),
        )
        verify_recovery_route(signed, pin, crypto_provider=runtime.service.crypto_provider)
        content = canonical_bytes(signed.model_dump(mode="json"))
        if len(content) > MAX_ROUTE_BYTES:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Recovery route exceeds its limit"
            )
        root = runtime.root / "recovery-routes"
        with private_state_directory(root):
            path = root / (request.route_id + ".json")
            if path.exists():
                if read_private(root, path.name, limit=MAX_ROUTE_BYTES) != content:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT,
                        "Recovery route ID already binds another destination",
                    )
            else:
                write_private(path, content)
        return signed
