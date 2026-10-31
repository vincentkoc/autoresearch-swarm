"""Export a credential-free destination from the elected target's native custody."""

from filelock import FileLock

from research_swarm.checkpoints.admission import verify_admitted
from research_swarm.checkpoints.restore import RestoreCustody
from research_swarm.control.recovery_route_issuance import (
    normalize_route_request,
    route_from_snapshot,
)
from research_swarm.control.recovery_routes import verify_recovery_route
from research_swarm.crypto import sign
from research_swarm.domain.backup import BackupCustodyPin, RecoveryAdmission, RestoreReceipt
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.recovery_routes import (
    RecoveryRouteRequest,
    SignedWorkerRecoveryRoute,
    WorkerRecoveryRoute,
)
from research_swarm.runtime import LocalRuntime, private_state_directory
from research_swarm.state.database import Transaction
from research_swarm.state.private_files import read_private, restore_admission_path, write_private
from research_swarm.state.sql import SqlSession

MAX_ROUTE_BYTES = 4 * 1024 * 1024


def issue_recovery_route(
    runtime: LocalRuntime, context: CommandContext, request: RecoveryRouteRequest
) -> SignedWorkerRecoveryRoute:
    context.principal.require("*")
    if context.principal.workspace_id != runtime.identity.workspace_id:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery route workspace differs")
    request = normalize_route_request(request)
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

        def read(sql: SqlSession) -> tuple[WorkerRecoveryRoute, BackupCustodyPin]:
            transaction = Transaction(sql, context)
            admitted = RecoveryAdmission.model_validate(
                transaction.get("recovery_admission", receipt.restore_id)["data"]
            )
            pin = BackupCustodyPin.model_validate(
                transaction.get("backup_custody", receipt.source_custody_id)["data"]
            )
            return route_from_snapshot(sql, context, request, receipt, admitted), pin

        route, pin = runtime.database.store.read(read)
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
