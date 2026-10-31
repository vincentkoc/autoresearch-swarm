"""Native operator activation after original execution, byte and driver custody closes."""

from dataclasses import asdict

from filelock import FileLock

from research_swarm.checkpoints.admission import verify_admitted
from research_swarm.checkpoints.restore import RestoreCustody
from research_swarm.control.activation_view import inspect_activation
from research_swarm.control.recovery_activation import activate_in as _activate
from research_swarm.control.recovery_activation import reject_activation as _reject
from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.crypto import public_key, sign
from research_swarm.domain.assignments import signing_key_id
from research_swarm.domain.backup import (
    ActivateRecovery,
    BackupCustodyPin,
    RecoveryActivation,
    RestoreReceipt,
)
from research_swarm.domain.canonical import content_digest, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal, RecoveryActivationGuard
from research_swarm.domain.types import utc_now
from research_swarm.runtime import LocalRuntime, RuntimeIdentity
from research_swarm.state.database import JsonObject, Transaction
from research_swarm.state.private_files import read_private, restore_admission_path
from research_swarm.storage.config import FilesystemConfig


def _private_identity(runtime: LocalRuntime) -> str:
    files = {
        name: read_private(runtime.root, name, limit=limit)
        for name, limit in (
            ("identity.json", 16_384),
            ("operator.token", 256),
            ("dispatch.key", 32),
            ("assignment.key", 32),
            ("restore.json", 1024**2),
            ("restore-custody.json", 16_384),
        )
    }
    identity = RuntimeIdentity.model_validate(strict_json_loads(files["identity.json"]))
    if (
        identity != runtime.identity
        or runtime.service.workers is None
        or not isinstance(identity.storage, FilesystemConfig)
        or identity.initialized_at is None
        or files["dispatch.key"] != runtime.service.workers.dispatch_key
        or len(files["assignment.key"]) != 32
        or signing_key_id(
            public_key(runtime.service.crypto_provider, files["assignment.key"]).hex()
        )
        != identity.assignment_key_id
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Native activation identity differs")
    operator = runtime.credentials.resolve(files["operator.token"].decode("ascii").strip())
    if operator != Principal(
        id=identity.operator_id, workspace_id=identity.workspace_id, scopes=frozenset({"*"})
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Native activation operator differs")
    return record_digest(
        "research.activation-private-files.v1",
        {name: content_digest(content) for name, content in files.items()},
    )


def activate_recovery(
    runtime: LocalRuntime, context: CommandContext, request: ActivateRecovery
) -> JsonObject:
    context.principal.require("*")
    if context.principal.workspace_id != runtime.identity.workspace_id:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Activation workspace differs")
    request = ActivateRecovery.model_validate(request)
    payload = request.model_dump(mode="json")

    def preflight() -> JsonObject:
        with FileLock(restore_admission_path(runtime.root).with_suffix(".lock")):
            private_digest = _private_identity(runtime)
            receipt = RestoreReceipt.model_validate(
                strict_json_loads(read_private(runtime.root, "restore.json", limit=1024**2))
            )
            custody = RestoreCustody.model_validate(
                strict_json_loads(read_private(runtime.root, "restore-custody.json", limit=16_384))
            )
            if (
                request.restore_id != receipt.restore_id
                or request.expected_control_epoch != receipt.target_control_epoch
                or custody.public_key != receipt.target_public_key
                or custody.intent.restore_id != receipt.restore_id
                or custody.intent.manifest_digest != receipt.manifest_digest
                or custody.intent.destination != str(runtime.root.resolve())
                or not verify_admitted(runtime.database, receipt)
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Activation target custody differs")
            pin = BackupCustodyPin.model_validate(
                runtime.database.read_record(context, "backup_custody", receipt.source_custody_id)[
                    "data"
                ]
            )
            if (pin.workspace_id, pin.controller_id, pin.control_epoch, pin.public_key) != (
                receipt.workspace_id,
                receipt.controller_id,
                receipt.source_control_epoch,
                receipt.source_public_key,
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Activation source custody differs")
            authority = recovery_authority(
                runtime.database, context, crypto_provider=runtime.service.crypto_provider
            )
            if authority is None or authority.restore_id != receipt.restore_id:
                raise ResearchError(ErrorCode.STALE_FENCE, "Activation admission is absent")
            view = runtime.database.store.read(
                lambda connection: inspect_activation(
                    Transaction(connection, context, observe_waits=False), receipt
                )
            )
            if (
                sum(item.reference.size_bytes for item in view.artifacts)
                > runtime.identity.storage_limit_bytes
            ):
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Activation storage exceeds limit"
                )
            for blob in view.artifacts:
                runtime.artifacts.store.verify(receipt.workspace_id, blob)
            if _private_identity(runtime) != private_digest:
                raise ResearchError(ErrorCode.STALE_FENCE, "Activation private identity changed")
            activation = RecoveryActivation(
                request=request,
                workspace_id=receipt.workspace_id,
                controller_id=receipt.controller_id,
                restoration_digest=receipt.digest,
                admission_digest=authority.admission_digest,
                view_digest=view.digest,
                artifact_digest=view.artifact_digest,
                private_files_digest=private_digest,
                evidence_through=view.evidence_through,
                gate_revision=authority.gate_revision,
                operator_id=context.principal.id,
                activated_at=utc_now(),
                target_signature="0" * 128,
            )
            activation = activation.model_copy(
                update={
                    "target_signature": sign(
                        runtime.service.crypto_provider,
                        bytes.fromhex(custody.private_key),
                        activation.proof_digest.encode("ascii"),
                    ).hex(),
                }
            )
            verified = context.model_copy()
            verified._recovery_activation = RecoveryActivationGuard(
                **asdict(authority),
                view_digest=view.digest,
                artifact_digest=view.artifact_digest,
                request_digest=record_digest(
                    "research.command",
                    {
                        "operation": "workspace.activate_recovery",
                        "expected_revision": context.expected_revision,
                        "payload": payload,
                    },
                ),
            )
            return runtime.database.execute(
                verified,
                "workspace.activate_recovery",
                payload,
                "*",
                lambda transaction: _activate(transaction, activation),
                on_rejection=_reject,
            )

    return runtime.database.with_preflight(
        context, "workspace.activate_recovery", payload, "*", preflight
    )
