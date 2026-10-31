"""Admit original-custody recovery without reopening scientific execution."""

from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from filelock import FileLock

from research_swarm.checkpoints.admission_state import admit_in as _admit
from research_swarm.checkpoints.admission_state import reject_admission as _reject_admission
from research_swarm.checkpoints.admission_state import research_ids as _research_ids
from research_swarm.checkpoints.backup import verify_backup
from research_swarm.checkpoints.restore import RestoreCustody, RestoreIntent, _verify_completed
from research_swarm.control.recovery_proofs import validate_claim_binding
from research_swarm.control.recovery_proofs import verify_admission as verify_admission
from research_swarm.domain.backup import (
    RecoveryAdmission,
    RestoreReceipt,
    SignedRestoreClaim,
    WorkspaceSeal,
)
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, RecoveryGuard
from research_swarm.domain.types import utc_now
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Database, JsonObject
from research_swarm.state.private_files import read_private, restore_admission_path
from research_swarm.state.sql import SqlSession


def verify_admitted(database: Database, receipt: RestoreReceipt) -> bool:
    """A signed lifecycle record replaces pristine-import comparison after admission."""

    def verify(connection: SqlSession) -> bool:
        row = connection.execute(
            "SELECT state,payload FROM records WHERE workspace_id=? "
            "AND kind='recovery_admission' AND id=?",
            (receipt.workspace_id, receipt.restore_id),
        ).fetchone()
        if row is None:
            return False
        admission = RecoveryAdmission.model_validate_json(row["payload"])
        verify_admission(admission, receipt)
        authority = connection.execute(
            "SELECT controller_id,control_epoch FROM workspaces WHERE id=?", (receipt.workspace_id,)
        ).one()
        gate = connection.execute(
            "SELECT state,payload FROM records WHERE workspace_id=? "
            "AND kind='workspace_gate' AND id=?",
            (receipt.workspace_id, receipt.workspace_id),
        ).one()
        seal = WorkspaceSeal.model_validate_json(gate["payload"])
        if (
            row["state"] != "immutable"
            or gate["state"] != "recovering"
            or (authority["controller_id"], authority["control_epoch"])
            != (receipt.controller_id, receipt.target_control_epoch)
            or (seal.backup_id, seal.control_epoch)
            != (receipt.backup_id, receipt.source_control_epoch)
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Recovery lifecycle differs")
        original = connection.execute(
            "SELECT state,payload FROM records WHERE workspace_id=? "
            "AND kind='restoration' AND id=?",
            (receipt.workspace_id, receipt.restore_id),
        ).one()
        if (
            original["state"] != "inactive"
            or RestoreReceipt.model_validate_json(original["payload"]) != receipt
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Original restoration receipt differs")
        for kind, identifiers, states in (
            ("orchestration_binding", admission.orchestration_campaign_ids, {"fenced"}),
            ("campaign", admission.paused_campaign_ids, {"pausing", "paused"}),
        ):
            for identifier in identifiers:
                current = connection.execute(
                    "SELECT state FROM records WHERE workspace_id=? AND kind=? AND id=?",
                    (receipt.workspace_id, kind, identifier),
                ).fetchone()
                if current is None or current["state"] not in states:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Recovery research fence differs")
        return True

    return database.store.read(verify)


def admit_recovery(
    runtime: LocalRuntime, context: CommandContext, bundle: Path, signed_claim: SignedRestoreClaim
) -> JsonObject:
    context.principal.require("*")
    if context.principal.workspace_id != runtime.identity.workspace_id:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery workspace differs")
    signed_claim = SignedRestoreClaim.model_validate(signed_claim)
    payload = signed_claim.model_dump(mode="json")
    request = signed_claim.claim.request

    def preflight() -> JsonObject:
        with FileLock(restore_admission_path(runtime.root).with_suffix(".lock")):
            backup = verify_backup(bundle, expected_digest=request.manifest_digest)
            intent = RestoreIntent(
                restore_id=request.restore_id,
                manifest_digest=request.manifest_digest,
                destination=str(runtime.root.resolve()),
            )
            receipt = _verify_completed(backup, runtime.root, intent)
            validate_claim_binding(receipt, signed_claim)
            custody = RestoreCustody.model_validate(
                strict_json_loads(read_private(runtime.root, "restore-custody.json", limit=16_384))
            )
            gate = runtime.database.read_record(context, "workspace_gate", receipt.workspace_id)
            if gate["state"] != "restoring" or backup.custody_pin is None:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Recovery requires a pinned inactive restore"
                )
            bindings, campaigns = runtime.database.store.read(
                lambda connection: _research_ids(connection, receipt.workspace_id)
            )
            watermark = runtime.database.store.read(
                lambda connection: (
                    connection.execute(
                        "SELECT next_sequence FROM workspaces WHERE id=?",
                        (receipt.workspace_id,),
                    ).one()["next_sequence"]
                    - 1
                )
            )
            admission = RecoveryAdmission(
                signed_claim=signed_claim,
                restoration_digest=receipt.digest,
                orchestration_campaign_ids=bindings,
                paused_campaign_ids=campaigns,
                operator_id=context.principal.id,
                admitted_at=utc_now(),
                target_signature="0" * 128,
            )
            key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(custody.private_key))
            admission = admission.model_copy(
                update={
                    "target_signature": key.sign(admission.proof_digest.encode("ascii")).hex(),
                }
            )
            identity = runtime.database.authority_identity
            verified = context.model_copy()
            verified._recovery_guard = RecoveryGuard(
                authority=(identity.backend, identity.location),
                request_digest=record_digest(
                    "research.command",
                    {
                        "operation": "workspace.begin_recovery",
                        "expected_revision": context.expected_revision,
                        "payload": payload,
                    },
                ),
                restore_id=receipt.restore_id,
                restoration_digest=receipt.digest,
                custody_pin_digest=record_digest(
                    "research.backup-custody-pin.v1", backup.custody_pin.model_dump(mode="json")
                ),
                target_public_key=custody.public_key,
                gate_revision=gate["revision"],
                control_epoch=receipt.target_control_epoch,
                watermark=watermark,
            )
            return runtime.database.execute(
                verified,
                "workspace.begin_recovery",
                payload,
                "*",
                lambda transaction: _admit(transaction, admission),
                on_rejection=_reject_admission,
            )

    return runtime.database.with_preflight(
        context, "workspace.begin_recovery", payload, "*", preflight
    )
