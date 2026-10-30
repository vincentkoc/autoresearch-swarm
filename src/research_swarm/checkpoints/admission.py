"""Admit original-custody recovery without reopening scientific execution."""

from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from filelock import FileLock

from research_swarm.checkpoints.backup import verify_backup
from research_swarm.checkpoints.claims import verify_claim
from research_swarm.checkpoints.restore import RestoreCustody, RestoreIntent, _verify_completed
from research_swarm.control.campaigns import request_drain_pause, settle_pause
from research_swarm.control.orchestration import fence_binding
from research_swarm.domain.backup import (
    RecoveryAdmission,
    RestoreClaimRequest,
    RestoreReceipt,
    SignedRestoreClaim,
    WorkspaceSeal,
)
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, RecoveryGuard
from research_swarm.domain.types import utc_now
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.private_files import read_private, restore_admission_path
from research_swarm.state.sql import SqlSession


def _check_claim(receipt: RestoreReceipt, signed: SignedRestoreClaim) -> None:
    if receipt.source_public_key is None:
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Restore has no source key binding")
    claim = verify_claim(signed, source_public_key=receipt.source_public_key)
    expected = RestoreClaimRequest(
        backup_id=receipt.backup_id,
        manifest_digest=receipt.manifest_digest,
        workspace_id=receipt.workspace_id,
        controller_id=receipt.controller_id,
        source_custody_id=receipt.source_custody_id,
        source_control_epoch=receipt.source_control_epoch,
        restore_id=receipt.restore_id,
        target_public_key=receipt.target_public_key,
        target_control_epoch=receipt.target_control_epoch,
    )
    if claim.request != expected:
        raise ResearchError(ErrorCode.STALE_FENCE, "Source claim elected another restoration")


def verify_admission(admission: RecoveryAdmission, receipt: RestoreReceipt) -> None:
    _check_claim(receipt, admission.signed_claim)
    if admission.restoration_digest != receipt.digest:
        raise ResearchError(ErrorCode.STALE_FENCE, "Recovery admission restoration differs")
    try:
        Ed25519PublicKey.from_public_bytes(bytes.fromhex(receipt.target_public_key)).verify(
            bytes.fromhex(admission.target_signature), admission.proof_digest.encode("ascii")
        )
    except InvalidSignature as error:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Recovery target proof differs"
        ) from error


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


def _research_ids(
    connection: SqlSession, workspace: str
) -> tuple[tuple[str, ...], tuple[str, ...]]:
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


def _admit(transaction: Transaction, admission: RecoveryAdmission) -> JsonObject:
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


def _reject_admission(transaction: Transaction, error: ResearchError) -> None:
    # A failed native transition must leave the pristine import retryable. Do not
    # append a rejection acknowledgment that its next preservation check rejects.
    raise error


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
            _check_claim(receipt, signed_claim)
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
