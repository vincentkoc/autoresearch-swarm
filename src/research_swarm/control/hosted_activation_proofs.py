"""Offline hosted activation verification against independently retained target/source pins."""

from research_swarm.checkpoints.elections import verify_election
from research_swarm.control.recovery_proofs import verify_admission
from research_swarm.control.time_witnesses import witness_signing_bytes
from research_swarm.crypto import Ed25519Provider, resolve_crypto, verify
from research_swarm.domain.backup import ActivateRecovery, RecoveryAdmission, RestoreReceipt
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.checkpoint_elections import LogicalElectionAnchor, SignedLogicalElection
from research_swarm.domain.checkpoint_images import LogicalImportIntent
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.recovery_runtime import HostedActivationRuntime, HostedRecoveryActivation
from research_swarm.state.database import JsonObject
from research_swarm.state.sql import StoreIdentity
from research_swarm.state.time_providers import ProviderSnapshot, TimeProviders

ACTIVATION_OPERATION = "workspace.activate_recovery"


def activation_key(request_id: str) -> str:
    return "logical-activation:" + request_id


def activation_witness_payload(
    request: ActivateRecovery,
    *,
    runtime_digest: str,
    view_digest: str,
    artifact_digest: str,
    retired_credential_ids: tuple[str, ...],
) -> JsonObject:
    return {
        "request": request.model_dump(mode="json"),
        "runtime_digest": runtime_digest,
        "view_digest": view_digest,
        "artifact_digest": artifact_digest,
        "retired_credential_ids": list(retired_credential_ids),
    }


def verify_hosted_activation(
    activation: HostedRecoveryActivation,
    admission: RecoveryAdmission,
    receipt: RestoreReceipt,
    election: SignedLogicalElection,
    *,
    runtime: HostedActivationRuntime,
    expected_intent: LogicalImportIntent,
    expected_source: LogicalElectionAnchor,
    crypto_provider: Ed25519Provider | None = None,
) -> None:
    """Verify historical evidence; this does not grant live execution or prove elapsed time."""
    activation = HostedRecoveryActivation.model_validate(activation)
    runtime = HostedActivationRuntime.model_validate(runtime)
    crypto = resolve_crypto(crypto_provider)
    verify_election(
        election,
        expected_intent=expected_intent,
        expected_backup_id=receipt.backup_id,
        expected_source=expected_source.source_authority,
        expected_deployment_digest=expected_source.source_deployment_digest,
        expected_custody=expected_source.custody_pin,
        crypto=crypto,
    )
    verify_admission(admission, receipt, crypto_provider=crypto)
    plan, scope = runtime.plan, runtime.provider.scope
    if (
        admission.logical_election_digest != election.digest
        or admission.signed_claim != election.election.signed_claim
        or activation.logical_election_digest != election.digest
        or activation.restoration_digest != receipt.digest
        or activation.admission_digest
        != record_digest(
            "research.recovery-admission-envelope.v1", admission.model_dump(mode="json")
        )
        or activation.runtime_digest != runtime.digest
        or activation.deployment_digest != plan.deployment_digest
        or plan.deployment_digest != expected_intent.target_deployment_digest
        or (activation.request.restore_id, plan.restore_id)
        != (receipt.restore_id, receipt.restore_id)
        or activation.request.expected_control_epoch != receipt.target_control_epoch
        or (activation.workspace_id, activation.controller_id)
        != (receipt.workspace_id, receipt.controller_id)
        or (scope.workspace_id, scope.controller_id, scope.control_epoch)
        != (receipt.workspace_id, receipt.controller_id, receipt.target_control_epoch)
        or activation.operator_id != admission.operator_id
        or activation.operator_id != expected_intent.operator_id
        or scope.assignment_key_id == expected_source.source_authority.assignment_key_id
        or set(activation.retired_credential_ids) & {item.id for item in plan.credentials}
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Hosted activation lifecycle differs")
    context = CommandContext(
        principal=Principal(
            id=activation.operator_id, workspace_id=activation.workspace_id, scopes=frozenset()
        ),
        command_id=activation.command_id,
        idempotency_key=activation_key(activation.request_id),
        expected_revision=activation.gate_revision,
    )
    payload = activation_witness_payload(
        activation.request,
        runtime_digest=activation.runtime_digest,
        view_digest=activation.view_digest,
        artifact_digest=activation.artifact_digest,
        retired_credential_ids=activation.retired_credential_ids,
    )
    providers = TimeProviders(
        StoreIdentity(expected_intent.target_backend, expected_intent.target_location)
    )
    expected_digest = providers._digest(
        context,
        ProviderSnapshot(runtime.provider_revision, "qualified", runtime.provider),
        ACTIVATION_OPERATION,
        payload,
    )
    witness, source = activation.final_witness, runtime.provider.source
    observed = witness.payload
    if (
        witness.key_id != source.key_id
        or observed.source_id != source.id
        or observed.source_epoch != runtime.provider.source_epoch
        or observed.native_profile != source.native_profile
        or observed.policy != source.policy
        or observed.request.scope != scope
        or observed.request.operation_id != activation.command_id
        or observed.request.request_digest != expected_digest
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Hosted activation witness differs")
    try:
        verify(
            crypto,
            bytes.fromhex(source.public_key),
            witness_signing_bytes(witness.key_id, observed),
            bytes.fromhex(witness.signature),
        )
        verify(
            crypto,
            bytes.fromhex(receipt.target_public_key),
            activation.proof_digest.encode("ascii"),
            bytes.fromhex(activation.target_signature),
        )
    except ValueError as error:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Hosted activation signature differs"
        ) from error
