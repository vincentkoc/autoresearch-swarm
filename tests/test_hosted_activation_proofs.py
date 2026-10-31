"""Hosted activation evidence binds the election, prepared runtime and final witness."""

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from research_swarm.control.hosted_activation_proofs import (
    ACTIVATION_OPERATION,
    activation_key,
    activation_witness_payload,
    verify_hosted_activation,
)
from research_swarm.control.recovery_proofs import verify_activation
from research_swarm.control.time_witnesses import WitnessSigner
from research_swarm.crypto import sign
from research_swarm.domain.backup import ActivateRecovery, RecoveryActivation
from research_swarm.domain.canonical import canonical_bytes, content_digest, record_digest
from research_swarm.domain.checkpoint_elections import LogicalElectionAnchor
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.recovery_runtime import (
    HostedActivationRuntime,
    HostedRecoveryActivation,
    RecoveryRuntimeCredential,
    RecoveryRuntimePlan,
)
from research_swarm.domain.time_providers import TimeProviderData
from research_swarm.domain.time_witnesses import TimeWitnessRequest, WitnessScope
from research_swarm.state.sql import StoreIdentity
from research_swarm.state.time_providers import ProviderSnapshot, TimeProviders
from tests.test_checkpoint_elections import TARGET_SEED
from tests.test_recovery_elections import election as election
from tests.test_recovery_elections import proof as proof
from tests.test_time_witnesses import SEED, identifier, payload, source


def signed(p, activation):
    return activation.model_copy(
        update={
            "target_signature": sign(
                p.e.crypto, TARGET_SEED, activation.proof_digest.encode()
            ).hex()
        }
    )


def make_activation(p):
    e, receipt = p.e, p.receipt
    principal = Principal(id=e.intent.operator_id, workspace_id=receipt.workspace_id, scopes={"*"})
    authority = e.manifest.source_authority.model_copy(
        update={
            "state_authority_id": content_digest(b"target state"),
            "activation_id": identifier(201),
            "control_epoch": receipt.target_control_epoch,
            "assignment_key_id": content_digest(b"target signing key"),
        }
    )
    plan = RecoveryRuntimePlan(
        restore_id=receipt.restore_id,
        deployment_digest=e.intent.target_deployment_digest,
        configuration_digest=content_digest(b"target configuration"),
        authority=authority,
        source=source(),
        credentials=(
            RecoveryRuntimeCredential(
                id=identifier(202),
                principal=principal,
                token_digest="a" * 64,
            ),
        ),
    )
    runtime = HostedActivationRuntime(
        plan=plan,
        preparation_digest=content_digest(b"runtime preparation"),
        provider_revision=2,
        provider=TimeProviderData(
            scope=WitnessScope(**authority.model_dump(), provider_generation=identifier(203)),
            source=source(),
            source_epoch=identifier(7),
        ),
    )
    request_id, command_id, retired = identifier(204), identifier(205), (identifier(206),)
    request = ActivateRecovery(
        restore_id=receipt.restore_id,
        expected_control_epoch=receipt.target_control_epoch,
        reason="Original custody closed and target runtime prepared",
    )
    context = CommandContext(
        principal=principal,
        idempotency_key=activation_key(request_id),
        command_id=command_id,
        expected_revision=2,
    )
    view_digest, artifact_digest = content_digest(b"activation view"), content_digest(b"artifacts")
    data = activation_witness_payload(
        request,
        runtime_digest=runtime.digest,
        view_digest=view_digest,
        artifact_digest=artifact_digest,
        retired_credential_ids=retired,
    )
    providers = TimeProviders(StoreIdentity(e.intent.target_backend, e.intent.target_location))
    witness_request = TimeWitnessRequest(
        scope=runtime.provider.scope,
        operation_id=command_id,
        nonce=identifier(207),
        request_digest=providers._digest(
            context, ProviderSnapshot(2, "qualified", runtime.provider), ACTIVATION_OPERATION, data
        ),
    )
    activation = HostedRecoveryActivation(
        request_id=request_id,
        command_id=command_id,
        request=request,
        workspace_id=receipt.workspace_id,
        controller_id=receipt.controller_id,
        restoration_digest=receipt.digest,
        admission_digest=record_digest(
            "research.recovery-admission-envelope.v1", p.logical.model_dump(mode="json")
        ),
        logical_election_digest=e.election.digest,
        deployment_digest=plan.deployment_digest,
        runtime_digest=runtime.digest,
        view_digest=view_digest,
        artifact_digest=artifact_digest,
        retired_credential_ids=retired,
        final_witness=WitnessSigner(source(), SEED).sign(payload(witness_request)),
        evidence_through=10,
        gate_revision=2,
        operator_id=principal.id,
        activated_at="2026-01-01T00:00:03.000Z",
        target_signature="0" * 128,
    )
    anchor = LogicalElectionAnchor(
        source_authority=e.manifest.source_authority,
        source_location="synthetic/source",
        source_deployment_digest=e.manifest.source_deployment_digest,
        custody_pin=e.manifest.source_custody,
    )
    return SimpleNamespace(p=p, runtime=runtime, activation=signed(p, activation), anchor=anchor)


@pytest.fixture
def activation(proof):
    return make_activation(proof)


def verify(a, **changes):
    return verify_hosted_activation(
        changes.pop("activation", a.activation),
        changes.pop("admission", a.p.logical),
        changes.pop("receipt", a.p.receipt),
        changes.pop("election", a.p.e.election),
        runtime=changes.pop("runtime", a.runtime),
        expected_intent=changes.pop("intent", a.p.e.intent),
        expected_source=changes.pop("anchor", a.anchor),
        crypto_provider=a.p.e.crypto,
    )


def test_roundtrip_has_separate_native_and_hosted_wire_profiles(activation):
    a = activation
    verify(a)
    value = canonical_bytes(a.activation.model_dump(mode="json"))
    assert HostedRecoveryActivation.model_validate_json(value) == a.activation
    assert "private_files_digest" not in a.activation.model_dump(mode="json")
    with pytest.raises(ValidationError):
        RecoveryActivation.model_validate_json(value)
    with pytest.raises(ResearchError, match="Logical recovery"):
        verify_activation(a.activation, a.p.logical, a.p.receipt)


def test_runtime_activation_signature_and_witness_preimages_are_frozen(activation):
    a = activation
    assert (
        a.runtime.digest
        == "sha256:792780604722b041701fa450aa217f09fc568199871eb65c6c1ad5a5361adf58"
    )
    assert a.activation.proof_digest == (
        "sha256:f0c98ac64747779508d81b9ad8875f880cd432d410e1db1001024d23779b4a3c"
    )
    assert a.activation.final_witness.payload.request.request_digest == (
        "sha256:445e43f18408c717f78d131bbf96e937b692508e9954016f523ad4dc041e1ff6"
    )
    assert a.activation.target_signature == (
        "f42403046ac413fcab47ca7fd454ad54023476dc9a8e11ba3424b1e5eb8a050f4"
        "f6eee8f48aa65363437dd22b6b1b0b76a28b5107c12d69d5cf57bbdb6d6d507"
    )


@pytest.mark.parametrize(
    "field",
    [
        "restoration_digest",
        "admission_digest",
        "logical_election_digest",
        "deployment_digest",
        "runtime_digest",
        "view_digest",
        "artifact_digest",
    ],
)
def test_resigned_target_cannot_rebind_runtime_election_or_witness(activation, field):
    a = activation
    changed = signed(a.p, a.activation.model_copy(update={field: content_digest(b"different")}))
    with pytest.raises(ResearchError):
        verify(a, activation=changed)


@pytest.mark.parametrize("field", ["command_id", "request_id", "operator_id", "controller_id"])
def test_actor_and_operation_identity_remain_bound_even_with_a_valid_target_signature(
    activation, field
):
    a = activation
    changed = signed(a.p, a.activation.model_copy(update={field: identifier(299)}))
    with pytest.raises(ResearchError):
        verify(a, activation=changed)


@pytest.mark.parametrize("change", ["revision", "reason", "retired", "target_credential"])
def test_final_witness_is_bound_to_revision_request_and_retirement(activation, change):
    a = activation
    if change == "revision":
        value = {"gate_revision": 3}
    elif change == "reason":
        value = {"request": a.activation.request.model_copy(update={"reason": "Different request"})}
    else:
        value = {
            "retired_credential_ids": (
                a.runtime.plan.credentials[0].id
                if change == "target_credential"
                else identifier(299),
            )
        }
    with pytest.raises(ResearchError):
        verify(a, activation=signed(a.p, a.activation.model_copy(update=value)))


@pytest.mark.parametrize("change", ["epoch", "scope", "operation", "digest", "signature"])
def test_final_witness_requires_the_qualified_origin_and_complete_request(activation, change):
    a = activation
    witness = a.activation.final_witness
    observed = witness.payload
    if change == "epoch":
        observed = observed.model_copy(update={"source_epoch": identifier(299)})
    elif change == "scope":
        observed = observed.model_copy(
            update={
                "request": observed.request.model_copy(
                    update={
                        "scope": observed.request.scope.model_copy(
                            update={"provider_generation": identifier(299)}
                        )
                    }
                )
            }
        )
    elif change in {"operation", "digest"}:
        update = (
            {"operation_id": identifier(299)}
            if change == "operation"
            else {"request_digest": content_digest(b"other")}
        )
        observed = observed.model_copy(
            update={"request": observed.request.model_copy(update=update)}
        )
    witness = WitnessSigner(source(), SEED).sign(observed)
    if change == "signature":
        witness = witness.model_copy(update={"signature": "0" * 128})
    changed = signed(a.p, a.activation.model_copy(update={"final_witness": witness}))
    with pytest.raises(ResearchError):
        verify(a, activation=changed)


@pytest.mark.parametrize(
    "change",
    ["target_signature", "election", "admission", "source_pin", "target_location", "preparation"],
)
def test_independent_pins_and_all_nested_signatures_are_required(activation, change):
    a = activation
    options = {}
    if change == "target_signature":
        options["activation"] = a.activation.model_copy(update={"target_signature": "0" * 128})
    elif change == "election":
        options["election"] = a.p.e.election.model_copy(update={"signature": "0" * 128})
    elif change == "admission":
        options["admission"] = a.p.logical.model_copy(update={"target_signature": "0" * 128})
    elif change == "source_pin":
        options["anchor"] = a.anchor.model_copy(
            update={"custody_pin": a.anchor.custody_pin.model_copy(update={"public_key": "0" * 64})}
        )
    elif change == "target_location":
        options["intent"] = a.p.e.intent.model_copy(update={"target_location": "synthetic/other"})
    else:
        options["runtime"] = a.runtime.model_copy(
            update={"preparation_digest": content_digest(b"other")}
        )
    with pytest.raises(ResearchError):
        verify(a, **options)


@pytest.mark.parametrize(
    "inventory", [(identifier(2), identifier(1)), (identifier(1), identifier(1))]
)
def test_retirement_set_is_canonical(activation, inventory):
    with pytest.raises(ValidationError):
        HostedRecoveryActivation.model_validate(
            activation.activation.model_copy(update={"retired_credential_ids": inventory})
        )
