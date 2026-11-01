"""Constructed completed history; this does not qualify the live activation adapter."""

import json

import pytest

from adapters.cloudflare.platform.provisioning import (
    ProvisioningReceipt,
    complete_deployment_in,
    require_deployment,
    write_deployment_intent,
)
from adapters.cloudflare.recovery.activation_history import require_completed_activation
from adapters.cloudflare.recovery.checkpoint_target import retained_target_binding
from research_swarm.control.hosted_activation_proofs import (
    ACTIVATION_OPERATION,
    activation_key,
    activation_witness_payload,
)
from research_swarm.control.recovery_activation import activate_in
from research_swarm.crypto import sign
from research_swarm.domain.backup import ActivateRecovery, RecoveryAdmission
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.recovery_runtime import HostedRecoveryActivation
from research_swarm.domain.types import new_id, utc_now
from research_swarm.state.database import Transaction, encode
from research_swarm.state.time_providers import ProviderSnapshot, TimeProviders
from tests.adapters.cloudflare.hosted import test_hosted_activation_cut as fixtures
from tests.adapters.cloudflare.hosted.test_hosted_activation_cut import inspect
from tests.adapters.cloudflare.recovery.test_checkpoint_import import snapshot
from tests.adapters.cloudflare.recovery.test_checkpoint_runtime import records

deployment = fixtures.deployment
bootstrap = fixtures.bootstrap
source = fixtures.source
sealed = fixtures.sealed
recovery = fixtures.recovery
configured = fixtures.configured
imported = fixtures.imported
elected = fixtures.elected
target = fixtures.target
prepared = fixtures.prepared


@pytest.fixture
async def completed(prepared):
    e = prepared
    e.binding = retained_target_binding(
        json.dumps(e.target_config),
        e.manifest,
        e.target_secrets,
        identity=e.target.identity,
        crypto=e.source.crypto,
    )
    cut = inspect(e)
    admission = RecoveryAdmission.model_validate_json(
        records(e, "recovery_admission")[0]["payload"]
    )
    request_id = new_id()
    context = e.context.model_copy(
        update={
            "idempotency_key": activation_key(request_id),
            "expected_revision": records(e, "workspace_gate")[0]["revision"],
        }
    )
    request = ActivateRecovery(
        restore_id=e.restoration.restore_id,
        expected_control_epoch=e.restoration.target_control_epoch,
        reason="Synthetic completed history fixture",
    )
    payload = activation_witness_payload(
        request,
        runtime_digest=e.runtime.digest,
        view_digest=cut.view.digest,
        artifact_digest=cut.view.artifact_digest,
        retired_credential_ids=cut.retired_credential_ids,
    )
    registry = TimeProviders(e.target.identity, crypto_provider=e.source.crypto)
    _, pending = registry.prepare(
        context,
        ProviderSnapshot(e.runtime.provider_revision, "qualified", e.runtime.provider),
        ACTIVATION_OPERATION,
        payload,
    )
    proof = pending.complete(await e.witness(pending.request))
    activation = HostedRecoveryActivation(
        request_id=request_id,
        command_id=context.command_id,
        request=request,
        workspace_id=e.restoration.workspace_id,
        controller_id=e.restoration.controller_id,
        restoration_digest=e.restoration.digest,
        admission_digest=record_digest(
            "research.recovery-admission-envelope.v1", admission.model_dump(mode="json")
        ),
        logical_election_digest=admission.logical_election_digest,
        deployment_digest=e.manifest.digest,
        runtime_digest=e.runtime.digest,
        view_digest=cut.view.digest,
        artifact_digest=cut.view.artifact_digest,
        retired_credential_ids=cut.retired_credential_ids,
        final_witness=proof.witness,
        evidence_through=cut.view.evidence_through,
        gate_revision=context.expected_revision,
        operator_id=context.principal.id,
        activated_at=utc_now(),
        target_signature="0" * 128,
    )
    e.activation = activation.model_copy(
        update={
            "target_signature": sign(
                e.source.crypto,
                bytes.fromhex(e.target_config["custody_seed_hex"]),
                activation.proof_digest.encode(),
            ).hex()
        }
    )

    def install(sql):
        for identifier in cut.retired_credential_ids:
            sql.execute("UPDATE credentials SET revoked=1 WHERE id=?", (identifier,))
        activate_in(Transaction(sql, context), e.activation)
        write_deployment_intent(sql, e.manifest)
        complete_deployment_in(
            sql,
            e.manifest,
            ProvisioningReceipt(
                manifest_digest=e.manifest.digest,
                configuration_digest=e.manifest.configuration.digest,
                workspace_id=e.restoration.workspace_id,
                controller_id=e.restoration.controller_id,
                activation_id=e.runtime.plan.authority.activation_id,
                provider_generation=e.runtime.provider.scope.provider_generation,
                source_epoch=e.runtime.provider.source_epoch,
                provisioned_at=e.activation.activated_at,
                recovery_activation_digest=e.activation.digest,
            ),
        )

    e.target.write(install)
    return e


def verify(e, **changes):
    return require_deployment(
        e.target,
        e.manifest,
        crypto=e.source.crypto,
        **{"recovery_binding": e.binding, **changes},
    )


async def test_completed_history_verifies_without_writes(completed):
    e = completed
    before = snapshot(e.target)
    result = e.target.read(
        lambda sql: require_completed_activation(
            sql,
            e.manifest,
            e.binding,
            e.activation.digest,
            crypto=e.source.crypto,
        )
    )
    assert result.activation == e.activation and result.runtime == e.runtime
    assert verify(e)["recovery_activation_digest"] == e.activation.digest
    assert snapshot(e.target) == before


@pytest.mark.parametrize("change", ["missing", "custody", "source", "intent"])
async def test_completed_history_requires_independent_binding(completed, change):
    e = completed
    if change == "missing":
        binding = None
    else:
        field, update = {
            "custody": ("expected_custody", {"public_key": "b" * 64}),
            "source": ("expected_source", {"activation_id": new_id()}),
            "intent": ("intent", {"restore_id": new_id()}),
        }[change]
        binding = e.binding.model_copy(
            update={field: getattr(e.binding, field).model_copy(update=update)}
        )
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        verify(e, recovery_binding=binding)
    assert snapshot(e.target) == before


@pytest.mark.parametrize(
    "kind,field,value",
    [
        ("recovery_admission", "target_signature", "0" * 128),
        ("recovery_activation", "target_signature", "0" * 128),
        ("recovery_runtime_preparation", "plan_digest", "sha256:" + "0" * 64),
        ("recovery_runtime_plan", "configuration_digest", "sha256:" + "0" * 64),
        ("workspace_seal", "backup_id", "00000000-0000-4000-8000-000000000099"),
    ],
)
async def test_changed_history_cannot_reopen(completed, kind, field, value):
    e = completed
    e.target.write(
        lambda sql: sql.execute(
            "UPDATE records SET payload=json_set(payload,?,?) WHERE kind=?",
            ("$." + field, value, kind),
        )
    )
    with pytest.raises(ResearchError):
        verify(e)


@pytest.mark.parametrize("mode", ["generation", "source_epoch", "downgrade", "retired", "deleted"])
async def test_completed_seal_and_retirement_cannot_be_rewritten(completed, mode):
    e = completed

    def damage(sql):
        if mode in {"generation", "source_epoch"}:
            field = "provider_generation" if mode == "generation" else mode
            sql.execute(
                "UPDATE hosted_provisioning SET receipt=json_set(receipt,?,?)",
                ("$." + field, new_id()),
            )
        elif mode == "downgrade":
            sql.execute("DROP TABLE hosted_checkpoint_import")
            sql.execute(
                "UPDATE hosted_provisioning "
                "SET receipt=json_remove(receipt,'$.recovery_activation_digest')"
            )
        elif mode == "retired":
            sql.execute(
                "UPDATE credentials SET revoked=0 WHERE id=?",
                (e.activation.retired_credential_ids[0],),
            )
        else:
            sql.execute(
                "DELETE FROM credentials WHERE id=?", (e.activation.retired_credential_ids[0],)
            )

    e.target.write(damage)
    with pytest.raises(ResearchError):
        verify(e)


async def test_later_research_and_provider_state_do_not_rewrite_activation(completed):
    e = completed

    def advance(sql):
        sql.execute(
            "UPDATE records SET revision=revision+1,state='unqualified' "
            "WHERE kind='time_provider' AND id=?",
            (e.runtime.plan.authority.activation_id,),
        )
        Transaction(sql, e.context).insert(
            "research_note", new_id(), {"finding": "provisional"}, state="immutable"
        )

    e.target.write(advance)
    assert verify(e)["recovery_activation_digest"] == e.activation.digest


@pytest.mark.parametrize("mode", ["valid", "old_epoch", "missing_history", "wrong_revision"])
async def test_completed_history_checks_successor_seal(completed, mode):
    e = completed

    def reseal(sql):
        tx = Transaction(sql, e.context)
        gate = tx.get("workspace_gate", e.restoration.workspace_id)
        data = dict(gate["data"], backup_id=new_id(), control_epoch=2)
        if mode == "old_epoch":
            data["control_epoch"] = 1
        if mode != "missing_history":
            tx.insert("workspace_seal", data["backup_id"], data, state="immutable")
        sql.execute(
            "UPDATE records SET state='sealed',payload=?,revision=revision+? "
            "WHERE kind='workspace_gate'",
            (encode(data), 0 if mode == "wrong_revision" else 1),
        )

    e.target.write(reseal)
    if mode == "valid":
        assert verify(e)["recovery_activation_digest"] == e.activation.digest
    else:
        with pytest.raises(ResearchError):
            verify(e)
