"""Private elected-target activation with one atomic deployment and acknowledgment."""

import asyncio
from dataclasses import asdict, dataclass

from research_swarm.checkpoints.image_schema import require_portable_image_schema
from research_swarm.control.hosted_activation_proofs import (
    ACTIVATION_OPERATION,
    activation_key,
    activation_witness_payload,
)
from research_swarm.control.recovery_activation import activate_in, reject_activation
from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.control.time_witnesses import WitnessSourceChanged
from research_swarm.crypto import sign
from research_swarm.domain.backup import ActivateRecovery, RecoveryAdmission, RestoreReceipt
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import (
    CommandContext,
    RecoveryActivationGuard,
    RecoveryAuthority,
)
from research_swarm.domain.recovery_runtime import HostedActivationRuntime, HostedRecoveryActivation
from research_swarm.domain.types import ID, Revision, VersionedRecord, utc_now
from research_swarm.state.database import Database, Transaction, _request_digest, encode
from research_swarm.state.time_providers import (
    ProviderChanged,
    ProviderDiscontinuity,
    ProviderSnapshot,
    TimeProviders,
    fencing_payload,
)

if __package__:
    from .activation_cut import (
        HostedActivationCut,
        inspect_activation_cut,
        require_activation_bounds,
    )
    from .activation_history import require_completed_activation
    from .checkpoint_admission import AdmissionStore, HostedRecoveryVerifier
    from .checkpoint_import import verify_target_configuration
    from .checkpoint_runtime import guarded as runtime_guarded
    from .checkpoint_runtime import inspect_preparation, runtime_plan
    from .checkpoint_source import parse_checkpoint_request
    from .checkpoint_target import load_target
    from .exclusive import exclusive
    from .provisioning import (
        ProvisioningReceipt,
        complete_deployment_in,
        exists,
        require_deployment_in,
        write_deployment_intent,
    )
else:
    from activation_cut import (
        HostedActivationCut,
        inspect_activation_cut,
        require_activation_bounds,
    )
    from activation_history import require_completed_activation
    from checkpoint_admission import AdmissionStore, HostedRecoveryVerifier
    from checkpoint_import import verify_target_configuration
    from checkpoint_runtime import guarded as runtime_guarded
    from checkpoint_runtime import inspect_preparation, runtime_plan
    from checkpoint_source import parse_checkpoint_request
    from checkpoint_target import load_target
    from exclusive import exclusive
    from provisioning import (
        ProvisioningReceipt,
        complete_deployment_in,
        exists,
        require_deployment_in,
        write_deployment_intent,
    )


class CheckpointActivationCommand(VersionedRecord):
    request_id: ID
    expected_revision: Revision
    payload: ActivateRecovery


@dataclass(frozen=True)
class ActivationSelection:
    authority: RecoveryAuthority
    restoration: RestoreReceipt
    admission: RecoveryAdmission
    provider: ProviderSnapshot
    runtime: HostedActivationRuntime
    cut: HostedActivationCut


def completed_reply(sql, store, manifest, binding, context, request, crypto):
    require_portable_image_schema(sql)
    receipt = require_deployment_in(sql, manifest, crypto=crypto, recovery_binding=binding)
    completed = require_completed_activation(
        sql, manifest, binding, receipt["recovery_activation_digest"], crypto=crypto
    )
    activation = completed.activation
    if (activation.request_id, activation.gate_revision, activation.request) != (
        request.request_id,
        request.expected_revision,
        request.payload,
    ):
        raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Completed activation intent differs")
    # Database.replay reads this response eagerly; bound the exact row first.
    bounded = sql.execute(
        "SELECT 1 FROM commands WHERE workspace_id=? AND actor_id=? AND idempotency_key=? "
        "AND typeof(response)='text' AND length(CAST(response AS BLOB))<=65536 "
        "AND typeof(request_digest)='text' AND length(CAST(request_digest AS BLOB))=71 "
        "AND command_id=?",
        (
            context.principal.workspace_id,
            context.principal.id,
            context.idempotency_key,
            activation.command_id,
        ),
    ).fetchone()
    if bounded is None:
        raise ResearchError(ErrorCode.STALE_FENCE, "Activation acknowledgment is unavailable")
    bounded_record = sql.execute(
        "SELECT 1 FROM records WHERE workspace_id=? AND kind='recovery_activation' AND id=? "
        "AND state='immutable' AND revision=1 AND parent_id IS NULL AND campaign_id IS NULL "
        "AND typeof(payload)='text' AND length(CAST(payload AS BLOB))<=65536 "
        "AND typeof(created_at)='text' AND length(CAST(created_at AS BLOB))<=64 "
        "AND typeof(updated_at)='text' AND length(CAST(updated_at AS BLOB))<=64",
        (activation.workspace_id, activation.request.restore_id),
    ).fetchone()
    if bounded_record is None:
        raise ResearchError(ErrorCode.STALE_FENCE, "Activation record is unavailable")
    database = Database.from_store(AdmissionStore(store.identity, sql))
    reply = database.replay(
        context, ACTIVATION_OPERATION, request.payload.model_dump(mode="json"), "*"
    )
    if (
        reply is None
        or reply.get("disposition") != "accepted"
        or reply.get("command_id") != activation.command_id
        or reply.get("result")
        != Transaction(sql, context).get("recovery_activation", activation.request.restore_id)
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Activation acknowledgment differs")
    return encode(reply)


async def activate_target(
    state,
    open_store,
    manifest,
    secrets,
    *,
    identity,
    credential,
    target_json,
    request_json,
    crypto,
    objects,
    acquire,
):
    custody = load_target(target_json, manifest, secrets, credential=credential)
    binding = custody.binding
    request = parse_checkpoint_request(request_json, CheckpointActivationCommand)
    payload = request.payload.model_dump(mode="json")

    def configuration(actual):
        operator = verify_target_configuration(
            manifest,
            secrets,
            binding.intent,
            identity=actual,
            credential=credential,
            target_seed=custody.seed,
            expected_source=binding.expected_source,
            expected_custody=binding.expected_custody,
            crypto=crypto,
        )
        storage = manifest.configuration.storage
        if objects.config != storage or objects.transport.configuration_digest != storage.digest:
            raise ResearchError(ErrorCode.STALE_FENCE, "Activation artifact binding differs")
        return operator

    operator = configuration(identity)
    plan = runtime_plan(manifest, secrets, custody, crypto)
    if (request.payload.restore_id, request.payload.expected_control_epoch) != (
        plan.restore_id,
        plan.authority.control_epoch,
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Activation target intent differs")

    async def activate():
        store = open_store()
        context = CommandContext(
            principal=operator,
            idempotency_key=activation_key(request.request_id),
            expected_revision=request.expected_revision,
        )
        verifier = HostedRecoveryVerifier(manifest, binding)
        registry = TimeProviders(store.identity, crypto_provider=crypto)

        def select(sql):
            configuration(store.identity)
            if runtime_plan(manifest, secrets, custody, crypto) != plan:
                raise ResearchError(ErrorCode.STALE_FENCE, "Protected activation inputs changed")
            if exists(sql, "hosted_provisioning") or exists(sql, "hosted_authority_config"):
                return completed_reply(sql, store, manifest, binding, context, request, crypto)
            require_activation_bounds(sql)
            database = Database.from_store(
                AdmissionStore(store.identity, sql), recovery_verifier=verifier
            )
            authority = recovery_authority(database, context, crypto_provider=crypto)
            if authority is None or authority.gate_revision != request.expected_revision:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Activation gate revision differs")
            restoration, admission = verifier.read_admission(
                sql, authority=store.identity, context=context, crypto=crypto
            )
            current, prepared = inspect_preparation(sql, context, plan, registry, authority)
            if prepared is None:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Target runtime is unprepared"
                )
            runtime = HostedActivationRuntime(
                plan=plan,
                preparation_digest=prepared.digest,
                provider_revision=prepared.provider_revision,
                provider=prepared.provider,
            )
            cut = inspect_activation_cut(
                sql, context, restoration, runtime, manifest.configuration.storage
            )
            if (
                sum(blob.reference.size_bytes for blob in cut.view.artifacts)
                > manifest.configuration.storage_limit_bytes
            ):
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Activation storage exceeds limit"
                )
            return ActivationSelection(authority, restoration, admission, current, runtime, cut)

        selected = store.read(select)
        if isinstance(selected, str):
            return selected
        cut, runtime = selected.cut, selected.runtime
        task = asyncio.current_task()
        for blob in cut.view.artifacts:
            await objects.read(operator.workspace_id, blob)
            if task is None or task.cancelling():
                raise asyncio.CancelledError("Activation owner was cancelled")
        after_bytes = store.read(select)
        if isinstance(after_bytes, str):
            return after_bytes
        if after_bytes != selected:
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Activation state changed during artifact I/O"
            )
        witness_payload = activation_witness_payload(
            request.payload,
            runtime_digest=runtime.digest,
            view_digest=cut.view.digest,
            artifact_digest=cut.view.artifact_digest,
            retired_credential_ids=cut.retired_credential_ids,
        )
        operation, pending = registry.prepare(
            context, selected.provider, ACTIVATION_OPERATION, witness_payload
        )

        def retire(evidence):
            # Authenticated clock loss is durable even though activation is rejected.
            def fence(sql):
                require_activation_bounds(sql)
                configuration(store.identity)
                verifier.read_admission(
                    sql, authority=store.identity, context=context, crypto=crypto
                )
                if runtime_plan(manifest, secrets, custody, crypto) != plan:
                    raise ResearchError(
                        ErrorCode.STALE_FENCE, "Protected activation inputs changed"
                    )
                current, prepared = inspect_preparation(
                    sql, context, plan, registry, selected.authority
                )
                if (
                    current != selected.provider
                    or prepared is None
                    or prepared.digest != runtime.preparation_digest
                ):
                    raise ResearchError(
                        ErrorCode.STALE_FENCE, "Runtime changed before clock retirement"
                    )
                owned = runtime_guarded(
                    context,
                    selected.authority,
                    plan,
                    "time_provider.fence",
                    fencing_payload(evidence, ACTIVATION_OPERATION, witness_payload),
                )
                registry.fence(
                    Transaction(sql, owned),
                    evidence,
                    name=ACTIVATION_OPERATION,
                    payload=witness_payload,
                )
                require_activation_bounds(sql)

            store.write(fence)

        timeout = asyncio.timeout(10)
        try:
            async with timeout:
                response = await acquire(pending.request)
            if timeout.expired():
                raise TimeoutError("Activation witness arrived after its deadline")
        except TimeoutError:
            raise ResearchError(
                ErrorCode.TRANSPORT_UNAVAILABLE, "Activation witness unavailable", retryable=True
            ) from None
        if task is None or task.cancelling():
            raise asyncio.CancelledError("Activation owner was cancelled")
        try:
            proof = pending.complete(response)
        except WitnessSourceChanged as error:
            retire(ProviderDiscontinuity(operation, error.evidence.witness))
            raise
        activation = HostedRecoveryActivation(
            request_id=request.request_id,
            command_id=context.command_id,
            request=request.payload,
            workspace_id=operator.workspace_id,
            controller_id=selected.restoration.controller_id,
            restoration_digest=selected.restoration.digest,
            admission_digest=selected.authority.admission_digest,
            logical_election_digest=selected.admission.logical_election_digest,
            deployment_digest=manifest.digest,
            runtime_digest=runtime.digest,
            view_digest=cut.view.digest,
            artifact_digest=cut.view.artifact_digest,
            retired_credential_ids=cut.retired_credential_ids,
            final_witness=proof.witness,
            evidence_through=cut.view.evidence_through,
            gate_revision=request.expected_revision,
            operator_id=operator.id,
            activated_at=utc_now(),
            target_signature="0" * 128,
        )
        activation = activation.model_copy(
            update={
                "target_signature": sign(
                    crypto, custody.seed, activation.proof_digest.encode("ascii")
                ).hex()
            }
        )
        guarded = context.model_copy()
        guarded._recovery_activation = RecoveryActivationGuard(
            **asdict(selected.authority),
            view_digest=cut.view.digest,
            artifact_digest=cut.view.artifact_digest,
            request_digest=_request_digest(context, ACTIVATION_OPERATION, payload),
        )

        def commit(sql):
            current = select(sql)  # Bounds precede Database.execute and its activation guard.
            if isinstance(current, str):
                return current
            if current != selected:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Activation state changed during witness I/O"
                )
            database = Database.from_store(
                AdmissionStore(store.identity, sql), recovery_verifier=verifier
            )

            def apply(tx):
                registry.check(
                    tx, operation, proof, name=ACTIVATION_OPERATION, payload=witness_payload
                )
                for identifier in cut.retired_credential_ids:
                    sql.execute(
                        "UPDATE credentials SET revoked=1 WHERE workspace_id=? AND id=?",
                        (operator.workspace_id, identifier),
                    )
                write_deployment_intent(sql, manifest)
                complete_deployment_in(
                    sql,
                    manifest,
                    ProvisioningReceipt(
                        manifest_digest=manifest.digest,
                        configuration_digest=manifest.configuration.digest,
                        workspace_id=operator.workspace_id,
                        controller_id=activation.controller_id,
                        activation_id=plan.authority.activation_id,
                        provider_generation=runtime.provider.scope.provider_generation,
                        source_epoch=runtime.provider.source_epoch,
                        provisioned_at=activation.activated_at,
                        recovery_activation_digest=activation.digest,
                    ),
                )
                return activate_in(tx, activation)

            reply = database.execute(
                guarded, ACTIVATION_OPERATION, payload, "*", apply, on_rejection=reject_activation
            )
            # Newly generated evidence and the acknowledgment must fit the same profile.
            require_activation_bounds(sql)
            require_deployment_in(sql, manifest, crypto=crypto, recovery_binding=binding)
            return encode(reply)

        try:
            return store.write(commit)
        except ProviderChanged as error:
            retire(error.evidence)
            raise

    return await exclusive(state, activate)
