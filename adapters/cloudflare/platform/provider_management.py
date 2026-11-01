"""Private clock recovery for an existing active deployment; no provisioning or repair."""

import asyncio

from research_swarm.control.recovery_activation import reject_activation
from research_swarm.control.time_witnesses import witness_signing_bytes
from research_swarm.crypto import public_key, verify
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.time_providers import (
    RequalifyTimeProvider,
    TimeProviderGeneration,
    TimeProviderRequalification,
)
from research_swarm.domain.types import utc_now
from research_swarm.state.database import Database, Transaction, encode
from research_swarm.state.time_providers import ProviderSnapshot, TimeProviders
from research_swarm.state.workspace_gate import require_workspace_mutation

if __package__:
    from ..recovery.checkpoint_admission import AdmissionStore
    from ..recovery.checkpoint_capture import require_source_operator_in
    from ..recovery.checkpoint_runtime import require_bounded_provider
    from ..recovery.checkpoint_source import parse_checkpoint_request
    from ..recovery.runtime_history import read_provider_retirement
    from ..research.research_router import dispatch_key_digest
    from .deployment import secret_digest
    from .exclusive import exclusive
    from .provisioning import require_deployment_in, require_roster
else:
    from checkpoint_admission import AdmissionStore
    from checkpoint_capture import require_source_operator_in
    from checkpoint_runtime import require_bounded_provider
    from checkpoint_source import parse_checkpoint_request
    from deployment import secret_digest
    from exclusive import exclusive
    from provisioning import require_deployment_in, require_roster
    from research_router import dispatch_key_digest
    from runtime_history import read_provider_retirement

OPERATION = "workspace.requalify_time_provider"
RECEIPT_KIND = "time_provider_requalification"


def bounded_record(sql, context, kind, identifier, *, parent_id=None):
    row = sql.execute(
        "SELECT 1 FROM records WHERE workspace_id=? AND kind=? AND id=? "
        "AND state='immutable' AND revision=1 AND parent_id IS ? AND campaign_id IS NULL "
        "AND typeof(payload)='text' AND length(CAST(payload AS BLOB))<=65536 "
        "AND typeof(created_at)='text' AND length(CAST(created_at AS BLOB))<=64 "
        "AND typeof(updated_at)='text' AND length(CAST(updated_at AS BLOB))<=64",
        (context.principal.workspace_id, kind, identifier, parent_id),
    ).fetchone()
    if row is None:
        raise ResearchError(ErrorCode.STALE_FENCE, "Provider management evidence is unavailable")
    return Transaction(sql, context).get(kind, identifier)


async def manage_provider(
    state,
    open_store,
    manifest,
    secrets,
    *,
    identity,
    credential,
    crypto,
    recovery_binding=None,
    request_json=None,
    acquire=None,
):
    """None selects status internally; the mutation RPC separately requires a string."""
    config = manifest.configuration

    def configuration(actual):
        secrets.require_bootstrap(credential)
        manifest.require_store(actual)
        if (
            secret_digest("bootstrap", credential) != manifest.bootstrap_credential_digest
            or secret_digest("witness", secrets.witness_consumer)
            != manifest.witness_credential_digest
            or public_key(crypto, secrets.assignment_seed).hex() != config.assignment_public_key
            or dispatch_key_digest(secrets.dispatch_key) != config.dispatch_key_digest
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Provider management configuration differs")

    configuration(identity)
    request = (
        parse_checkpoint_request(request_json, RequalifyTimeProvider)
        if request_json is not None
        else None
    )
    member = next(
        item for item in manifest.credentials if item.principal.id == manifest.provisioning_actor_id
    )
    payload = {
        "manifest_digest": manifest.digest,
        "request": None if request is None else request.model_dump(mode="json"),
    }
    context = CommandContext(
        principal=member.principal,
        idempotency_key="provider-status"
        if request is None
        else "provider-requalify:" + request.request_id,
        expected_revision=None if request is None else request.expected_provider_revision,
    )

    async def operate():
        store = open_store()
        registry = TimeProviders(store.identity, crypto_provider=crypto)

        def database(sql):
            return Database.from_store(AdmissionStore(store.identity, sql))

        def admitted(sql):
            configuration(store.identity)
            require_deployment_in(sql, manifest, crypto=crypto, recovery_binding=recovery_binding)
            require_roster(sql, manifest.credentials, live_ids={member.id})
            require_source_operator_in(sql, manifest, secrets.credentials[member.id])
            gate = sql.execute(
                "SELECT 1 FROM records WHERE workspace_id=? AND kind='workspace_gate' AND id=? "
                "AND state!='active'",
                (member.principal.workspace_id, member.principal.workspace_id),
            ).fetchone()
            if gate is not None:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Provider management requires active authority"
                )
            # Bound the gate before the shared guard's eager payload read.
            invalid = sql.execute(
                "SELECT 1 FROM records WHERE workspace_id=? AND kind='workspace_gate' AND id=? "
                "AND (typeof(payload)!='text' OR length(CAST(payload AS BLOB))>65536)",
                (member.principal.workspace_id, member.principal.workspace_id),
            ).fetchone()
            if invalid is not None:
                raise ResearchError(ErrorCode.STALE_FENCE, "Provider management gate is invalid")
            require_bounded_provider(
                sql, member.principal.workspace_id, config.authority.activation_id
            )
            current = registry.load(Transaction(sql, context), config.authority)
            if current.data.source != config.source:
                raise ResearchError(ErrorCode.STALE_FENCE, "Provider management source differs")
            return current

        def retained_reply(sql, reply):
            receipt = TimeProviderRequalification.model_validate(reply["result"]["data"])
            generation = receipt.provider.scope.provider_generation
            if (
                reply.get("disposition") != "accepted"
                or receipt.request != request
                or receipt.configuration_digest != config.digest
                or receipt.provider.scope.authority != config.authority
                or receipt.provider.source != config.source
                or reply["result"] != bounded_record(sql, context, RECEIPT_KIND, generation)
            ):
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Provider management acknowledgment differs"
                )
            retained = TimeProviderGeneration.model_validate(
                bounded_record(
                    sql,
                    context,
                    "time_provider_generation",
                    generation,
                    parent_id=config.authority.activation_id,
                )["data"]
            )
            witness = retained.qualification
            previous = ProviderSnapshot(
                request.expected_provider_revision, "unqualified", receipt.previous
            )
            original_context = context.model_copy(update={"command_id": reply["command_id"]})
            if (
                retained.provider != receipt.provider
                or retained.admitted_by != member.principal.id
                or witness is None
                or witness.key_id != config.source.key_id
                or witness.payload.request.scope != receipt.previous.scope
                or witness.payload.request.operation_id != reply["command_id"]
                or witness.payload.request.request_digest
                != registry._digest(original_context, previous, OPERATION, payload)
                or witness.payload.source_epoch != receipt.provider.source_epoch
                or witness.payload.source_id != config.source.id
                or witness.payload.native_profile != config.source.native_profile
                or witness.payload.policy != config.source.policy
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Provider qualification witness differs")
            try:
                verify(
                    crypto,
                    bytes.fromhex(config.source.public_key),
                    witness_signing_bytes(witness.key_id, witness.payload),
                    bytes.fromhex(witness.signature),
                )
            except ValueError as error:
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Provider qualification signature differs"
                ) from error
            read_provider_retirement(
                sql,
                config.authority,
                receipt.previous,
                request.expected_provider_revision - 1,
                crypto=crypto,
            )
            return encode(reply)

        def select(sql):
            current = admitted(sql)
            if request is None:
                return encode(
                    {
                        "schema_version": "1.0",
                        "manifest_digest": manifest.digest,
                        "provider": {
                            "revision": current.revision,
                            "state": current.state,
                            "data": current.data.model_dump(mode="json"),
                        },
                    }
                )
            invalid = sql.execute(
                "SELECT 1 FROM commands WHERE workspace_id=? AND actor_id=? AND idempotency_key=? "
                "AND (typeof(response)!='text' OR length(CAST(response AS BLOB))>65536 "
                "OR typeof(request_digest)!='text' OR length(CAST(request_digest AS BLOB))!=71 "
                "OR typeof(command_id)!='text' OR length(CAST(command_id AS BLOB))!=36)",
                (member.principal.workspace_id, member.principal.id, context.idempotency_key),
            ).fetchone()
            if invalid is not None:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Provider acknowledgment exceeds its contract"
                )
            previous = database(sql).replay(context, OPERATION, payload, "auth.manage")
            if previous is not None:
                return retained_reply(sql, previous)
            if (
                current.state != "unqualified"
                or current.revision != request.expected_provider_revision
                or current.data.scope.provider_generation != request.expected_provider_generation
            ):
                raise ResearchError(
                    ErrorCode.REVISION_CONFLICT, "Provider is not the selected fenced revision"
                )
            read_provider_retirement(
                sql, config.authority, current.data, current.revision - 1, crypto=crypto
            )
            return current

        selected = store.read(select)
        if isinstance(selected, str):
            return selected
        operation, pending = registry.prepare(context, selected, OPERATION, payload)
        owner, timeout = asyncio.current_task(), asyncio.timeout(10)
        try:
            async with timeout:
                response = await acquire(pending.request)
            if timeout.expired():
                raise TimeoutError("Provider qualification witness arrived after deadline")
        except TimeoutError:
            raise ResearchError(
                ErrorCode.TRANSPORT_UNAVAILABLE, "Provider witness unavailable", retryable=True
            ) from None
        if owner is None or owner.cancelling():
            raise asyncio.CancelledError("Provider management owner was cancelled")
        proof = pending.complete(response)

        def commit(sql):
            current = select(sql)
            if isinstance(current, str):
                return current
            if current != selected:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Provider management state changed during I/O"
                )
            require_workspace_mutation(sql, member.principal.workspace_id, OPERATION)

            def apply(tx):
                qualified = registry.requalify(
                    tx, operation, proof, name=OPERATION, payload=payload
                )
                receipt = TimeProviderRequalification(
                    request=request,
                    configuration_digest=config.digest,
                    previous=selected.data,
                    provider=qualified.data,
                    provider_revision=qualified.revision,
                    qualified_at=utc_now(),
                )
                record = tx.insert(
                    RECEIPT_KIND,
                    qualified.data.scope.provider_generation,
                    receipt.model_dump(mode="json"),
                    state="immutable",
                )
                tx.event(
                    "workspace.time_provider_requalified",
                    record,
                    {"configuration_digest": config.digest},
                )
                return record

            result = database(sql).execute(
                context, OPERATION, payload, "auth.manage", apply, on_rejection=reject_activation
            )
            return retained_reply(sql, result)

        return store.write(commit)

    return await exclusive(state, operate)
