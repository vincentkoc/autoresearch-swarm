"""Bounded immutable readiness lineage; selection never follows mutable provider state."""

from research_swarm.control.time_witnesses import witness_signing_bytes
from research_swarm.crypto import verify
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.recovery_runtime import (
    RuntimePreparationReceipt,
    RuntimeRequalificationReceipt,
)
from research_swarm.domain.time_providers import (
    TimeProviderFence,
    TimeSourceRetirement,
    source_origin_id,
)

if __package__:
    from .checkpoint_history import history_record
else:
    from checkpoint_history import history_record

PREPARATION_KIND = "recovery_runtime_preparation"
REQUALIFICATION_KIND = "recovery_runtime_requalification"
MAX_REQUALIFICATIONS = 16


def fence_digest(fence):
    return record_digest("research.runtime-requalification-fence.v1", fence.model_dump(mode="json"))


def require_qualification_identity(receipt, plan, restoration_digest, admission_digest):
    if (
        receipt.plan_digest != plan.digest
        or receipt.restoration_digest != restoration_digest
        or receipt.admission_digest != admission_digest
        or receipt.provider.scope.authority != plan.authority
        or receipt.provider.source != plan.source
        or receipt.provider.source_epoch is None
        or receipt.provider_revision < 1
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Runtime qualification identity differs")


def read_retirement(sql, plan, previous, *, crypto):
    return read_provider_retirement(
        sql, plan.authority, previous.provider, previous.provider_revision, crypto=crypto
    )


def read_provider_retirement(sql, authority, provider, provider_revision, *, crypto):
    """Verify one retired provider without selecting any later lifecycle state."""
    workspace = authority.workspace_id
    fence = TimeProviderFence.model_validate_json(
        history_record(
            sql,
            workspace,
            "time_provider_fence",
            provider.scope.provider_generation,
            limit=65536,
        )["payload"]
    )
    retired = TimeSourceRetirement.model_validate_json(
        history_record(
            sql,
            workspace,
            "time_source_retirement",
            fence.origin_id,
            limit=65536,
        )["payload"]
    )
    if (
        provider.scope.authority != authority
        or fence.provider != provider
        or fence.provider_revision != provider_revision
        or retired.origin_id != fence.origin_id
        or fence.origin_id != source_origin_id(provider.source, provider.source_epoch)
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Runtime retirement history differs")
    # The first retirement of a shared origin survives every later provider fence.
    # Its signed observation belongs to its own provider, not necessarily this one.
    witness, source = retired.observed, retired.provider.source
    observed = witness.payload
    if (
        witness.key_id != source.key_id
        or observed.request.scope != retired.provider.scope
        or (
            observed.source_epoch == retired.provider.source_epoch
            and observed.source_id == source.id
            and observed.native_profile == source.native_profile
            and observed.policy == source.policy
        )
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Runtime retirement observation differs")
    try:
        verify(
            crypto,
            bytes.fromhex(source.public_key),
            witness_signing_bytes(witness.key_id, observed),
            bytes.fromhex(witness.signature),
        )
    except ValueError as error:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Runtime retirement signature differs"
        ) from error
    return fence


def read_qualification(
    sql, plan, *, generation, restoration_digest, admission_digest, crypto, reserve_successor=False
):
    workspace = plan.authority.workspace_id
    initial = RuntimePreparationReceipt.model_validate_json(
        history_record(
            sql,
            workspace,
            PREPARATION_KIND,
            plan.restore_id,
            limit=65536,
        )["payload"]
    )
    require_qualification_identity(initial, plan, restoration_digest, admission_digest)
    chain, visited = [], set()
    while generation != initial.provider.scope.provider_generation:
        if generation in visited or len(chain) >= MAX_REQUALIFICATIONS - int(reserve_successor):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Runtime qualification lineage exceeds limit"
            )
        visited.add(generation)
        receipt = RuntimeRequalificationReceipt.model_validate_json(
            history_record(
                sql,
                workspace,
                REQUALIFICATION_KIND,
                generation,
                limit=65536,
            )["payload"]
        )
        require_qualification_identity(receipt, plan, restoration_digest, admission_digest)
        if receipt.provider.scope.provider_generation != generation:
            raise ResearchError(ErrorCode.STALE_FENCE, "Runtime qualification generation differs")
        chain.append(receipt)
        generation = receipt.previous_generation
    previous = initial
    origins = {initial.provider.source_epoch}
    for successor in reversed(chain):
        if (
            successor.request.previous_qualification_digest != previous.digest
            or successor.request.expected_provider_revision != previous.provider_revision + 1
            or successor.previous_generation != previous.provider.scope.provider_generation
            or successor.provider.source_epoch in origins
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Runtime qualification predecessor differs")
        fence = read_retirement(sql, plan, previous, crypto=crypto)
        if successor.fence_digest != fence_digest(fence):
            raise ResearchError(ErrorCode.STALE_FENCE, "Runtime qualification fence differs")
        previous = successor
        origins.add(successor.provider.source_epoch)
    return previous
