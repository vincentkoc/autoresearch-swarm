"""Explicit private qualification of a retired target clock, without credential repair."""

import asyncio
from dataclasses import dataclass

from research_swarm.control.recovery_activation import reject_activation
from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, RecoveryAuthority
from research_swarm.domain.recovery_runtime import (
    RecoveryRuntimePlan,
    RequalifyRecoveryRuntime,
    RuntimePreparationReceipt,
    RuntimeRequalificationReceipt,
)
from research_swarm.domain.types import utc_now
from research_swarm.state.database import Database, Transaction, encode
from research_swarm.state.recovery_runtime import PLAN_KIND
from research_swarm.state.time_providers import (
    ProviderSnapshot,
    TimeProviders,
    qualification_payload,
)

if __package__:
    from .activation_cut import require_activation_bounds
    from .checkpoint_admission import AdmissionStore, HostedRecoveryVerifier
    from .checkpoint_history import history_record
    from .checkpoint_import import verify_target_configuration
    from .checkpoint_runtime import guarded, require_bounded_provider, runtime_plan, saved_record
    from .checkpoint_source import parse_checkpoint_request
    from .checkpoint_target import load_target
    from .exclusive import exclusive
    from .provisioning import exists, require_roster
    from .runtime_history import (
        REQUALIFICATION_KIND,
        fence_digest,
        read_qualification,
        read_retirement,
    )
else:
    from activation_cut import require_activation_bounds
    from checkpoint_admission import AdmissionStore, HostedRecoveryVerifier
    from checkpoint_history import history_record
    from checkpoint_import import verify_target_configuration
    from checkpoint_runtime import guarded, require_bounded_provider, runtime_plan, saved_record
    from checkpoint_source import parse_checkpoint_request
    from checkpoint_target import load_target
    from exclusive import exclusive
    from provisioning import exists, require_roster
    from runtime_history import (
        REQUALIFICATION_KIND,
        fence_digest,
        read_qualification,
        read_retirement,
    )

OPERATION = "workspace.requalify_runtime"


@dataclass(frozen=True)
class RequalificationSelection:
    authority: RecoveryAuthority
    provider: ProviderSnapshot
    previous: RuntimePreparationReceipt | RuntimeRequalificationReceipt
    fence_digest: str


async def requalify_target_runtime(
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
    acquire,
):
    custody = load_target(target_json, manifest, secrets, credential=credential)
    binding = custody.binding
    request = parse_checkpoint_request(request_json, RequalifyRecoveryRuntime)
    payload = request.model_dump(mode="json")

    def configuration(actual):
        return verify_target_configuration(
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

    operator = configuration(identity)
    plan = runtime_plan(manifest, secrets, custody, crypto)

    async def requalify():
        store = open_store()
        context = CommandContext(
            principal=operator,
            idempotency_key="runtime-requalify:" + request.request_id,
            expected_revision=request.expected_provider_revision,
        )
        verifier = HostedRecoveryVerifier(manifest, binding)
        registry = TimeProviders(store.identity, crypto_provider=crypto)

        def database(sql):
            return Database.from_store(
                AdmissionStore(store.identity, sql), recovery_verifier=verifier
            )

        def verify(sql):
            require_activation_bounds(sql)
            configuration(store.identity)
            if runtime_plan(manifest, secrets, custody, crypto) != plan:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Protected requalification inputs changed"
                )
            if exists(sql, "hosted_provisioning") or exists(sql, "hosted_authority_config"):
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Runtime requalification requires recovery"
                )
            history_record(
                sql, operator.workspace_id, "recovery_admission", plan.restore_id, limit=65536
            )
            authority = recovery_authority(database(sql), context, crypto_provider=crypto)
            if authority is None or authority.restore_id != plan.restore_id:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Runtime requalification admission differs"
                )
            saved = saved_record(sql, operator.workspace_id, PLAN_KIND, plan.restore_id)
            if saved is None or RecoveryRuntimePlan.model_validate_json(saved["payload"]) != plan:
                raise ResearchError(ErrorCode.STALE_FENCE, "Runtime requalification plan differs")
            require_roster(sql, plan.credentials)
            require_bounded_provider(sql, operator.workspace_id, plan.authority.activation_id)
            return authority

        def qualification(sql, authority, generation, *, reserve_successor=False):
            return read_qualification(
                sql,
                plan,
                generation=generation,
                restoration_digest=authority.restoration_digest,
                admission_digest=authority.admission_digest,
                crypto=crypto,
                reserve_successor=reserve_successor,
            )

        def select(sql):
            authority = verify(sql)
            replayed = database(sql).replay(context, OPERATION, payload, "auth.manage")
            if replayed is not None:
                receipt = RuntimeRequalificationReceipt.model_validate(replayed["result"]["data"])
                generation = receipt.provider.scope.provider_generation
                if (
                    replayed.get("disposition") != "accepted"
                    or receipt.request != request
                    or qualification(sql, authority, generation) != receipt
                    or replayed["result"]
                    != Transaction(sql, context).get(REQUALIFICATION_KIND, generation)
                ):
                    raise ResearchError(
                        ErrorCode.STALE_FENCE, "Runtime requalification acknowledgment differs"
                    )
                return encode(replayed)
            current = registry.load(Transaction(sql, context), plan.authority)
            if (
                current.state != "unqualified"
                or current.revision != request.expected_provider_revision
            ):
                raise ResearchError(
                    ErrorCode.REVISION_CONFLICT,
                    "Runtime provider is not the selected fenced revision",
                )
            previous = qualification(
                sql, authority, current.data.scope.provider_generation, reserve_successor=True
            )
            if (
                previous.digest != request.previous_qualification_digest
                or previous.provider != current.data
                or previous.provider_revision + 1 != current.revision
            ):
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Runtime requalification predecessor differs"
                )
            fence = read_retirement(sql, plan, previous, crypto=crypto)
            return RequalificationSelection(authority, current, previous, fence_digest(fence))

        selected = store.read(select)
        if isinstance(selected, str):
            return selected
        operation, pending = registry.prepare(context, selected.provider, OPERATION, payload)
        root = guarded(context, selected.authority, plan, OPERATION, payload)
        leaf = guarded(
            context,
            selected.authority,
            plan,
            "time_provider.requalify",
            qualification_payload(operation, OPERATION, payload),
        )
        task, timeout = asyncio.current_task(), asyncio.timeout(10)
        try:
            async with timeout:
                response = await acquire(pending.request)
            if timeout.expired():
                raise TimeoutError("Runtime requalification witness arrived after deadline")
        except TimeoutError:
            raise ResearchError(
                ErrorCode.TRANSPORT_UNAVAILABLE,
                "Runtime requalification witness unavailable",
                retryable=True,
            ) from None
        if task is None or task.cancelling():
            raise asyncio.CancelledError("Runtime requalification owner was cancelled")
        proof = pending.complete(response)

        def commit(sql):
            current = select(sql)
            if isinstance(current, str):
                return current
            if current != selected:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Runtime requalification state changed during I/O"
                )

            def apply(tx):
                qualified = registry.requalify(
                    Transaction(sql, leaf), operation, proof, name=OPERATION, payload=payload
                )
                receipt = RuntimeRequalificationReceipt(
                    request=request,
                    plan_digest=plan.digest,
                    restoration_digest=selected.authority.restoration_digest,
                    admission_digest=selected.authority.admission_digest,
                    previous_generation=selected.provider.data.scope.provider_generation,
                    fence_digest=selected.fence_digest,
                    provider_revision=qualified.revision,
                    provider=qualified.data,
                    qualified_at=utc_now(),
                )
                record = tx.insert(
                    REQUALIFICATION_KIND,
                    qualified.data.scope.provider_generation,
                    receipt.model_dump(mode="json"),
                    state="immutable",
                )
                tx.event("workspace.runtime_requalified", record, {"plan_digest": plan.digest})
                return record

            result = database(sql).execute(
                root, OPERATION, payload, "auth.manage", apply, on_rejection=reject_activation
            )
            require_activation_bounds(sql)
            qualification(
                sql,
                selected.authority,
                result["result"]["data"]["provider"]["scope"]["provider_generation"],
            )
            return encode(result)

        return store.write(commit)

    return await exclusive(state, requalify)
