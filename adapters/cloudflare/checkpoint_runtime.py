"""Prepare a fixed elected target; no public configuration or execution activation."""

import asyncio
from dataclasses import asdict

from research_swarm.control.auth import CredentialStore
from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.crypto import public_key
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, RecoveryRuntimeGuard
from research_swarm.domain.recovery_runtime import RecoveryRuntimePlan
from research_swarm.domain.recovery_runtime import (
    RuntimePreparationReceipt as RuntimePreparationReceipt,
)
from research_swarm.domain.types import utc_now
from research_swarm.state.database import Database, Transaction, _request_digest, encode
from research_swarm.state.recovery_guard import require_recovery_authority
from research_swarm.state.recovery_runtime import PLAN_KIND
from research_swarm.state.time_providers import (
    TimeProviders,
    qualification_payload,
    registration_payload,
)

if __package__:
    from .checkpoint_admission import HostedRecoveryVerifier
    from .checkpoint_import import verify_target_configuration
    from .checkpoint_target import load_target
    from .deployment import secret_digest
    from .exclusive import exclusive
    from .provisioning import exists, require_roster
    from .research_router import dispatch_key_digest
else:
    from checkpoint_admission import HostedRecoveryVerifier
    from checkpoint_import import verify_target_configuration
    from checkpoint_target import load_target
    from deployment import secret_digest
    from exclusive import exclusive
    from provisioning import exists, require_roster
    from research_router import dispatch_key_digest

OPERATION = "workspace.prepare_runtime"
RECEIPT_KIND = "recovery_runtime_preparation"


def runtime_plan(manifest, secrets, custody, crypto):
    config = manifest.configuration
    if (
        config.authority.assignment_key_id == custody.binding.expected_source.assignment_key_id
        or public_key(crypto, secrets.assignment_seed).hex() != config.assignment_public_key
        or dispatch_key_digest(secrets.dispatch_key) != config.dispatch_key_digest
        or secret_digest("witness", secrets.witness_consumer) != manifest.witness_credential_digest
        or set(secrets.credentials) != {item.id for item in manifest.credentials}
        or any(
            CredentialStore._digest(secrets.credentials[item.id]) != item.token_digest
            for item in manifest.credentials
        )
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Target runtime custody differs")
    return RecoveryRuntimePlan(
        restore_id=custody.binding.intent.restore_id,
        deployment_digest=manifest.digest,
        configuration_digest=config.digest,
        authority=config.authority,
        source=config.source,
        credentials=tuple(item.model_dump(mode="json") for item in manifest.credentials),
    )


def guarded(context, authority, plan, operation, payload):
    result = context.model_copy()
    result._recovery_runtime = RecoveryRuntimeGuard(
        **asdict(authority),
        plan_digest=plan.digest,
        operation=operation,
        request_digest=_request_digest(context, operation, payload),
    )
    return result


def saved_record(sql, workspace, kind, identifier):
    row = sql.execute(
        "SELECT CASE WHEN state='immutable' THEN state END AS state, "
        "CASE WHEN typeof(revision)='integer' AND revision=1 THEN revision END AS revision, "
        "CASE WHEN typeof(payload)='text' "
        "AND length(CAST(payload AS BLOB))<=1048576 THEN payload ELSE NULL END AS payload "
        "FROM records WHERE workspace_id=? AND kind=? AND id=?",
        (workspace, kind, identifier),
    ).fetchone()
    if row is not None and (
        row["state"] != "immutable"
        or row["revision"] != 1
        or type(row["payload"]) is not str
        or len(row["payload"].encode()) > 1024 * 1024
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Runtime preparation record changed")
    return row


def require_bounded_provider(sql, workspace, identifier):
    # Transaction.get materializes the full row. Bound every non-key value before
    # calling it, in the same transaction/snapshot as the subsequent provider read.
    row = sql.execute(
        "SELECT 1 FROM records WHERE workspace_id=? AND kind='time_provider' AND id=? "
        "AND typeof(payload)='text' AND length(CAST(payload AS BLOB))<=65536 "
        "AND typeof(revision)='integer' AND revision BETWEEN 1 AND 9007199254740991 "
        "AND state IN ('qualified','unqualified') AND campaign_id IS NULL AND parent_id IS NULL "
        "AND typeof(created_at)='text' AND length(CAST(created_at AS BLOB))<=64 "
        "AND typeof(updated_at)='text' AND length(CAST(updated_at AS BLOB))<=64",
        (workspace, identifier),
    ).fetchone()
    if row is None:
        raise ResearchError(ErrorCode.STALE_FENCE, "Prepared runtime provider is unavailable")


def inspect_preparation(sql, context, plan, registry, authority):
    require_recovery_authority(sql, context, registry.store, authority)
    workspace = context.principal.workspace_id
    saved = saved_record(sql, workspace, PLAN_KIND, plan.restore_id)
    if saved is None or RecoveryRuntimePlan.model_validate_json(saved["payload"]) != plan:
        raise ResearchError(ErrorCode.STALE_FENCE, "Protected runtime plan changed")
    require_roster(sql, plan.credentials)
    require_bounded_provider(sql, workspace, plan.authority.activation_id)
    current = registry.load(Transaction(sql, context), plan.authority)
    if current.data.source != plan.source:
        raise ResearchError(ErrorCode.STALE_FENCE, "Prepared runtime source differs")
    row = saved_record(sql, workspace, RECEIPT_KIND, plan.restore_id)
    if row is None:
        if current.state != "unqualified":
            raise ResearchError(ErrorCode.STALE_FENCE, "Runtime qualification receipt is absent")
        return current, None
    receipt = RuntimePreparationReceipt.model_validate_json(row["payload"])
    if (
        current.state != "qualified"
        or receipt.plan_digest != plan.digest
        or receipt.restoration_digest != authority.restoration_digest
        or receipt.admission_digest != authority.admission_digest
        or receipt.provider_revision != current.revision
        or receipt.provider != current.data
        or receipt.provider.source_epoch is None
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Runtime qualification receipt differs")
    return current, receipt


async def prepare_target_runtime(
    state, open_store, manifest, secrets, *, identity, credential, target_json, crypto, acquire
):
    custody = load_target(target_json, manifest, secrets, credential=credential)
    binding = custody.binding

    def verify_configuration(actual_identity):
        return verify_target_configuration(
            manifest,
            secrets,
            binding.intent,
            identity=actual_identity,
            credential=credential,
            target_seed=custody.seed,
            expected_source=binding.expected_source,
            expected_custody=binding.expected_custody,
            crypto=crypto,
        )

    operator = verify_configuration(identity)
    plan = runtime_plan(manifest, secrets, custody, crypto)

    async def prepare():
        store = open_store()
        verify_configuration(store.identity)
        verifier = HostedRecoveryVerifier(manifest, binding)
        database = Database.from_store(store, recovery_verifier=verifier)
        context = CommandContext(
            principal=operator,
            idempotency_key="runtime-prepare:" + plan.digest,
            expected_revision=0,
        )
        authority = recovery_authority(database, context, crypto_provider=crypto)
        if authority is None:
            raise ResearchError(ErrorCode.STALE_FENCE, "Runtime preparation requires recovery")
        registry = TimeProviders(store.identity, crypto_provider=crypto)

        def verify(sql):
            verify_configuration(store.identity)
            if runtime_plan(manifest, secrets, custody, crypto) != plan:
                raise ResearchError(ErrorCode.STALE_FENCE, "Protected runtime inputs changed")
            verifier.read_admission(sql, authority=store.identity, context=context, crypto=crypto)
            require_recovery_authority(sql, context, store.identity, authority)
            if exists(sql, "hosted_provisioning") or exists(sql, "hosted_authority_config"):
                raise ResearchError(ErrorCode.STALE_FENCE, "Recovery runtime is already deployed")

        def install(sql):
            verify(sql)
            if saved_record(sql, operator.workspace_id, PLAN_KIND, plan.restore_id) is None:
                tx = Transaction(sql, context)
                tx.insert(
                    PLAN_KIND, plan.restore_id, plan.model_dump(mode="json"), state="immutable"
                )
                for member in plan.credentials:
                    leaf = {
                        "id": member.id,
                        "principal": member.principal.model_dump(mode="json"),
                        "token_digest": member.token_digest,
                    }
                    owned = guarded(context, authority, plan, "credential.install", leaf)
                    CredentialStore.install_in(
                        sql,
                        operator,
                        member.principal,
                        identifier=member.id,
                        token=secrets.credentials[member.id],
                        context=owned,
                        authority=store.identity,
                    )
                owned = guarded(
                    context,
                    authority,
                    plan,
                    "time_provider.register",
                    registration_payload(plan.authority, plan.source),
                )
                registry.register(Transaction(sql, owned), plan.authority, plan.source)
            return inspect_preparation(sql, context, plan, registry, authority)

        current, completed = store.write(install)
        if completed is not None:
            return encode({"schema_version": "1.0", "receipt": completed.model_dump(mode="json")})
        context = context.model_copy(update={"expected_revision": current.revision})
        payload = {"plan_digest": plan.digest}
        operation, pending = registry.prepare(context, current, OPERATION, payload)
        qualified_context = guarded(
            context,
            authority,
            plan,
            "time_provider.requalify",
            qualification_payload(operation, OPERATION, payload),
        )
        task, timeout = asyncio.current_task(), asyncio.timeout(10)
        try:
            async with timeout:
                response = await acquire(pending.request)
            if timeout.expired():
                raise TimeoutError("Runtime preparation witness arrived after its deadline")
        except TimeoutError:
            raise ResearchError(
                ErrorCode.TRANSPORT_UNAVAILABLE,
                "Runtime preparation witness unavailable",
                retryable=True,
            ) from None
        if task is None or task.cancelling():
            raise asyncio.CancelledError("Runtime preparation owner was cancelled")
        proof = pending.complete(response)

        def complete(sql):
            verify(sql)
            previous, receipt = inspect_preparation(sql, context, plan, registry, authority)
            if previous != current or receipt is not None:
                raise ResearchError(ErrorCode.STALE_FENCE, "Runtime preparation changed during I/O")
            tx = Transaction(sql, qualified_context)
            qualified = registry.requalify(tx, operation, proof, name=OPERATION, payload=payload)
            receipt = RuntimePreparationReceipt(
                plan_digest=plan.digest,
                restoration_digest=authority.restoration_digest,
                admission_digest=authority.admission_digest,
                provider_revision=qualified.revision,
                provider=qualified.data,
                prepared_at=utc_now(),
            )
            record = tx.insert(
                RECEIPT_KIND, plan.restore_id, receipt.model_dump(mode="json"), state="immutable"
            )
            tx.event("workspace.runtime_prepared", record, {"plan_digest": plan.digest})
            return encode({"schema_version": "1.0", "receipt": receipt.model_dump(mode="json")})

        return store.write(complete)

    return await exclusive(state, prepare)
