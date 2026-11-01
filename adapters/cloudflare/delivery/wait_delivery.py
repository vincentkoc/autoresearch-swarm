"""Deliver saved waits across explicit clock requalification without rewriting history."""

from research_swarm.control.orchestration_waits import wait_outcome
from research_swarm.control.wait_models import OrchestrationWaitTarget
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import OrchestrationOwner
from research_swarm.orchestration.delivery_contract import current_binding, verify_work
from research_swarm.orchestration.delivery_models import WorkReference
from research_swarm.state.database import Transaction
from research_swarm.state.drive_guard import require_driver_started

OPERATION = "orchestration.reconcile_wait"


def wait_delivery_key(wait_id, generation):
    return record_digest(
        "research.hosted-wait-delivery-v1",
        {"wait_id": wait_id, "provider_generation": generation},
    )


def _require_fence(router, transaction, snapshot):
    if snapshot.state != "unqualified" or snapshot.data.source_epoch is None:
        raise ResearchError(ErrorCode.STALE_FENCE, "Wait clock has no admitted retirement")
    # Timer profiles require the production history modules; older bundles do not.
    if __package__:
        from ..recovery.runtime_history import read_provider_retirement
    else:
        from runtime_history import read_provider_retirement

    read_provider_retirement(
        transaction.connection,
        router.configuration.authority,
        snapshot.data,
        snapshot.revision - 1,
        crypto=router.registry.crypto_provider,
    )


def _pending():
    raise ResearchError(
        ErrorCode.RESOURCE_UNAVAILABLE,
        "Wait requires explicit clock requalification",
        retryable=True,
    )


async def reconcile_wait(client, binding, request):
    router = client.router
    intent = OrchestrationWaitTarget.model_validate(request)
    work = WorkReference(binding, "wait", intent.wait_id, client.actor_id)

    def read(*, expected=None):
        context = client._context("hosted-wait-delivery-read")
        context.principal.require("read")
        context.principal.require("orchestration.drive")
        router.orchestration._identity(client.token, context)

        def inspect(sql):
            transaction = Transaction(sql, context)
            row = transaction.get("orchestration_wait", intent.wait_id)
            verify_work(work, row["data"])
            if row["state"] != "waiting":
                if expected is not None:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Wait changed during clock failure")
                # Delivery can read immutable readiness without issuing a new command.
                return None, {"result": wait_outcome(transaction, row)}
            router.orchestration._preflight(context, OPERATION, intent)
            current, active = current_binding(
                binding, transaction.get("orchestration_binding", binding.campaign_id)
            )
            owner = OrchestrationOwner.model_validate(
                transaction.get("orchestration_owner", client.actor_id)["data"]
            )
            if (
                not active
                or current.active_wait_id != intent.wait_id
                or owner.campaign_id != binding.campaign_id
                or owner.epoch != binding.epoch
                or owner.actor_id != client.actor_id
                or owner.route != row["data"]["route"]
                or current.routes.get(owner.route) != client.actor_id
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Wait delivery destination changed")
            require_driver_started(transaction, current, owner.route)
            snapshot = router.registry.load(transaction, router.configuration.authority)
            if snapshot.data.source != router.configuration.source:
                raise ResearchError(ErrorCode.STALE_FENCE, "Wait witness source differs")
            if expected is not None and (
                snapshot.data != expected.data
                or snapshot.revision != expected.revision + 1
                or snapshot.state != "unqualified"
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Wait clock transition differs")
            if snapshot.state == "unqualified":
                _require_fence(router, transaction, snapshot)
            return snapshot, None

        return router.database.store.read(inspect)

    snapshot, ready = read()
    if ready is not None:
        return ready
    if snapshot.state == "unqualified":
        _pending()
    context = client._context(
        wait_delivery_key(intent.wait_id, snapshot.data.scope.provider_generation)
    )
    try:
        return await router._command_in_context(client.token, context, OPERATION, request)
    except ResearchError as error:
        if error.code != ErrorCode.STALE_FENCE:
            raise
        _, ready = read(expected=snapshot)
        if ready is not None:
            return ready
        _pending()
