"""Production routing and reopen policy, independent of the Workers Python bridge."""

import re
from urllib.parse import urlsplit

from research_swarm.domain.errors import ErrorCode, ResearchError

if __package__:
    from ..delivery.delivery_registry import DeploymentDeliveryRegistry
    from ..delivery.delivery_router import HostedDelivery, delivery_error
    from ..recovery.recovery_router import RecoveryHTTPPolicy
    from ..research.hosted_http import failure, handle, route, uncertain
    from ..research.research_router import ExistingResearchRouter
    from .provisioning import require_deployment
else:
    from delivery_registry import DeploymentDeliveryRegistry
    from delivery_router import HostedDelivery, delivery_error
    from hosted_http import failure, handle, route, uncertain
    from provisioning import require_deployment
    from recovery_router import RecoveryHTTPPolicy
    from research_router import ExistingResearchRouter


def public_route(method, target):
    """Reject unknown/internal paths before loading deployment inputs or opening state."""
    try:
        url = urlsplit(target)
        if url.path.startswith("/v1/delivery/"):
            if (
                method != "POST"
                or not re.fullmatch(r"/v1/delivery/(bootstrap|inspect|execute|wait)", url.path)
                or url.query
                or url.fragment
            ):
                raise ResearchError(ErrorCode.NOT_FOUND, "Research route not found")
            return "delivery"
        kind, _, _ = route(method, target)
        if kind in {"upload", "worker-upload"}:
            return "upload_part"
        return "command" if kind in {"command", "finalize"} else "query"
    except ResearchError:
        raise
    except (ValueError, TypeError):
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid request target") from None


def fixed_target(namespace, manifest, *, actual_id=None):
    """Resolve only the configured name and verify its actual namespace-derived identity."""
    identifier = namespace.idFromName(manifest.authority_name)
    if str(identifier) != manifest.object_id or (
        actual_id is not None and str(actual_id) != manifest.object_id
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Deployment physical authority differs")
    return identifier


def recovery_route(method, target):
    """Only the named recovery entrypoint selects this policy; request data cannot."""
    try:
        kind, identifier, _ = route(method, target)
        RecoveryHTTPPolicy.admit(kind, identifier)
        return (
            "upload_part"
            if kind == "worker-upload"
            else "command"
            if kind == "command"
            else "query"
        )
    except ResearchError:
        raise
    except (ValueError, TypeError):
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid recovery target") from None


def runtime_failure(error=None, *, mode="query"):
    # Opening/bridge exceptions may contain provider URLs or protected bindings.
    if mode == "delivery":
        return delivery_error(error)
    if error is None:
        return uncertain(mode)
    return failure(
        ResearchError(error.code, "Research authority is unavailable", retryable=error.retryable)
    )


async def open_router(
    state, open_existing, manifest, secrets, *, crypto, acquire, objects, recovery_binding=None
):
    """The ordinary entrypoint has no initializer, credential installer or repair path."""

    def sealed_store():
        store = open_existing()
        require_deployment(store, manifest, crypto=crypto, recovery_binding=recovery_binding)
        return store

    router = await ExistingResearchRouter.open(
        state,
        sealed_store,
        manifest.configuration,
        dispatch_key=secrets.dispatch_key,
        assignment_key=secrets.assignment_seed,
        crypto=crypto,
        acquire=acquire,
        objects=objects,
    )
    router.delivery_registry = DeploymentDeliveryRegistry(router, manifest, secrets)
    router.orchestration.registration_policy = router.delivery_registry.preflight
    return router


async def serve(router, manifest, secrets, method, target, headers, read_body):
    mode = public_route(method, target)
    if mode == "delivery":
        return await HostedDelivery(
            router, {}, resolve_registration=router.delivery_registry.resolve
        ).handle(method, target, headers, read_body)
    return await handle(router, method, target, headers, read_body)
