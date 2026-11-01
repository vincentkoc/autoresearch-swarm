"""Bounded delivery facade over one trusted registration and the existing DO gate."""

import asyncio
import re
from dataclasses import asdict, dataclass, field
from types import MappingProxyType
from urllib.parse import urlsplit

from pydantic import ValidationError

from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.orchestration.async_delivery import AsyncDeliveryKernel, AsyncDriverConnection
from research_swarm.orchestration.delivery_models import delivery_id
from research_swarm.orchestration.gateway_models import GatewayBinding, GatewayStartup, GatewayWork

if __package__:
    from ..platform.exclusive import exclusive
    from ..research.hosted_http import HTTPResponse, json_response
    from ..research.research_router import supports_orchestration, supports_timers
    from .wait_delivery import reconcile_wait
else:
    from exclusive import exclusive
    from hosted_http import HTTPResponse, json_response
    from research_router import supports_orchestration, supports_timers
    from wait_delivery import reconcile_wait

MAX_DELIVERY_BYTES = 16 * 1024
GATEWAY_SCOPE = "orchestration.deliver"


def delivery_error(error=None, *, authenticated=False):
    """Sanitize both facade failures and failures opening its runtime boundary."""
    if error is None:
        error = ResearchError(ErrorCode.DELIVERY_UNCERTAIN, "Delivery is uncertain", retryable=True)
    uncertain = error.code in {ErrorCode.TRANSPORT_UNAVAILABLE, ErrorCode.DELIVERY_UNCERTAIN}
    status = (
        503
        if uncertain
        else {
            ErrorCode.AUTHORIZATION_DENIED: 403 if authenticated else 401,
            ErrorCode.CAPABILITY_UNAVAILABLE: 501,
            ErrorCode.PROJECT_CONTRACT_INVALID: 422,
        }.get(error.code, 409)
    )
    return json_response(
        {
            "schema_version": "1.0",
            "error": {"code": error.code.value, "retryable": error.retryable or uncertain},
        },
        status,
    )


@dataclass(frozen=True)
class DeliveryRegistration:
    binding: GatewayBinding
    route_tokens: dict = field(repr=False)


class _RouteClient:
    def __init__(self, router, token, actor_id, check_owner, binding):
        self.router, self.token, self.actor_id = router, token, actor_id
        self.check_owner = check_owner
        self.binding = binding

    def _context(self, key):
        self.check_owner()
        context = self.router._context(self.token, idempotency_key=key)
        if context.principal.id != self.actor_id:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Registered route actor differs")
        return context

    async def get(self, kind, identifier):
        startup = kind in {"orchestration_start", "orchestration_start_ack"}
        if not startup and kind not in {
            "orchestration_binding",
            "drive_turn",
            "orchestration_wait",
            "campaign",
        }:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Delivery query is unavailable")
        context = self._context(new_id())
        reader = self.router.transfers if startup else self.router.orchestration
        return reader.query(self.token, context, "get", {"kind": kind, "id": identifier})

    async def command(self, operation, request, *, idempotency_key):
        if operation not in {"orchestration.execute_turn", "orchestration.reconcile_wait"}:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Delivery command is unavailable")
        context = self._context(idempotency_key)
        if operation == "orchestration.reconcile_wait" and supports_timers(
            self.router.configuration
        ):
            response = await reconcile_wait(self, self.binding, request)
        else:
            response = await self.router._command_in_context(
                self.token, context, operation, request
            )
        self.check_owner()
        return response


class HostedDelivery:
    def __init__(self, router, registrations, *, resolve_registration=None):
        self.router = router
        if resolve_registration is not None and registrations:
            raise ValueError("Delivery requires one registration authority")
        self._resolve_registration = resolve_registration
        self._registrations = MappingProxyType(
            {actor: self._validated(actor, value) for actor, value in registrations.items()}
        )

    def _validated(self, actor, registration):
        binding = GatewayBinding.model_validate(registration.binding.model_dump(mode="json"))
        tokens = dict(registration.route_tokens)
        if (
            actor in binding.actors.values()
            or binding.workspace_id != self.router.configuration.authority.workspace_id
            or set(tokens) != set(binding.actors)
            or any(
                not isinstance(token, str) or not 1 <= len(token) <= 256
                for token in tokens.values()
            )
        ):
            raise ValueError("Invalid trusted delivery registration")
        return DeliveryRegistration(binding, MappingProxyType(tokens))

    async def handle(self, method, target, headers, read_body):
        authenticated = False

        async def invoke():
            nonlocal authenticated
            if not supports_orchestration(self.router.configuration):
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Delivery profile is unavailable"
                )
            url = urlsplit(target)
            match = re.fullmatch(r"/v1/delivery/(bootstrap|inspect|execute|wait)", url.path)
            if method != "POST" or match is None or url.query or url.fragment:
                raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid delivery route")
            operation = match[1]
            authorization = {key.lower(): value for key, value in headers.items()}.get(
                "authorization", ""
            )
            if not re.fullmatch(r"Bearer [!-~]{1,256}", authorization):
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Delivery authentication is required"
                )
            token = authorization[7:]
            principal = self.router._context(token, idempotency_key=new_id()).principal
            authenticated = True
            principal.require(GATEWAY_SCOPE)
            registration = (
                self._resolve_registration(principal, token)
                if self._resolve_registration is not None
                else self._registrations.get(principal.id)
            )
            if registration is None:
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Gateway is not registered")
            registration = self._validated(principal.id, registration)
            body = await read_body(MAX_DELIVERY_BYTES)
            try:
                if not isinstance(body, bytes) or len(body) > MAX_DELIVERY_BYTES:
                    raise ValueError("Invalid body")
                raw = strict_json_loads(body)
                model = {
                    "bootstrap": GatewayStartup,
                    "inspect": GatewayBinding,
                    "execute": GatewayWork,
                    "wait": GatewayWork,
                }[operation]
                payload = model.model_validate(raw)
            except (ValueError, UnicodeError, ValidationError, RecursionError):
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid delivery request"
                ) from None
            binding = (
                payload if isinstance(payload, GatewayBinding) else payload.binding
            ).reference()
            if registration.binding.reference() != binding:
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Delivery binding differs")
            owner, active = asyncio.current_task(), True

            def check_owner():
                if (
                    not active
                    or owner is None
                    or owner.cancelling()
                    or asyncio.current_task() is not owner
                ):
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED, "Delivery request owner ended"
                    )
                current = self.router._context(token, idempotency_key=new_id()).principal
                if current != principal:
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED, "Gateway credential changed"
                    )
                current.require(GATEWAY_SCOPE)
                if self._resolve_registration is not None:
                    resolved = self._resolve_registration(current, token)
                    if resolved is None or self._validated(current.id, resolved) != registration:
                        raise ResearchError(
                            ErrorCode.AUTHORIZATION_DENIED, "Delivery registration changed"
                        )

            try:
                check_owner()
                clients = {
                    route: _RouteClient(
                        self.router, route_token, binding.actors[route], check_owner, binding
                    )
                    for route, route_token in registration.route_tokens.items()
                }
                for client in clients.values():
                    client._context(new_id())
                connection = AsyncDriverConnection(binding, clients)
                kernel = AsyncDeliveryKernel({delivery_id(binding): connection})
                if isinstance(payload, GatewayStartup):
                    result = {"state": await kernel.bootstrap(payload.reference())}
                elif isinstance(payload, GatewayBinding):
                    pending = await kernel.inspect(binding)
                    result = {
                        "state": pending.state,
                        "work": GatewayWork.from_reference(pending.work).model_dump(mode="json")
                        if pending.work is not None
                        else None,
                    }
                else:
                    outcome = (
                        await kernel.execute(payload.reference())
                        if operation == "execute"
                        else await kernel.wait(payload.reference())
                    )
                    result = asdict(outcome)
                check_owner()
                data = canonical_bytes({"schema_version": "1.0", **result})
                if len(data) > MAX_DELIVERY_BYTES:
                    raise ResearchError(
                        ErrorCode.DELIVERY_UNCERTAIN,
                        "Delivery reply exceeds its bound",
                        retryable=True,
                    )
                return HTTPResponse(
                    200, data, {"content-type": "application/json", "cache-control": "no-store"}
                )
            finally:
                active = False

        try:
            return await exclusive(self.router.state, invoke)
        except ResearchError as error:
            return delivery_error(error, authenticated=authenticated)
        except Exception:
            # Let the gate observe unexpected failure before returning uncertainty.
            return delivery_error()
