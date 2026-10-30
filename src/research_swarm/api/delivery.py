"""Optional delivery gateway; native research authority stays behind registered clients."""

from collections.abc import Mapping
from dataclasses import asdict
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import ValidationError

from research_swarm.api.http import read_body
from research_swarm.control.auth import CredentialStore
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.orchestration.delivery import DeliveryKernel, DriverConnection, run_owned_step
from research_swarm.orchestration.delivery_models import delivery_id
from research_swarm.orchestration.gateway_models import GatewayBinding, GatewayStartup, GatewayWork
from research_swarm.state.database import JsonObject

MAX_DELIVERY_BYTES = 16 * 1024
GATEWAY_SCOPE = "orchestration.deliver"
DeliveryOperation = Literal["bootstrap", "inspect", "execute", "wait"]


def _failure(code: ErrorCode, status: int, *, retryable: bool = False) -> JSONResponse:
    # Provider histories never receive exception details, request bodies or credentials.
    return JSONResponse(
        {"schema_version": "1.0", "error": {"code": code.value, "retryable": retryable}},
        status_code=status,
        headers={"cache-control": "no-store"},
    )


def create_delivery_app(
    credentials: CredentialStore, registrations: Mapping[str, DriverConnection]
) -> FastAPI:
    """Register each gateway actor against one exact immutable campaign binding.

    Registration is trusted application configuration, not an HTTP operation.
    Gateway credentials are distinct from the control/evaluator client credentials.
    """
    registered = {}
    for actor, connection in registrations.items():
        binding = GatewayBinding.from_reference(connection.binding).reference()
        if actor in binding.actors.values():
            raise ValueError("Gateway actors must be distinct from research route actors")
        registered[actor] = DriverConnection(binding, dict(connection.clients))
    app = FastAPI(title="Research delivery gateway", docs_url=None, redoc_url=None)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, error: RequestValidationError) -> JSONResponse:
        return _failure(ErrorCode.PROJECT_CONTRACT_INVALID, 422)

    @app.post("/v1/delivery/{operation}")
    async def deliver(operation: DeliveryOperation, request: Request) -> Response:
        authorization = request.headers.get("authorization", "")
        if not authorization.startswith("Bearer ") or len(authorization) > 1024:
            return _failure(ErrorCode.AUTHORIZATION_DENIED, 401)
        token = authorization[7:]
        try:
            raw = strict_json_loads(await read_body(request, MAX_DELIVERY_BYTES))
            payload: GatewayStartup | GatewayBinding | GatewayWork
            if operation == "bootstrap":
                payload = GatewayStartup.model_validate(raw)
                binding = payload.binding.reference()
            elif operation == "inspect":
                payload = GatewayBinding.model_validate(raw)
                binding = payload.reference()
            else:
                payload = GatewayWork.model_validate(raw)
                binding = payload.binding.reference()
        except (ValueError, UnicodeError, ValidationError, RecursionError, ResearchError):
            return _failure(ErrorCode.PROJECT_CONTRACT_INVALID, 422)

        def invoke() -> JsonObject:
            try:
                principal = credentials.resolve(token)
            except ResearchError:
                return {"authentication_failed": True}
            principal.require(GATEWAY_SCOPE)
            connection = registered.get(principal.id)
            if (
                principal.workspace_id != binding.workspace_id
                or connection is None
                or connection.binding != binding
            ):
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Delivery binding differs")
            kernel = DeliveryKernel({delivery_id(binding): connection})
            if isinstance(payload, GatewayStartup):
                return {"state": kernel.bootstrap(payload.reference())}
            if isinstance(payload, GatewayBinding):
                pending = kernel.inspect(binding)
                return {
                    "state": pending.state,
                    "work": (
                        GatewayWork.from_reference(pending.work).model_dump(mode="json")
                        if pending.work is not None
                        else None
                    ),
                }
            outcome = (
                kernel.execute(payload.reference())
                if operation == "execute"
                else kernel.wait(payload.reference())
            )
            return asdict(outcome)

        try:
            result = await run_owned_step(invoke)
            if result.get("authentication_failed"):
                return _failure(ErrorCode.AUTHORIZATION_DENIED, 401)
            body = canonical_bytes({"schema_version": "1.0", **result})
            if len(body) > MAX_DELIVERY_BYTES:
                return _failure(ErrorCode.DELIVERY_UNCERTAIN, 503, retryable=True)
            return Response(
                body, media_type="application/json", headers={"cache-control": "no-store"}
            )
        except ResearchError as error:
            status = 403 if error.code == ErrorCode.AUTHORIZATION_DENIED else 409
            uncertain = error.code in {
                ErrorCode.TRANSPORT_UNAVAILABLE,
                ErrorCode.DELIVERY_UNCERTAIN,
            }
            if uncertain:
                status = 503
            return _failure(error.code, status, retryable=error.retryable or uncertain)
        except Exception:
            # An unclassified transport failure may follow a committed domain command.
            return _failure(ErrorCode.DELIVERY_UNCERTAIN, 503, retryable=True)

    return app
