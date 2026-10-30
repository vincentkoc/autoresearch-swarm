"""HTTP maps authenticated requests onto the same local research operations."""

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import ValidationError
from starlette.types import Receive, Scope, Send

from research_swarm.control.auth import CredentialStore
from research_swarm.control.dispatch import COMMANDS, capabilities, dispatch_command
from research_swarm.control.queries import RecordKind, RecordQuery, ResearchQueries
from research_swarm.control.service import ResearchService
from research_swarm.control.worker_queries import WorkerQueries, WorkerQuery, WorkerRead
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.state.artifacts import BeginUpload
from research_swarm.state.database import JsonObject

MAX_COMMAND_BYTES = 1024 * 1024
MAX_PART_BYTES = 4 * 1024 * 1024


class AuthenticationError(ResearchError):
    """A missing identity differs from a known actor without permission."""


class ArtifactResponse(StreamingResponse):
    def __init__(
        self,
        content: AsyncIterator[bytes],
        *,
        close: Callable[[], None],
        media_type: str,
        headers: dict[str, str],
    ) -> None:
        super().__init__(content, media_type=media_type, headers=headers)
        self.close_reader = close

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            # Own the reader before the first response byte, including failed header sends.
            closing = asyncio.create_task(asyncio.to_thread(self.close_reader))
            try:
                await asyncio.shield(closing)
            except asyncio.CancelledError:
                await closing
                raise


async def read_body(request: Request, limit: int) -> bytes:
    content = bytearray()
    async for block in request.stream():
        if len(content) + len(block) > limit:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Request body exceeds its limit"
            )
        content.extend(block)
    return bytes(content)


async def read_json(request: Request) -> JsonObject:
    try:
        value = strict_json_loads(await read_body(request, MAX_COMMAND_BYTES))
        if not isinstance(value, dict):
            raise ValueError("A command body must be an object")
        return dict(value)
    except (ValueError, UnicodeError) as error:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid JSON command body"
        ) from error


def openapi_references(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: (
                item.replace("#/$defs/", "#/components/schemas/")
                if key == "$ref"
                else openapi_references(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [openapi_references(item) for item in value]
    return value


def create_app(service: ResearchService, credentials: CredentialStore) -> FastAPI:
    if service.database.path.resolve() != credentials.database.path.resolve():
        raise ValueError("Credentials and research must share an authority")
    app = FastAPI(
        title="Autoresearch Swarm", version="1.0", docs_url=None, redoc_url=None, openapi_url=None
    )
    queries = ResearchQueries(service.database)
    workers = WorkerQueries(service.database)

    @app.exception_handler(ResearchError)
    async def domain_error(request: Request, error: ResearchError) -> JSONResponse:
        if isinstance(error, AuthenticationError):
            return JSONResponse(
                error.as_dict(), status_code=401, headers={"WWW-Authenticate": "Bearer"}
            )
        status = {
            ErrorCode.AUTHORIZATION_DENIED: 403,
            ErrorCode.NOT_FOUND: 404,
            ErrorCode.PROJECT_CONTRACT_INVALID: 422,
            ErrorCode.SCHEMA_UNSUPPORTED: 422,
            ErrorCode.CAPABILITY_UNAVAILABLE: 501,
        }.get(error.code, 409)
        return JSONResponse(error.as_dict(), status_code=status)

    @app.exception_handler(ValidationError)
    @app.exception_handler(RequestValidationError)
    async def input_error(request: Request, error: Exception) -> JSONResponse:
        return JSONResponse(
            ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Input does not match its schema"
            ).as_dict(),
            status_code=422,
        )

    async def principal(authorization: Annotated[str | None, Header()] = None) -> Principal:
        if authorization is None or not authorization[:7].casefold() == "bearer ":
            raise AuthenticationError(
                ErrorCode.AUTHORIZATION_DENIED, "Bearer authentication is required"
            )
        try:
            return await asyncio.to_thread(credentials.resolve, authorization[7:])
        except ResearchError as error:
            raise AuthenticationError(error.code, error.message) from error

    async def read_context(actor: Annotated[Principal, Depends(principal)]) -> CommandContext:
        actor.require("read")
        return CommandContext(principal=actor, idempotency_key=new_id())

    async def transfer_context(actor: Annotated[Principal, Depends(principal)]) -> CommandContext:
        actor.require("artifact.upload")
        return CommandContext(principal=actor, idempotency_key=new_id())

    async def worker_context(actor: Annotated[Principal, Depends(principal)]) -> CommandContext:
        actor.require("worker.query")
        return CommandContext(principal=actor, idempotency_key=new_id())

    async def write_context(
        actor: Annotated[Principal, Depends(principal)],
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
        expected_revision: Annotated[int | None, Header(alias="X-Expected-Revision", ge=0)] = None,
    ) -> CommandContext:
        return CommandContext(
            principal=actor, idempotency_key=idempotency_key, expected_revision=expected_revision
        )

    @app.get("/healthz")
    async def health() -> JsonObject:
        return {"status": "alive"}

    @app.get("/v1/capabilities")
    async def get_capabilities(
        context: Annotated[CommandContext, Depends(read_context)],
    ) -> JsonObject:
        return capabilities()

    @app.get("/v1/openapi.json", include_in_schema=False)
    async def openapi(context: Annotated[CommandContext, Depends(read_context)]) -> JsonObject:
        return dict(app.openapi())

    def command_endpoint(operation: str) -> Any:
        async def execute(
            request: Request, context: Annotated[CommandContext, Depends(write_context)]
        ) -> JsonObject:
            payload = await read_json(request)
            return await asyncio.to_thread(dispatch_command, service, context, operation, payload)

        return execute

    for operation, definition in COMMANDS.items():
        schema = definition.request.model_json_schema()

        app.post(
            "/v1/commands/" + operation,
            operation_id=operation.replace(".", "_"),
            summary=definition.description,
            openapi_extra={
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": openapi_references(
                                {key: value for key, value in schema.items() if key != "$defs"}
                            )
                        }
                    },
                }
            },
        )(command_endpoint(operation))

    @app.get("/v1/records/{kind}/{identifier}")
    async def get_record(
        kind: RecordKind, identifier: str, context: Annotated[CommandContext, Depends(read_context)]
    ) -> JsonObject:
        return await asyncio.to_thread(queries.get, context, kind, identifier)

    @app.post("/v1/worker/get")
    async def worker_get(
        request: Request, context: Annotated[CommandContext, Depends(worker_context)]
    ) -> JsonObject:
        parsed = WorkerRead.model_validate(await read_json(request))
        return await asyncio.to_thread(workers.get, context, parsed)

    @app.post("/v1/worker/list")
    async def worker_list(
        request: Request, context: Annotated[CommandContext, Depends(worker_context)]
    ) -> JsonObject:
        parsed = WorkerQuery.model_validate(await read_json(request))
        return await asyncio.to_thread(workers.list, context, parsed)

    @app.post("/v1/query")
    async def query(
        request: Request, context: Annotated[CommandContext, Depends(read_context)]
    ) -> JsonObject:
        parsed = RecordQuery.model_validate(await read_json(request))
        return await asyncio.to_thread(queries.list, context, parsed)

    @app.get("/v1/campaigns/{campaign_id}/context")
    async def campaign_context(
        campaign_id: str,
        context: Annotated[CommandContext, Depends(read_context)],
        limit: Annotated[int, Query(ge=1, le=50)] = 10,
    ) -> JsonObject:
        return await asyncio.to_thread(queries.context, context, campaign_id, limit=limit)

    @app.get("/v1/campaigns/{campaign_id}/budget")
    async def budget(
        campaign_id: str, context: Annotated[CommandContext, Depends(read_context)]
    ) -> JsonObject:
        return await asyncio.to_thread(queries.budget, context, campaign_id)

    @app.get("/v1/events")
    async def events(
        context: Annotated[CommandContext, Depends(read_context)],
        after: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> JsonObject:
        result = await asyncio.to_thread(service.database.events, context, after=after, limit=limit)
        return {
            "events": [event.model_dump(mode="json") for event in result],
            "cursor": result[-1].sequence if result else after,
        }

    @app.get("/v1/events/stream")
    async def event_stream(
        request: Request,
        context: Annotated[CommandContext, Depends(read_context)],
        after: Annotated[int, Query(ge=0)] = 0,
    ) -> StreamingResponse:
        async def stream() -> AsyncIterator[bytes]:
            cursor = after
            while not await request.is_disconnected():
                # Re-resolve on every polling batch so revocation affects long-lived streams.
                try:
                    await principal(request.headers.get("authorization"))
                except ResearchError:
                    return
                batch = await asyncio.to_thread(
                    service.database.events, context, after=cursor, limit=100
                )
                for event in batch:
                    yield (
                        b"id: "
                        + str(event.sequence).encode("ascii")
                        + b"\nevent: research\ndata: "
                        + canonical_bytes(event.model_dump(mode="json"))
                        + b"\n\n"
                    )
                    cursor = event.sequence
                if not batch:
                    yield b": heartbeat\n\n"
                    await asyncio.sleep(1)

        return StreamingResponse(
            stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"}
        )

    @app.post("/v1/artifact-uploads")
    async def begin_upload(
        request: Request, context: Annotated[CommandContext, Depends(write_context)]
    ) -> JsonObject:
        parsed = BeginUpload.model_validate(await read_json(request))
        return await asyncio.to_thread(service.artifacts.begin_upload, context, parsed)

    @app.put("/v1/artifact-uploads/{identifier}/parts")
    async def upload_part(
        identifier: str,
        request: Request,
        context: Annotated[CommandContext, Depends(transfer_context)],
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> JsonObject:
        content = await read_body(request, MAX_PART_BYTES)
        size = await asyncio.to_thread(
            service.artifacts.upload_part, context, identifier, offset, content
        )
        return {"offset": size}

    @app.get("/v1/artifact-uploads/{identifier}")
    async def upload_status(
        identifier: str, context: Annotated[CommandContext, Depends(transfer_context)]
    ) -> JsonObject:
        return await asyncio.to_thread(service.artifacts.upload_status, context, identifier)

    @app.post("/v1/artifact-uploads/{identifier}/finalize")
    async def finalize_upload(
        identifier: str, context: Annotated[CommandContext, Depends(write_context)]
    ) -> JsonObject:
        return await asyncio.to_thread(service.artifacts.finalize_upload, context, identifier)

    @app.get("/v1/artifacts/{identifier}")
    async def get_artifact(
        identifier: str, context: Annotated[CommandContext, Depends(read_context)]
    ) -> JsonObject:
        return (await asyncio.to_thread(service.artifacts.get, context, identifier)).model_dump(
            mode="json"
        )

    @app.get("/v1/artifacts/{identifier}/content")
    async def get_content(
        identifier: str, context: Annotated[CommandContext, Depends(read_context)]
    ) -> StreamingResponse:
        record = await asyncio.to_thread(service.artifacts.get, context, identifier)

        reader = service.artifacts.reader(context, record.reference)
        opening = asyncio.create_task(asyncio.to_thread(reader.__enter__))
        try:
            source = await asyncio.shield(opening)
        except asyncio.CancelledError:
            try:
                await opening
            finally:
                await asyncio.to_thread(reader.__exit__, None, None, None)
            raise

        async def content() -> AsyncIterator[bytes]:
            while block := await asyncio.to_thread(source.read, 64 * 1024):
                yield block

        def close_reader() -> None:
            reader.__exit__(None, None, None)

        return ArtifactResponse(
            content(),
            close=close_reader,
            media_type=record.reference.media_type,
            headers={
                "Content-Length": str(record.reference.size_bytes),
                "ETag": '"' + record.reference.digest + '"',
                "Content-Disposition": 'attachment; filename="' + record.reference.id + '"',
                "X-Content-Type-Options": "nosniff",
            },
        )

    # Add command-model definitions to the generated document, without a second validator.
    generated = app.openapi()
    components = generated.setdefault("components", {}).setdefault("schemas", {})
    for definition in COMMANDS.values():
        for name, schema in definition.request.model_json_schema().get("$defs", {}).items():
            components[name] = openapi_references(schema)
    for path, model in (("/v1/worker/get", WorkerRead), ("/v1/worker/list", WorkerQuery)):
        schema = model.model_json_schema()
        generated["paths"][path]["post"]["requestBody"] = {
            "required": True,
            "content": {
                "application/json": {
                    "schema": openapi_references(
                        {key: value for key, value in schema.items() if key != "$defs"}
                    )
                }
            },
        }
        for name, definition in schema.get("$defs", {}).items():
            components[name] = openapi_references(definition)
    generated["components"]["securitySchemes"] = {
        "BearerAuth": {"type": "http", "scheme": "bearer"}
    }
    for path, operations in generated["paths"].items():
        if path != "/healthz":
            for operation in operations.values():
                operation["security"] = [{"BearerAuth": []}]
    app.openapi_schema = generated
    return app
