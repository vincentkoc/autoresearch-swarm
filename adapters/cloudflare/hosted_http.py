"""Bounded HTTP mapping for a provisioned router; no server or deployment owner.

The runtime supplies a body reader that enforces the requested bound while
consuming input. It sends responses only after the router releases its gate.
"""

import re
from dataclasses import dataclass
from types import MappingProxyType
from urllib.parse import parse_qsl, urlsplit

from pydantic import ValidationError

from research_swarm.control.dispatch import COMMANDS as SHARED_COMMANDS
from research_swarm.control.worker_queries import WorkerQuery, WorkerRead
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.clocks import ClockChallenge
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.artifacts import BeginUpload

if __package__:
    from .artifact_router import COMMANDS as ARTIFACT_COMMANDS
    from .artifact_router import UploadTarget
    from .research_router import COMMANDS as WORKER_COMMANDS
else:
    from artifact_router import COMMANDS as ARTIFACT_COMMANDS
    from artifact_router import UploadTarget
    from research_router import COMMANDS as WORKER_COMMANDS


MAX_JSON_BYTES = 1024**2
UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
UPLOAD = re.compile(r"^/v1/artifact-uploads/(" + UUID + r")(/parts|/finalize)?$")
ARTIFACT = re.compile(r"^/v1/artifacts/(" + UUID + r")(/content)?$")
BASE_HEADERS = {"cache-control": "no-store, no-transform", "x-content-type-options": "nosniff"}


@dataclass(frozen=True)
class HTTPResponse:
    status: int
    body: bytes
    headers: dict

    def __post_init__(self):
        object.__setattr__(self, "headers", MappingProxyType(dict(self.headers)))


def json_response(value, status=200, *, headers=None):
    body = canonical_bytes(value)
    return HTTPResponse(
        status,
        body,
        {
            **BASE_HEADERS,
            "content-type": "application/json",
            "content-length": str(len(body)),
            **(headers or {}),
        },
    )


def failure(error, *, unauthenticated=False):
    status = {
        ErrorCode.AUTHORIZATION_DENIED: 401 if unauthenticated else 403,
        ErrorCode.NOT_FOUND: 404,
        ErrorCode.PROJECT_CONTRACT_INVALID: 422,
        ErrorCode.SCHEMA_UNSUPPORTED: 422,
        ErrorCode.CAPABILITY_UNAVAILABLE: 501,
        ErrorCode.TRANSPORT_UNAVAILABLE: 502,
        ErrorCode.DELIVERY_UNCERTAIN: 502,
    }.get(error.code, 409)
    return json_response(
        error.as_dict(), status, headers={"www-authenticate": "Bearer"} if status == 401 else None
    )


def capabilities(configuration):
    commands = {
        name: {
            "description": SHARED_COMMANDS[name].description,
            "scope": SHARED_COMMANDS[name].scope,
            "required_scopes": [
                SHARED_COMMANDS[name].scope,
                *SHARED_COMMANDS[name].additional_scopes,
            ],
            "expected_revision": SHARED_COMMANDS[name].expected_revision,
            "request_schema": SHARED_COMMANDS[name].request.model_json_schema(),
        }
        for name in sorted(WORKER_COMMANDS)
    }
    result = {
        "schema_version": "1.0",
        "profile": configuration.profile,
        "commands": commands,
        "queries": ["capabilities"],
        "worker_queries": {
            "scope": "worker.query",
            "operations": {
                "worker.get": WorkerRead.model_json_schema(),
                "worker.list": WorkerQuery.model_json_schema(),
                "worker.clock": ClockChallenge.model_json_schema(),
            },
        },
        "execution": "unavailable",
        "credentials": "non-expiring-only",
        "clock": {
            "authority_profile": "hosted-native-witness-v1",
            "worker_admission": "unqualified",
        },
    }
    if configuration.profile == "worker-artifacts-witness-v1":
        for name in sorted(ARTIFACT_COMMANDS):
            request = BeginUpload if name == "artifact.begin_upload" else UploadTarget
            commands[name] = {
                "scope": "artifact.upload",
                "required_scopes": ["artifact.upload", "read"],
                "expected_revision": False,
                "request_schema": request.model_json_schema(),
            }
        result["queries"] += ["artifact.get", "artifact.upload_status"]
        result["artifacts"] = {
            "max_object_bytes": configuration.storage.max_object_bytes,
            "max_part_bytes": configuration.storage.max_object_bytes,
            "transfer": "complete-body-offset-zero",
            "cleanup": "explicit-command",
            "download": "verified-buffer",
            "range_requests": False,
            "storage_limit_bytes": configuration.storage_limit_bytes,
        }
    return result


def route(method, target):
    url = urlsplit(target)
    if url.fragment or len(url.query) > 2048:
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid request target")
    pairs = parse_qsl(url.query, keep_blank_values=True, strict_parsing=True)
    query = dict(pairs)
    if len(query) != len(pairs):
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Repeated query parameter")
    path = url.path
    if method == "PUT" and (match := UPLOAD.fullmatch(path)) and match[2] == "/parts":
        if set(query) - {"offset"} or not re.fullmatch(
            r"0|[1-9][0-9]{0,15}", query.get("offset", "0")
        ):
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid upload offset")
        return "upload", match[1], int(query.get("offset", "0"))
    if query:
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Unsupported query parameter")
    if method == "GET":
        if path == "/v1/capabilities":
            return "capabilities", None, None
        if (match := UPLOAD.fullmatch(path)) and match[2] is None:
            return "status", match[1], None
        if match := ARTIFACT.fullmatch(path):
            return "content" if match[2] else "metadata", match[1], None
    if method == "POST":
        if path == "/v1/artifact-uploads":
            return "command", "artifact.begin_upload", None
        if (match := UPLOAD.fullmatch(path)) and match[2] == "/finalize":
            return "finalize", match[1], None
        if re.fullmatch(r"/v1/commands/[a-z_]+\.[a-z_]+", path):
            return "command", path.removeprefix("/v1/commands/"), None
        if path in {"/v1/worker/get", "/v1/worker/list", "/v1/worker/clock"}:
            return "query", path.removeprefix("/v1/").replace("/", "."), None
    raise ResearchError(ErrorCode.NOT_FOUND, "Research route not found")


async def handle(router, method, target, headers, read_body):
    """Return standard SDK wire responses; never log tokens, bodies or provider errors."""
    authenticated, entered_operation = False, False
    mode = "query"
    try:
        kind, identifier, offset = route(method, target)
        headers = {key.lower(): value for key, value in headers.items()}
        authorization = headers.get("authorization", "")
        if not re.fullmatch(r"Bearer [!-~]{1,4096}", authorization, re.IGNORECASE):
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Bearer authentication is required")
        token = authorization[7:]
        # Authenticate before body consumption; operations reauthenticate afterward.
        actor = await router.authenticate(token)
        authenticated = True
        if "range" in headers or headers.get("content-encoding", "identity") != "identity":
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Ranges and encoded bodies are unavailable"
            )
        if (
            kind in {"upload", "status", "metadata", "content", "finalize"}
            and router.configuration.profile != "worker-artifacts-witness-v1"
        ):
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Artifact profile is unavailable")
        if kind == "command":
            allowed = WORKER_COMMANDS | (
                ARTIFACT_COMMANDS
                if router.configuration.profile == "worker-artifacts-witness-v1"
                else frozenset()
            )
            if identifier not in allowed:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Command is outside this profile"
                )
        if kind in {"upload", "status", "finalize"} or (
            kind == "command" and identifier in ARTIFACT_COMMANDS
        ):
            scopes = ("artifact.upload", "read")
        elif kind == "command":
            definition = SHARED_COMMANDS[identifier]
            scopes = (definition.scope, *definition.additional_scopes)
        else:
            scopes = ("worker.query",) if kind == "query" else ("read",)
        for scope in scopes:
            actor.require(scope)
        mode = (
            "upload_part"
            if kind == "upload"
            else "command"
            if kind in {"command", "finalize"}
            else "query"
        )
        key, revision = headers.get("idempotency-key"), headers.get("x-expected-revision")
        if kind in {"command", "finalize"}:
            if key is None or not 1 <= len(key) <= 128:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Idempotency-Key is required"
                )
            if revision is not None and not re.fullmatch(r"0|[1-9][0-9]{0,15}", revision):
                raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid expected revision")
        limit = (
            router.configuration.storage.max_object_bytes
            if kind == "upload"
            else MAX_JSON_BYTES
            if kind in {"query", "command"}
            else 0
        )
        body = await read_body(limit)
        if not isinstance(body, bytes) or len(body) > limit:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Request body exceeds its bound"
            )
        if kind in {"query", "command"}:
            payload = strict_json_loads(body)
            if not isinstance(payload, dict):
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Request body must be an object"
                )
        entered_operation = True
        if kind in {"command", "finalize"}:
            result = await router.command(
                token,
                "artifact.finalize_upload" if kind == "finalize" else identifier,
                {"upload_id": identifier} if kind == "finalize" else payload,
                idempotency_key=key,
                expected_revision=int(revision) if revision is not None else None,
            )
        elif kind == "query":
            result = await router.query(token, identifier, payload)
        elif kind == "upload":
            result = {"offset": await router.upload_part(token, identifier, offset, body)}
        elif kind == "status":
            result = await router.query(token, "artifact.upload_status", {"upload_id": identifier})
        elif kind == "metadata":
            result = await router.query(token, "artifact.get", {"id": identifier})
        elif kind == "content":
            reference, content = await router.artifact_content(token, identifier)
            return HTTPResponse(
                200,
                content,
                {
                    **BASE_HEADERS,
                    "content-type": reference.media_type,
                    "content-length": str(reference.size_bytes),
                    "content-encoding": "identity",
                    "etag": '"' + reference.digest + '"',
                    "content-disposition": 'attachment; filename="' + reference.id + '"',
                },
            )
        else:
            (await router.authenticate(token)).require("read")
            result = capabilities(router.configuration)
        return json_response(result)
    except ResearchError as error:
        return failure(error, unauthenticated=not authenticated)
    except (ValueError, UnicodeError, ValidationError):
        if not entered_operation:
            return failure(
                ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid research request")
            )
        return uncertain(mode)
    except Exception:
        return uncertain(mode if entered_operation else "query")


def uncertain(mode):
    return failure(
        ResearchError(
            ErrorCode.TRANSPORT_UNAVAILABLE if mode == "query" else ErrorCode.DELIVERY_UNCERTAIN,
            "Research response is unavailable",
            retryable=True,
            details={
                "recovery": {
                    "query": "retry_query",
                    "command": "replay_exact_command",
                    "upload_part": "inspect_upload_then_replay_exact_range",
                }[mode]
            },
        )
    )
