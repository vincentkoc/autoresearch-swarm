"""Typed agent tools share the client contract and return bounded evidence."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any, Literal

from mcp.server.mcpserver import MCPServer
from mcp_types import CallToolResult, TextContent, ToolAnnotations
from pydantic import Field, JsonValue, ValidationError

from research_swarm.client import ResearchClient
from research_swarm.control.dispatch import COMMANDS, CommandDefinition
from research_swarm.control.queries import RecordKind, RecordQuery, summarize
from research_swarm.control.search_models import ProposalRead
from research_swarm.control.worker_artifacts import WorkerArtifactRead
from research_swarm.control.worker_outputs import WorkerUploadTarget
from research_swarm.control.worker_queries import WorkerQuery, WorkerRead
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.clocks import ClockChallenge
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Name, Revision, VersionedRecord

MAX_TOOL_BYTES = 128 * 1024
ContextLimit = Annotated[int, Field(strict=True, ge=1, le=50)]
EventLimit = Annotated[int, Field(strict=True, ge=1, le=100)]
LocalPath = Annotated[str, Field(min_length=1, max_length=4096)]


class ToolReply(VersionedRecord):
    command_id: ID | None = None
    disposition: Literal["accepted"] | None = None
    result: JsonValue


ToolCall = Annotated[CallToolResult, ToolReply]


def compact_records(value: Any) -> Any:
    if isinstance(value, dict):
        if {"kind", "id", "revision", "data", "state", "campaign_id", "parent_id"} <= value.keys():
            return summarize(value)
        return {key: compact_records(item) for key, item in value.items()}
    if isinstance(value, list):
        return [compact_records(item) for item in value]
    return value


def tool_result(call: Callable[[], Any], *, compact: bool = True) -> CallToolResult:
    try:
        value = call()
        if compact:
            value = compact_records(value)
        if isinstance(value, dict) and "command_id" in value and "result" in value:
            reply = ToolReply.model_validate(value)
        else:
            reply = ToolReply(result=value)
        payload = reply.model_dump(mode="json")
        encoded = canonical_bytes(payload)
        if len(encoded) > MAX_TOOL_BYTES:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "Tool response exceeds 128 KiB. Request a summary, smaller page, or HTTP detail",
            )
        return CallToolResult(
            content=[TextContent(type="text", text=encoded.decode("utf-8"))],
            structured_content=payload,
        )
    except ResearchError as error:
        failure = error.as_dict()
    except ValidationError:
        failure = ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Input does not match its schema"
        ).as_dict()
    if len(canonical_bytes(failure)) > MAX_TOOL_BYTES:
        failure["error"]["details"] = {"omitted": "Error details exceeded the tool bound"}
    return CallToolResult(
        content=[TextContent(type="text", text=canonical_bytes(failure).decode("utf-8"))],
        structured_content=failure,
        is_error=True,
    )


def make_mcp(
    client: ResearchClient, *, authorize_local_capture: Callable[[], None] | None = None
) -> MCPServer:
    server = MCPServer(
        "autoresearch-swarm",
        instructions=(
            "Use capability schemas and bounded campaign context. "
            "Preserve command keys on retries. "
            "A command acknowledgment is not completed execution or validated scientific progress."
        ),
    )

    def command_tool(operation: str, definition: CommandDefinition) -> Callable[..., ToolCall]:
        def invoke(
            request: VersionedRecord, idempotency_key: Name, expected_revision: Revision
        ) -> ToolCall:
            return tool_result(
                lambda: client.command(
                    operation,
                    request,
                    idempotency_key=idempotency_key,
                    expected_revision=expected_revision,
                )
            )

        def unversioned(
            request: VersionedRecord,
            idempotency_key: Name,
            expected_revision: Revision | None = None,
        ) -> ToolCall:
            return tool_result(
                lambda: client.command(
                    operation,
                    request,
                    idempotency_key=idempotency_key,
                    expected_revision=expected_revision,
                )
            )

        function = invoke if definition.expected_revision else unversioned
        function.__annotations__["request"] = definition.request
        return function

    for operation, definition in COMMANDS.items():
        server.add_tool(
            command_tool(operation, definition),
            name="research_" + operation.replace(".", "_"),
            description=definition.description,
            annotations=ToolAnnotations(
                read_only_hint=False, destructive_hint=False, idempotent_hint=True
            ),
            structured_output=True,
        )

    read_only = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True)

    @server.tool(name="research_capabilities", annotations=read_only, structured_output=True)
    def get_capabilities() -> ToolCall:
        """Inspect supported commands and exact request schemas."""
        return tool_result(client.capabilities)

    @server.tool(name="research_get_context", annotations=read_only, structured_output=True)
    def get_context(campaign_id: ID, limit: ContextLimit = 10) -> ToolCall:
        """Read bounded campaign evidence and its consistent event watermark."""
        return tool_result(lambda: client.context(campaign_id, limit=limit), compact=False)

    @server.tool(name="research_query_records", annotations=read_only, structured_output=True)
    def query_records(request: RecordQuery) -> ToolCall:
        """Read a bounded summary page. Preserve the cursor to continue the same query."""
        return tool_result(lambda: client.list(request), compact=False)

    @server.tool(name="research_get_record", annotations=read_only, structured_output=True)
    def get_record(kind: RecordKind, identifier: ID, detail: bool = False) -> ToolCall:
        """Read one record, summarized by default. Large detail requires the HTTP API."""
        return tool_result(lambda: client.get(kind, identifier), compact=not detail)

    @server.tool(name="research_proposal_get", annotations=read_only, structured_output=True)
    def proposal_get(request: ProposalRead) -> ToolCall:
        """Read this agent's assigned immutable context and preserved response."""
        return tool_result(lambda: client.proposal_get(request), compact=False)

    @server.tool(name="research_worker_query", annotations=read_only, structured_output=True)
    def worker_query(request: WorkerQuery) -> ToolCall:
        """Read this worker's pending assignments or eligible job offers."""
        return tool_result(lambda: client.worker_list(request), compact=False)

    @server.tool(name="research_worker_get", annotations=read_only, structured_output=True)
    def worker_get(request: WorkerRead) -> ToolCall:
        """Read this worker's enrollment or assigned records. Large detail uses the HTTP API."""
        return tool_result(lambda: client.worker_get(request))

    @server.tool(name="research_worker_input", annotations=read_only, structured_output=True)
    def worker_input(request: WorkerArtifactRead) -> ToolCall:
        """Read the exact metadata of an authorized input without workspace read access."""
        return tool_result(lambda: client.worker_input(request).model_dump(mode="json"))

    @server.tool(name="research_worker_clock", annotations=read_only, structured_output=True)
    def worker_clock(request: ClockChallenge) -> ToolCall:
        """Obtain a nonce-bound signed time sample for this enrolled worker."""
        return tool_result(lambda: client.worker_clock(request).model_dump(mode="json"))

    @server.tool(name="research_worker_output", annotations=read_only, structured_output=True)
    def worker_output(request: WorkerUploadTarget) -> ToolCall:
        """Inspect this attempt's upload progress, canonical reference, and cleanup state."""
        return tool_result(lambda: client.worker_output(request))

    @server.tool(name="research_get_budget", annotations=read_only, structured_output=True)
    def get_budget(campaign_id: ID) -> ToolCall:
        """Read exact consumption, reservations, uncertainty, and remaining budget."""
        return tool_result(lambda: client.budget(campaign_id))

    @server.tool(name="research_get_events", annotations=read_only, structured_output=True)
    def get_events(after: Revision = 0, limit: EventLimit = 50) -> ToolCall:
        """Read a bounded event page strictly after the supplied workspace cursor."""
        return tool_result(lambda: client.events(after=after, limit=limit))

    if authorize_local_capture is not None:

        @server.tool(
            name="research_capture_project",
            structured_output=True,
            annotations=ToolAnnotations(
                read_only_hint=False, destructive_hint=False, idempotent_hint=True
            ),
        )
        def capture_project(
            path: LocalPath,
            idempotency_key: Name,
            manifest_path: str = "research.toml",
            revision: str | None = None,
        ) -> ToolCall:
            """Capture declared project files. Requires source.capture scope."""

            def capture() -> Any:
                assert authorize_local_capture is not None
                authorize_local_capture()
                return client.capture_project(
                    Path(path),
                    idempotency_key=idempotency_key,
                    manifest_path=manifest_path,
                    revision=revision,
                ).model_dump(mode="json")

            return tool_result(capture)

    return server
