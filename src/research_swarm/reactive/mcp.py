"""Dedicated stdio tools for one operator-selected local reactive session."""

from __future__ import annotations

from typing import Annotated, Any

from mcp.server.mcpserver import MCPServer
from mcp_types import CallToolResult, TextContent, ToolAnnotations
from pydantic import JsonValue

from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import VersionedRecord
from research_swarm.reactive.client import (
    MAX_REPLY_BYTES,
    DecisionClient,
    ExportRequest,
    RespondRequest,
)


class ReactiveReply(VersionedRecord):
    result: JsonValue


ReactiveToolCall = Annotated[CallToolResult, ReactiveReply]


def make_reactive_mcp(client: DecisionClient) -> MCPServer:
    server = MCPServer(
        "autoresearch-reactive",
        instructions=(
            "Operate only the explicitly granted local training session. Inspect its contract "
            "and preserve request identity/revision. A response is a proposal, not an applied "
            "control change or evaluator result. No training ownership or application "
            "tools are exposed."
        ),
    )

    def call(operation: str, arguments: dict[str, Any]) -> CallToolResult:
        try:
            result = ReactiveReply(result=client.dispatch(operation, arguments)).model_dump(
                mode="json"
            )
            encoded = canonical_bytes(result)
            if len(encoded) > MAX_REPLY_BYTES:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE,
                    "Reactive tool response exceeds its byte limit",
                )
            return CallToolResult(
                content=[TextContent(type="text", text=encoded.decode("utf-8"))],
                structured_content=result,
            )
        except ResearchError as error:
            result = error.as_dict()
            return CallToolResult(
                content=[TextContent(type="text", text=canonical_bytes(result).decode("utf-8"))],
                structured_content=result,
                is_error=True,
            )

    read = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True)

    @server.tool(name="reactive_inspect", annotations=read, structured_output=True)
    def inspect() -> ReactiveToolCall:
        """Inspect the granted session, numeric controls, diagnostic allowlist and limits."""
        return call("inspect", {})

    @server.tool(name="reactive_pending", annotations=read, structured_output=True)
    def pending() -> ReactiveToolCall:
        """Read the outstanding request and its full identity without claiming ownership."""
        return call("pending", {})

    @server.tool(
        name="reactive_respond",
        annotations=ToolAnnotations(
            read_only_hint=False, destructive_hint=False, idempotent_hint=True
        ),
        structured_output=True,
    )
    def respond(request: RespondRequest) -> ReactiveToolCall:
        """Submit a bounded proposal; identical repeats preserve the original response."""
        return call("respond", request.model_dump(mode="json"))

    @server.tool(name="reactive_export", annotations=read, structured_output=True)
    def export(request: ExportRequest) -> ReactiveToolCall:
        """Read an append-only evidence page bounded by event count and serialized bytes."""
        return call("export", request.model_dump(mode="json"))

    return server
