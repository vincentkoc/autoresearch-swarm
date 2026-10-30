from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from mcp import Client, StdioServerParameters

from research_swarm import ResearchClient
from research_swarm.api.mcp import MAX_TOOL_BYTES, make_mcp, tool_result
from research_swarm.control.commands import CreateCampaign
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime, write_private
from tests.support import LIMITS, ResearchFixture


def process_environment() -> dict[str, str]:
    names = ("PATH", "SYSTEMROOT", "SystemRoot", "WINDIR", "TEMP", "TMP", "HOME", "USERPROFILE")
    return {name: os.environ[name] for name in names if name in os.environ}


def cli(state: Path, *arguments: str, payload: dict[str, Any] | None = None) -> Any:
    result = subprocess.run(
        [sys.executable, "-m", "research_swarm.cli", "--state-dir", str(state), *arguments],
        input=b"" if payload is None else canonical_bytes(payload),
        capture_output=True,
        env=process_environment(),
        timeout=30,
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
    return strict_json_loads(result.stdout)


def stdio(state: Path, *arguments: str) -> StdioServerParameters:
    return StdioServerParameters(
        command=sys.executable,
        args=["-m", "research_swarm.cli", "--state-dir", str(state), *arguments, "mcp"],
        env=process_environment(),
    )


async def test_cli_mcp_and_sdk_share_durable_commands(
    research: ResearchFixture, tmp_path: Path
) -> None:
    state = tmp_path / "agent-runtime"
    identity = cli(state, "init")
    project = cli(state, "project", "register", str(research.source), "--key", "agent-source")
    request = CreateCampaign(
        project_id=project["result"]["project"]["id"],
        name="Agent campaign",
        primary_metric="val_bpb",
        limits=LIMITS,
    ).model_dump(mode="json")
    created = cli(state, "command", "campaign.create", "--key", "agent-campaign", payload=request)
    async with Client(stdio(state), read_timeout_seconds=20, cache=None) as agent:
        listed = {tool.name: tool for tool in (await agent.list_tools()).tools}
        assert "research_capture_project" in listed
        schema = listed["research_campaign_create"].input_schema
        assert "request" in schema["properties"]
        assert listed["research_campaign_create"].output_schema is not None
        replay = await agent.call_tool(
            "research_campaign_create", {"request": request, "idempotency_key": "agent-campaign"}
        )
        assert not replay.is_error
        assert replay.structured_content is not None
        assert replay.structured_content["command_id"] == created["command_id"]
        campaign = created["result"]
        for operation in ("seal", "start"):
            result = await agent.call_tool(
                "research_campaign_" + operation,
                {
                    "request": {"campaign_id": campaign["id"]},
                    "idempotency_key": "agent-" + operation,
                    "expected_revision": campaign["revision"],
                },
            )
            assert not result.is_error
            assert result.structured_content is not None
            campaign = result.structured_content["result"]
        conflict = await agent.call_tool(
            "research_campaign_create",
            {"request": {**request, "name": "Changed"}, "idempotency_key": "agent-campaign"},
        )
        assert conflict.is_error
        assert conflict.structured_content is not None
        assert conflict.structured_content["error"]["code"] == "IDEMPOTENCY_CONFLICT"
        context = await agent.call_tool("research_get_context", {"campaign_id": campaign["id"]})
        assert not context.is_error
        assert context.structured_content is not None
        assert context.structured_content["result"]["campaign"]["state"] == "running"
        queried = await agent.call_tool("research_query_records", {"request": {"kind": "campaign"}})
        assert not queried.is_error
        assert queried.structured_content is not None
        assert queried.structured_content["result"]["items"][0]["id"] == campaign["id"]
    with ResearchClient.local(state) as client:
        assert (
            client.command("campaign.create", request, idempotency_key="agent-campaign") == created
        )
        assert cli(state, "init") == identity
        assert client.get("campaign", campaign["id"])["state"] == "running"


async def test_stdio_rechecks_selected_credential_without_operator_fallback(tmp_path: Path) -> None:
    state = tmp_path / "scoped-agent"
    runtime = LocalRuntime.open(state)
    operator = runtime.principal()
    reader = runtime.credentials.issue(
        operator,
        Principal(id=new_id(), workspace_id=operator.workspace_id, scopes=frozenset({"read"})),
    )
    token = state / "reader.token"
    write_private(token, reader.token.encode("ascii"))
    async with Client(
        stdio(state, "--token-file", str(token)), read_timeout_seconds=20, cache=None
    ) as agent:
        assert not (await agent.call_tool("research_capabilities")).is_error
        capture = await agent.call_tool(
            "research_capture_project", {"path": str(tmp_path), "idempotency_key": "not-authorized"}
        )
        assert capture.is_error
        assert capture.structured_content is not None
        assert capture.structured_content["error"]["code"] == "AUTHORIZATION_DENIED"
        runtime.credentials.revoke(operator, reader.id)
        assert (await agent.call_tool("research_capabilities")).is_error


async def test_remote_tool_catalog_has_no_local_file_capture(tmp_path: Path) -> None:
    with ResearchClient.local(tmp_path / "tool-catalog") as client:
        tools = await make_mcp(client).list_tools()
        assert "research_capture_project" not in {tool.name for tool in tools}
    result = tool_result(lambda: {"large": "x" * MAX_TOOL_BYTES})
    assert result.is_error
    assert len(canonical_bytes(result.structured_content)) < MAX_TOOL_BYTES


@pytest.mark.parametrize("content", [b'{"id":1,"id":2}', b'{"id":NaN}', b"[]"])
def test_cli_rejects_invalid_json_with_clean_error(tmp_path: Path, content: bytes) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "research_swarm.cli",
            "--state-dir",
            str(tmp_path / "invalid"),
            "command",
            "campaign.create",
            "--key",
            "bad-input",
        ],
        input=content,
        capture_output=True,
        env=process_environment(),
        timeout=20,
    )
    assert result.returncode == 2
    assert result.stdout == b""
    assert strict_json_loads(result.stderr)["error"]["code"] == "PROJECT_CONTRACT_INVALID"
