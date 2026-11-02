from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from mcp import Client, StdioServerParameters

import research_swarm
from research_swarm.cli import main
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.reactive.client import DecisionClient
from research_swarm.reactive.journal import DecisionJournal
from tests.reactive.support import receipt_for, request_for, response_for


def source_environment() -> dict[str, str]:
    names = ("PATH", "SYSTEMROOT", "SystemRoot", "WINDIR", "TEMP", "TMP", "HOME", "USERPROFILE")
    environment = {name: os.environ[name] for name in names if name in os.environ}
    # Qualify this source tree even when the interpreter belongs to another environment.
    environment["PYTHONPATH"] = str(Path(research_swarm.__file__).resolve().parent.parent)
    return environment


def command(path: Path, *arguments: str) -> list[str]:
    return ["-m", "research_swarm.cli", "reactive", "--session", str(path), *arguments]


def cli(path: Path, *arguments: str) -> Any:
    result = subprocess.run(
        [sys.executable, *command(path, *arguments)],
        env=source_environment(),
        capture_output=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
    return strict_json_loads(result.stdout)


async def test_python_cli_and_stdio_mcp_share_one_restricted_session(
    journal: DecisionJournal, tmp_path: Path
) -> None:
    client = DecisionClient(journal.path)
    with journal.trainer() as trainer:
        request = request_for(journal)
        trainer.request(request)
        assert cli(journal.path, "inspect")["id"] == client.inspect().id
        assert cli(journal.path, "pending")["request"]["id"] == request.id
        proposal = response_for(request)
        payload = tmp_path / "proposal.json"
        payload.write_text(proposal.model_dump_json())
        accepted = cli(journal.path, "respond", "--file", str(payload))
        assert accepted["status"] == "responded"
        parameters = StdioServerParameters(
            command=sys.executable, args=command(journal.path, "mcp"), env=source_environment()
        )
        async with Client(parameters, read_timeout_seconds=10, cache=None) as agent:
            tools = {tool.name: tool for tool in (await agent.list_tools()).tools}
            assert set(tools) == {
                "reactive_inspect",
                "reactive_pending",
                "reactive_respond",
                "reactive_export",
            }
            assert tools["reactive_respond"].output_schema is not None
            repeated = await agent.call_tool(
                "reactive_respond", {"request": {"proposal": proposal.model_dump(mode="json")}}
            )
            assert not repeated.is_error and repeated.structured_content is not None
            assert repeated.structured_content["result"] == accepted
            page = await agent.call_tool("reactive_export", {"request": {"limit": 1}})
            assert not page.is_error and page.structured_content is not None
            assert len(page.structured_content["result"]["events"]) == 1
            invalid = await agent.call_tool(
                "reactive_respond",
                {
                    "request": {
                        "proposal": proposal.model_dump(mode="json"),
                        "application_receipt": {},
                    }
                },
            )
            assert invalid.is_error
        assert client.respond(proposal).model_dump(mode="json") == accepted
        assert sum(event.kind == "responded" for event in journal.events()) == 1
        receipt = receipt_for(trainer.claim_application(request.id, step=12))
        assert client.pending().status == "applying"  # type: ignore[union-attr]
        assert client.inspect().status == "active"
        trainer.record_application(receipt)
    assert client.inspect().status == "closed"


def test_export_bounds_bytes_count_and_preserves_cursor(journal: DecisionJournal) -> None:
    with journal.trainer() as trainer:
        for step in range(5, 30, 5):
            request = request_for(journal, step)
            trainer.request(request)
            trainer.settle(request.id, "expired")
    client = DecisionClient(journal.path)
    cursor, sequences = -1, []
    while True:
        page = client.export(after=cursor, limit=2, max_bytes=4096)
        assert len(page.events) <= 2
        assert len(canonical_bytes(page.model_dump(mode="json"))) <= 4096
        sequences.extend(event.sequence for event in page.events)
        cursor = page.next_after
        if not page.has_more:
            break
    assert sequences == [event.sequence for event in journal.events()]
    assert not client.export(after=cursor).events
    with pytest.raises(ResearchError) as failure:
        client.export(max_bytes=1024)
    assert failure.value.code == ErrorCode.CAPABILITY_UNAVAILABLE


def test_shared_dispatch_rejects_path_changes_and_application_authority(
    journal: DecisionJournal,
) -> None:
    client = DecisionClient(journal.path)
    before = journal.inspect()
    for operation, arguments in (
        ("inspect", {"path": "another.sqlite"}),
        ("record_application", {}),
        ("close", {}),
        ("export", {"limit": 101}),
    ):
        with pytest.raises(ResearchError):
            client.dispatch(operation, arguments)
    assert journal.inspect() == before
    assert not hasattr(client, "trainer")


def test_cli_does_not_mix_local_session_and_controller_credentials(
    journal: DecisionJournal, capsys: pytest.CaptureFixture[str]
) -> None:
    assert (
        main(
            [
                "--controller",
                "https://controller.example.test",
                "reactive",
                "--session",
                str(journal.path),
                "inspect",
            ]
        )
        == 2
    )
    output = capsys.readouterr()
    assert not output.out
    assert strict_json_loads(output.err)["error"]["code"] == "PROJECT_CONTRACT_INVALID"
