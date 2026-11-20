from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from mcp import Client, StdioServerParameters

import research_swarm
from research_swarm.api.campaign import campaign_app
from research_swarm.swarm.reader import CampaignReader
from research_swarm.swarm.report import CampaignReader as ReportReader
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec, trial


def test_missing_campaign_fails_consistently_without_selecting_a_neighbor(tmp_path: Path) -> None:
    path = tmp_path / "campaign.db"
    queue = SQLiteStore(path)
    queue.create_campaign(spec())
    queue.submit(trial(1))
    legacy, current = CampaignReader(path), ReportReader(path)
    for read in (legacy.snapshot, current.budget):
        with pytest.raises(ValueError, match="^Unknown campaign absent$"):
            read("absent")
    assert legacy.budget("test") == current.budget("test")
    assert current.budget("test")["trials"] == 1


def test_budget_and_snapshot_on_archive_without_selection_column(tmp_path: Path) -> None:
    path = tmp_path / "archive.db"
    SQLiteStore(path).create_campaign(spec())
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("ALTER TABLE swarm_campaigns DROP COLUMN selected_trial_id")
    original = path.read_bytes()
    reader = CampaignReader(path)
    assert reader.budget("test")["trials"] == 0
    with pytest.raises(
        ValueError, match="Cannot read the campaign database: no such column: selected_trial_id"
    ):
        reader.snapshot("test")
    assert path.read_bytes() == original
    assert set(tmp_path.iterdir()) == {path}


async def test_sdk_http_and_stdio_read_same_database(tmp_path: Path) -> None:
    path = tmp_path / "campaign.db"
    queue = SQLiteStore(path)
    queue.create_campaign(spec())
    queue.submit(trial(1))
    queue.create_campaign(spec(campaign_id="second"))
    reader = CampaignReader(path)
    with TestClient(campaign_app(reader, "synthetic-reader")) as http:
        for route in (
            "/v1/campaigns",
            "/v1/events",
            "/v1/campaigns/test/view",
            "/v1/campaigns/test/budget",
        ):
            assert http.get(route).status_code == 401
            assert http.get(route, headers={"Authorization": "Bearer wrong"}).status_code == 401
        assert http.get("/").status_code == 200
        http.headers["Authorization"] = "Bearer synthetic-reader"
        page = http.get("/v1/campaigns", params={"limit": 1}).json()
        assert page == reader.campaigns(limit=1)
        assert (
            http.get("/v1/campaigns", params={"cursor": page["next_cursor"]}).json()["items"][0][
                "id"
            ]
            == "test"
        )
        assert http.get("/v1/campaigns/test/budget").json() == reader.budget("test")
        assert http.get("/v1/campaigns/test/view").status_code == 200
        assert http.get("/v1/campaigns/missing/snapshot").status_code == 404
        assert http.get("/v1/events", params={"campaign_id": "test"}).json() == reader.events(
            campaign_id="test"
        )
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "research_swarm", "mcp", "--db", str(path)],
        env={"PYTHONPATH": str(Path(research_swarm.__file__).resolve().parent.parent)},
    )
    async with Client(params, read_timeout_seconds=10, cache=None) as agent:
        tools = (await agent.list_tools()).tools
        assert {tool.name for tool in tools} == {
            "research_campaigns",
            "research_campaign_snapshot",
            "research_budget",
            "research_events",
        }
        assert all(tool.annotations and tool.annotations.read_only_hint for tool in tools)
        result = await agent.call_tool("research_budget", {"campaign_id": "test"})
        assert not result.is_error
        assert json.loads(result.content[0].text) == reader.budget("test")
