"""Campaign SDK reads share the local queue and require an authenticated remote route."""

from pathlib import Path

import httpx
import pytest

from research_swarm import AsyncResearchClient, ResearchClient
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec


async def test_sdk_reopens_local_state_and_reads_the_same_event_page(tmp_path: Path) -> None:
    path = tmp_path / "swarm.sqlite"
    SQLiteStore(path).create_campaign(spec())
    with ResearchClient.local(tmp_path) as client:
        events = client.events(limit=1, campaign_id="test")
        budget = client.budget("test")
        campaigns = client.campaigns()
    async with AsyncResearchClient.local(tmp_path) as client_async:
        assert await client_async.events(limit=1, campaign_id="test") == events
        assert await client_async.budget("test") == budget
        assert await client_async.campaigns() == campaigns
    with pytest.raises(ValueError, match="filesystem"):
        ResearchClient.local(tmp_path, token_path=tmp_path / "missing")


def test_remote_reader_passes_credentials_only_to_its_configured_endpoint() -> None:
    with pytest.raises(ValueError, match="HTTPS"):
        ResearchClient.connect("http://remote.example.test", "synthetic")

    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"items": [], "next_cursor": None})

    with (
        httpx.Client(
            base_url="https://reader.example.test",
            transport=httpx.MockTransport(respond),
            headers={"Authorization": "Bearer synthetic"},
        ) as http,
        ResearchClient(http) as client,
    ):
        assert client.events(after=8, limit=1, campaign_id="test")["items"] == []
    (request,) = requests
    assert request.method == "GET" and request.url.path == "/v1/events"
    assert dict(request.url.params) == {"after": "8", "limit": "1", "campaign_id": "test"}
    assert request.headers["Authorization"] == "Bearer synthetic"
