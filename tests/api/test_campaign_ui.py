"""Campaign browser uses the existing authority, pagination and read-only evidence."""

import httpx

from research_swarm.control.queries import RecordQuery, ResearchQueries
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from tests.support import APIFixture, ResearchFixture


def test_campaign_browser_auth_pagination_and_read_only_capture(
    api: APIFixture, research: ResearchFixture
) -> None:
    first = research.campaign()
    second = research.campaign()
    queries = ResearchQueries(research.service.database)
    before = queries.context(research.context(), first["id"])["watermark"]
    with httpx.Client(base_url=api.url) as anonymous:
        shell = anonymous.get("/")
        assert shell.status_code == 200
        assert "Read credential" in shell.text
        assert first["id"] not in shell.text
        for path in (
            "/v1/campaigns",
            f"/v1/campaigns/{first['id']}/snapshot",
            f"/v1/campaigns/{first['id']}/view",
        ):
            assert anonymous.get(path).status_code == 401

    one = api.client.get("/v1/campaigns", params={"limit": 1}).json()
    assert [row["id"] for row in one["items"]] == [first["id"]]
    two = api.client.get("/v1/campaigns", params={"limit": 1, "cursor": one["next_cursor"]}).json()
    assert [row["id"] for row in two["items"]] == [second["id"]]
    assert two["next_cursor"] is None

    for _ in range(2):
        snapshot = api.client.get(f"/v1/campaigns/{first['id']}/snapshot")
        assert snapshot.status_code == 200, snapshot.text
        assert snapshot.json()["campaign_id"] == first["id"]
        view = api.client.get(f"/v1/campaigns/{first['id']}/view")
        assert view.status_code == 200, view.text
        assert "data-panel=lineage" in view.text
        assert "data-tab=score" in view.text and "data-tab=lineage" in view.text
        assert "<select data-run-selector" not in view.text
        assert view.headers["cache-control"] == "no-store"
    assert queries.context(research.context(), first["id"])["watermark"] == before
    assert queries.list(research.context(), RecordQuery(kind="report"))["items"] == []


def test_campaign_browser_workspace_isolation(api: APIFixture, research: ResearchFixture) -> None:
    campaign = research.campaign()
    other = Principal(id=new_id(), workspace_id=new_id(), scopes=frozenset({"*"}))
    research.service.database.bootstrap(other.workspace_id, new_id())
    token = api.credentials.issue(other, other).token
    headers = {"Authorization": "Bearer " + token}
    assert api.client.get("/v1/campaigns", headers=headers).json()["items"] == []
    for suffix in ("view", "snapshot"):
        response = api.client.get(f"/v1/campaigns/{campaign['id']}/{suffix}", headers=headers)
        assert response.status_code == 404


def test_campaign_browser_handles_empty_store(api: APIFixture) -> None:
    assert api.client.get("/v1/campaigns").json()["items"] == []
    assert api.client.get("/v1/campaigns", params={"limit": 0}).status_code == 422
