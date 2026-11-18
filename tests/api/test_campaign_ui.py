"""Campaign browser reads are restricted to one database and preserve its event history."""

from pathlib import Path

from fastapi.testclient import TestClient

from research_swarm.api.campaign import campaign_app
from research_swarm.swarm.reader import CampaignReader
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec


def test_browser_keeps_its_database_private_and_renders_read_only(tmp_path: Path) -> None:
    path = tmp_path / "first.sqlite"
    other = tmp_path / "second.sqlite"
    SQLiteStore(path).create_campaign(spec(campaign_id="private/campaign"))
    SQLiteStore(other).create_campaign(spec(campaign_id="other-campaign"))
    reader = CampaignReader(path)
    before = reader.events()
    with TestClient(campaign_app(reader, "reader-one")) as http:
        shell = http.get("/")
        assert "Read credential" in shell.text and "private/campaign" not in shell.text
        assert http.get("/v1/campaigns").status_code == 401
        http.headers["Authorization"] = "Bearer reader-one"
        page = http.get("/v1/campaigns").json()
        assert [item["id"] for item in page["items"]] == ["private/campaign"]
        assert http.get("/v1/campaigns/other-campaign/snapshot").status_code == 404
        for _ in range(2):
            view = http.get("/v1/campaigns/private%2Fcampaign/view")
            assert view.status_code == 200 and view.headers["cache-control"] == "no-store"
            assert "data-panel=lineage" in view.text and "data-tab=score" in view.text
            assert "<select data-run-selector" not in view.text
    assert reader.events() == before
