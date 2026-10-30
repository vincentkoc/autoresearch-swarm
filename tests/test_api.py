from __future__ import annotations

import socket
import threading
import time
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
import uvicorn
from pydantic import ValidationError

from research_swarm.api.http import MAX_COMMAND_BYTES, ArtifactResponse, create_app
from research_swarm.control.auth import CredentialStore
from research_swarm.control.commands import CampaignTarget, CreateCampaign
from research_swarm.control.dispatch import dispatch_command
from research_swarm.control.queries import RecordQuery, ResearchQueries
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from tests.support import LIMITS, ResearchFixture


@dataclass
class APIFixture:
    client: httpx.Client
    credentials: CredentialStore
    root: Principal
    url: str


@pytest.fixture
def api(research: ResearchFixture) -> Iterator[APIFixture]:
    credentials = CredentialStore(research.service.database)
    root = research.context().principal
    issued = credentials.issue(root, root)
    app = create_app(research.service, credentials)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    url = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(
        uvicorn.Config(app, log_level="error", access_log=False, lifespan="off")
    )
    thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while not server.started:
            if not thread.is_alive() or time.monotonic() >= deadline:
                raise AssertionError("Owned HTTP server did not start")
            time.sleep(0.01)
        with httpx.Client(
            base_url=url, headers={"Authorization": "Bearer " + issued.token}, timeout=5
        ) as client:
            yield APIFixture(client, credentials, root, url)
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        listener.close()
        assert not thread.is_alive(), "Owned HTTP server did not stop"


def post_command(
    api: APIFixture,
    operation: str,
    payload: dict[str, Any],
    *,
    revision: int | None = None,
    key: str | None = None,
) -> httpx.Response:
    headers = {"Idempotency-Key": key or new_id()}
    if revision is not None:
        headers["X-Expected-Revision"] = str(revision)
    return api.client.post("/v1/commands/" + operation, json=payload, headers=headers)


def test_real_http_lifecycle_shares_durable_command_results(
    api: APIFixture, research: ResearchFixture
) -> None:
    request = CreateCampaign(
        project_id=research.project["id"],
        name="HTTP research",
        primary_metric="val_bpb",
        limits=LIMITS,
    )
    key = new_id()
    response = post_command(api, "campaign.create", request.model_dump(mode="json"), key=key)
    assert response.status_code == 200, response.text
    assert (
        post_command(api, "campaign.create", request.model_dump(mode="json"), key=key).json()
        == response.json()
    )
    assert (
        dispatch_command(
            research.service,
            research.context(key),
            "campaign.create",
            request.model_dump(mode="json"),
        )
        == response.json()
    )
    campaign = response.json()["result"]
    for operation in ("campaign.seal", "campaign.start"):
        response = post_command(
            api, operation, {"campaign_id": campaign["id"]}, revision=campaign["revision"]
        )
        assert response.status_code == 200, response.text
        campaign = response.json()["result"]
    context = api.client.get(f"/v1/campaigns/{campaign['id']}/context").json()
    assert context["campaign"]["state"] == "running"
    assert context["budget"][0]["consumed"] == "0"
    events = api.client.get("/v1/events").json()
    assert events["cursor"] >= context["watermark"]
    later = api.client.get("/v1/events", params={"after": events["cursor"]}).json()
    assert later["events"] == []


def test_http_identity_scope_workspace_and_revocation(
    api: APIFixture, research: ResearchFixture
) -> None:
    with httpx.Client(base_url=api.url) as anonymous:
        assert anonymous.get("/healthz").status_code == 200
        denied = anonymous.get("/v1/capabilities")
        assert denied.status_code == 401
        assert denied.headers["www-authenticate"] == "Bearer"
    reader = api.credentials.issue(
        api.root,
        Principal(id=new_id(), workspace_id=api.root.workspace_id, scopes=frozenset({"read"})),
    )
    assert reader.token not in repr(reader)
    headers = {"Authorization": "Bearer " + reader.token, "Idempotency-Key": new_id()}
    assert api.client.get("/v1/capabilities", headers=headers).status_code == 200
    denied = api.client.post("/v1/commands/campaign.create", json={}, headers=headers)
    assert denied.status_code == 403
    assert denied.json()["error"]["code"] == "AUTHORIZATION_DENIED"
    other = Principal(id=new_id(), workspace_id=new_id(), scopes=frozenset({"*"}))
    research.service.database.bootstrap(other.workspace_id, new_id())
    other_token = api.credentials.issue(other, other)
    absent = api.client.get(
        "/v1/records/project/" + research.project["id"],
        headers={"Authorization": "Bearer " + other_token.token},
    )
    assert absent.status_code == 404
    api.credentials.revoke(api.root, reader.id)
    assert api.client.get("/v1/capabilities", headers=headers).status_code == 401
    with research.service.database.connect() as connection:
        stored = " ".join(
            str(tuple(row)) for row in connection.execute("SELECT * FROM credentials")
        )
        assert reader.token not in stored and other_token.token not in stored


def test_credential_issuer_cannot_escalate_scope_or_workspace(api: APIFixture) -> None:
    manager = Principal(
        id=new_id(), workspace_id=api.root.workspace_id, scopes=frozenset({"auth.manage", "read"})
    )
    with pytest.raises(ResearchError, match="exceeds issuer"):
        api.credentials.issue(manager, api.root)
    with pytest.raises(ResearchError, match="workspace differs"):
        api.credentials.issue(api.root, manager.model_copy(update={"workspace_id": new_id()}))


def test_credential_expiry_requires_canonical_timestamp(api: APIFixture) -> None:
    with pytest.raises(ValidationError):
        api.credentials.issue(api.root, api.root, expires_at="2099-1-01T00:00:00.000Z")
    issued = api.credentials.issue(api.root, api.root, expires_at="2099-01-01T00:00:00.000Z")
    with api.credentials.database.connect() as connection:
        connection.execute(
            "UPDATE credentials SET expires_at=? WHERE id=?", ("2099-1-01T00:00:00.000Z", issued.id)
        )
    with pytest.raises(ResearchError, match="Invalid credential"):
        api.credentials.resolve(issued.token)


def test_http_rejects_ambiguous_oversized_and_unversioned_inputs(
    api: APIFixture, research: ResearchFixture
) -> None:
    headers = {"Idempotency-Key": new_id(), "Content-Type": "application/json"}
    duplicate = api.client.post(
        "/v1/commands/campaign.seal",
        content='{"campaign_id":"a","campaign_id":"b"}',
        headers=headers,
    )
    assert duplicate.status_code == 422
    oversized = api.client.post(
        "/v1/commands/campaign.seal", content=b" " * (MAX_COMMAND_BYTES + 1), headers=headers
    )
    assert oversized.status_code == 422
    unknown = post_command(
        api, "campaign.seal", {"campaign_id": new_id(), "schema_version": "9.0"}, revision=1
    )
    assert unknown.status_code == 422
    campaign = research.campaign(start=False)
    stale = post_command(
        api, "campaign.seal", CampaignTarget(campaign_id=campaign["id"]).model_dump(mode="json")
    )
    assert stale.status_code == 409 and stale.json()["error"]["code"] == "REVISION_CONFLICT"
    malformed = api.client.post(
        "/v1/commands/campaign.seal",
        json={},
        headers={**headers, "X-Expected-Revision": "private-invalid-header"},
    )
    assert malformed.status_code == 422 and "private-invalid-header" not in malformed.text


def test_openapi_command_models_have_resolvable_references(api: APIFixture) -> None:
    document = api.client.get("/v1/openapi.json").json()
    assert "/v1/commands/trial.submit" in document["paths"]

    def inspect(value: Any) -> None:
        if isinstance(value, dict):
            if "$ref" in value:
                resolved = document
                for part in value["$ref"].removeprefix("#/").split("/"):
                    resolved = resolved[part]
            for item in value.values():
                inspect(item)
        elif isinstance(value, list):
            for item in value:
                inspect(item)

    inspect(document)


def test_artifact_http_transfer_and_missing_bytes_fail_before_headers(
    api: APIFixture, research: ResearchFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    response = api.client.get("/v1/artifacts/" + research.artifact.id + "/content")
    assert response.status_code == 200
    assert response.headers["content-disposition"].startswith("attachment;")
    assert len(response.content) == research.artifact.size_bytes

    def missing(*args: Any, **kwargs: Any) -> None:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Required bytes are absent")

    monkeypatch.setattr(research.service.artifacts.store, "reader", missing)
    response = api.client.get("/v1/artifacts/" + research.artifact.id + "/content")
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "ARTIFACT_UNVERIFIED"


@pytest.mark.parametrize(
    "media_type", ["application/🦊", "text/plain\r\nInjected: yes", "text/plain; charset=unbounded"]
)
def test_upload_rejects_invalid_header_metadata(api: APIFixture, media_type: str) -> None:
    from research_swarm.domain.canonical import content_digest

    response = api.client.post(
        "/v1/artifact-uploads",
        json={"digest": content_digest(b"abc"), "size_bytes": 3, "media_type": media_type},
        headers={"Idempotency-Key": new_id()},
    )
    assert response.status_code == 422


def test_upload_parts_use_explicit_range_idempotency(api: APIFixture) -> None:
    from research_swarm.domain.canonical import content_digest

    upload = api.client.post(
        "/v1/artifact-uploads",
        json={"digest": content_digest(b"abcdef"), "size_bytes": 6},
        headers={"Idempotency-Key": new_id()},
    ).json()["result"]
    path = "/v1/artifact-uploads/" + upload["id"] + "/parts"
    assert api.client.put(path, params={"offset": 0}, content=b"abc").json() == {"offset": 3}
    assert api.client.put(path, params={"offset": 0}, content=b"abc").json() == {"offset": 3}
    assert api.client.put(path, params={"offset": 0}, content=b"xyz").status_code == 409
    assert api.client.put(path, params={"offset": 3}, content=b"def").json() == {"offset": 6}
    result = api.client.post(
        "/v1/artifact-uploads/" + upload["id"] + "/finalize", headers={"Idempotency-Key": new_id()}
    )
    assert result.status_code == 200
    artifact_id = result.json()["result"]["id"]
    assert api.client.get("/v1/artifacts/" + artifact_id + "/content").content == b"abcdef"


def test_query_membership_excludes_later_insertions_and_binds_scope(
    research: ResearchFixture,
) -> None:
    campaigns = [research.campaign(start=False) for _ in range(3)]
    queries = ResearchQueries(research.service.database)
    first = queries.list(research.context(), RecordQuery(kind="campaign", limit=1))
    research.campaign(start=False)
    second = queries.list(
        research.context(), RecordQuery(kind="campaign", limit=100, cursor=first["next_cursor"])
    )
    assert [row["id"] for row in first["items"] + second["items"]] == [
        campaign["id"] for campaign in campaigns
    ]
    assert second["next_cursor"] is None
    with pytest.raises(ResearchError, match="cursor"):
        queries.list(research.context(), RecordQuery(kind="project", cursor=first["next_cursor"]))
    with pytest.raises(ResearchError, match="cursor"):
        queries.list(research.context(), RecordQuery(kind="campaign", cursor="not-a-cursor"))


async def test_failed_header_send_closes_reader_before_body_iteration() -> None:
    closed: list[bool] = []

    async def body() -> AsyncIterator[bytes]:
        raise AssertionError("The failed send must occur before body iteration")
        yield b"unreachable"

    async def send(message: Any) -> None:
        raise OSError("Disconnected before headers")

    async def receive() -> Any:
        return {"type": "http.disconnect"}

    response = ArtifactResponse(
        body(), close=lambda: closed.append(True), media_type="application/octet-stream", headers={}
    )
    from starlette.requests import ClientDisconnect

    with pytest.raises(ClientDisconnect):
        await response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
    assert closed == [True]
