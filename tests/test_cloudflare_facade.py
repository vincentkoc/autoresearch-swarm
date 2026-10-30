"""Agent clients use the same native authority through actual local workerd."""

import asyncio
import hashlib
import sys
from threading import Event

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse
from mcp import Client, StdioServerParameters

from research_swarm.api.http import create_app
from research_swarm.client import ResearchClient
from research_swarm.control.commands import PauseCampaign
from research_swarm.control.reports import ReportCampaign
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import write_private
from tests.test_cli_mcp import process_environment
from tests.test_cloudflare_native import build_current_sources, server
from tests.test_cloudflare_native import pytestmark as pytestmark
from tests.test_http_delivery import serving
from tests.test_local_pipeline import pipeline as pipeline


def test_agent_sdk_mcp_artifacts_and_events_share_native_authority(pipeline, tmp_path):
    build_current_sources()
    runtime = pipeline.runtime
    owner = runtime.principal()
    issued = runtime.credentials.issue(
        owner, Principal(id=new_id(), workspace_id=owner.workspace_id, scopes=frozenset({"*"}))
    )
    reader = runtime.credentials.issue(
        owner, Principal(id=new_id(), workspace_id=owner.workspace_id, scopes=frozenset({"read"}))
    )
    app = create_app(runtime.service, runtime.credentials)
    seen, lost, corrupt = [], [], []

    @app.middleware("http")
    async def intercept(request: Request, call_next):
        seen.append((request.url.path, request.headers.get("authorization")))
        response = await call_next(request)
        if request.headers.get("idempotency-key") == "facade-report" and not lost:
            lost.append(True)
            return Response("synthetic lost acknowledgment", status_code=503)
        if corrupt and request.url.path.endswith("/content"):
            return Response(b"bad", media_type="application/octet-stream")
        return response

    with serving(app) as native:
        variables = {"NATIVE_API_ORIGIN": native, "ALLOW_LOCAL_API": "true"}
        with (
            server(
                tmp_path, sequence=1, variables=variables, config="wrangler.facade.jsonc"
            ) as url,
            ResearchClient.connect(url, issued.token) as client,
            ResearchClient.connect(url, reader.token) as readonly,
            httpx.Client(
                base_url=url,
                headers={"Authorization": "Bearer " + issued.token},
                trust_env=False,
                timeout=20,
            ) as raw,
        ):
            assert client.capabilities() == pipeline.client.capabilities()
            assert (
                raw.get(
                    "/v1/capabilities", headers={"Authorization": "bearer " + issued.token}
                ).status_code
                == 200
            )
            request = ReportCampaign(campaign_id=pipeline.campaign["id"])
            with pytest.raises(ResearchError) as uncertain:
                client.command("report.create", request, idempotency_key="facade-report")
            assert uncertain.value.code == ErrorCode.DELIVERY_UNCERTAIN
            accepted = client.command("report.create", request, idempotency_key="facade-report")
            assert (
                client.command("report.create", request, idempotency_key="facade-report")
                == accepted
            )
            with pytest.raises(ResearchError) as denied:
                readonly.command("report.create", request, idempotency_key="read-cannot-report")
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED

            token_path = tmp_path / "agent-token"
            write_private(token_path, (issued.token + "\n").encode())

            async def agent():
                parameters = StdioServerParameters(
                    command=sys.executable,
                    args=[
                        "-m",
                        "research_swarm.cli",
                        "--controller",
                        url,
                        "--token-file",
                        str(token_path),
                        "mcp",
                    ],
                    env=process_environment(),
                )
                async with Client(parameters, read_timeout_seconds=20, cache=None) as mcp:
                    result = await mcp.call_tool(
                        "research_report_create",
                        {
                            "request": request.model_dump(mode="json"),
                            "idempotency_key": "facade-report",
                        },
                    )
                    assert not result.is_error
                    assert result.structured_content["command_id"] == accepted["command_id"]

            asyncio.run(agent())
            source = tmp_path / "binary-source"
            source.write_bytes(bytes(range(256)) * 37000)
            artifact = client.upload_file(source, idempotency_key="facade-binary")
            destination = tmp_path / "binary-copy"
            assert client.download(artifact.id, destination) == artifact
            with raw.stream("GET", f"/v1/artifacts/{artifact.id}/content") as content:
                assert content.headers["content-length"] == str(artifact.size_bytes)
                assert content.headers["etag"] == '"' + artifact.digest + '"'
                assert len(content.read()) == artifact.size_bytes
            assert hashlib.sha256(destination.read_bytes()).hexdigest() == artifact.digest[7:]
            corrupt.append(True)
            with pytest.raises(ResearchError):
                client.download(artifact.id, tmp_path / "corrupt-copy")
            assert not (tmp_path / "corrupt-copy").exists()
            corrupt.clear()

            current = client.get("campaign", pipeline.campaign["id"])
            pause = PauseCampaign(campaign_id=current["id"], mode="drain")
            with pytest.raises(ResearchError) as stale:
                client.command(
                    "campaign.pause",
                    pause,
                    idempotency_key="stale-pause",
                    expected_revision=current["revision"] + 1,
                )
            assert stale.value.code == ErrorCode.REVISION_CONFLICT
            client.command(
                "campaign.pause",
                pause,
                idempotency_key="valid-pause",
                expected_revision=current["revision"],
            )

            count = len(seen)
            for path in (
                "/adapter/start",
                "/v1/delivery/execute",
                "/v1/commands/not.registered",
                "/v1/credentials",
            ):
                assert raw.post(path, json={}).status_code == 404
            assert raw.post("/v1/query", content=b"x" * (1024 * 1024 + 1)).status_code == 413
            assert len(seen) == count
            with raw.stream(
                "GET",
                "/v1/events/stream?after=0",
                headers={"Authorization": "Bearer " + reader.token},
            ) as stream:
                lines = stream.iter_lines()
                assert next(lines).startswith("id: ")
                runtime.credentials.revoke(owner, reader.id)
                assert list(lines)[-1:] in ([], [""])
            with pytest.raises(ResearchError) as revoked:
                readonly.capabilities()
            assert revoked.value.code == ErrorCode.AUTHORIZATION_DENIED
            identities = {identity for _, identity in seen}
            assert "Bearer " + issued.token in identities and "Bearer " + reader.token in identities


def test_facade_refuses_native_redirect_without_forwarding_caller_identity(tmp_path):
    target, native, seen = FastAPI(), FastAPI(), []

    @target.get("/target")
    async def receive():
        seen.append(True)
        return {}

    with serving(target) as target_url:

        @native.get("/v1/capabilities")
        @native.post("/v1/query")
        async def redirect():
            return RedirectResponse(target_url + "/target", status_code=307)

        with (
            serving(native) as native_url,
            server(
                tmp_path,
                sequence=2,
                variables={"NATIVE_API_ORIGIN": native_url, "ALLOW_LOCAL_API": "true"},
                config="wrangler.facade.jsonc",
            ) as url,
            httpx.Client(trust_env=False) as client,
        ):
            response = client.get(
                url + "/v1/capabilities", headers={"Authorization": "Bearer synthetic-user-token"}
            )
            assert response.status_code == 502
            assert response.json()["error"]["code"] == "TRANSPORT_UNAVAILABLE"
            assert "location" not in response.headers
            query = client.post(
                url + "/v1/query", json={}, headers={"Authorization": "Bearer synthetic-user-token"}
            )
            assert query.json()["error"]["code"] == "TRANSPORT_UNAVAILABLE"
            assert query.json()["error"]["details"]["recovery"] == "retry_query"
    assert seen == []


def test_facade_stalled_upstream_has_bounded_custody_after_downstream_close(tmp_path):
    native, closed = FastAPI(), Event()
    identifier = new_id()

    @native.get("/v1/artifacts/{identifier}/content")
    async def content():
        async def blocks():
            try:
                yield b"abc"
                await asyncio.sleep(30)
            finally:
                closed.set()

        return StreamingResponse(blocks(), headers={"Content-Length": "100"})

    with (
        serving(native) as native_url,
        server(
            tmp_path,
            sequence=3,
            variables={
                "NATIVE_API_ORIGIN": native_url,
                "ALLOW_LOCAL_API": "true",
                "STREAM_IDLE_TIMEOUT_SECONDS": "2",
            },
            config="wrangler.facade.jsonc",
        ) as url,
        httpx.Client(trust_env=False, timeout=10) as client,
    ):
        with client.stream(
            "GET",
            url + f"/v1/artifacts/{identifier}/content",
            headers={"Authorization": "Bearer synthetic-user-token"},
        ) as response:
            assert response.status_code == 200, response.read().decode(errors="replace")[:500]
            assert response.headers.get("content-length") == "100", dict(response.headers)
            assert next(response.iter_bytes()) == b"abc"
        assert closed.wait(5), "Native stalled stream outlived the configured read deadline"
