from __future__ import annotations

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from research_swarm import AsyncResearchClient, ResearchClient
from research_swarm.client import LocalTransport
from research_swarm.control.commands import CampaignTarget, CreateCampaign
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime, read_private, write_private
from tests.support import LIMITS, APIFixture, ResearchFixture


def test_local_runtime_initializes_one_identity_under_contention(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    with ThreadPoolExecutor(max_workers=4) as pool:
        runtimes = list(pool.map(lambda _: LocalRuntime.open(root), range(4)))
    assert len({runtime.identity.workspace_id for runtime in runtimes}) == 1
    assert len({runtime.principal().id for runtime in runtimes}) == 1
    with runtimes[0].database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM credentials").fetchone()[0] == 1
    if sys.platform != "win32":
        assert root.stat().st_mode & 0o077 == 0
        assert (root / "operator.token").stat().st_mode & 0o077 == 0
    assert len(read_private(root, "dispatch.key", limit=32)) == 32


def test_local_sdk_captures_registers_replays_and_reopens(
    research: ResearchFixture, tmp_path: Path
) -> None:
    state = tmp_path / "client-state"
    with ResearchClient.local(state) as client:
        registered = client.register_project(research.source, idempotency_key="register-baseline")
        assert (
            client.register_project(research.source, idempotency_key="register-baseline")
            == registered
        )
        project = registered["result"]["project"]
        request = CreateCampaign(
            project_id=project["id"], name="SDK campaign", primary_metric="val_bpb", limits=LIMITS
        )
        campaign = client.command("campaign.create", request, idempotency_key="campaign-1")[
            "result"
        ]
        for operation in ("campaign.seal", "campaign.start"):
            campaign = client.command(
                operation,
                CampaignTarget(campaign_id=campaign["id"]),
                idempotency_key=operation,
                expected_revision=campaign["revision"],
            )["result"]
        assert client.context(campaign["id"])["campaign"]["state"] == "running"
        source = registered["result"]["project_revision"]["data"]["source_artifact"]
        metadata = client.artifact(source["id"])
        assert metadata.state == "verified"
        assert metadata.reference.model_dump(mode="json") == source
        destination = tmp_path / "download.zip"
        assert client.download(source["id"], destination).digest == source["digest"]
        with pytest.raises(FileExistsError):
            client.download(source["id"], destination)
        assert destination.stat().st_size == source["size_bytes"]
    with ResearchClient.local(state) as restarted:
        assert (
            restarted.command("campaign.create", request, idempotency_key="campaign-1")["result"][
                "id"
            ]
            == campaign["id"]
        )
        assert len(restarted.list(RecordQuery(kind="trial"))["items"]) == 0


def test_http_sdk_matches_control_and_replays_complete_upload(
    api: APIFixture, research: ResearchFixture, tmp_path: Path
) -> None:
    issued = api.credentials.issue(api.root, api.root)
    with ResearchClient.connect(api.url, issued.token) as client:
        registered = client.register_project(research.source, idempotency_key="http-sdk-project")
        assert (
            client.register_project(research.source, idempotency_key="http-sdk-project")
            == registered
        )
        project = registered["result"]["project"]
        assert client.get("project", project["id"]) == project
        assert client.capabilities()["schema_version"] == "1.0"
        source = registered["result"]["project_revision"]["data"]["source_artifact"]
        metadata = client.artifact(source["id"])
        assert metadata.state == "verified"
        assert metadata.reference.model_dump(mode="json") == source
        target = tmp_path / "http-source.zip"
        assert client.download(source["id"], target).digest == source["digest"]
        events = client.events()
        # Reader release can commit just after the last response byte reaches the client.
        later = client.events(after=events["cursor"])
        assert all(event["sequence"] > events["cursor"] for event in later["events"])
        assert not (
            {event["id"] for event in events["events"]} & {event["id"] for event in later["events"]}
        )
        api.credentials.revoke(api.root, issued.id)
        with pytest.raises(ResearchError) as failure:
            client.capabilities()
        assert failure.value.code == ErrorCode.AUTHORIZATION_DENIED
        with pytest.raises(ResearchError) as revoked_metadata:
            client.artifact(source["id"])
        assert revoked_metadata.value.code == ErrorCode.AUTHORIZATION_DENIED


def test_local_scoped_credential_has_no_operator_fallback(tmp_path: Path) -> None:
    root = tmp_path / "scoped-state"
    runtime = LocalRuntime.open(root)
    reader = runtime.credentials.issue(
        runtime.principal(),
        Principal(
            id=new_id(), workspace_id=runtime.identity.workspace_id, scopes=frozenset({"read"})
        ),
    )
    token_path = root / "reader.token"
    write_private(token_path, reader.token.encode("ascii"))
    with ResearchClient.local(root, token_path=token_path) as client:
        assert client.capabilities()["schema_version"] == "1.0"
        with pytest.raises(ResearchError) as failure:
            client.command("campaign.create", {}, idempotency_key="denied")
        assert failure.value.code == ErrorCode.AUTHORIZATION_DENIED
        runtime.credentials.revoke(runtime.principal(), reader.id)
        with pytest.raises(ResearchError):
            client.capabilities()
    with ResearchClient.local(root) as operator:
        assert operator.capabilities()["schema_version"] == "1.0"


async def test_async_client_uses_same_command_acknowledgment(
    research: ResearchFixture, tmp_path: Path
) -> None:
    client = ResearchClient.local(tmp_path / "async-state")
    try:
        registered = client.register_project(research.source, idempotency_key="async-project")[
            "result"
        ]
        project = registered["project"]
        request = CreateCampaign(
            project_id=project["id"], name="Async campaign", primary_metric="val_bpb", limits=LIMITS
        )
        asynchronous = AsyncResearchClient(client)
        source = registered["project_revision"]["data"]["source_artifact"]
        metadata = await asynchronous.artifact(source["id"])
        assert metadata == client.artifact(source["id"])
        result = await asynchronous.command(
            "campaign.create", request, idempotency_key="async-campaign"
        )
        assert (
            client.command("campaign.create", request, idempotency_key="async-campaign") == result
        )
        assert (await asynchronous.get("campaign", result["result"]["id"]))["state"] == "draft"
    finally:
        client.close()


def test_sdk_recovers_published_bytes_before_catalog_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = LocalRuntime.open(tmp_path / "interrupted-upload")
    source = tmp_path / "source.bin"
    source.write_bytes(b"recover this exact publication")
    client = ResearchClient(LocalTransport(runtime))
    original = runtime.database.execute

    def interrupted(context: Any, operation: str, *args: Any, **kwargs: Any) -> Any:
        if operation == "artifact.finalize_upload":
            raise RuntimeError("Crash after physical publication")
        return original(context, operation, *args, **kwargs)

    monkeypatch.setattr(runtime.database, "execute", interrupted)
    with pytest.raises(RuntimeError, match="physical publication"):
        client.upload_file(source, idempotency_key="interrupted-publication")
    monkeypatch.setattr(runtime.database, "execute", original)
    reference = client.upload_file(source, idempotency_key="interrupted-publication")
    destination = tmp_path / "recovered.bin"
    assert client.download(reference.id, destination) == reference
    assert destination.read_bytes() == source.read_bytes()


def test_empty_command_key_is_rejected_locally(tmp_path: Path) -> None:
    with ResearchClient.local(tmp_path / "empty-key") as client, pytest.raises(ValidationError):
        client.command("campaign.create", {}, idempotency_key="")


@pytest.mark.parametrize(
    "missing",
    ["identity.json", "dispatch.key", "assignment.key", "operator.token", "research.sqlite"],
)
def test_damaged_runtime_is_not_replaced(tmp_path: Path, missing: str) -> None:
    root = tmp_path / "damaged-runtime"
    runtime = LocalRuntime.open(root)
    (root / missing).unlink()
    remaining = {
        path.name: path.read_bytes()
        for path in root.iterdir()
        if path.is_file() and path.name != "initialize.lock"
    }
    with pytest.raises(ResearchError, match="recovery is required"):
        LocalRuntime.open(root)
    assert not (root / missing).exists()
    assert remaining == {
        path.name: path.read_bytes()
        for path in root.iterdir()
        if path.is_file() and path.name != "initialize.lock"
    }
    assert runtime.identity.initialized_at is not None


def test_pinned_git_capture_ignores_dirty_manifest(
    research: ResearchFixture, tmp_path: Path
) -> None:
    environment = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
    for arguments in (
        ["init", "--quiet"],
        ["add", "."],
        [
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ],
    ):
        subprocess.run(
            ["git", *arguments],
            cwd=research.source,
            env=environment,
            check=True,
            capture_output=True,
        )
    manifest = research.source / "research.toml"
    manifest.write_text("invalid = [toml", encoding="utf-8")
    with ResearchClient.local(tmp_path / "pinned-client") as client:
        response = client.register_project(
            research.source, revision="HEAD", idempotency_key="pinned-register"
        )
    assert response["result"]["project_revision"]["data"]["resolved"]["source"]["base_revision"]
    assert manifest.read_text(encoding="utf-8") == "invalid = [toml"
