from pathlib import Path

import pytest

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import ReleaseArtifact, RetainArtifact
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from tests.orchestration.test_http_delivery import serving


def test_public_manual_retention_replays_and_cannot_release_required_evidence(tmp_path: Path):
    runtime = LocalRuntime.open(tmp_path / "runtime")
    local = ResearchClient(LocalTransport(runtime))
    source = tmp_path / "report.json"
    source.write_bytes(b'{"fixture":"completed benchmark"}')
    artifact = local.upload_file(source, idempotency_key="report")
    request = RetainArtifact(artifact_id=artifact.id, owner_id=new_id(), purpose="benchmark")
    token = runtime.credentials.issue(runtime.principal(), runtime.principal()).token
    reader = Principal(
        id=new_id(), workspace_id=runtime.identity.workspace_id, scopes=frozenset({"read"})
    )
    reader_token = runtime.credentials.issue(runtime.principal(), reader).token
    with serving(create_app(runtime.service, runtime.credentials)) as url:
        with ResearchClient.connect(url, reader_token) as restricted:
            with pytest.raises(ResearchError) as denied:
                restricted.command("artifact.retain", request, idempotency_key="denied")
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        with ResearchClient.connect(url, token) as remote:
            result = remote.command("artifact.retain", request, idempotency_key="retain")
            assert local.command("artifact.retain", request, idempotency_key="retain") == result
            retention = result["result"]
            assert retention["data"]["retention"]["required"] is False
            assert retention["data"]["retention"]["owner_kind"] == "manual"
            with pytest.raises(ResearchError, match="retained references"):
                runtime.service.artifacts.delete(
                    CommandContext(
                        principal=runtime.principal(),
                        idempotency_key="delete-held",
                        expected_revision=1,
                    ),
                    artifact.id,
                    "fixture",
                )
            released = remote.command(
                "artifact.release",
                ReleaseArtifact(reference_id=retention["id"]),
                idempotency_key="release",
            )
            assert released["result"]["state"] == "released"
            required = runtime.service.artifacts.retain(
                CommandContext(principal=runtime.principal(), idempotency_key="required"),
                ArtifactRetention(
                    artifact_id=artifact.id,
                    owner_kind="trial",
                    owner_id=new_id(),
                    purpose="required evidence",
                ),
            )["result"]
            with pytest.raises(ResearchError, match="retention lifecycle"):
                remote.command(
                    "artifact.release",
                    ReleaseArtifact(reference_id=required["id"]),
                    idempotency_key="cannot-release-required",
                )
            downloaded = tmp_path / "download.json"
            remote.download(artifact.id, downloaded)
            assert downloaded.read_bytes() == source.read_bytes()


def test_equal_manual_holds_have_independent_actor_custody_and_direct_service_requires_read(
    tmp_path: Path,
):
    runtime = LocalRuntime.open(tmp_path / "runtime")
    root = ResearchClient(LocalTransport(runtime))
    path = tmp_path / "report.json"
    path.write_bytes(b"{}")
    artifact = root.upload_file(path, idempotency_key="fixture")
    request = RetainArtifact(artifact_id=artifact.id, owner_id=new_id(), purpose="shared analysis")
    first, second = (
        Principal(
            id=new_id(),
            workspace_id=runtime.identity.workspace_id,
            scopes=frozenset({"read", "artifact.retain", "artifact.release"}),
        )
        for _ in range(2)
    )
    a = ResearchClient(LocalTransport(runtime, lambda: first))
    b = ResearchClient(LocalTransport(runtime, lambda: second))
    one = a.command("artifact.retain", request, idempotency_key="one")["result"]
    two = b.command("artifact.retain", request, idempotency_key="two")["result"]
    assert one["id"] != two["id"]
    assert a.command("artifact.retain", request, idempotency_key="again")["result"] == one
    with pytest.raises(ResearchError) as foreign:
        a.command(
            "artifact.release", ReleaseArtifact(reference_id=two["id"]), idempotency_key="foreign"
        )
    assert foreign.value.code == ErrorCode.AUTHORIZATION_DENIED
    a.command(
        "artifact.release", ReleaseArtifact(reference_id=one["id"]), idempotency_key="release"
    )
    assert b.command("artifact.retain", request, idempotency_key="still-held")["result"] == two
    without_read = second.model_copy(
        update={"scopes": frozenset({"artifact.retain", "artifact.release"})}
    )
    # Even an existing acknowledgment cannot bypass a scope revoked before a direct service call.
    for command, payload, key in (
        (runtime.service.retain_artifact, request, "two"),
        (
            runtime.service.release_artifact,
            ReleaseArtifact(reference_id=two["id"]),
            "release-no-read",
        ),
    ):
        with pytest.raises(ResearchError) as denied:
            command(CommandContext(principal=without_read, idempotency_key=key), payload)
        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
