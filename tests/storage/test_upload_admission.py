from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.artifacts import BeginUpload


@pytest.mark.parametrize("surface", ["local", "http"])
def test_upload_scopes_are_checked_before_durable_admission(tmp_path: Path, surface: str) -> None:
    runtime = LocalRuntime.open(tmp_path / "runtime")
    runtime.artifacts.storage_limit_bytes = 8
    root = CommandContext(principal=runtime.principal(), idempotency_key="inspect")
    actor = Principal(
        id=new_id(), workspace_id=root.principal.workspace_id, scopes={"artifact.upload"}
    )
    request = BeginUpload(digest=content_digest(b"12345678"), size_bytes=8)

    def counts() -> tuple[int, int, int, int]:
        with runtime.database.connect() as sql:
            commands = sql.execute("SELECT COUNT(*) FROM commands").fetchone()[0]
            uploads = sql.execute("SELECT COUNT(*) FROM records WHERE kind='upload'").fetchone()[0]
        events = runtime.database.events(root)
        return (
            commands,
            uploads,
            len([event for event in events if event.type == "artifact.upload_started"]),
            runtime.artifacts.occupancy(root)["staging_reserved_bytes"],
        )

    with TestClient(create_app(runtime.service, runtime.credentials)) as http:

        def begin(principal: Principal) -> dict[str, Any]:
            if surface == "local":
                return LocalTransport(runtime, lambda: principal).begin_upload(request, "upload")
            token = runtime.credentials.issue(root.principal, principal).token
            response = http.post(
                "/v1/artifact-uploads",
                json=request.model_dump(mode="json"),
                headers={"Authorization": "Bearer " + token, "Idempotency-Key": "upload"},
            )
            if response.status_code == 403:
                error = response.json()["error"]
                raise ResearchError(ErrorCode(error["code"]), error["message"])
            assert response.status_code == 200, response.json()
            return dict(response.json()["result"])

        for _ in range(2):
            with pytest.raises(ResearchError) as denied:
                begin(actor)
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
            assert counts() == (0, 0, 0, 0)

        permitted = actor.model_copy(update={"scopes": frozenset({"artifact.upload", "read"})})
        first = begin(permitted)
        assert first["state"] == "pending"
        assert begin(permitted) == first
        assert counts() == (1, 1, 1, 8)

        with pytest.raises(ResearchError) as denied_replay:
            begin(actor)
        assert denied_replay.value.code == ErrorCode.AUTHORIZATION_DENIED
        assert counts() == (1, 1, 1, 8)
