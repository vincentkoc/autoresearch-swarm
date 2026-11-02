from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from research_swarm import ResearchClient
from research_swarm.api.http import create_app
from research_swarm.client import HTTPTransport, LocalTransport
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.runtime import LocalRuntime


@pytest.mark.parametrize("operation", ["future.command", "future/command"])
def test_http_unknown_command_matches_local_rejection(tmp_path: Path, operation: str) -> None:
    runtime = LocalRuntime.open(tmp_path / "runtime")
    token = runtime.credentials.issue(runtime.principal(), runtime.principal()).token
    transport = HTTPTransport("http://127.0.0.1", token)
    transport.http.close()
    with TestClient(
        create_app(runtime.service, runtime.credentials),
        headers={"Authorization": "Bearer " + token},
    ) as http:
        transport.http = http
        for client in (ResearchClient(LocalTransport(runtime)), ResearchClient(transport)):
            with pytest.raises(ResearchError) as rejected:
                client.command(operation, {}, idempotency_key="unknown-command")
            assert rejected.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
            assert rejected.value.retryable is False
            assert rejected.value.command_id is None
            assert "recovery" not in rejected.value.details

        response = http.post(
            "/v1/commands/" + operation, json={}, headers={"Idempotency-Key": "unknown-command"}
        )
        assert response.status_code == 501
        assert response.json()["error"]["code"] == "CAPABILITY_UNAVAILABLE"
        http.headers.pop("Authorization")
        assert http.post("/v1/commands/" + operation, json={}).status_code == 401

    with runtime.database.connect() as sql:
        assert sql.execute("SELECT COUNT(*) FROM commands").fetchone()[0] == 0
