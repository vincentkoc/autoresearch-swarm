from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
from starlette.types import Message, Scope

from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.control.native_time_witnesses import NativeWitnessSource
from research_swarm.control.time_witnesses import MAX_WITNESS_BYTES
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import SignedTimeWitness, TimeWitnessRequest
from tests.test_http_delivery import serving
from tests.test_native_time_witnesses import pending
from tests.test_time_witnesses import SEED, identifier, scope

# Public fixture credential, unrelated to any deployed service.
TOKEN = "public_fixture_consumer_" + "x" * 32


class RecordingSource(NativeWitnessSource):
    def __init__(self) -> None:
        super().__init__(scope().authority, identifier(1), SEED)
        self.observations = 0
        self.failure: Exception | None = None

    def observe(self, request: TimeWitnessRequest) -> SignedTimeWitness:
        self.observations += 1
        if self.failure is not None:
            raise self.failure
        return super().observe(request)


def headers(token: str = TOKEN) -> dict[str, str]:
    return {"authorization": "Bearer " + token, "content-type": "application/json"}


def http_scope(raw_headers: list[tuple[bytes, bytes]]) -> Scope:
    return {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/v1/time-witness",
        "raw_path": b"/v1/time-witness",
        "query_string": b"",
        "root_path": "",
        "headers": raw_headers,
        "server": ("127.0.0.1", 80),
        "client": ("127.0.0.1", 12345),
    }


def test_real_http_returns_a_nonce_bound_native_observation() -> None:
    native = RecordingSource()
    app = create_time_witness_app(native, witness_consumer_digest(TOKEN))
    waiting = pending(native)
    with serving(app) as url, httpx.Client(base_url=url, trust_env=False, timeout=3) as client:
        response = client.post(
            "/v1/time-witness",
            content=canonical_bytes(waiting.request.model_dump(mode="json")),
            headers=headers(),
        )
        assert response.status_code == 200
        assert response.headers["cache-control"] == "no-store"
        verified = waiting.complete(response.content)
        assert verified.witness.payload.request == waiting.request
        assert native.observations == 1
        assert client.get("/openapi.json").status_code == 404


@pytest.mark.parametrize("case", ["missing", "wrong", "duplicate", "oversized"])
async def test_authentication_precedes_body_reads(case: str) -> None:
    native = RecordingSource()
    app = create_time_witness_app(native, witness_consumer_digest(TOKEN))
    raw_headers = [(b"content-type", b"application/json")]
    if case != "missing":
        token = "z" * 43 if case == "wrong" else TOKEN
        if case == "oversized":
            token = "z" * 1025
        raw_headers.append((b"authorization", ("Bearer " + token).encode()))
        if case == "duplicate":
            raw_headers.append(raw_headers[-1])

    async def receive() -> Message:
        pytest.fail("Unauthenticated request body was consumed")

    messages: list[Message] = []

    async def send(message: Message) -> None:
        messages.append(message)

    await app(http_scope(raw_headers), receive, send)
    assert messages[0]["status"] == 401
    assert native.observations == 0


@pytest.mark.parametrize("body", [b"{", b'{"nonce":1,"nonce":2}', b"x" * (MAX_WITNESS_BYTES + 1)])
async def test_invalid_bodies_are_bounded_and_do_not_observe(body: bytes) -> None:
    native = RecordingSource()
    app = create_time_witness_app(native, witness_consumer_digest(TOKEN))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/v1/time-witness", content=body, headers=headers())
    assert response.status_code == 422
    assert len(response.content) < 200
    assert native.observations == 0


@pytest.mark.parametrize("case", ["timeout", "disconnect"])
async def test_incomplete_body_never_reaches_native_source(case: str) -> None:
    native = RecordingSource()
    app = create_time_witness_app(native, witness_consumer_digest(TOKEN), body_timeout_seconds=0.02)
    messages: list[Message] = []

    async def receive() -> Message:
        if case == "disconnect":
            return {"type": "http.disconnect"}
        await asyncio.sleep(1)
        return {"type": "http.request", "body": b"", "more_body": True}

    async def send(message: Message) -> None:
        messages.append(message)

    async with asyncio.timeout(1):
        await app(
            http_scope([(name.encode(), value.encode()) for name, value in headers().items()]),
            receive,
            send,
        )
    assert messages[0]["status"] == (408 if case == "timeout" else 400)
    assert native.observations == 0


@pytest.mark.parametrize("case", ["scope", "clock", "infrastructure"])
async def test_source_refusals_are_sanitized(case: str) -> None:
    native = RecordingSource()
    app = create_time_witness_app(native, witness_consumer_digest(TOKEN))
    waiting = pending(native, **({"workspace_id": identifier(99)} if case == "scope" else {}))
    if case == "clock":
        native.failure = ResearchError(ErrorCode.STALE_FENCE, "private clock diagnostic")
    if case == "infrastructure":
        native.failure = RuntimeError("private source diagnostic")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/v1/time-witness",
            content=canonical_bytes(waiting.request.model_dump(mode="json")),
            headers=headers(),
        )
    assert response.status_code == {"scope": 403, "clock": 409, "infrastructure": 503}[case]
    assert "private" not in response.text and TOKEN not in response.text
    assert response.headers["cache-control"] == "no-store"
    assert set(response.json()["error"]) == {"code", "retryable"}


@pytest.mark.parametrize("timeout", [0, -1, 11, True, float("inf"), float("nan")])
def test_timeout_configuration_is_finite_and_bounded(timeout: Any) -> None:
    with pytest.raises(ValueError):
        create_time_witness_app(
            RecordingSource(), witness_consumer_digest(TOKEN), body_timeout_seconds=timeout
        )


async def test_encoding_is_explicit_and_json_is_not_compressed() -> None:
    native = RecordingSource()
    app = create_time_witness_app(native, witness_consumer_digest(TOKEN))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for changed in ({"content-type": "text/plain"}, {"content-encoding": "gzip"}):
            response = await client.post(
                "/v1/time-witness", content=json.dumps({}), headers={**headers(), **changed}
            )
            assert response.status_code == 415
        for second in ("identity", "gzip"):
            response = await client.post(
                "/v1/time-witness",
                content=b"{}",
                headers=[
                    *headers().items(),
                    ("content-encoding", "identity"),
                    ("content-encoding", second),
                ],
            )
            assert response.status_code == 415
    assert native.observations == 0
