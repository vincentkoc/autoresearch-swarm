"""Existing SDK wire contracts against the bounded adapter; no provider claim."""

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace

import httpx
import pytest

from adapters.cloudflare.research.hosted_http import handle
from research_swarm.client import ResearchClient
from research_swarm.domain.canonical import canonical_bytes, content_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.state.artifacts import BeginUpload
from tests.adapters.cloudflare.hosted.test_hosted_artifacts import artifacts as artifacts
from tests.adapters.cloudflare.hosted.test_hosted_router import counts
from tests.adapters.cloudflare.hosted.test_hosted_router import hosted as hosted


@dataclass(frozen=True)
class ScopedHTTPPolicy:
    allowed: frozenset = frozenset({"capabilities", "command", "query", "worker-upload"})
    limit: int = 16

    def admit(self, kind, identifier):
        if kind not in self.allowed:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Outside fixture policy")

    def scopes(self, kind, identifier):
        return ("worker.query",)

    def body_limit(self, kind):
        return self.limit

    def describe(self):
        return {"profile": "scoped-fixture", "execution": "unavailable"}


@pytest.mark.parametrize("second_scopes,expected", [({"worker.query"}, 200), ({"read"}, 403)])
async def test_alternative_capabilities_recheck_their_scopes_after_body_await(
    second_scopes, expected
):
    actor = Principal(id=new_id(), workspace_id=new_id(), scopes={"worker.query"})
    calls = []

    async def authenticate(token):
        calls.append("authenticate")
        return actor

    async def read(limit):
        nonlocal actor
        calls.append(limit)
        actor = actor.model_copy(update={"scopes": second_scopes})
        return b""

    result = await handle(
        SimpleNamespace(authenticate=authenticate),
        "GET",
        "/v1/capabilities",
        {"authorization": "Bearer fixture"},
        read,
        policy=ScopedHTTPPolicy(),
    )
    assert calls == ["authenticate", 16, "authenticate"]
    assert result.status == expected
    if expected == 200:
        assert strict_json_loads(result.body) == ScopedHTTPPolicy().describe()


@pytest.mark.parametrize(
    "refusal,expected", [("authentication", 401), ("scope", 403), ("policy", 501)]
)
async def test_alternative_policy_refusals_do_not_consume_body(refusal, expected):
    consumed = []

    async def authenticate(token):
        if refusal == "authentication":
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Invalid fixture token")
        return Principal(
            id=new_id(),
            workspace_id=new_id(),
            scopes={"read" if refusal == "scope" else "worker.query"},
        )

    async def read(limit):
        consumed.append(limit)
        return b"{}"

    result = await handle(
        SimpleNamespace(authenticate=authenticate),
        "POST",
        "/v1/commands/worker.submit_evidence",
        {"authorization": "Bearer fixture", "idempotency-key": "fixture"},
        read,
        policy=ScopedHTTPPolicy(
            allowed=frozenset() if refusal == "policy" else frozenset({"command"})
        ),
    )
    assert result.status == expected and consumed == []


@pytest.mark.parametrize(
    "body,expected", [(b"{}", 200), (b'{"excess":123456789}', 422), (b"[]", 422)]
)
async def test_alternative_policy_bounds_and_dispatches_the_existing_command_wire(body, expected):
    calls = []

    async def authenticate(token):
        return Principal(id=new_id(), workspace_id=new_id(), scopes={"worker.query"})

    async def read(limit):
        calls.append(limit)
        return body

    async def command(token, operation, payload, **kwargs):
        calls.append((token, operation, payload, kwargs))
        return {"accepted": True}

    result = await handle(
        SimpleNamespace(authenticate=authenticate, command=command),
        "POST",
        "/v1/commands/worker.submit_evidence",
        {"authorization": "Bearer fixture", "idempotency-key": "once", "x-expected-revision": "3"},
        read,
        policy=ScopedHTTPPolicy(),
    )
    assert result.status == expected and calls[0] == 16
    assert calls[1:] == (
        [
            (
                "fixture",
                "worker.submit_evidence",
                {},
                {"idempotency_key": "once", "expected_revision": 3},
            )
        ]
        if expected == 200
        else []
    )


@pytest.mark.parametrize(
    "method,path,code,recovery",
    [
        (
            "POST",
            "/v1/commands/worker.submit_evidence",
            "DELIVERY_UNCERTAIN",
            "replay_exact_command",
        ),
        ("POST", "/v1/worker/get", "TRANSPORT_UNAVAILABLE", "retry_query"),
        (
            "PUT",
            "/v1/worker/output/parts?"
            + "&".join(
                name + "=00000000-0000-4000-8000-000000000001"
                for name in ("worker_id", "session_id", "attempt_id", "upload_id")
            ),
            "DELIVERY_UNCERTAIN",
            "inspect_upload_then_replay_exact_range",
        ),
    ],
)
async def test_alternative_policy_keeps_shared_delivery_uncertainty(method, path, code, recovery):
    async def authenticate(token):
        return Principal(id=new_id(), workspace_id=new_id(), scopes={"worker.query"})

    async def read(limit):
        return b"{}"

    async def lost(*args, **kwargs):
        raise ValueError("Private failure after dispatch")

    result = await handle(
        SimpleNamespace(
            authenticate=authenticate, command=lost, query=lost, worker_upload_part=lost
        ),
        method,
        path,
        {"authorization": "Bearer fixture", "idempotency-key": "once"},
        read,
        policy=ScopedHTTPPolicy(),
    )
    error = strict_json_loads(result.body)["error"]
    assert result.status == 502 and error["code"] == code
    assert error["details"]["recovery"] == recovery and b"Private" not in result.body


async def send(h, method, path, *, payload=None, body=b"", token=None, headers=None, consume=None):
    async def read(limit):
        if consume is not None:
            consume.append(limit)
        return canonical_bytes(payload) if payload is not None else body

    return await handle(
        h.router,
        method,
        path,
        {
            "authorization": "Bearer " + (h.operator.token if token is None else token),
            **(headers or {}),
        },
        read,
    )


async def test_existing_sdk_uploads_and_downloads_binary_and_empty_files(artifacts, tmp_path):
    h = artifacts
    loop = asyncio.get_running_loop()
    paths = []

    def route(request):
        paths.append((request.method, request.url.path))

        async def deliver():
            async def read(limit):
                assert len(request.content) <= limit
                return request.content

            return await handle(
                h.router, request.method, str(request.url), dict(request.headers), read
            )

        result = asyncio.run_coroutine_threadsafe(deliver(), loop).result(timeout=5)
        return httpx.Response(result.status, content=result.body, headers=dict(result.headers))

    def exercise():
        with ResearchClient.connect("http://127.0.0.1", h.operator.token) as client:
            client.transport.http.close()
            client.transport.http = httpx.Client(
                base_url="http://127.0.0.1",
                transport=httpx.MockTransport(route),
                headers={"Authorization": "Bearer " + h.operator.token},
            )
            for name, body in (("binary", bytes(range(256))), ("empty", b"")):
                source = tmp_path / name
                source.write_bytes(body)
                reference = client.upload_file(source, idempotency_key=name)
                destination = tmp_path / (name + ".download")
                assert client.download(reference.id, destination) == reference
                assert destination.read_bytes() == body
            duplicate = client.transport.begin_upload(
                BeginUpload(digest=content_digest(bytes(range(256))), size_bytes=256), "duplicate"
            )
            client.transport.append_upload(duplicate["id"], 0, bytes(range(256)))
            client.transport.finish_upload(duplicate["id"], "duplicate-finalize")
            assert client.transport.upload_status(duplicate["id"])["state"] == "cleanup"
            finished = client.command(
                "artifact.cleanup_upload", {"upload_id": duplicate["id"]}, idempotency_key="cleanup"
            )
            assert finished["result"]["state"] == "finalized"

    await asyncio.wait_for(asyncio.to_thread(exercise), 10)
    assert sum(method == "PUT" for method, _ in paths) == 2  # Empty file sends no part.
    assert ("POST", "/v1/commands/artifact.cleanup_upload") in paths


async def test_authentication_and_scopes_precede_body_consumption(artifacts):
    h = artifacts
    consumed = []
    missing = await send(h, "POST", "/v1/artifact-uploads", token="invalid", consume=consumed)
    assert missing.status == 401 and missing.headers["www-authenticate"] == "Bearer"
    reader = h.credentials.issue(h.principal, h.principal.model_copy(update={"scopes": {"read"}}))
    denied = await send(h, "POST", "/v1/artifact-uploads", token=reader.token, consume=consumed)
    assert denied.status == 403
    assert consumed == []


@pytest.mark.parametrize(
    "target,headers,body",
    [
        ("/v1/artifact-uploads", {}, b"{}"),
        ("/v1/artifact-uploads", {"idempotency-key": "key"}, b"[]"),
        ("/v1/artifact-uploads", {"idempotency-key": "key"}, b'{"size_bytes":1,"size_bytes":2}'),
        ("/v1/artifact-uploads?unexpected=1", {"idempotency-key": "key"}, b"{}"),
        ("/v1/artifact-uploads", {"idempotency-key": "key", "x-expected-revision": "-1"}, b"{}"),
    ],
)
async def test_malformed_commands_never_acquire_witness_or_change_ledger(
    artifacts, target, headers, body
):
    h = artifacts
    before = counts(h), len(h.witness.requests)
    result = await send(h, "POST", target, headers=headers, body=body)
    assert result.status == 422
    assert strict_json_loads(result.body)["error"]["code"] == "PROJECT_CONTRACT_INVALID"
    assert (counts(h), len(h.witness.requests)) == before


async def test_worker_only_capabilities_do_not_advertise_artifacts(hosted):
    response = await send(hosted, "GET", "/v1/capabilities")
    value = strict_json_loads(response.body)
    assert response.status == 200 and value["profile"] == "worker-control-witness-v1"
    assert "artifacts" not in value and "artifact.cleanup_upload" not in value["commands"]


async def test_artifact_profile_capabilities_are_explicitly_bounded(artifacts):
    response = await send(artifacts, "GET", "/v1/capabilities")
    value = strict_json_loads(response.body)
    assert response.status == 200 and value["execution"] == "unavailable"
    assert "project.create" not in value["commands"] and "worker.claim" not in value["commands"]
    assert value["artifacts"]["max_part_bytes"] == 4096
    assert value["artifacts"]["transfer"] == "complete-body-offset-zero"
    assert "artifact.cleanup_upload" in value["commands"]


async def test_lost_final_response_is_uncertain_and_exact_retry_uses_existing_acknowledgment(
    artifacts,
):
    h = artifacts
    request = {"digest": content_digest(b"fixture"), "size_bytes": 7}
    original = h.router.command

    async def lost(*args, **kwargs):
        await original(*args, **kwargs)
        raise ValueError("Private serialization failure after commit")

    h.router.command = lost
    uncertain = await send(
        h, "POST", "/v1/artifact-uploads", payload=request, headers={"idempotency-key": "once"}
    )
    result = strict_json_loads(uncertain.body)
    assert uncertain.status == 502 and result["error"]["code"] == "DELIVERY_UNCERTAIN"
    assert result["error"]["details"]["recovery"] == "replay_exact_command"
    assert b"Private" not in uncertain.body
    before = counts(h), len(h.witness.requests)
    h.router.command = original
    replay = await send(
        h, "POST", "/v1/artifact-uploads", payload=request, headers={"idempotency-key": "once"}
    )
    assert replay.status == 200 and strict_json_loads(replay.body)["disposition"] == "accepted"
    assert (counts(h), len(h.witness.requests)) == before


async def test_uncertain_part_response_requires_progress_inspection(artifacts):
    h = artifacts

    async def unavailable(*args, **kwargs):
        raise RuntimeError("Fixture dropped response")

    h.router.upload_part = unavailable
    response = await send(h, "PUT", "/v1/artifact-uploads/" + new_id() + "/parts", body=b"fixture")
    error = strict_json_loads(response.body)["error"]
    assert response.status == 502 and error["code"] == "DELIVERY_UNCERTAIN"
    assert error["details"]["recovery"] == "inspect_upload_then_replay_exact_range"


async def test_ranges_encoded_bodies_and_oversized_parts_are_refused(artifacts):
    h = artifacts
    part = "/v1/artifact-uploads/" + new_id() + "/parts"
    for method, path, headers, expected in (
        ("PUT", part, {"content-encoding": "gzip"}, 501),
        ("GET", "/v1/artifacts/" + new_id() + "/content", {"range": "bytes=0-1"}, 501),
        ("PUT", part + "?offset=0&offset=1", {}, 422),
    ):
        assert (await send(h, method, path, headers=headers)).status == expected
    response = await send(h, "PUT", part, body=bytes(4097))
    assert response.status == 422 and h.transport.calls == []


async def test_capabilities_reauthenticate_after_body_reader_suspends(artifacts):
    h = artifacts

    async def revoked(limit):
        assert limit == 0
        h.credentials.revoke(h.principal, h.operator.id)
        return b""

    response = await handle(
        h.router,
        "GET",
        "/v1/capabilities",
        {
            "authorization": "Bearer " + h.operator.token,
        },
        revoked,
    )
    assert response.status == 403


async def test_preflight_failure_never_consumes_an_outside_profile_body(hosted):
    consumed = []
    result = await send(hosted, "POST", "/v1/commands/artifact.cleanup_upload", consume=consumed)
    assert result.status == 501 and consumed == []
