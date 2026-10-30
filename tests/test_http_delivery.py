from __future__ import annotations

import asyncio
import ipaddress
import json
import socket
import ssl
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from research_swarm.api.http import create_app
from research_swarm.client import HTTPTransport, ResearchClient
from research_swarm.control.auth import CredentialStore
from research_swarm.control.commands import CreateCampaign
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.runtime import write_private
from tests.support import LIMITS, APIFixture, ResearchFixture


@contextmanager
def serving(app: ASGIApp, **tls: Any) -> Iterator[str]:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    scheme = "https" if tls else "http"
    url = f"{scheme}://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            log_level="error",
            access_log=False,
            lifespan="off",
            **tls,
        )
    )
    thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while not server.started:
            assert thread.is_alive() and time.monotonic() < deadline, "Owned server did not start"
            time.sleep(0.01)
        yield url
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        listener.close()
        assert not thread.is_alive(), "Owned server did not stop"


def test_lost_http_ack_replays_one_committed_campaign(research: ResearchFixture) -> None:
    credentials = CredentialStore(research.service.database)
    principal = research.context().principal
    token = credentials.issue(principal, principal).token
    app = create_app(research.service, credentials)
    committed, release = threading.Event(), threading.Event()
    held = False

    async def hold_first_ack(scope: Scope, receive: Receive, send: Send) -> None:
        async def gated(message: Message) -> None:
            nonlocal held
            if (
                scope["path"] == "/v1/commands/campaign.create"
                and message["type"] == "http.response.start"
                and not held
            ):
                held = True
                committed.set()
                assert await asyncio.to_thread(release.wait, 5)
            await send(message)

        await app(scope, receive, gated)

    project = research.project
    request = CreateCampaign(
        project_id=project["id"],
        name="Lost acknowledgment",
        primary_metric="val_bpb",
        limits=LIMITS,
    )
    with serving(hold_first_ack) as url, ResearchClient.connect(url, token, timeout=0.5) as client:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(client.command, "campaign.create", request, idempotency_key="same")
            try:
                assert committed.wait(5), "Command did not reach the post-commit gate"
                with pytest.raises(ResearchError) as uncertain:
                    future.result(timeout=5)
                assert uncertain.value.code == ErrorCode.DELIVERY_UNCERTAIN
                assert uncertain.value.command_id is None
                assert uncertain.value.details == {"recovery": "replay_exact_command"}
                with research.service.database.connect() as connection:
                    stored = connection.execute(
                        "SELECT response FROM commands WHERE idempotency_key='same'"
                    ).fetchone()[0]
                original = json.loads(stored)
            finally:
                release.set()
        replay = client.command("campaign.create", request, idempotency_key="same")
        assert replay == original and replay["disposition"] == "accepted"
    with research.service.database.connect() as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM records WHERE kind='campaign'").fetchone()[0]
            == 1
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM commands WHERE idempotency_key='same'"
            ).fetchone()[0]
            == 1
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM events "
                "WHERE json_extract(payload,'$.type')='campaign.created'"
            ).fetchone()[0]
            == 1
        )


def test_http_durable_rejection_keeps_command_identity_but_auth_refusal_does_not(
    api: APIFixture,
) -> None:
    credential = api.credentials.issue(api.root, api.root)
    with ResearchClient.connect(api.url, credential.token) as client:
        request = CreateCampaign(
            project_id=new_id(), name="Missing project", primary_metric="loss", limits=LIMITS
        )
        errors = []
        for _ in range(2):
            with pytest.raises(ResearchError) as failure:
                client.command("campaign.create", request, idempotency_key="missing")
            errors.append(failure.value)
        assert errors[0].code == ErrorCode.NOT_FOUND
        assert errors[0].command_id is not None
        assert errors[0].as_dict() == errors[1].as_dict()
        assert errors[0].as_dict()["disposition"] == "rejected"
        api.credentials.revoke(api.root, credential.id)
        with pytest.raises(ResearchError) as refusal:
            client.command("campaign.create", request, idempotency_key="missing")
        assert refusal.value.code == ErrorCode.AUTHORIZATION_DENIED
        assert refusal.value.command_id is None


@pytest.mark.parametrize(
    "status,body",
    [
        (200, b"{}"),
        (200, b"<html>gateway</html>"),
        (200, b'{"result":{}}'),
        (502, b"{}"),
        (409, b'{"error":{"code":"RESOURCE_UNAVAILABLE"}}'),
        (409, b'{"error":{"code":"FUTURE_CODE","message":"x","retryable":true,"details":{}}}'),
        (307, b"{}"),
        (200, b'{"command_id":"invalid","result":{}}'),
    ],
)
def test_invalid_command_response_is_uncertain(status: int, body: bytes) -> None:
    transport = HTTPTransport("https://controller.example.invalid", "synthetic-token")
    transport.http.close()
    transport.http = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(status, content=body)),
        base_url="https://controller.example.invalid",
    )
    try:
        with pytest.raises(ResearchError) as failure:
            transport.command("campaign.create", {}, "unchanged-key", None)
        assert failure.value.code == ErrorCode.DELIVERY_UNCERTAIN
        assert "synthetic-token" not in str(failure.value)
        assert failure.value.command_id is None
    finally:
        transport.close()


def test_queries_and_range_uploads_have_distinct_recovery(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = HTTPTransport("http://127.0.0.1", "synthetic-token")
    transport.http.close()
    transport.http = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"invalid")),
        base_url="http://127.0.0.1",
    )
    try:
        with pytest.raises(ResearchError) as query:
            transport.query("list", {"kind": "campaign"})
        assert query.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
        assert query.value.details["recovery"] == "retry_query"
        with pytest.raises(ResearchError) as part:
            transport.append_upload(new_id(), 0, b"synthetic")
        assert part.value.code == ErrorCode.DELIVERY_UNCERTAIN
        assert part.value.details["recovery"] == "inspect_upload_then_replay_exact_range"
        monkeypatch.setattr("research_swarm.client.MAX_RESPONSE_BYTES", 1)
        with pytest.raises(ResearchError) as oversized:
            transport.command("campaign.create", {}, "same", None)
        assert oversized.value.code == ErrorCode.DELIVERY_UNCERTAIN
    finally:
        transport.close()


@pytest.mark.parametrize(
    "url",
    [
        "http://controller.example.invalid",
        "http://localhost.example.invalid",
        "http://0.0.0.0",
        "http://[::]",
        "http://192.0.2.1",
        "http://127.0.0.1:0",
        "http://127.0.0.1:65536",
        "http:///path",
        "http://user:password@localhost",
        "http://localhost?token=secret",
        "http://localhost#fragment",
        "http://local\nhost",
        "http://[::1%lo0]",
    ],
)
def test_unsafe_controller_url_is_rejected_before_constructing_http(
    url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("HTTP client constructed before URL validation")

    monkeypatch.setattr(httpx, "Client", forbidden)
    with pytest.raises(ValueError):
        HTTPTransport(url, "synthetic-token")


def test_tls_trust_and_hostname_verification_are_required(tmp_path: Path) -> None:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, "Synthetic test server")])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(days=1))
        .not_valid_after(datetime.now(UTC) + timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = tmp_path / "server.pem", tmp_path / "key.pem"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    write_private(
        key_path,
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )
    trusted = ssl.create_default_context(cafile=str(cert_path))

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b'{"schema_version":"1.0"}'})

    with serving(app, ssl_certfile=str(cert_path), ssl_keyfile=str(key_path)) as url:
        with ResearchClient.connect(url, "synthetic-token", tls=trusted) as client:
            assert client.capabilities()["schema_version"] == "1.0"
        for endpoint, trust in ((url, None), (url.replace("127.0.0.1", "localhost"), trusted)):
            with ResearchClient.connect(endpoint, "synthetic-token", tls=trust) as client:
                with pytest.raises(ResearchError) as failure:
                    client.capabilities()
                assert failure.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
    trusted.check_hostname = False
    with pytest.raises(ValueError, match="verify certificates and hostnames"):
        HTTPTransport("https://controller.example.invalid", "synthetic-token", tls=trusted)
