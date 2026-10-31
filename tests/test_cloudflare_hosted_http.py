"""Unmodified SDK over real local Workers HTTP, Durable Objects and R2."""

import hashlib
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import httpx
import pytest

from research_swarm.client import ResearchClient
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.artifacts import BeginUpload
from tests.test_cloudflare_native import build_current_sources, server
from tests.test_cloudflare_router import deliver
from tests.test_cloudflare_router import pytestmark as pytestmark
from tests.test_cloudflare_witness_ordering import WitnessHTTP


def key(method, path, idempotency=""):
    return (
        "http-"
        + hashlib.sha256((method + "\0" + path + "\0" + idempotency).encode()).hexdigest()[:48]
    )


def connect(url, origin, token):
    client = ResearchClient.connect(url, token)
    client.transport.http.headers["x-fixture-witness-origin"] = origin
    return client


def inspect(url, origin, name):
    return deliver(url, origin, "inspect", name, artifact_profile=True)


def test_sdk_http_upload_download_replay_and_read_exclusion_in_workerd(tmp_path):
    build_current_sources()
    witness = WitnessHTTP()
    body = bytes(range(256)) * 2
    source, empty = tmp_path / "source.bin", tmp_path / "empty.bin"
    source.write_bytes(body)
    empty.write_bytes(b"")
    with witness.serving() as origin:
        with server(tmp_path, sequence=1) as url:
            setup = deliver(url, origin, "provision", "http-provision", artifact_profile=True)
            token = setup["operator_token"]
            with connect(url, origin, token) as client:
                capabilities = client.capabilities()
                assert capabilities["profile"] == "worker-artifacts-witness-v1"
                assert capabilities["artifacts"]["download"] == "verified-buffer"
                missing = client.transport.http.get("/v1/missing")
                assert missing.status_code == 404
                malformed = client.transport.http.get("/v1/capabilities?missing")
                assert malformed.status_code == 422
                reference = client.upload_file(source, idempotency_key="binary")
                destination = tmp_path / "download.bin"
                assert client.download(reference.id, destination) == reference
                assert destination.read_bytes() == body
                empty_ref = client.upload_file(empty, idempotency_key="empty")
                client.download(empty_ref.id, tmp_path / "empty.download")
                assert (tmp_path / "empty.download").read_bytes() == b""
                response = client.transport.http.get(f"/v1/artifacts/{reference.id}/content")
                assert response.status_code == 200 and response.content == body
                assert response.headers["etag"] == '"' + reference.digest + '"'
                assert response.headers["content-length"] == str(len(body))
                assert response.headers["cache-control"] == "no-store, no-transform"

                # The SDK sees an unreadable successful reply after the real commit.
                client.transport.http.headers["x-fixture-drop-finalize"] = "true"
                with pytest.raises(ResearchError) as uncertain:
                    client.upload_file(source, idempotency_key="lost")
                assert uncertain.value.code == ErrorCode.DELIVERY_UNCERTAIN
                del client.transport.http.headers["x-fixture-drop-finalize"]
                before = inspect(url, origin, "after-lost")
                calls = sum(map(len, witness.nonces.values()))
                client.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
                assert client.upload_file(source, idempotency_key="lost") == reference
                assert inspect(url, origin, "replayed-lost") == before
                assert sum(map(len, witness.nonces.values())) == calls
                del client.transport.http.headers["x-fixture-r2-fault"]

                duplicate = client.transport.begin_upload(
                    BeginUpload(digest=content_digest(body), size_bytes=len(body)),
                    "manual-duplicate",
                )
                client.transport.append_upload(duplicate["id"], 0, body)
                assert (
                    client.transport.finish_upload(duplicate["id"], "manual-finalize") == reference
                )
                assert client.transport.upload_status(duplicate["id"])["state"] == "cleanup"
                cleanup = client.command(
                    "artifact.cleanup_upload",
                    {"upload_id": duplicate["id"]},
                    idempotency_key="manual-cleanup",
                )
                assert cleanup["result"]["state"] == "finalized"
                with httpx.Client(trust_env=False, timeout=10) as unauthenticated:
                    denied = unauthenticated.post(
                        url + "/v1/artifact-uploads",
                        content=bytes(5000),
                        headers={
                            "authorization": "Bearer invalid",
                            "x-fixture-witness-origin": origin,
                        },
                    )
                assert denied.status_code == 401
                forbidden = client.transport.http.get(
                    f"/v1/artifacts/{reference.id}/content", headers={"range": "bytes=0-1"}
                )
                assert forbidden.status_code == 501
                before_restart = inspect(url, origin, "before-http-restart")
                calls = sum(map(len, witness.nonces.values()))

        with server(tmp_path, sequence=2) as url, connect(url, origin, token) as client:
            client.transport.http.headers["x-fixture-r2-fault"] = "unavailable"
            assert client.upload_file(source, idempotency_key="binary") == reference
            assert client.upload_file(empty, idempotency_key="empty") == empty_ref
            assert (
                client.command(
                    "artifact.cleanup_upload",
                    {"upload_id": duplicate["id"]},
                    idempotency_key="manual-cleanup",
                )
                == cleanup
            )
            assert inspect(url, origin, "reopened-http") == before_restart
            assert sum(map(len, witness.nonces.values())) == calls
            del client.transport.http.headers["x-fixture-r2-fault"]

            path = f"/v1/artifacts/{reference.id}/content"
            held = witness.hold(key("GET", path))

            def download():
                with httpx.Client(trust_env=False, timeout=20) as reader:
                    return reader.get(
                        url + path,
                        headers={
                            "authorization": "Bearer " + token,
                            "x-fixture-witness-origin": origin,
                            "x-fixture-r2-fault": "held-get",
                        },
                    )

            with ThreadPoolExecutor(max_workers=2) as pool:
                reading = pool.submit(download)
                try:
                    if not witness.event(witness.arrivals, key("GET", path)).wait(10):
                        pytest.fail(
                            f"Read returned before hold: {reading.result(timeout=10).text!r}"
                        )
                    revoke = pool.submit(
                        deliver,
                        url,
                        origin,
                        "revoke",
                        "revoke-during-read",
                        artifact_profile=True,
                    )
                    assert witness.event(witness.ingress, "revoke-during-read").wait(5)
                    with pytest.raises(TimeoutError):
                        revoke.result(timeout=0.2)
                finally:
                    held.set()
                verified = reading.result(timeout=15)
                assert verified.status_code == 200 and verified.content == body
                revoke.result(timeout=15)
            with pytest.raises(ResearchError) as revoked:
                client.download(reference.id, tmp_path / "revoked.download")
            assert revoked.value.code == ErrorCode.AUTHORIZATION_DENIED
            assert not (tmp_path / "revoked.download").exists()
