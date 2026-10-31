"""Authoritative upload lifecycle on actual DO SQL and R2, with owned restarts."""

import io
import zipfile
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import pytest

from research_swarm.domain.canonical import content_digest
from tests.test_cloudflare_native import build_current_sources, server
from tests.test_cloudflare_router import deliver
from tests.test_cloudflare_router import pytestmark as pytestmark
from tests.test_cloudflare_witness_ordering import WitnessHTTP


class Client:
    def __init__(self, url, origin, token="", *, bypass=False):
        self.url, self.origin, self.token, self.bypass = url, origin, token, bypass

    def call(self, action, key, **fields):
        return deliver(
            self.url,
            self.origin,
            action,
            key,
            artifact_profile=True,
            bypass_gate=self.bypass,
            token=self.token,
            **fields,
        )

    def command(self, operation, key, payload, **fields):
        return self.call(
            "command", key, operation="artifact." + operation, payload=payload, **fields
        )

    def begin(self, body, key, **fields):
        return self.command(
            "begin_upload",
            key,
            {
                "digest": content_digest(body),
                "size_bytes": len(body),
                "media_type": "application/zip",
            },
            **fields,
        )

    def upload(self, upload_id, body, key, **fields):
        return self.call(
            "upload", key, payload={"upload_id": upload_id}, content_hex=body.hex(), **fields
        )

    def status(self, upload_id, key):
        return self.call(
            "query", key, operation="artifact.upload_status", payload={"upload_id": upload_id}
        )

    def bytes(self, upload_id, key):
        return self.call("bytes", key, payload={"upload_id": upload_id})


def source_zip():
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("train.py", "print('public research fixture')\n")
    # ZIP permits trailing bytes. Fill the configured object bound to exercise quota exactly.
    body = stream.getvalue()
    return body + bytes(4096 - len(body))


def prove_missing_gate_allows_cleanup_before_a_late_put(url, origin, witness):
    client = Client(url, origin, bypass=True)
    client.token = client.call("provision", "bypass-provision")["operator_token"]
    body = b"negative gate control"
    upload = client.begin(body, "bypass-begin")["result"]["id"]
    target = {"upload_id": upload}
    held = witness.hold("bypass-put")
    with ThreadPoolExecutor(max_workers=1) as pool:
        transfer = pool.submit(client.upload, upload, body, "bypass-put", r2_fault="held-put")
        try:
            if not witness.event(witness.arrivals, "bypass-put").wait(10):
                pytest.fail(
                    f"Transfer returned before storage hold: {transfer.result(timeout=10)!r}"
                )
            assert (
                client.command("abort_upload", "bypass-abort", target)["result"]["state"]
                == "aborting"
            )
            assert (
                client.command("cleanup_upload", "bypass-cleanup", target)["result"]["state"]
                == "aborted"
            )
            assert not transfer.done()
        finally:
            held.set()
        assert transfer.result(timeout=15)["error"]["code"] == "INVALID_TRANSITION"
    assert client.bytes(upload, "bypass-retired")["error"]["code"] == "INVALID_TRANSITION"


def test_artifact_publication_cleanup_and_replay_survive_real_provider_restart(tmp_path):
    build_current_sources()
    witness, body = WitnessHTTP(), source_zip()
    with witness.serving() as origin:
        with server(tmp_path, sequence=1) as url:
            prove_missing_gate_allows_cleanup_before_a_late_put(url, origin, witness)
            client = Client(url, origin)
            client.token = client.call("provision", "provision")["operator_token"]
            token = client.token
            first = client.begin(body, "begin")["result"]["id"]
            target = {"upload_id": first}
            lost = client.upload(first, body, "lost-put", r2_fault="lost-put")
            assert lost["error"]["code"] == "TRANSPORT_UNAVAILABLE"
            assert client.status(first, "progress")["transfer"] == {
                "schema_version": "1.0",
                "received_bytes": len(body),
                "published": True,
            }
            assert client.call("inspect", "unpublished")["artifact_effects"] == 1
            held = witness.hold("finalize")
            with ThreadPoolExecutor(max_workers=2) as pool:
                finalizing = pool.submit(client.command, "finalize_upload", "finalize", target)
                try:
                    if not witness.event(witness.arrivals, "finalize").wait(10):
                        pytest.fail(
                            f"Finalize returned before witness: {finalizing.result(timeout=10)!r}"
                        )
                    pending = pool.submit(client.status, first, "held-status")
                    assert witness.event(witness.ingress, "held-status").wait(5)
                    with pytest.raises(TimeoutError):
                        pending.result(timeout=0.2)
                finally:
                    held.set()
                published = finalizing.result(timeout=15)
                assert published["result"]["data"]["storage_generation"] == first
                assert pending.result(timeout=15)["state"] == "finalized"
            assert client.bytes(first, "source")["content_hex"] == body.hex()
            empty = client.begin(b"", "empty-begin")["result"]["id"]
            assert (
                client.command("finalize_upload", "empty-finalize", {"upload_id": empty})["result"][
                    "state"
                ]
                == "verified"
            )
            assert client.bytes(empty, "empty-bytes") == {"content_hex": ""}
            second = client.begin(body, "duplicate-begin")["result"]["id"]
            duplicate_target = {"upload_id": second}
            assert client.upload(second, body, "duplicate-put")["received_bytes"] == len(body)
            duplicate = client.command("finalize_upload", "duplicate-finalize", duplicate_target)
            assert duplicate["result"] == published["result"]
            assert client.status(second, "duplicate-status")["state"] == "cleanup"
            full = client.begin(b"x", "full")
            assert full["error"]["code"] == "RESOURCE_UNAVAILABLE" and full["command_id"]
            held = witness.hold("cleanup")
            try:
                uncertain = client.command(
                    "cleanup_upload", "cleanup", duplicate_target, timeout_ms=500
                )
                assert uncertain["error"]["code"] == "TRANSPORT_UNAVAILABLE"
                assert "command_id" not in uncertain
            finally:
                held.set()
            assert client.status(second, "still-cleanup")["state"] == "cleanup"
            assert (
                client.bytes(second, "physically-retired")["error"]["code"] == "INVALID_TRANSITION"
            )
            before_restart = client.call("inspect", "before-restart")

        with server(tmp_path, sequence=2) as url:
            client = Client(url, origin, token)
            # Reopen existing state; never provision again after this process restart.
            assert client.call("inspect", "reopened") == before_restart
            assert client.status(second, "reopened-status")["state"] == "cleanup"
            assert client.begin(b"x", "still-full")["error"]["code"] == "RESOURCE_UNAVAILABLE"
            finished = client.command("cleanup_upload", "cleanup", duplicate_target)
            assert finished["result"]["state"] == "finalized"
            assert len(set(witness.nonces["cleanup"])) == 2
            assert client.bytes(first, "canonical-after-cleanup")["content_hex"] == body.hex()
            third = client.begin(body, "third-begin")["result"]["id"]
            third_target = {"upload_id": third}
            held = witness.hold("third-put")
            with ThreadPoolExecutor(max_workers=2) as pool:
                transfer = pool.submit(client.upload, third, body, "third-put", r2_fault="held-put")
                try:
                    if not witness.event(witness.arrivals, "third-put").wait(10):
                        pytest.fail(
                            f"Transfer returned before hold: {transfer.result(timeout=10)!r}"
                        )
                    abort = pool.submit(client.command, "abort_upload", "abort", third_target)
                    assert witness.event(witness.ingress, "abort").wait(5)
                    with pytest.raises(TimeoutError):
                        abort.result(timeout=0.2)
                finally:
                    held.set()
                assert transfer.result(timeout=15) == {"received_bytes": len(body)}
                aborted = abort.result(timeout=15)
                assert aborted["result"]["state"] == "aborting"
            assert (
                client.command("cleanup_upload", "abort-cleanup", third_target)["result"]["state"]
                == "aborted"
            )
            assert client.bytes(third, "third-retired")["error"]["code"] == "INVALID_TRANSITION"
            cases = (
                ("finalize_upload", "finalize", target, published),
                ("finalize_upload", "duplicate-finalize", duplicate_target, duplicate),
                ("cleanup_upload", "cleanup", duplicate_target, finished),
                ("abort_upload", "abort", third_target, aborted),
            )
            for operation, key, payload, acknowledgment in cases:
                count = witness.count(key)
                witness.holds[key] = witness.hold("offline-" + key)
                try:
                    assert (
                        client.command(operation, key, payload, r2_fault="unavailable")
                        == acknowledgment
                    )
                finally:
                    witness.holds[key].set()
                assert witness.count(key) == count
            assert client.status(third, "terminal-status")["state"] == "aborted"
            assert client.begin(b"x", "full", r2_fault="unavailable") == full
            client.call("revoke", "revoke")
            revoked = client.command("finalize_upload", "finalize", target, r2_fault="unavailable")
            assert revoked["error"]["code"] == "AUTHORIZATION_DENIED"
