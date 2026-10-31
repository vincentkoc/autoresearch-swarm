"""Actual workerd ordering across independent HTTP I/O; timestamps remain synthetic."""

from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import NAMESPACE_URL, uuid5

import httpx
import pytest

from research_swarm.control.time_witnesses import WitnessSigner
from research_swarm.crypto import public_key, resolve_crypto
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.clocks import ClockPolicy
from research_swarm.domain.time_witnesses import (
    TimeWitnessPayload,
    TimeWitnessRequest,
    WitnessSource,
    witness_key_id,
)
from tests.test_cloudflare_native import build_current_sources, server

pytestmark = pytest.mark.skipif(
    os.environ.get("RESEARCH_CLOUDFLARE_TESTS") != "1" or os.name != "posix",
    reason="Explicit opt-in and POSIX-owned local workerd process group required",
)
SEED = bytes.fromhex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
TOKEN = "Bearer public_fixture_consumer_" + "x" * 32


def identifier(name):
    return str(uuid5(NAMESPACE_URL, "research-witness-qualification/" + name))


class WitnessHTTP:
    def __init__(self):
        public = public_key(resolve_crypto(None), SEED).hex()
        self.signer = WitnessSigner(
            WitnessSource(
                id=identifier("source"),
                public_key=public,
                key_id=witness_key_id(public),
                native_profile="linux-boottime-v1",
                policy=ClockPolicy(),
            ),
            SEED,
        )
        self.lock = threading.Lock()
        self.ingress, self.arrivals, self.holds, self.nonces = {}, {}, {}, {}
        self.responses = {}
        self.epochs = {}

    def event(self, mapping, key):
        with self.lock:
            return mapping.setdefault(key, threading.Event())

    def hold(self, key):
        return self.event(self.holds, key)

    def count(self, key):
        with self.lock:
            return len(self.nonces.get(key, []))

    @contextmanager
    def serving(self):
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                assert self.headers.get("authorization") == TOKEN
                size = int(self.headers["content-length"])
                assert 0 <= size <= 16384
                body = self.rfile.read(size)
                kind, key = self.path.strip("/").split("/")
                if kind == "ingress":
                    fixture.event(fixture.ingress, key).set()
                    encoded = b"{}"
                else:
                    assert kind == "witness"
                    request = TimeWitnessRequest.model_validate_json(body)
                    with fixture.lock:
                        fixture.nonces.setdefault(key, []).append(request.nonce)
                        held = fixture.holds.get(key)
                    fixture.event(fixture.arrivals, key).set()
                    if held is not None:
                        assert held.wait(15), "Owned witness fixture was never released"
                    encoded = canonical_bytes(
                        fixture.signer.sign(
                            TimeWitnessPayload(
                                request=request,
                                source_id=fixture.signer.source.id,
                                source_epoch=identifier(fixture.epochs.get(key, "epoch-1")),
                                native_profile=fixture.signer.source.native_profile,
                                policy=fixture.signer.source.policy,
                                authority_time_ms=1_793_000_000_000,
                            )
                        ).model_dump(mode="json")
                    )
                try:
                    self.send_response(200)
                    self.send_header("content-type", "application/json")
                    self.send_header("content-length", str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)
                except (BrokenPipeError, ConnectionResetError):
                    pass  # The timeout test deliberately abandons a late response.
                finally:
                    if kind == "witness":
                        fixture.event(fixture.responses, request.nonce).set()

        listener = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=listener.serve_forever)
        thread.start()
        try:
            yield f"http://127.0.0.1:{listener.server_port}"
        finally:
            for release in self.holds.values():
                release.set()
            listener.shutdown()
            listener.server_close()
            thread.join(timeout=5)
            assert not thread.is_alive()


def deliver(url, origin, key, *, case="sample", timeout_ms=8000, bypass_gate=False):
    with httpx.Client(trust_env=False, timeout=20) as client:
        response = client.post(
            url + "/witness-ordering",
            json={
                "key": key,
                "case": case,
                "witness_origin": origin,
                "timeout_ms": timeout_ms,
                "bypass_gate": bypass_gate,
            },
        )
    assert response.status_code == 200, response.text[-3000:]
    return response.json()


def test_awaited_witness_excludes_other_operations_and_retry_discards_late_response(tmp_path):
    build_current_sources()
    fixture = WitnessHTTP()
    with fixture.serving() as origin:
        with server(tmp_path, sequence=1) as url:
            for bypass in (False, True):
                prefix = "bypass-" if bypass else ""
                first_key, second_key = prefix + "first", prefix + "second"
                held = fixture.hold(first_key)
                with ThreadPoolExecutor(max_workers=2) as pool:
                    first = pool.submit(deliver, url, origin, first_key, bypass_gate=bypass)
                    try:
                        assert fixture.event(fixture.arrivals, first_key).wait(10)
                        second = pool.submit(deliver, url, origin, second_key, bypass_gate=bypass)
                        assert fixture.event(fixture.ingress, second_key).wait(5)
                        if bypass:
                            assert fixture.event(fixture.arrivals, second_key).wait(5)
                            # This exact probe must detect the missing gate.
                            b = second.result(timeout=5)
                        else:
                            assert not fixture.event(fixture.arrivals, second_key).wait(0.2)
                            assert not second.done()
                    finally:
                        held.set()
                    a, b = first.result(timeout=15), second.result(timeout=15)
                assert a["disposition"] == b["disposition"] == "accepted"
                earlier, later = (b, a) if bypass else (a, b)
                assert earlier["result"]["previous_effects"] == 0
                assert later["result"]["previous_effects"] == 1
                assert earlier["result"]["sequence"] < later["result"]["sequence"]
                assert a["closed"] and b["closed"]

            late = fixture.hold("timeout")
            try:
                timed_out = deliver(url, origin, "timeout", timeout_ms=1000)
                assert timed_out["code"] == "TRANSPORT_UNAVAILABLE"
                assert timed_out["command_id"] is None
                assert timed_out["provider_state"] == "qualified"
                assert timed_out["counts"] == {
                    "effects": 2,
                    "retirements": 0,
                    "domain_fences": 0,
                    "acknowledgments": 2,
                }
                expired_nonce = fixture.nonces["timeout"][0]
                with fixture.lock:
                    retry_hold = fixture.holds["timeout"] = threading.Event()
                    fixture.arrivals["timeout"].clear()
                with ThreadPoolExecutor(max_workers=1) as pool:
                    pending = pool.submit(deliver, url, origin, "timeout")
                    try:
                        assert fixture.event(fixture.arrivals, "timeout").wait(5)
                        late.set()
                        assert fixture.event(fixture.responses, expired_nonce).wait(5)
                        assert not pending.done()
                    finally:
                        retry_hold.set()
                    retry = pending.result(timeout=10)
            finally:
                late.set()
            assert retry["disposition"] == "accepted"
            assert retry["result"]["previous_effects"] == 2
            assert retry["counts"]["effects"] == retry["counts"]["acknowledgments"] == 3
            assert fixture.count("timeout") == 2
            assert len(set(fixture.nonces["timeout"])) == 2
            replay = deliver(url, origin, "timeout")
            assert replay == retry and fixture.count("timeout") == 2

            rejected = deliver(url, origin, "domain", case="domain-reject")
            assert rejected["code"] == "ACCEPTANCE_EXPIRED"
            assert rejected["counts"] == {
                "effects": 0,
                "retirements": 0,
                "domain_fences": 1,
                "acknowledgments": 1,
            }
            fixture.epochs["changed"] = "epoch-2"
            changed = deliver(url, origin, "changed", case="changed-source")
            assert changed["code"] == "STALE_FENCE"
            assert changed["counts"] == {
                "effects": 0,
                "retirements": 1,
                "domain_fences": 0,
                "acknowledgments": 1,
            }
        with server(tmp_path, sequence=2) as url:
            for key, case, original in (
                ("timeout", "sample", retry),
                ("domain", "domain-reject", rejected),
                ("changed", "changed-source", changed),
            ):
                count = fixture.count(key)
                assert deliver(url, origin, key, case=case) == original
                assert fixture.count(key) == count
