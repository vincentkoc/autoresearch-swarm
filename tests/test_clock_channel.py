from __future__ import annotations

import hashlib
import io
import json
import os
import socket
import struct
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from research_swarm.domain.errors import ResearchError
from research_swarm.domain.types import new_id
from research_swarm.domain.workers import WorkerBinding
from research_swarm.workers import clock_channel as channel
from research_swarm.workers.clock_channel import (
    Bootstrap,
    ChannelScope,
    GateDecision,
    GateRequest,
    OriginServer,
    SupervisorChannel,
    bootstrap_stdin,
)
from research_swarm.workers.clocks import native_clock
from research_swarm.workers.supervision import base_environment
from research_swarm.workers.timed_launch import CapsRecord
from tests.test_assignment_signatures import identifier


def golden_request() -> tuple[Bootstrap, GateRequest]:
    digest = "sha256:" + "a" * 64
    caps = CapsRecord(
        clock_epoch=identifier(6),
        assignment_digest=digest,
        origin_session_id=identifier(7),
        origin_profile="linux-boottime-v1",
        launch_ns=str(2**53 + 123),
        hard_ns=str(2**53 + 1123),
    )
    bound = ChannelScope(
        binding=WorkerBinding(
            workspace_id=identifier(1),
            controller_id=identifier(2),
            control_epoch=1,
            worker_id=identifier(3),
            worker_session_id=identifier(4),
        ),
        attempt_id=identifier(5),
        assignment_digest=digest,
        timing_digest="sha256:" + "b" * 64,
        journal_identity=identifier(8),
        origin_session_id=identifier(7),
        caps=caps,
    )
    # Published synthetic capability; never used to enroll a real channel.
    return Bootstrap(12345, identifier(9), bytes(range(32))), GateRequest(
        scope=bound, supervisor_id=identifier(10), nonce=identifier(11), sequence=1, phase="claim"
    )


def test_channel_canonical_fixture_binds_direction_and_channel() -> None:
    bootstrap, request = golden_request()
    frame = channel._pack(bootstrap, "request", request)
    assert channel._unpack(bootstrap, "request", frame) == request.model_dump(mode="json")
    assert (
        hashlib.sha256(frame).hexdigest()
        == "36bdad6c539fc414eef53a2a8bd6ddbfd98f2ae1a53957e13e905e213df9fec2"
    )
    with pytest.raises(ResearchError, match="authentication"):
        channel._unpack(bootstrap, "reply", frame)
    with pytest.raises(ResearchError, match="authentication"):
        channel._unpack(replace(bootstrap, channel_id=identifier(12)), "request", frame)


def scope() -> ChannelScope:
    now = native_clock().continuous_ns()
    origin = new_id()
    digest = "sha256:" + "a" * 64
    caps = CapsRecord(
        clock_epoch=new_id(),
        assignment_digest=digest,
        origin_session_id=origin,
        origin_profile=native_clock().profile,
        launch_ns=str(now + 10_000_000_000),
        hard_ns=str(now + 20_000_000_000),
    )
    return ChannelScope(
        binding=WorkerBinding(
            workspace_id=new_id(),
            controller_id=new_id(),
            control_epoch=1,
            worker_id=new_id(),
            worker_session_id=new_id(),
        ),
        attempt_id=new_id(),
        assignment_digest=digest,
        timing_digest=digest,
        journal_identity=new_id(),
        origin_session_id=origin,
        caps=caps,
    )


def decision(request: GateRequest) -> GateDecision:
    return GateDecision(
        allowed=True, journal_revision=request.sequence + 1, caps=request.scope.caps
    )


def test_private_bootstrap_and_two_phases_cross_a_real_process(tmp_path: Path) -> None:
    bound = scope()
    server = OriginServer(bound, deadline_ns=int(bound.caps.launch_ns))
    assert not server._listener.get_inheritable()
    requests = []

    def authorize(request: GateRequest) -> GateDecision:
        requests.append(request)
        return decision(request)

    script = (
        "import sys,json; from research_swarm.workers.clock_channel import "
        "Bootstrap,ChannelScope,SupervisorChannel; "
        "from research_swarm.domain.types import new_id; "
        "b=Bootstrap.read(sys.stdin.buffer); s=ChannelScope.model_validate_json(sys.argv[1]); "
        "c=SupervisorChannel(b,s,new_id(),deadline_ns=int(s.caps.launch_ns)); "
        "a=c.request('claim'); z=c.request('release'); "
        "print(json.dumps([a.allowed,z.allowed,a.journal_revision,z.journal_revision]))"
    )
    argv = [sys.executable, "-c", script, bound.model_dump_json()]
    assert server.bootstrap.secret.hex() not in repr(argv)
    assert server.bootstrap.secret.hex() not in repr(server.bootstrap)
    with ThreadPoolExecutor(max_workers=1) as pool:
        served = pool.submit(server.serve, authorize)
        try:
            with bootstrap_stdin(server.bootstrap) as stdin:
                assert not stdin.closed
                assert not os.get_inheritable(stdin.fileno())
                result = subprocess.run(
                    argv,
                    stdin=stdin,
                    capture_output=True,
                    text=True,
                    env=base_environment(),
                    timeout=15,
                )
            assert result.returncode == 0, result.stderr
            assert json.loads(result.stdout) == [True, True, 2, 3]
            served.result(timeout=2)
        finally:
            server.close()
    assert [request.phase for request in requests] == ["claim", "release"]
    assert requests[0].nonce != requests[1].nonce
    assert requests[0].supervisor_id == requests[1].supervisor_id
    assert server._listener.fileno() == -1
    with pytest.raises(ResearchError, match="already consumed"):
        server.serve(authorize)


def test_stalled_unauthenticated_peer_and_wrong_key_cannot_authorize() -> None:
    bound = scope()
    server = OriginServer(bound, deadline_ns=int(bound.caps.launch_ns))
    admitted = []

    def authorize(request: GateRequest) -> GateDecision:
        admitted.append(request)
        return decision(request)

    with ThreadPoolExecutor(max_workers=1) as pool:
        served = pool.submit(server.serve, authorize)
        stalled = socket.create_connection(("127.0.0.1", server.bootstrap.port), timeout=1)
        bad = SupervisorChannel(
            replace(server.bootstrap, secret=b"x" * 32),
            bound,
            new_id(),
            deadline_ns=int(bound.caps.launch_ns),
        )
        try:
            with pytest.raises((EOFError, OSError, ResearchError)):
                bad.request("claim")
            assert admitted == []
            good = SupervisorChannel(
                server.bootstrap, bound, new_id(), deadline_ns=int(bound.caps.launch_ns)
            )
            assert good.request("claim").allowed
            assert good.request("release").allowed
            served.result(timeout=2)
        finally:
            stalled.close()
            bad.close()
            server.close()
    assert len(admitted) == 2


def test_close_between_accept_and_registration_never_authorizes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bound = scope()
    server = OriginServer(bound, deadline_ns=int(bound.caps.launch_ns))
    accepted, resume = threading.Event(), threading.Event()
    accept = socket.socket.accept
    called = []

    def interrupted(listener: socket.socket) -> Any:
        value = accept(listener)
        if listener is server._listener:
            accepted.set()
            assert resume.wait(timeout=2)
        return value

    monkeypatch.setattr(socket.socket, "accept", interrupted)
    with ThreadPoolExecutor(max_workers=1) as pool:
        served = pool.submit(
            server.serve, lambda request: called.append(request) or decision(request)
        )
        with socket.create_connection(
            ("127.0.0.1", server.bootstrap.port), timeout=1
        ) as connection:
            assert accepted.wait(timeout=2)
            server.close()
            resume.set()
            assert connection.recv(1) == b""
        with pytest.raises(ResearchError, match="closed"):
            served.result(timeout=2)
    assert called == []


def test_deep_unauthenticated_json_does_not_consume_legitimate_channel() -> None:
    bound = scope()
    server = OriginServer(bound, deadline_ns=int(bound.caps.launch_ns))
    payload = b'{"payload":' + b"[" * 1500 + b"0" + b"]" * 1500 + b',"mac":"' + b"0" * 64 + b'"}'
    assert len(payload) < channel.MAX_FRAME_BYTES
    with ThreadPoolExecutor(max_workers=1) as pool:
        served = pool.submit(server.serve, decision)
        with socket.create_connection(("127.0.0.1", server.bootstrap.port), timeout=1) as invalid:
            channel._send(invalid, payload, server.deadline_ns)
            assert invalid.recv(1) == b""
        client = SupervisorChannel(
            server.bootstrap, bound, new_id(), deadline_ns=server.deadline_ns
        )
        assert client.request("claim").allowed
        assert client.request("release").allowed
        served.result(timeout=2)


@pytest.mark.parametrize("change", ["scope", "phase", "sequence", "direction"])
def test_wrong_authenticated_request_never_reaches_authorizer(change: str) -> None:
    bound = scope()
    server = OriginServer(bound, deadline_ns=int(bound.caps.launch_ns))
    request = GateRequest(
        scope=bound, supervisor_id=new_id(), sequence=1, phase="claim", nonce=new_id()
    )
    if change == "scope":
        request = request.model_copy(
            update={"scope": bound.model_copy(update={"attempt_id": new_id()})}
        )
    elif change == "phase":
        request = request.model_copy(update={"phase": "release"})
    elif change == "sequence":
        request = request.model_copy(update={"sequence": 2})
    called = []
    with ThreadPoolExecutor(max_workers=1) as pool:
        served = pool.submit(server.serve, lambda value: called.append(value) or decision(value))
        with socket.create_connection(
            ("127.0.0.1", server.bootstrap.port), timeout=1
        ) as connection:
            channel._send(
                connection,
                channel._pack(
                    server.bootstrap, "reply" if change == "direction" else "request", request
                ),
                server.deadline_ns,
            )
            if change == "direction":
                assert connection.recv(1) == b""
                server.close()
        with pytest.raises((OSError, ResearchError)):
            served.result(timeout=2)
    assert called == []


def test_reused_nonce_or_extended_second_permit_is_rejected() -> None:
    for variant in ("nonce", "extension"):
        bound = scope()
        server = OriginServer(bound, deadline_ns=int(bound.caps.launch_ns))
        tightened = bound.caps.model_copy(update={"hard_ns": str(int(bound.caps.hard_ns) - 1)})

        def authorize(
            request: GateRequest, first: CapsRecord = tightened, last: CapsRecord = bound.caps
        ) -> GateDecision:
            return decision(request).model_copy(
                update={"caps": first if request.phase == "claim" else last}
            )

        with ThreadPoolExecutor(max_workers=1) as pool:
            served = pool.submit(server.serve, authorize)
            with socket.create_connection(
                ("127.0.0.1", server.bootstrap.port), timeout=1
            ) as connection:
                request = GateRequest(
                    scope=bound, supervisor_id=new_id(), sequence=1, phase="claim", nonce=new_id()
                )
                channel._send(
                    connection,
                    channel._pack(server.bootstrap, "request", request),
                    server.deadline_ns,
                )
                channel._receive(connection, server.deadline_ns)
                request = request.model_copy(
                    update={
                        "sequence": 2,
                        "phase": "release",
                        "nonce": request.nonce if variant == "nonce" else new_id(),
                    }
                )
                channel._send(
                    connection,
                    channel._pack(server.bootstrap, "request", request),
                    server.deadline_ns,
                )
                assert connection.recv(1) == b""
            with pytest.raises(ResearchError):
                served.result(timeout=2)


def test_origin_loss_after_claim_never_produces_a_release_permit() -> None:
    bound = scope()
    server = OriginServer(bound, deadline_ns=int(bound.caps.launch_ns))
    with ThreadPoolExecutor(max_workers=1) as pool:
        served = pool.submit(server.serve, decision)
        client = SupervisorChannel(
            server.bootstrap, bound, new_id(), deadline_ns=server.deadline_ns
        )
        assert client.request("claim").allowed
        server.close()
        with pytest.raises((OSError, EOFError, ResearchError)):
            client.request("release")
        with pytest.raises((OSError, EOFError, ResearchError)):
            served.result(timeout=2)
        with pytest.raises(ResearchError, match="consumed"):
            client.request("release")


@pytest.mark.parametrize("tamper", ["nonce", "cap"])
def test_authenticated_reply_cannot_change_request_or_caps(
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
) -> None:
    bound = scope()
    server = OriginServer(bound, deadline_ns=int(bound.caps.launch_ns))
    pack = channel._pack

    def changed(bootstrap: Bootstrap, direction: str, value: Any) -> bytes:
        if direction == "reply":
            if tamper == "nonce":
                value = value.model_copy(
                    update={"request": value.request.model_copy(update={"nonce": new_id()})}
                )
            else:
                caps = value.decision.caps.model_copy(
                    update={"hard_ns": str(int(value.decision.caps.hard_ns) + 1)}
                )
                value = value.model_copy(
                    update={"decision": value.decision.model_copy(update={"caps": caps})}
                )
        return pack(bootstrap, direction, value)

    monkeypatch.setattr(channel, "_pack", changed)
    with ThreadPoolExecutor(max_workers=1) as pool:
        served = pool.submit(server.serve, decision)
        client = SupervisorChannel(
            server.bootstrap, bound, new_id(), deadline_ns=server.deadline_ns
        )
        with pytest.raises(ResearchError):
            client.request("claim")
        with pytest.raises((EOFError, OSError, ResearchError)):
            served.result(timeout=2)


def test_denied_claim_closes_channel_and_cannot_be_retried() -> None:
    bound = scope()
    server = OriginServer(bound, deadline_ns=int(bound.caps.launch_ns))
    with ThreadPoolExecutor(max_workers=1) as pool:
        served = pool.submit(
            server.serve, lambda request: decision(request).model_copy(update={"allowed": False})
        )
        client = SupervisorChannel(
            server.bootstrap, bound, new_id(), deadline_ns=server.deadline_ns
        )
        assert not client.request("claim").allowed
        assert client._connection.fileno() == -1
        with pytest.raises(ResearchError, match="consumed"):
            client.request("release")
        served.result(timeout=2)


class Fragmented:
    def __init__(self, connection: socket.socket) -> None:
        self.connection = connection

    def settimeout(self, seconds: float) -> None:
        self.connection.settimeout(seconds)

    def send(self, content: bytes) -> int:
        return self.connection.send(content[:3])

    def recv(self, size: int) -> bytes:
        return self.connection.recv(min(size, 2))


def test_partial_frames_preserve_one_fixed_budget_and_reject_oversized_allocation() -> None:
    deadline = native_clock().continuous_ns() + 2_000_000_000
    left, right = socket.socketpair()
    with left, right, ThreadPoolExecutor(max_workers=1) as pool:
        sent = pool.submit(channel._send, Fragmented(left), b"synthetic" * 100, deadline)
        assert channel._receive(Fragmented(right), deadline) == b"synthetic" * 100
        sent.result(timeout=2)
        left.sendall(struct.pack("!I", channel.MAX_FRAME_BYTES + 1))
        with pytest.raises(ResearchError, match="bound"):
            channel._receive(right, deadline)


def test_suspend_during_fragment_read_cannot_restart_allowance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = 1
    monkeypatch.setattr(channel, "_now", lambda: current)

    class Suspended(Fragmented):
        def recv(self, size: int) -> bytes:
            nonlocal current
            value = super().recv(size)
            current = 2_000_000_000
            return value

    left, right = socket.socketpair()
    with left, right:
        left.sendall(struct.pack("!I", 100))
        with pytest.raises(ResearchError, match="deadline expired"):
            channel._receive(Suspended(right), 1_000_000_000)


def test_ambiguous_bootstrap_is_rejected_without_echoing_capability() -> None:
    original = Bootstrap(1234, new_id(), b"s" * 32)
    assert Bootstrap.read(io.BytesIO(original.encode())) == original
    for content in (
        original.encode() + b" " * 512,
        original.encode().replace(b'"version":1', b'"version":true'),
        b'{"secret":"do-not-echo"}',
    ):
        with pytest.raises(ResearchError) as rejected:
            Bootstrap.read(io.BytesIO(content))
        assert "do-not-echo" not in str(rejected.value)
        assert original.secret.hex() not in str(rejected.value)


def test_late_authorizer_does_not_return_a_fresh_permit(monkeypatch: pytest.MonkeyPatch) -> None:
    bound = scope()
    server = OriginServer(bound, deadline_ns=int(bound.caps.launch_ns))
    entered, release = threading.Event(), threading.Event()

    def authorize(request: GateRequest) -> GateDecision:
        entered.set()
        assert release.wait(timeout=2)
        return decision(request)

    with ThreadPoolExecutor(max_workers=2) as pool:
        served = pool.submit(server.serve, authorize)
        client = SupervisorChannel(
            server.bootstrap, bound, new_id(), deadline_ns=server.deadline_ns
        )
        claimed = pool.submit(client.request, "claim")
        assert entered.wait(timeout=2)
        monkeypatch.setattr(channel, "_now", lambda: server.deadline_ns + 1)
        release.set()
        with pytest.raises((EOFError, OSError, ResearchError)):
            claimed.result(timeout=2)
        with pytest.raises(ResearchError, match="expired"):
            served.result(timeout=2)
