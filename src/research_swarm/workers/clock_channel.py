"""Private bounded clock-origin RPC; possession never replaces journal admission."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import socket
import struct
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from typing import BinaryIO, Literal

from pydantic import Field, TypeAdapter, model_validator

from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Digest, Revision, VersionedRecord, new_id
from research_swarm.domain.workers import WorkerBinding
from research_swarm.workers.clocks import native_clock
from research_swarm.workers.timed_launch import CapsRecord

MAX_FRAME_BYTES = 8192
MAX_BOOTSTRAP_BYTES = 512
POLL_SECONDS = 0.05
Phase = Literal["claim", "release"]


class ChannelScope(VersionedRecord):
    binding: WorkerBinding
    attempt_id: ID
    assignment_digest: Digest
    timing_digest: Digest
    journal_identity: ID
    origin_session_id: ID
    caps: CapsRecord

    @model_validator(mode="after")
    def matching_caps(self) -> ChannelScope:
        if (
            self.caps.assignment_digest != self.assignment_digest
            or self.caps.origin_session_id != self.origin_session_id
        ):
            raise ValueError("Channel caps differ from its authority")
        return self


class GateRequest(VersionedRecord):
    scope: ChannelScope
    supervisor_id: ID
    sequence: int = Field(strict=True, ge=1, le=2)
    phase: Phase
    nonce: ID


class GateDecision(VersionedRecord):
    allowed: bool = Field(strict=True)
    journal_revision: Revision
    caps: CapsRecord


class GateReply(VersionedRecord):
    request: GateRequest
    decision: GateDecision


@dataclass(frozen=True)
class Bootstrap:
    port: int
    channel_id: str
    secret: bytes = field(repr=False)

    def __post_init__(self) -> None:
        TypeAdapter(ID).validate_python(self.channel_id)
        if (
            type(self.port) is not int
            or not 1 <= self.port <= 65535
            or type(self.secret) is not bytes
            or len(self.secret) != 32
        ):
            raise ValueError("Invalid clock bootstrap identity")

    def encode(self) -> bytes:
        content = canonical_bytes(
            {
                "version": 1,
                "port": self.port,
                "channel_id": self.channel_id,
                "secret": self.secret.hex(),
            }
        )
        if len(content) > MAX_BOOTSTRAP_BYTES:
            raise ValueError("Clock bootstrap exceeds its private pipe bound")
        return content

    @classmethod
    def read(cls, stream: BinaryIO) -> Bootstrap:
        """The launcher supplies a preloaded pipe whose writer is already closed."""
        try:
            content = stream.read(MAX_BOOTSTRAP_BYTES + 1)
            if len(content) > MAX_BOOTSTRAP_BYTES:
                raise ValueError("Oversized bootstrap")
            value = strict_json_loads(content)
            if (
                not isinstance(value, dict)
                or set(value) != {"version", "port", "channel_id", "secret"}
                or value["version"] != 1
                or type(value["version"]) is not int
                or type(value["port"]) is not int
                or not 1 <= value["port"] <= 65535
            ):
                raise ValueError("Invalid bootstrap shape")
            channel_id = TypeAdapter(ID).validate_python(value["channel_id"])
            secret = bytes.fromhex(value["secret"])
            if len(secret) != 32 or value["secret"] != secret.hex():
                raise ValueError("Invalid bootstrap capability")
            result = cls(value["port"], channel_id, secret)
            result.encode()
            return result
        except (ValueError, TypeError, ResearchError):
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Invalid private clock bootstrap"
            ) from None


@contextmanager
def bootstrap_stdin(bootstrap: Bootstrap) -> Iterator[BinaryIO]:
    """Preload less than POSIX PIPE_BUF before spawn, then close every writer."""
    content = bootstrap.encode()
    reader, writer = os.pipe()
    try:
        os.set_inheritable(reader, False)
        os.set_inheritable(writer, False)
        if os.write(writer, content) != len(content):
            raise OSError("Incomplete clock bootstrap write")
    except BaseException:
        os.close(reader)
        raise
    finally:
        os.close(writer)
    with os.fdopen(reader, "rb") as stream:
        yield stream


def _now() -> int:
    return native_clock().continuous_ns()


def _remaining(deadline_ns: int) -> float:
    remaining = deadline_ns - _now()
    if remaining <= 0:
        raise ResearchError(ErrorCode.STALE_FENCE, "Clock channel deadline expired")
    return min(POLL_SECONDS, remaining / 1_000_000_000)


def _send(connection: socket.socket, payload: bytes, deadline_ns: int) -> None:
    if not 0 < len(payload) <= MAX_FRAME_BYTES:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Clock frame exceeds its bound")
    pending = memoryview(struct.pack("!I", len(payload)) + payload)
    while pending:
        connection.settimeout(_remaining(deadline_ns))
        try:
            count = connection.send(pending)
        except TimeoutError:
            continue
        if not count:
            raise EOFError("Clock channel closed")
        pending = pending[count:]
    _remaining(deadline_ns)


def _read(connection: socket.socket, size: int, deadline_ns: int) -> bytes:
    content = bytearray()
    while len(content) < size:
        connection.settimeout(_remaining(deadline_ns))
        try:
            part = connection.recv(size - len(content))
        except TimeoutError:
            continue
        if not part:
            raise EOFError("Clock channel closed")
        content.extend(part)
    _remaining(deadline_ns)
    return bytes(content)


def _receive(connection: socket.socket, deadline_ns: int) -> bytes:
    size = struct.unpack("!I", _read(connection, 4, deadline_ns))[0]
    if not 0 < size <= MAX_FRAME_BYTES:
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Clock frame exceeds its bound")
    return _read(connection, size, deadline_ns)


def _authenticate(bootstrap: Bootstrap, direction: str, payload: dict[str, object]) -> bytes:
    return canonical_bytes(
        {
            "domain": "research.clock-channel-v1",
            "direction": direction,
            "channel_id": bootstrap.channel_id,
            "payload": payload,
        }
    )


def _pack(bootstrap: Bootstrap, direction: str, value: VersionedRecord) -> bytes:
    payload = value.model_dump(mode="json")
    signature = hmac.new(
        bootstrap.secret, _authenticate(bootstrap, direction, payload), hashlib.sha256
    ).hexdigest()
    return canonical_bytes({"payload": payload, "mac": signature})


def _unpack(bootstrap: Bootstrap, direction: str, frame: bytes) -> dict[str, object]:
    try:
        return _parse_frame(bootstrap, direction, frame)
    except RecursionError:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Clock frame nesting exceeds its bound"
        ) from None


def _parse_frame(bootstrap: Bootstrap, direction: str, frame: bytes) -> dict[str, object]:
    value = strict_json_loads(frame)
    if (
        not isinstance(value, dict)
        or set(value) != {"payload", "mac"}
        or not isinstance(value["payload"], dict)
        or not isinstance(value["mac"], str)
        or len(value["mac"]) != 64
        or any(char not in "0123456789abcdef" for char in value["mac"])
    ):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Invalid clock frame")
    expected = hmac.new(
        bootstrap.secret, _authenticate(bootstrap, direction, value["payload"]), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(value["mac"], expected):
        raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Clock channel authentication failed")
    return dict(value["payload"])


class OriginServer:
    """One intended supervisor, two exchanges, one fixed admission deadline."""

    def __init__(self, scope: ChannelScope, *, deadline_ns: int) -> None:
        self.scope = ChannelScope.model_validate(scope)
        if self.scope.caps.origin_profile != native_clock().profile:
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock channel native profile differs")
        if type(deadline_ns) is not int or deadline_ns < 0:
            raise ValueError("Channel requires an absolute counter deadline")
        deadline_ns = min(deadline_ns, int(self.scope.caps.launch_ns))
        _remaining(deadline_ns)
        self.deadline_ns = deadline_ns
        self._pid = os.getpid()
        self._used = False
        self._closed = False
        self._use_lock = threading.Lock()
        self._active: socket.socket | None = None
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._listener.set_inheritable(False)
            self._listener.bind(("127.0.0.1", 0))
            self._listener.listen(4)
        except BaseException:
            self._listener.close()
            raise
        self.bootstrap = Bootstrap(
            self._listener.getsockname()[1], new_id(), secrets.token_bytes(32)
        )

    def close(self) -> None:
        if os.getpid() == self._pid:
            with self._use_lock:
                self._closed = True
                active = self._active
        else:
            active = self._active
        self._listener.close()
        if active is not None:
            with suppress(OSError):
                active.shutdown(socket.SHUT_RDWR)
            active.close()

    def serve(self, authorize: Callable[[GateRequest], GateDecision]) -> None:
        if os.getpid() != self._pid:
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock channel origin is no longer current")
        with self._use_lock:
            if self._used or self._closed:
                raise ResearchError(ErrorCode.STALE_FENCE, "Clock channel was already consumed")
            self._used = True
        try:
            for _ in range(8):
                while True:
                    self._listener.settimeout(_remaining(self.deadline_ns))
                    try:
                        connection, _ = self._listener.accept()
                        break
                    except TimeoutError:
                        continue
                with connection:
                    with self._use_lock:
                        if self._closed:
                            raise ResearchError(ErrorCode.STALE_FENCE, "Clock channel is closed")
                        self._active = connection
                    connection.set_inheritable(False)
                    try:
                        first = GateRequest.model_validate(
                            _unpack(
                                self.bootstrap,
                                "request",
                                _receive(connection, min(self.deadline_ns, _now() + 500_000_000)),
                            )
                        )
                    except (OSError, EOFError, ValueError, ResearchError):
                        continue
                    self._exchange(connection, first, authorize)
                    return
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Clock handshake limit exceeded")
        finally:
            self.close()

    def _exchange(
        self,
        connection: socket.socket,
        request: GateRequest,
        authorize: Callable[[GateRequest], GateDecision],
    ) -> None:
        supervisor, seen = request.supervisor_id, set()
        caps, deadline_ns = self.scope.caps.caps(), self.deadline_ns
        for sequence, phase in ((1, "claim"), (2, "release")):
            if (
                request.scope != self.scope
                or request.supervisor_id != supervisor
                or request.sequence != sequence
                or request.phase != phase
                or request.nonce in seen
            ):
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Clock exchange identity or phase differs"
                )
            seen.add(request.nonce)
            if self._closed:
                raise ResearchError(ErrorCode.STALE_FENCE, "Clock channel is closed")
            _remaining(deadline_ns)
            decision = GateDecision.model_validate(authorize(request))
            if (
                decision.caps.origin_session_id != self.scope.origin_session_id
                or decision.caps.assignment_digest != self.scope.assignment_digest
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Clock reply authority differs")
            if caps.intersect(decision.caps.caps()) != decision.caps.caps():
                raise ResearchError(ErrorCode.STALE_FENCE, "Clock reply cannot extend frozen caps")
            caps = decision.caps.caps()
            deadline_ns = min(deadline_ns, caps.launch_ns)
            _send(
                connection,
                _pack(self.bootstrap, "reply", GateReply(request=request, decision=decision)),
                deadline_ns,
            )
            if not decision.allowed or phase == "release":
                return
            request = GateRequest.model_validate(
                _unpack(self.bootstrap, "request", _receive(connection, deadline_ns))
            )


class SupervisorChannel:
    def __init__(
        self, bootstrap: Bootstrap, scope: ChannelScope, supervisor_id: str, *, deadline_ns: int
    ) -> None:
        self.bootstrap, self.scope = bootstrap, ChannelScope.model_validate(scope)
        if self.scope.caps.origin_profile != native_clock().profile:
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock channel native profile differs")
        self._pid = os.getpid()
        self.supervisor_id = TypeAdapter(ID).validate_python(supervisor_id)
        if type(deadline_ns) is not int or deadline_ns < 0:
            raise ValueError("Channel requires an absolute counter deadline")
        self.deadline_ns = min(deadline_ns, int(self.scope.caps.launch_ns))
        self._caps = self.scope.caps.caps()
        self._next = 1
        self._request_lock = threading.Lock()
        self._connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._connection.set_inheritable(False)

    def close(self) -> None:
        self._next = 3
        with suppress(OSError):
            self._connection.shutdown(socket.SHUT_RDWR)
        self._connection.close()

    def request(self, phase: Phase) -> GateDecision:
        if os.getpid() != self._pid:
            raise ResearchError(ErrorCode.STALE_FENCE, "Supervisor channel process changed")
        with self._request_lock:
            return self._request(phase)

    def _request(self, phase: Phase) -> GateDecision:
        if self._next > 2 or phase != ("claim" if self._next == 1 else "release"):
            raise ResearchError(ErrorCode.STALE_FENCE, "Clock channel phase was already consumed")
        request = GateRequest(
            scope=self.scope,
            supervisor_id=self.supervisor_id,
            sequence=self._next,
            phase=phase,
            nonce=new_id(),
        )
        self._next += 1
        try:
            payload = _pack(self.bootstrap, "request", request)
            if request.sequence == 1:
                # Local launch validation precedes claim. Do not consume the
                # origin's bounded unauthenticated handshake window during it.
                self._connection.settimeout(_remaining(self.deadline_ns))
                self._connection.connect(("127.0.0.1", self.bootstrap.port))
            _send(self._connection, payload, self.deadline_ns)
            reply = GateReply.model_validate(
                _unpack(self.bootstrap, "reply", _receive(self._connection, self.deadline_ns))
            )
            if reply.request != request:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Clock reply belongs to another exchange"
                )
            if self._caps.intersect(reply.decision.caps.caps()) != reply.decision.caps.caps():
                raise ResearchError(ErrorCode.STALE_FENCE, "Clock reply cannot extend frozen caps")
            self._caps = reply.decision.caps.caps()
            self.deadline_ns = min(self.deadline_ns, self._caps.launch_ns)
            _remaining(self.deadline_ns)
            if not reply.decision.allowed or phase == "release":
                self._next = 3
                self.close()
            return reply.decision
        except (OSError, EOFError, ValueError, ResearchError):
            self._next = 3
            self.close()
            raise
