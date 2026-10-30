"""One process-owned authority time domain shared by samples and timed admission."""

from __future__ import annotations

import os
import threading
from pathlib import Path

from research_swarm.control.time_authority import AuthorityTime, ClockAuthorityScope
from research_swarm.domain.clocks import ClockPolicy
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.sql import StoreIdentity
from research_swarm.workers.clocks import NativeClock, native_clock, require_continuity


class AuthorityClock:
    def __init__(self, clock: NativeClock | None = None, policy: ClockPolicy | None = None) -> None:
        self._clock = clock or native_clock()
        self.policy = ClockPolicy.model_validate(policy or ClockPolicy())
        self.epoch = new_id()
        self._anchor = self._clock.sample()
        self._last = self._anchor
        self._profile = self._clock.profile
        self._pid = os.getpid()
        self._invalid = False
        self._lock = threading.Lock()

    def observe(self) -> AuthorityTime:
        # A lock can be inherited while held by a vanished thread after fork.
        if os.getpid() != self._pid:
            raise ResearchError(ErrorCode.STALE_FENCE, "Controller clock was inherited after fork")
        with self._lock:
            if self._invalid:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Controller clock epoch is no longer qualified"
                )
            try:
                now = self._clock.sample()
                if self._clock.profile != self._profile:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Controller clock profile changed")
                require_continuity(self._last, now)
                require_continuity(self._anchor, now)
            except OSError as error:
                self._invalid = True
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Controller clock read failed"
                ) from error
            except ResearchError:
                self._invalid = True
                raise
            self._last = now
            elapsed = now.continuous_ns - self._anchor.continuous_ns
            return AuthorityTime(
                self.epoch,
                (self._anchor.wall_ns + elapsed) // 1_000_000,
                self._clock.profile,
                self.policy,
            )


_clocks: dict[tuple[str, str, str, int, str], AuthorityClock] = {}
_registry_lock = threading.Lock()


def _after_fork() -> None:
    global _registry_lock
    _registry_lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)


def process_authority_clock(
    database_path: Path,
    workspace_id: str,
    controller_id: str,
    control_epoch: int,
    key_id: str,
    policy: ClockPolicy | None = None,
) -> AuthorityClock:
    """Reopening a runtime cannot reset its process clock or invalidation latch.

    Entries intentionally survive runtime collection. A new process has a new
    epoch; an inherited entry is unusable after fork and is never re-anchored.
    """
    return _process_clock(
        str(database_path.resolve()),
        ClockAuthorityScope(workspace_id, controller_id, control_epoch, key_id),
        policy,
    )


def _process_clock(
    location: str, scope: ClockAuthorityScope, policy: ClockPolicy | None = None
) -> AuthorityClock:
    policy = ClockPolicy.model_validate(policy or ClockPolicy())
    identity = (
        location,
        scope.workspace_id,
        scope.controller_id,
        scope.control_epoch,
        scope.signing_key_id,
    )
    with _registry_lock:
        if identity not in _clocks:
            _clocks[identity] = AuthorityClock(policy=policy)
        if _clocks[identity].policy != policy:
            raise ResearchError(ErrorCode.STALE_FENCE, "Controller clock policy is process-frozen")
        return _clocks[identity]


class NativeAuthorityTimeProvider:
    def __init__(self, identity: StoreIdentity, policy: ClockPolicy | None = None) -> None:
        if identity.backend != "sqlite":
            raise ValueError("Native authority time requires native SQLite identity")
        self.authority_identity = StoreIdentity("sqlite", str(Path(identity.location).resolve()))
        self.policy = ClockPolicy.model_validate(policy or ClockPolicy())

    def observe(self, scope: ClockAuthorityScope) -> AuthorityTime:
        # Native clock construction stays lazy for installations using only UTC.
        # The process registry preserves epochs and poisoned clocks across reopen.
        return _process_clock(self.authority_identity.location, scope, self.policy).observe()
