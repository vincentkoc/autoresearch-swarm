"""Reader ownership is separate from leases and storage generations."""

import inspect
from collections.abc import Callable
from typing import Protocol

from pydantic import Field

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import Record, new_id
from research_swarm.state.database import JsonObject
from research_swarm.state.process_identity import LocalProcessIdentity


class ReaderOwner(Record):
    profile: str = Field(pattern=r"^[a-z][a-z0-9-]{0,63}$")
    identity: str = Field(min_length=1, max_length=4096)


class ReaderOwnership(Protocol):
    def capture(self) -> ReaderOwner:
        """Capture one immutable owner incarnation without acquiring artifact pins."""
        ...

    def require_closed(self, owner: ReaderOwner) -> None:
        """Prove this exact owner cannot resume reading; fail when proof is unavailable.

        Closure is permanent. Provider replacement, expiry and controller restart
        do not establish closure. Implementations must refuse foreign profiles.
        """
        ...


def _synchronous(operation: Callable[[], object]) -> object:
    value = operation()
    if inspect.isawaitable(value):
        if inspect.iscoroutine(value):
            value.close()
        raise RuntimeError("Reader ownership must complete synchronously")
    return value


def capture_owner(ownership: ReaderOwnership) -> ReaderOwner:
    result = _synchronous(ownership.capture)
    if not isinstance(result, ReaderOwner):
        raise TypeError("Reader ownership must return an immutable ReaderOwner")
    return result


def require_owner_closed(ownership: ReaderOwnership, owner: ReaderOwner) -> None:
    result = _synchronous(lambda: ownership.require_closed(owner))
    if result is not None:
        raise TypeError("Reader closure must return None after proving closure")


def decode_owner(data: JsonObject) -> ReaderOwner:
    if set(data) == {"host", "pid", "created_at_ms"}:
        # Existing local ledgers remain readable; no rewrite or provider fallback.
        identity = LocalProcessIdentity.model_validate(data)
        return ReaderOwner(profile="local-process-v1", identity=identity.model_dump_json())
    return ReaderOwner.model_validate(data)


class NativeReaderOwnership:
    def capture(self) -> ReaderOwner:
        return ReaderOwner(
            profile="local-process-v1", identity=LocalProcessIdentity.current().model_dump_json()
        )

    def require_closed(self, owner: ReaderOwner) -> None:
        if owner.profile != "local-process-v1":
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Reader ownership is not native")
        LocalProcessIdentity.model_validate_json(owner.identity).require_closed()


class OpaqueReaderOwnership:
    """Portable pin custody without an unqualified orphan-recovery claim."""

    def capture(self) -> ReaderOwner:
        return ReaderOwner(profile="opaque-incarnation-v1", identity=new_id())

    def require_closed(self, owner: ReaderOwner) -> None:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "This runtime cannot prove reader incarnation closure"
        )
