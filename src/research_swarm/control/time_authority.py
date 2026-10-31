"""Trusted synchronous time observations, independent of a controller runtime."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from research_swarm.domain.clocks import ClockPolicy
from research_swarm.state.sql import StoreIdentity


@dataclass(frozen=True)
class AuthorityTime:
    epoch: str
    milliseconds: int
    profile: str
    policy: ClockPolicy

    @property
    def datetime(self) -> datetime:
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=self.milliseconds)


@dataclass(frozen=True)
class ClockAuthorityScope:
    workspace_id: str
    controller_id: str
    control_epoch: int
    signing_key_id: str


class AuthorityTimeProvider(Protocol):
    @property
    def authority_identity(self) -> StoreIdentity: ...

    def observe(self, scope: ClockAuthorityScope) -> AuthorityTime:
        """Return operation-qualified time synchronously without network or storage I/O.

        Scope derives from verified controller and signing authority. Providers
        preserve their epoch/invalidation semantics; they never authorize a worker.
        Native providers sample fresh time. A serialized witness provider returns
        its current operation's fixed linearization instant and refuses use outside
        that operation. Its adapter must qualify the complete ordering contract.
        """
        ...
