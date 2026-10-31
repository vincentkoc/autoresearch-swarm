"""One-use witnessed time inside a checked synchronous operation.

The hosting adapter must serialize all authority operations from before witness
acquisition through commit. This local guard does not supply Durable Object
ordering, network authentication or hosted profile qualification.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, TypeVar

from research_swarm.control.time_authority import AuthorityTime, ClockAuthorityScope
from research_swarm.control.time_witnesses import (
    AuthenticatedSourceMismatch,
    PendingTimeWitness,
    VerifiedTimeWitness,
    WitnessSourceChanged,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.time_witnesses import TimeWitnessRequest
from research_swarm.state.database import Transaction
from research_swarm.state.execution import current_execution
from research_swarm.state.sql import StoreIdentity
from research_swarm.state.time_providers import (
    ProviderSnapshot,
    TimeProviders,
    WitnessOperationBinding,
)
from research_swarm.workers.clocks import NATIVE_CLOCK_PROFILES

HOSTED_WITNESS_PROFILE = "hosted-native-witness-v1"
T = TypeVar("T")


@dataclass(frozen=True)
class _Owner:
    process: int
    thread: int
    task: asyncio.Task[Any] | None


def _owner() -> _Owner:
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return _Owner(os.getpid(), threading.get_ident(), task)


class WitnessAuthorityTimeProvider:
    def __init__(self, identity: StoreIdentity) -> None:
        self._identity = identity
        self._process = os.getpid()
        self._lock = threading.Lock()
        self._turn: WitnessTurn | None = None
        self._active: tuple[WitnessTurn, object | None, AuthorityTime] | None = None

    @property
    def authority_identity(self) -> StoreIdentity:
        return self._identity

    @contextmanager
    def prepare(
        self,
        registry: TimeProviders,
        context: CommandContext,
        expected: ProviderSnapshot,
        name: str,
        payload: Mapping[str, Any],
    ) -> Iterator[WitnessTurn]:
        if os.getpid() != self._process or registry.store != self.authority_identity:
            raise ResearchError(ErrorCode.STALE_FENCE, "Witness operation authority differs")
        if not self._lock.acquire(blocking=False):
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Another witness turn is active")
        turn = None
        try:
            binding, pending = registry.prepare(context, expected, name, payload)
            turn = WitnessTurn(self, registry, binding, pending)
            self._turn = turn
            yield turn
        finally:
            if turn is not None:
                turn._close()
            self._active = None
            self._turn = None
            self._lock.release()

    def observe(self, scope: ClockAuthorityScope) -> AuthorityTime:
        active = self._active
        if active is None:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "No active witnessed operation")
        turn, execution, observation = active
        turn._require_owner()
        authority = turn.request.scope
        expected = ClockAuthorityScope(
            authority.workspace_id,
            authority.controller_id,
            authority.control_epoch,
            authority.assignment_key_id,
        )
        if turn._state != "executing" or current_execution() is not execution or scope != expected:
            raise ResearchError(ErrorCode.STALE_FENCE, "Witness time belongs to another operation")
        return observation


class WitnessTurn:
    """Created only by provider.prepare; no initializer accepts a verified timestamp."""

    def __init__(
        self,
        provider: WitnessAuthorityTimeProvider,
        registry: TimeProviders,
        binding: WitnessOperationBinding,
        pending: PendingTimeWitness,
    ) -> None:
        self._provider, self._registry = provider, registry
        self._binding, self._pending = binding, pending
        self._owner = _owner()
        self._state = "awaiting"
        self._proof: VerifiedTimeWitness | AuthenticatedSourceMismatch | None = None

    @property
    def request(self) -> TimeWitnessRequest:
        return self._pending.request

    def _require_owner(self) -> None:
        if self._owner != _owner() or self._provider._turn is not self or self._state == "closed":
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Witness turn is closed or has another owner"
            )

    def _close(self) -> None:
        self._state = "closed"
        self._proof = None

    def complete(self, data: bytes) -> None:
        self._require_owner()
        if self._state != "awaiting":
            self._close()
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Witness response was consumed")
        try:
            self._proof = self._pending.complete(data)
        except WitnessSourceChanged as error:
            # Preserve authenticated evidence for the transactional rejection hook.
            self._proof = error.evidence
        except BaseException:
            self._close()
            raise
        self._state = "verified"

    def run(
        self,
        transaction: Transaction,
        callback: Callable[[], T],
        *,
        name: str,
        payload: Mapping[str, Any],
    ) -> T:
        self._require_owner()
        if self._state != "verified" or self._proof is None:
            self._close()
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Witness is not ready for execution")
        proof = self._proof
        self._state = "executing"
        try:
            self._registry.check(transaction, self._binding, proof, name=name, payload=payload)
            assert isinstance(proof, VerifiedTimeWitness)
            if proof.source.native_profile not in NATIVE_CLOCK_PROFILES:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Native witness profile is unsupported"
                )
            observation = AuthorityTime(
                proof.epoch,
                proof.witness.payload.authority_time_ms,
                HOSTED_WITNESS_PROFILE,
                proof.source.policy,
            )
            self._provider._active = (self, current_execution(), observation)
            result = callback()
            if (
                inspect.isawaitable(result)
                or inspect.isgenerator(result)
                or inspect.isasyncgen(result)
            ):
                if inspect.iscoroutine(result) or inspect.isgenerator(result):
                    result.close()
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Witness operation must be synchronous"
                )
            return result
        finally:
            self._provider._active = None
            self._close()
