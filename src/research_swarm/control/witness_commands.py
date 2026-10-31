"""Prepared witnessed command and query decisions, including conservative fences."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any, TypeVar

from research_swarm.control.witness_time import WitnessAuthorityTimeProvider, WitnessTurn
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.state.database import Transaction
from research_swarm.state.time_providers import ProviderChanged, ProviderSnapshot, TimeProviders

T = TypeVar("T")


class WitnessExecutionBoundary:
    def __init__(self, registry: TimeProviders, provider: WitnessAuthorityTimeProvider) -> None:
        if registry.store != provider.authority_identity:
            raise ValueError("Witness registry and provider authority differ")
        self.registry, self.provider = registry, provider
        self._turn: WitnessTurn | None = None

    @contextmanager
    def prepare(
        self,
        context: CommandContext,
        expected: ProviderSnapshot,
        name: str,
        payload: Mapping[str, Any],
    ) -> Iterator[WitnessTurn]:
        with self.provider.prepare(self.registry, context, expected, name, payload) as turn:
            self._turn = turn
            try:
                yield turn
            finally:
                self._turn = None

    def invoke(
        self,
        transaction: Transaction,
        *,
        name: str,
        payload: Mapping[str, Any],
        perform: Callable[[], T],
        reject: Callable[[ResearchError], T],
    ) -> T:
        if self._turn is None:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Operation has no prepared witness"
            )
        started = False

        def decision() -> T:
            nonlocal started
            started = True
            return perform()

        try:
            return self._turn.run(transaction, decision, name=name, payload=payload)
        except ResearchError as error:
            if started:
                # A rejection hook or acknowledgment failure must abort the outer
                # transaction. It is not a second opportunity to reject the command.
                raise
            return reject(error)

    def rejected(
        self,
        transaction: Transaction,
        *,
        name: str,
        payload: Mapping[str, Any],
        error: ResearchError,
    ) -> None:
        if not isinstance(error, ProviderChanged):
            return
        if self._turn is None or error.evidence.operation.request != self._turn.request:
            raise ResearchError(ErrorCode.STALE_FENCE, "Provider fence belongs to another turn")
        self.registry.fence(transaction, error.evidence, name=name, payload=payload)


WitnessCommandBoundary = WitnessExecutionBoundary
