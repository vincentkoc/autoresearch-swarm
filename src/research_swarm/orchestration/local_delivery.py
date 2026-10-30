"""Default asyncio delivery with a small private checkpoint and the shared API kernel."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from filelock import FileLock, Timeout
from pydantic import model_validator

from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, VersionedRecord, new_id
from research_swarm.orchestration.delivery import DeliveryKernel, DriverConnection, run_owned_step
from research_swarm.orchestration.delivery_models import (
    BindingReference,
    StartupRequest,
    WorkOutcome,
    WorkReference,
    delivery_id,
)
from research_swarm.runtime import private_state_directory, read_private, write_private


class LocalDeliveryState(VersionedRecord):
    binding: BindingReference
    recovery_id: ID
    pending: WorkReference | None = None

    @model_validator(mode="after")
    def same_owner(self) -> LocalDeliveryState:
        if self.pending is not None and self.pending.binding != self.binding:
            raise ValueError("Pending delivery belongs to another binding")
        return self


@dataclass(frozen=True)
class LocalDeliveryResult:
    state: Literal["idle", "pending", "settled", "recovering", "retired", "terminal", "stopped"]
    outcome: WorkOutcome | None = None


class LocalDeliveryDriver:
    def __init__(self, root: Path, connection: DriverConnection) -> None:
        self.root = root.expanduser().absolute()
        self.binding = connection.binding
        self.delivery = DeliveryKernel({delivery_id(self.binding): connection})
        self._bootstrapped: str | None = None
        with self._locked():
            if (self.root / "delivery.json").exists():
                self._read()
            else:
                if any(path.name != "delivery.lock" for path in self.root.iterdir()):
                    raise ResearchError(ErrorCode.STALE_FENCE, "Delivery checkpoint is missing")
                self._write(LocalDeliveryState(binding=self.binding, recovery_id=new_id()))

    def _read(self) -> LocalDeliveryState:
        state = LocalDeliveryState.model_validate(
            strict_json_loads(read_private(self.root, "delivery.json", limit=16_384))
        )
        if state.binding != self.binding:
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Delivery checkpoint belongs to another binding"
            )
        return state

    def _write(self, state: LocalDeliveryState) -> None:
        write_private(self.root / "delivery.json", canonical_bytes(state.model_dump(mode="json")))

    @contextmanager
    def _locked(self) -> Iterator[None]:
        with private_state_directory(self.root):
            lock = FileLock(self.root / "delivery.lock", timeout=0)
            try:
                lock.acquire()
            except Timeout:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "Another delivery step owns this checkpoint"
                ) from None
            try:
                yield
            finally:
                lock.release()

    def step(self) -> LocalDeliveryResult:
        with self._locked():
            return self._step()

    def _step(self) -> LocalDeliveryResult:
        state = self._read()
        pending = state.pending
        if pending is None:
            if self._bootstrapped != state.recovery_id:
                startup = self.delivery.bootstrap(StartupRequest(self.binding, state.recovery_id))
                if startup == "retired":
                    return LocalDeliveryResult("retired")
                if startup == "expired":
                    self._write(state.model_copy(update={"recovery_id": new_id()}))
                    return LocalDeliveryResult("recovering")
                self._bootstrapped = state.recovery_id
            observed = self.delivery.inspect(self.binding)
            if observed.state != "work":
                return LocalDeliveryResult(observed.state)
            pending = observed.work
            assert pending is not None
            state = state.model_copy(update={"pending": pending})
            self._write(state)
        outcome = (
            self.delivery.execute(pending)
            if pending.kind == "turn"
            else self.delivery.wait(pending)
        )
        if outcome.state == "settled":
            self._write(state.model_copy(update={"pending": None}))
        return LocalDeliveryResult(outcome.state, outcome)

    async def step_async(self) -> LocalDeliveryResult:
        return await run_owned_step(self.step)

    async def run(self, stop: asyncio.Event, *, poll_seconds: float = 0.25) -> LocalDeliveryResult:
        if not 0.01 <= poll_seconds <= 60:
            raise ValueError("Local delivery polling must be between 0.01 and 60 seconds")
        while not stop.is_set():
            result = await self.step_async()
            if result.state in {"retired", "terminal"}:
                return result
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), poll_seconds)
        return LocalDeliveryResult("stopped")
