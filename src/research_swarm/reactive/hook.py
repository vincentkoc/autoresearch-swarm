"""Cooperative training boundaries; decision inference never runs in this hook."""

from __future__ import annotations

import asyncio
from collections import deque
from pathlib import Path
from types import TracebackType
from typing import Literal, Protocol, Self

from research_swarm.domain.canonical import MAX_SAFE_INTEGER, canonical_bytes
from research_swarm.domain.types import VersionedRecord, new_id
from research_swarm.projects.workload import WorkloadContext
from research_swarm.reactive.admission import create_session
from research_swarm.reactive.contracts import (
    ApplicationReceipt,
    DecisionRequest,
    NumericValue,
    Observation,
    ordered,
)
from research_swarm.reactive.journal import DecisionJournal, JournalConflict
from research_swarm.reactive.journal_records import ApplicationIntent


class ControlTarget(Protocol):
    """Workload-owned, synchronous, bounded read/apply operations without inference."""

    def read(self) -> tuple[NumericValue, ...]: ...

    def apply(self, values: tuple[NumericValue, ...]) -> None: ...


class BoundaryResult(VersionedRecord):
    status: Literal[
        "idle", "waiting", "requested", "applied", "kept", "expired", "uncertain", "closed"
    ]
    request: DecisionRequest | None = None
    receipt: ApplicationReceipt | None = None


class ReactiveHook:
    @classmethod
    def from_context(
        cls, context: WorkloadContext, target: ControlTarget, *, session_path: Path | None = None
    ) -> ReactiveHook:
        """Use the admitted declaration; no caller-supplied contract override is accepted."""
        return cls(create_session(context, path=session_path), target)

    def __init__(self, journal: DecisionJournal, target: ControlTarget) -> None:
        self.journal, self.target = journal, target
        state = journal.inspect()
        if (
            state.decision_count
            or state.control_revision
            or self._read() != state.contract.initial_controls()
        ):
            raise JournalConflict(
                "Training hook requires the declared initial controls and a fresh session"
            )
        self.session = journal.trainer()
        self.contract = state.contract
        self._history: deque[Observation] = deque(maxlen=32)
        self._last_step = -1
        self._busy, self._closed, self._uncertain = False, False, False

    def _read(self) -> tuple[NumericValue, ...]:
        values = self.target.read()
        if not isinstance(values, tuple) or not 1 <= len(values) <= 32:
            raise ValueError("Control readback must be a bounded tuple")
        return ordered(tuple(NumericValue.model_validate(value) for value in values))

    async def boundary(
        self, observation: Observation, *, step: int | None = None
    ) -> BoundaryResult:
        """Observe completed updates, poll once, and return without waiting for a decision."""
        if self._closed:
            return BoundaryResult(status="closed")
        if self._uncertain:
            return BoundaryResult(status="uncertain")
        if self._busy:
            raise JournalConflict("Concurrent training boundaries are not supported")
        observation = Observation.model_validate(observation)
        step = observation.step if step is None else step
        if (
            type(step) is not int
            or not max(self._last_step + 1, observation.step) <= step <= MAX_SAFE_INTEGER
        ):
            raise ValueError("Training boundaries require increasing completed-step integers")
        if {value.name for value in observation.diagnostics} - {
            item.name for item in self.contract.diagnostics
        }:
            raise ValueError("Observation contains an undeclared diagnostic")
        self._busy = True
        try:
            # Give cooperative inference tasks I/O time without waiting for their results.
            await asyncio.sleep(0)
            if self._closed:
                return BoundaryResult(status="closed")
            self._last_step = step
            pending = self.journal.pending()
            if pending is not None:
                if step - pending.request.observation.step > self.contract.max_lag_steps:
                    self.session.settle(
                        pending.request.id, "expired", detail="observation_step_limit"
                    )
                    return BoundaryResult(status="expired")
                if pending.status == "responded":
                    return self._apply(
                        self.session.claim_application(pending.request.id, step=step)
                    )
                return BoundaryResult(status="waiting")
            state = self.journal.inspect()
            due = (
                self.contract.interval_steps
                if state.last_observed_step is None
                else state.last_observed_step + self.contract.interval_steps
            )
            if observation.step < due or state.decision_count >= self.contract.max_decisions:
                return BoundaryResult(status="idle")
            history = tuple(item for item in self._history if item.step < observation.step)
            request = DecisionRequest(
                id=new_id(),
                sequence=state.decision_count + 1,
                binding=state.binding,
                contract_digest=self.contract.digest,
                policy_digest=self.contract.policy_digest,
                observation=observation,
                requested_step=step,
                control_revision=state.control_revision,
                current_controls=state.controls,
                history=history,
            )
            while (
                request.history
                and len(canonical_bytes(request.model_dump(mode="json")))
                > self.contract.max_prompt_bytes
            ):
                request = request.model_copy(update={"history": request.history[1:]})
            self.session.request(request)
            self._history.append(observation)
            return BoundaryResult(status="requested", request=request)
        finally:
            self._busy = False

    def _apply(self, intent: ApplicationIntent) -> BoundaryResult:
        detail = "control_read_failed"
        try:
            before = self._read()
            if before != intent.request.current_controls:
                detail = "control_drift"
                raise ValueError("Controls changed outside the reactive session")
            if intent.target != before:
                detail = "control_apply_failed"
                self.target.apply(intent.target)
            detail = "control_read_failed"
            after = self._read()
            if after != intent.target:
                detail = "readback_mismatch"
                raise ValueError("Control readback differs from the requested values")
        except BaseException as error:
            receipt = self._receipt(intent, None)
            self._uncertain = True
            self.session.record_application(receipt, detail=detail)
            if not isinstance(error, Exception):
                raise
            return BoundaryResult(status="uncertain", receipt=receipt)
        receipt = self._receipt(intent, after)
        # A failed journal write after mutation leaves a durable applying intent.
        # Fence this hook immediately; ownership recovery must not replay it.
        try:
            self.session.record_application(receipt)
        except BaseException:
            self._uncertain = True
            raise
        return BoundaryResult(
            status="kept" if receipt.outcome == "kept" else "applied", receipt=receipt
        )

    @staticmethod
    def _receipt(
        intent: ApplicationIntent, after: tuple[NumericValue, ...] | None
    ) -> ApplicationReceipt:
        before = intent.request.current_controls
        return ApplicationReceipt(
            binding=intent.request.binding,
            request_id=intent.request.id,
            request_digest=intent.request.digest,
            decision_digest=intent.proposal.digest,
            observed_step=intent.request.observation.step,
            requested_step=intent.request.requested_step,
            applied_step=intent.step,
            base_revision=intent.request.control_revision,
            new_revision=intent.request.control_revision + 1 if after is not None else None,
            before=before,
            after=after,
            outcome="uncertain" if after is None else "kept" if before == after else "applied",
        )

    def close(self) -> None:
        """Fence pending work locally without waiting for a decision provider."""
        self._closed = True
        self.session.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
