"""Cooperative training boundaries; decision inference never runs in this hook."""

from __future__ import annotations

import asyncio
import os
from collections import deque
from pathlib import Path
from types import TracebackType
from typing import Literal, Protocol, Self

from research_swarm.domain.canonical import MAX_SAFE_INTEGER, canonical_bytes
from research_swarm.swarm.projects.workload import WorkloadContext
from research_swarm.swarm.reactive.admission import context_declaration, create_session
from research_swarm.swarm.reactive.contracts import (
    ApplicationIntent,
    ApplicationReceipt,
    DecisionRequest,
    NumericValue,
    Observation,
    ReactiveContract,
    RunBinding,
    Step,
    ordered,
)
from research_swarm.swarm.reactive.journal import DecisionJournal, JournalConflict
from research_swarm.swarm.reactive.transcript import ControlTranscript
from research_swarm.swarm.records import Digest, VersionedRecord, new_id


class ControlTarget(Protocol):
    """Workload-owned, synchronous, bounded read/apply operations without inference."""

    def read(self) -> tuple[NumericValue, ...]: ...

    def apply(self, values: tuple[NumericValue, ...]) -> None: ...


def _read_controls(target: ControlTarget, label: str) -> tuple[NumericValue, ...]:
    values = target.read()
    if not isinstance(values, tuple) or not 1 <= len(values) <= 32:
        raise ValueError(f"{label} readback must be a bounded tuple")
    return ordered(tuple(NumericValue.model_validate(value) for value in values))


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
            or _read_controls(self.target, "Control") != state.contract.initial_controls()
        ):
            raise JournalConflict(
                "Training hook requires the declared initial controls and a fresh session"
            )
        self.session = journal.trainer()
        self.contract = state.contract
        self._history: deque[Observation] = deque(maxlen=32)
        self._last_step = -1
        self._busy, self._closed, self._uncertain = False, False, False

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
        verified = False
        try:
            before = _read_controls(self.target, "Control")
            if before != intent.request.current_controls:
                detail = "control_drift"
                raise ValueError("Controls changed outside the reactive session")
            if intent.target != before:
                detail = "control_apply_failed"
                self.target.apply(intent.target)
            detail = "control_read_failed"
            after = _read_controls(self.target, "Control")
            if after != intent.target:
                detail = "readback_mismatch"
                raise ValueError("Control readback differs from the requested values")
            verified = True
        except (OSError, RuntimeError, ValueError):
            pass  # A target error can leave a partially applied control.
        finally:
            if not verified:
                self._uncertain = True
                receipt = self._receipt(intent, None)
                self.session.record_application(receipt, detail=detail)
        if not verified:
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


class ReplayReceipt(VersionedRecord):
    source_receipt_digest: Digest
    applied_step: Step
    before: tuple[NumericValue, ...]
    after: tuple[NumericValue, ...] | None
    outcome: Literal["applied", "kept", "uncertain"]


class ReplaySummary(VersionedRecord):
    mode: Literal["control_schedule_replay"] = "control_schedule_replay"
    source_transcript_digest: Digest
    binding: RunBinding
    status: Literal["complete", "incomplete", "uncertain"]
    completed_applications: Step
    expected_applications: Step


class ReplayHook:
    @classmethod
    def from_context(
        cls,
        context: WorkloadContext,
        target: ControlTarget,
        transcript: ControlTranscript,
        *,
        trace_path: Path | None = None,
    ) -> ReplayHook:
        root, binding, contract = context_declaration(context)
        return cls(
            transcript, contract, binding, target, trace_path or root / "reactive-replay.jsonl"
        )

    def __init__(
        self,
        transcript: ControlTranscript,
        contract: ReactiveContract,
        binding: RunBinding,
        target: ControlTarget,
        trace_path: Path,
    ) -> None:
        self.transcript = ControlTranscript.model_validate(transcript)
        self.binding = RunBinding.model_validate(binding)
        self.target = target
        if self.binding == transcript.session.binding:
            raise ValueError("Replay requires a distinct run binding")
        if ReactiveContract.model_validate(contract) != transcript.session.contract:
            raise ValueError("Replay contract differs from the recorded declaration")
        self._controls = contract.initial_controls()
        if _read_controls(self.target, "Replay") != self._controls:
            raise ValueError("Replay requires the declared initial controls")
        self._applications = transcript.applications()
        self._index, self._step = 0, -1
        self._closed, self._uncertain, self._failed = False, False, False
        trace_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._stream = os.fdopen(
            os.open(trace_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb"
        )
        try:
            self._write("started", self.summary().model_dump(mode="json"))
        except BaseException:
            self._stream.close()
            raise

    def _write(self, kind: str, record: object) -> None:
        self._stream.write(canonical_bytes({"kind": kind, "record": record}) + b"\n")
        self._stream.flush()
        os.fsync(self._stream.fileno())

    def boundary(self, step: int) -> tuple[ReplayReceipt, ...]:
        if self._closed or self._uncertain or self._failed:
            raise ValueError("Replay is terminal")
        if type(step) is not int or not self._step < step <= MAX_SAFE_INTEGER:
            raise ValueError("Replay boundaries must be increasing completed-step integers")
        self._step = step
        receipts: list[ReplayReceipt] = []
        while self._index < len(self._applications):
            source = self._applications[self._index]
            if source.applied_step > step:
                break
            if source.applied_step < step:
                self._failed = True
                raise ValueError("Replay skipped a recorded application boundary")
            try:
                self._write("intent", source.model_dump(mode="json"))
            except BaseException:
                self._failed = True
                raise
            verified = False
            try:
                if _read_controls(self.target, "Replay") != self._controls:
                    raise ValueError("Replay controls drifted")
                assert source.after is not None
                if source.after != self._controls:
                    self.target.apply(source.after)
                after = _read_controls(self.target, "Replay")
                if after != source.after:
                    raise ValueError("Replay readback differs from the recorded controls")
                verified = True
            except (OSError, RuntimeError, ValueError):
                pass  # Readback is unknown after a target error.
            finally:
                if not verified:
                    self._uncertain = True
                    receipt = ReplayReceipt(
                        source_receipt_digest=source.digest,
                        applied_step=step,
                        before=self._controls,
                        after=None,
                        outcome="uncertain",
                    )
                    self._write("receipt", receipt.model_dump(mode="json"))
            if not verified:
                return (*receipts, receipt)
            receipt = ReplayReceipt(
                source_receipt_digest=source.digest,
                applied_step=step,
                before=self._controls,
                after=after,
                outcome=source.outcome,
            )
            try:
                self._write("receipt", receipt.model_dump(mode="json"))
            except BaseException:
                self._uncertain = True
                raise
            self._controls, self._index = after, self._index + 1
            receipts.append(receipt)
        return tuple(receipts)

    def summary(self) -> ReplaySummary:
        return ReplaySummary(
            source_transcript_digest=self.transcript.digest,
            binding=self.binding,
            status="uncertain"
            if self._uncertain
            else "complete"
            if self._index == len(self._applications) and not self._failed
            else "incomplete",
            completed_applications=self._index,
            expected_applications=len(self._applications),
        )

    def close(self) -> ReplaySummary:
        summary = self.summary()
        if not self._closed:
            self._closed = True
            try:
                self._write("closed", summary.model_dump(mode="json"))
            except BaseException:
                self._failed = True
                raise
            finally:
                self._stream.close()
        return summary
