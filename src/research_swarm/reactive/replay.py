"""Replay recorded numeric applications at exact completed-update boundaries."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from research_swarm.domain.canonical import MAX_SAFE_INTEGER, canonical_bytes
from research_swarm.domain.types import Digest, VersionedRecord
from research_swarm.projects.workload import WorkloadContext
from research_swarm.reactive.contracts import (
    NumericValue,
    ReactiveContract,
    RunBinding,
    Step,
    ordered,
)
from research_swarm.reactive.hook import ControlTarget
from research_swarm.reactive.transcript import ControlTranscript


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
        context = WorkloadContext.model_validate(context)
        context.verify()
        assert context.context_digest is not None
        return cls(
            transcript,
            ReactiveContract.model_validate(context.parameters.get("reactive")),
            RunBinding(
                trial_id=context.trial_id,
                attempt_id=context.attempt_id,
                stage_id=context.stage_id,
                spec_digest=context.spec_digest,
                context_digest=context.context_digest,
            ),
            target,
            trace_path or Path(context.output_root) / "reactive-replay.jsonl",
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
        if self._read() != self._controls:
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

    def _read(self) -> tuple[NumericValue, ...]:
        values = self.target.read()
        if not isinstance(values, tuple) or not 1 <= len(values) <= 32:
            raise ValueError("Replay readback must be a bounded tuple")
        return ordered(tuple(NumericValue.model_validate(value) for value in values))

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
            try:
                if self._read() != self._controls:
                    raise ValueError("Replay controls drifted")
                assert source.after is not None
                if source.after != self._controls:
                    self.target.apply(source.after)
                after = self._read()
                if after != source.after:
                    raise ValueError("Replay readback differs from the recorded controls")
            except BaseException as error:
                self._uncertain = True
                receipt = ReplayReceipt(
                    source_receipt_digest=source.digest,
                    applied_step=step,
                    before=self._controls,
                    after=None,
                    outcome="uncertain",
                )
                self._write("receipt", receipt.model_dump(mode="json"))
                if not isinstance(error, Exception):
                    raise
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
