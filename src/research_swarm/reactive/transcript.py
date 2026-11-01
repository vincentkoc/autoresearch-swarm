"""Bounded settled-session snapshots for control-schedule replay, not checkpoints."""

from __future__ import annotations

from pathlib import Path
from typing import Self

from pydantic import Field, model_validator

from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.types import VersionedRecord
from research_swarm.reactive.contracts import (
    ApplicationReceipt,
    proposed_controls,
    validate_request,
)
from research_swarm.reactive.journal import DecisionJournal
from research_swarm.reactive.journal_records import DecisionRecord, JournalEvent, SessionState

MAX_TRANSCRIPT_BYTES = 8 * 1024 * 1024


class ControlTranscript(VersionedRecord):
    session: SessionState
    decisions: tuple[DecisionRecord, ...] = Field(max_length=10_000)

    @model_validator(mode="after")
    def complete_schedule(self) -> Self:
        state = self.session
        if state.status != "closed" or state.outstanding_id or state.inflight_call_request_id:
            raise ValueError("Replay requires a closed session with settled calls")
        if len(self.decisions) != state.decision_count:
            raise ValueError("Transcript is missing decisions")
        if len({record.request.id for record in self.decisions}) != len(self.decisions):
            raise ValueError("Transcript request IDs must be unique")
        controls, revision, last_step, observed = state.contract.initial_controls(), 0, 0, None
        for sequence, record in enumerate(self.decisions, 1):
            request = record.request
            validate_request(state.contract, request)
            if (
                request.sequence != sequence
                or request.binding != state.binding
                or request.current_controls != controls
                or request.control_revision != revision
                or request.requested_step < last_step
                or (
                    observed is not None
                    and request.observation.step < observed + state.contract.interval_steps
                )
            ):
                raise ValueError("Transcript request chain differs from its session")
            observed, last_step = request.observation.step, request.requested_step
            if record.proposal is not None:
                proposed_controls(
                    state.contract, request, record.proposal, step=request.requested_step
                )
            if record.call is not None and record.call.outcome == "started":
                raise ValueError("Transcript contains an unfinished policy call")
            receipt = record.receipt
            if receipt is None:
                if (
                    record.status not in {"failed", "expired", "cancelled"}
                    or record.application_step is not None
                ):
                    raise ValueError("Transcript contains an incomplete application")
                continue
            proposal = record.proposal
            if (
                proposal is None
                or receipt.outcome == "uncertain"
                or record.status != receipt.outcome
            ):
                raise ValueError("Transcript contains an uncertain application")
            target = proposed_controls(state.contract, request, proposal, step=receipt.applied_step)
            if (
                receipt.binding != state.binding
                or receipt.request_id != request.id
                or receipt.request_digest != request.digest
                or receipt.decision_digest != proposal.digest
                or receipt.observed_step != request.observation.step
                or receipt.requested_step != request.requested_step
                or receipt.applied_step != record.application_step
                or receipt.base_revision != revision
                or receipt.before != controls
                or receipt.after != target
                or receipt.outcome != ("kept" if target == controls else "applied")
            ):
                raise ValueError("Transcript receipt differs from its request and proposal")
            controls, revision, last_step = target, revision + 1, receipt.applied_step
        if (
            controls != state.controls
            or revision != state.control_revision
            or last_step != state.last_step
        ):
            raise ValueError("Transcript final controls or step differ from the session")
        if observed != state.last_observed_step:
            raise ValueError("Transcript final observation differs from the session")
        if len(canonical_bytes(self.model_dump(mode="json"))) > MAX_TRANSCRIPT_BYTES:
            raise ValueError("Transcript exceeds the byte limit")
        return self

    @property
    def digest(self) -> str:
        return record_digest("research.reactive-transcript", self.model_dump(mode="json"))

    def applications(self) -> tuple[ApplicationReceipt, ...]:
        return tuple(record.receipt for record in self.decisions if record.receipt is not None)

    @classmethod
    def capture(cls, journal: DecisionJournal) -> ControlTranscript:
        # One read transaction gives a consistent snapshot even if a late reply arrives.
        with journal._connect() as connection:
            state = journal._state(connection)
            records: list[DecisionRecord] = []
            size = len(state.model_dump_json().encode())
            rows = connection.execute(
                "SELECT payload FROM events WHERE sequence IN "
                "(SELECT MAX(sequence) FROM events WHERE request_id IS NOT NULL "
                "GROUP BY request_id)"
            )
            for row in rows:
                size += len(row[0].encode())
                if size > MAX_TRANSCRIPT_BYTES or len(records) >= state.contract.max_decisions:
                    raise ValueError("Transcript exceeds its bounded capture limits")
                record = JournalEvent.model_validate_json(row[0]).decision
                assert record is not None
                records.append(record)
            return cls(
                session=state,
                decisions=tuple(sorted(records, key=lambda item: item.request.sequence)),
            )

    @classmethod
    def read(cls, path: Path) -> ControlTranscript:
        with path.open("rb") as stream:
            payload = stream.read(MAX_TRANSCRIPT_BYTES + 1)
        if len(payload) > MAX_TRANSCRIPT_BYTES:
            raise ValueError("Transcript exceeds the byte limit")
        return cls.model_validate(strict_json_loads(payload))
