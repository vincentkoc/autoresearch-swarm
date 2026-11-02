"""Durable decision-call claims, independent of training-process ownership."""

from __future__ import annotations

from typing import Literal

from research_swarm.domain.types import new_id, utc_now
from research_swarm.reactive.contracts import DecisionProposal, proposed_controls
from research_swarm.reactive.journal import DecisionJournal, JournalConflict
from research_swarm.reactive.journal_records import DecisionCall, DecisionRecord


def start_call(journal: DecisionJournal, request_id: str, policy_digest: str) -> DecisionCall:
    with journal._connect(write=True) as connection:
        state = journal._state(connection)
        record = journal._decision(connection, request_id)
        if (
            state.status != "active"
            or state.outstanding_id != request_id
            or record is None
            or record.status != "pending"
        ):
            raise JournalConflict("Decision is no longer pending")
        if policy_digest != state.contract.policy_digest:
            raise JournalConflict("Decision policy differs from the admitted fingerprint")
        if record.call is not None or state.inflight_call_request_id is not None:
            raise JournalConflict("A decision call is already recorded or still in flight")
        call = DecisionCall(
            id=new_id(),
            policy_digest=policy_digest,
            started_at=utc_now(),
            timeout_ms=state.contract.decision_timeout_ms,
        )
        updated = DecisionRecord.model_validate(record.model_dump() | {"call": call})
        journal._append(
            connection, state, "call_started", updated, inflight_call_request_id=request_id
        )
        return call


def fence_call(
    journal: DecisionJournal, request_id: str, call_id: str, reason: Literal["timeout", "cancelled"]
) -> DecisionRecord:
    if reason not in {"timeout", "cancelled"}:
        raise ValueError("Invalid call fence")
    with journal._connect(write=True) as connection:
        state = journal._state(connection)
        record = journal._decision(connection, request_id)
        if record is None or record.call is None or record.call.id != call_id:
            raise JournalConflict("Decision call identity changed")
        if record.call.outcome != "started" or record.call.fence is not None:
            return record
        call = DecisionCall.model_validate(record.call.model_dump() | {"fence": reason})
        changes: dict[str, object] = {"call": call}
        outstanding = state.outstanding_id
        if record.status == "pending":
            changes.update(
                status="expired" if reason == "timeout" else "cancelled",
                detail="decision_" + reason,
            )
            if outstanding == request_id:
                outstanding = None
        updated = DecisionRecord.model_validate(record.model_dump() | changes)
        journal._append(connection, state, "call_fenced", updated, outstanding_id=outstanding)
        return updated


def finish_call(
    journal: DecisionJournal,
    request_id: str,
    call_id: str,
    *,
    proposal: DecisionProposal | None = None,
    failure: Literal["exception", "invalid_response", "cancelled"] | None = None,
) -> DecisionRecord:
    if (proposal is None) == (failure is None):
        raise ValueError("A call needs either a proposal or a failure code")
    with journal._connect(write=True) as connection:
        state = journal._state(connection)
        record = journal._decision(connection, request_id)
        if record is None or record.call is None or record.call.id != call_id:
            raise JournalConflict("Decision call identity changed")
        if proposal is not None:
            proposal = DecisionProposal.model_validate(proposal)
            proposed_controls(
                state.contract, record.request, proposal, step=record.request.requested_step
            )
        if record.call.outcome != "started":
            if record.call.proposal != proposal or record.call.failure_code != failure:
                raise JournalConflict("Decision call completion differs from recorded evidence")
            return record
        call_changes: dict[str, object] = {
            "outcome": "returned"
            if proposal is not None
            else "cancelled"
            if failure == "cancelled"
            else "failed",
            "completed_at": utc_now(),
            "proposal": proposal,
            "failure_code": failure,
        }
        changes: dict[str, object] = {}
        outstanding = state.outstanding_id
        if proposal is not None:
            if record.proposal is not None:
                call_changes["disposition"] = (
                    "duplicate" if record.proposal == proposal else "conflict"
                )
            else:
                accepted = (
                    state.status == "active"
                    and record.status == "pending"
                    and record.call.fence is None
                )
                if accepted and (
                    state.outstanding_id != request_id
                    or record.request.binding != state.binding
                    or record.request.control_revision != state.control_revision
                    or record.request.current_controls != state.controls
                ):
                    raise JournalConflict("Decision call differs from current run or control state")
                disposition = "accepted" if accepted else "late"
                call_changes["disposition"] = disposition
                changes.update(proposal=proposal, response_disposition=disposition)
                if accepted:
                    changes["status"] = "responded"
        elif record.status == "pending":
            changes.update(
                status="cancelled" if failure == "cancelled" else "failed",
                detail="decision_" + str(failure),
            )
            if outstanding == request_id:
                outstanding = None
        changes["call"] = DecisionCall.model_validate(record.call.model_dump() | call_changes)
        updated = DecisionRecord.model_validate(record.model_dump() | changes)
        journal._append(
            connection,
            state,
            "call_finished",
            updated,
            outstanding_id=outstanding,
            inflight_call_request_id=None
            if state.inflight_call_request_id == request_id
            else state.inflight_call_request_id,
        )
        return updated
