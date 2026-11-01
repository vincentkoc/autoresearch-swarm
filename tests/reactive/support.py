from __future__ import annotations

from pathlib import Path

from research_swarm.domain.types import new_id
from research_swarm.reactive.contracts import (
    ApplicationReceipt,
    DecisionProposal,
    DecisionRequest,
    DiagnosticDefinition,
    NumericControl,
    NumericValue,
    Observation,
    ReactiveContract,
    RunBinding,
)
from research_swarm.reactive.journal import DecisionJournal
from research_swarm.reactive.journal_records import ApplicationIntent

DIGEST = "sha256:" + "a" * 64


def create_journal(path: Path) -> DecisionJournal:
    return DecisionJournal.create(
        path,
        RunBinding(
            trial_id=new_id(),
            attempt_id=new_id(),
            stage_id="train",
            spec_digest=DIGEST,
            context_digest=DIGEST,
        ),
        ReactiveContract(
            policy_name="test",
            policy_digest=DIGEST,
            controls=(NumericControl(name="lr", minimum=0.0, maximum=1.0, initial=0.1),),
            diagnostics=(DiagnosticDefinition(name="loss", source="train"),),
            interval_steps=5,
            max_lag_steps=10,
        ),
    )


def request_for(journal: DecisionJournal, step: int = 10, **changes: object) -> DecisionRequest:
    state = journal.inspect()
    return DecisionRequest.model_validate(
        {
            "id": new_id(),
            "sequence": state.decision_count + 1,
            "binding": state.binding,
            "contract_digest": state.contract.digest,
            "policy_digest": state.contract.policy_digest,
            "observation": Observation(
                step=step, diagnostics=(NumericValue(name="loss", value=2.0),)
            ),
            "requested_step": step,
            "control_revision": state.control_revision,
            "current_controls": state.controls,
            **changes,
        }
    )


def response_for(item: DecisionRequest, value: float = 0.2) -> DecisionProposal:
    return DecisionProposal(
        request_id=item.id,
        request_digest=item.digest,
        base_revision=item.control_revision,
        kind="set",
        values=(NumericValue(name="lr", value=value),),
    )


def receipt_for(intent: ApplicationIntent, **changes: object) -> ApplicationReceipt:
    return ApplicationReceipt.model_validate(
        {
            "binding": intent.request.binding,
            "request_id": intent.request.id,
            "request_digest": intent.request.digest,
            "decision_digest": intent.proposal.digest,
            "observed_step": intent.request.observation.step,
            "requested_step": intent.request.requested_step,
            "applied_step": intent.step,
            "base_revision": intent.request.control_revision,
            "new_revision": intent.request.control_revision + 1,
            "before": intent.request.current_controls,
            "after": intent.target,
            "outcome": "kept" if intent.target == intent.request.current_controls else "applied",
            **changes,
        }
    )
