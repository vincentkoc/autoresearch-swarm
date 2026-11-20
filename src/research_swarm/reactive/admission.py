"""Bind a protected declaration to the exact training attempt's sealed context."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import JsonValue

from research_swarm.domain.experiments import StageSpec
from research_swarm.reactive.contracts import ReactiveContract, RunBinding
from research_swarm.swarm.projects.workload import WorkloadContext

if TYPE_CHECKING:
    from research_swarm.reactive.journal import DecisionJournal


def training_parameters(
    contract: ReactiveContract | None, stage: StageSpec
) -> dict[str, JsonValue]:
    if contract is None or stage.kind != "train":
        return {}
    return {"reactive": contract.model_dump(mode="json")}


def create_session(context: WorkloadContext, *, path: Path | None = None) -> DecisionJournal:
    from research_swarm.reactive.journal import DecisionJournal

    context = WorkloadContext.model_validate(context)
    context.verify()
    contract = ReactiveContract.model_validate(context.parameters.get("reactive"))
    assert context.context_digest is not None
    binding = RunBinding(
        trial_id=context.trial_id,
        attempt_id=context.attempt_id,
        stage_id=context.stage_id,
        spec_digest=context.spec_digest,
        context_digest=context.context_digest,
    )
    return DecisionJournal.create(
        path or Path(context.output_root) / "reactive.sqlite", binding, contract
    )
