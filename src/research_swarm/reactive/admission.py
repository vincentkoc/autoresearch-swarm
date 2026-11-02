"""Bind a protected declaration to the exact training attempt's sealed context."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import JsonValue

from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.experiments import ExperimentSpec, StageSpec
from research_swarm.projects.workload import WorkloadContext
from research_swarm.reactive.contracts import ReactiveContract, RunBinding

if TYPE_CHECKING:
    from research_swarm.reactive.journal import DecisionJournal


def training_parameters(spec: ExperimentSpec, stage: StageSpec) -> dict[str, JsonValue]:
    if "reactive" not in spec.extensions:
        return {}
    contract = ReactiveContract.model_validate(spec.extensions["reactive"])
    return {"reactive": contract.model_dump(mode="json")} if stage.kind == "train" else {}


def verify_parameters(context: WorkloadContext, spec: ExperimentSpec, stage: StageSpec) -> None:
    expected = training_parameters(spec, stage)
    actual = (
        {"reactive": context.parameters["reactive"]} if "reactive" in context.parameters else {}
    )
    if canonical_bytes(actual) != canonical_bytes(expected):
        raise ValueError("Reactive workload context differs from the admitted training contract")


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
