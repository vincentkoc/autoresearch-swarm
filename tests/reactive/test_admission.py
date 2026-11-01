from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from research_swarm import ResearchClient
from research_swarm.client import LocalTransport
from research_swarm.control.models import TrialData
from research_swarm.control.workers import AttemptData
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.experiments import StageSpec
from research_swarm.domain.types import new_id
from research_swarm.projects.workload import WorkloadContext
from research_swarm.reactive.admission import training_parameters
from research_swarm.reactive.contracts import NumericValue
from research_swarm.reactive.hook import ReactiveHook
from research_swarm.reactive.journal import DecisionJournal
from research_swarm.runtime import LocalRuntime, write_private
from research_swarm.workers.prepared import PreparedExecution
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.reactive.support import DIGEST, REACTIVE_TOML
from tests.worker_support import dispatch
from tests.workers.test_registered_execution import registration, worker


@pytest.mark.parametrize("kind", ["train", "evaluate", "policy_train"])
def test_only_train_receives_reactive_parameters_and_prepared_binding_is_exact(
    tmp_path: Path, journal: DecisionJournal, kind: str
) -> None:
    stage = StageSpec(id="stage", kind=kind, argv=("python", "work.py"), timeout_ms=1000)
    verified, _ = dispatch(tmp_path, stage=stage)
    contract = journal.inspect().contract
    spec = verified.spec.model_copy(
        update={"spec_digest": None, "extensions": {"reactive": contract.model_dump(mode="json")}}
    ).sealed()
    envelope = verified.envelope.model_copy(update={"spec_digest": spec.spec_digest})
    parameters = training_parameters(spec, stage)
    assert ("reactive" in parameters) == (kind == "train")
    context = WorkloadContext(
        trial_id=envelope.trial_id,
        attempt_id=envelope.attempt_id,
        stage_id=stage.id,
        spec_digest=envelope.spec_digest,
        seed=spec.seed,
        time_budget_ms=100,
        output_root=str(tmp_path),
        report_path=str(tmp_path / "report.json"),
        parameters=parameters,
    ).sealed()
    prepared = PreparedExecution(
        context=context,
        context_path=str(tmp_path / "context.json"),
        executable=sys.executable,
        profile_digest=DIGEST,
        resource_registry=str(tmp_path / "resources.sqlite"),
        machine_claim_id=new_id(),
    )
    prepared.verify(envelope, spec)
    invalid = {} if kind == "train" else {"reactive": contract.model_dump(mode="json")}
    changed = context.model_copy(update={"parameters": invalid, "context_digest": None}).sealed()
    with pytest.raises(ResearchError, match="Reactive workload context"):
        prepared.model_copy(update={"context": changed}).verify(envelope, spec)
    if kind == "train":
        invalid = contract.model_dump(mode="json")
        invalid["controls"][0]["minimum"] = False
        changed = context.model_copy(
            update={"parameters": {"reactive": invalid}, "context_digest": None}
        ).sealed()
        with pytest.raises(ResearchError, match="Reactive workload context"):
            prepared.model_copy(update={"context": changed}).verify(envelope, spec)


def test_hook_uses_the_sealed_context_declaration(tmp_path: Path, journal: DecisionJournal) -> None:
    class Target:
        def read(self) -> tuple[NumericValue, ...]:
            return (NumericValue(name="lr", value=0.1),)

        def apply(self, values: tuple[NumericValue, ...]) -> None:
            raise AssertionError("No application requested")

    original = journal.inspect()
    context = WorkloadContext(
        trial_id=new_id(),
        attempt_id=new_id(),
        stage_id="train",
        spec_digest=DIGEST,
        seed=1,
        time_budget_ms=100,
        output_root=str(tmp_path / "outputs"),
        report_path=str(tmp_path / "report.json"),
        parameters={"reactive": original.contract.model_dump(mode="json")},
    ).sealed()
    changed = context.model_copy(update={"parameters": {}})
    with pytest.raises(ValueError, match="digest mismatch"):
        ReactiveHook.from_context(changed, Target())
    with ReactiveHook.from_context(context, Target()) as hook:
        state = hook.journal.inspect()
        assert state.contract == original.contract
        assert state.binding.context_digest == context.context_digest
        assert state.binding.attempt_id == context.attempt_id


@pytest.mark.parametrize("pipeline", [{"source_reactive_toml": REACTIVE_TOML}], indirect=True)
def test_admitted_contract_survives_restart_and_real_registered_pipeline(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    trial = TrialData.model_validate(pipeline.trial["data"])
    assert "reactive" in trial.spec.extensions
    assert {"timing", "campaign_contract_digest"} <= trial.spec.extensions.keys()
    restarted = ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
    assert (
        restarted.get("trial", pipeline.trial["id"])["data"]["spec"]
        == pipeline.trial["data"]["spec"]
    )
    registered = registration(pipeline, tmp_path)
    executor = worker(pipeline, registered, tmp_path)
    for _ in range(2):
        attempt = executor.claim(pipeline.ready())
        assert executor.execute(attempt["id"])["state"] == "succeeded"
        entry = executor.journal.get(attempt["id"])
        assert entry.prepared is not None
        stage = next(
            stage for stage in trial.spec.stages if stage.id == entry.prepared.context.stage_id
        )
        parameters = entry.prepared.context.parameters
        assert "engine" in parameters
        assert parameters.get("reactive") == (
            trial.spec.extensions["reactive"] if stage.kind == "train" else None
        )


@pytest.mark.parametrize("pipeline", [{"source_reactive_toml": REACTIVE_TOML}], indirect=True)
@pytest.mark.parametrize("tamper", ["missing", "changed", "extra-evaluate"])
def test_interrupted_prepared_marker_reuse_verifies_reactive_binding(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch, tamper: str
) -> None:
    executor = pipeline.worker
    if tamper == "extra-evaluate":
        first = executor.claim(pipeline.ready())
        assert executor.execute(first["id"])["state"] == "succeeded"
    attempt_row = executor.claim(pipeline.ready())
    attempt = AttemptData.model_validate(attempt_row["data"])
    trial = TrialData.model_validate(pipeline.trial["data"])
    original_bind = executor.journal.bind_prepared

    def interrupt(*args: object, **kwargs: object) -> None:
        raise RuntimeError("fixture interrupted after prepared marker")

    monkeypatch.setattr(executor.journal, "bind_prepared", interrupt)
    with pytest.raises(RuntimeError, match="fixture interrupted"):
        executor._prepare(attempt, trial)
    monkeypatch.setattr(executor.journal, "bind_prepared", original_bind)
    marker = executor.root / "work" / attempt.dispatch.attempt_id / "prepared.json"
    prepared = PreparedExecution.model_validate_json(marker.read_bytes())
    parameters = json.loads(json.dumps(prepared.context.parameters))
    if tamper == "missing":
        parameters.pop("reactive")
    elif tamper == "changed":
        parameters["reactive"]["controls"][0]["maximum"] = 2.0
    else:
        parameters["reactive"] = trial.spec.extensions["reactive"]
    context = prepared.context.model_copy(
        update={"parameters": parameters, "context_digest": None}
    ).sealed()
    altered = prepared.model_copy(update={"context": context})
    write_private(marker, altered.model_dump_json().encode())
    write_private(Path(prepared.context_path), context.model_dump_json().encode())
    with pytest.raises(ResearchError, match="Reactive workload context"):
        executor._prepare(attempt, trial)
