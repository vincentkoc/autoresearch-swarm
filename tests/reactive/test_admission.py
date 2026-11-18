from pathlib import Path

import pytest

from research_swarm.domain.experiments import StageSpec
from research_swarm.domain.types import new_id
from research_swarm.projects.workload import WorkloadContext
from research_swarm.reactive.admission import training_parameters
from research_swarm.reactive.contracts import NumericValue
from research_swarm.reactive.hook import ReactiveHook
from research_swarm.reactive.journal import DecisionJournal
from tests.reactive.support import DIGEST


@pytest.mark.parametrize("kind", ["train", "evaluate", "policy_train"])
def test_only_train_receives_reactive_parameters(journal: DecisionJournal, kind: str) -> None:
    stage = StageSpec(id="step", kind=kind, argv=("python", "work.py"))
    contract = journal.inspect().contract
    assert training_parameters(contract, stage) == (
        {"reactive": contract.model_dump(mode="json")} if kind == "train" else {}
    )
    assert training_parameters(None, stage) == {}


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
