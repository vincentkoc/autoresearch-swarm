from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ResearchError
from research_swarm.evaluation.statistics import FixedPairedPolicy


@pytest.fixture
def learning_example(monkeypatch):
    root = Path(__file__).resolve().parents[1] / "examples" / "learning_cycle"
    monkeypatch.syspath_prepend(str(root))
    previous = sys.modules.pop("run", None)
    try:
        yield importlib.import_module("run")
    finally:
        sys.modules.pop("run", None)
        if previous is not None:
            sys.modules["run"] = previous


def definition(module):
    return module.Definition.model_validate(
        strict_json_loads((module.HERE / "definition.json").read_bytes())
    )


def test_learning_example_rejects_overlapping_or_impossible_panels(learning_example):
    module = learning_example
    values = definition(module).model_dump(mode="json")
    values["future_probe_seed"] = values["development_seeds"][0]
    with pytest.raises(ValidationError, match="distinct"):
        module.Definition.model_validate(values)
    values = definition(module).model_dump(mode="json")
    values["gate"]["seeds"] = [101]
    with pytest.raises(ValidationError, match="cannot meet"):
        module.Definition.model_validate(values)


@pytest.mark.asyncio
async def test_native_learning_cycle_reopens_after_lost_reply_without_resampling(
    learning_example,
    tmp_path,
    monkeypatch,
):
    if os.environ.get("RESEARCH_BENCHMARK_TESTS") != "1":
        pytest.skip("Explicit empirical CPU qualification")
    module = learning_example
    plan = definition(module).model_copy(
        update={
            "development_seeds": (1,),
            "gate": FixedPairedPolicy(seeds=(101, 102, 103, 104, 105)),
        }
    )
    root, registry = tmp_path / "cycle", tmp_path / "machine.sqlite"
    cycle = module.LearningCycle(root, plan, machine_registry=registry)
    command = cycle.client.command
    lost = False

    def lose(operation, request, **kwargs):
        nonlocal lost
        result = command(operation, request, **kwargs)
        if operation == "learning.start_benchmark" and not lost:
            lost = True
            raise TimeoutError("Lost committed benchmark reply")
        return result

    monkeypatch.setattr(cycle.client, "command", lose)
    with pytest.raises(TimeoutError, match="Lost committed"):
        await cycle.run()
    reopened = module.LearningCycle(root, plan, machine_registry=registry)
    result = await reopened.run()
    assert result["gate"]["observed_pairs"] == 5
    assert len(result["cases"]) == 10
    assert sum(result["trial_states"].values()) == 24
    assert result["attempt_states"] == {"succeeded": 48}
    assert result["all_campaigns_consumed"]["trial_admissions"] == "24"
    assert result["benchmark_consumed"]["trial_admissions"] == "20"
    assert result["training"]["update_error"] <= 1e-12
    assert (result["adoption"] is not None) == (result["gate"]["outcome"] == "promote")
    assert not result["future_probe"]["candidate_executed"]
    expected_revision = 2 if result["adoption"] else 1
    assert result["future_probe"]["binding"]["reference"]["revision"] == expected_revision
    assert all(row["state"] != "pending" for row in reopened.rows("proposal_request"))
    before = {kind: reopened.rows(kind) for kind in ("trial", "attempt", "learning_decision")}
    replay = module.LearningCycle(root, plan, machine_registry=registry)
    assert await replay.run() == result
    assert {kind: replay.rows(kind) for kind in before} == before
    with pytest.raises(ResearchError, match="Frozen qualification inputs differ"):
        module.LearningCycle(
            root, plan.model_copy(update={"training_rate": 1.0}), machine_registry=registry
        )
