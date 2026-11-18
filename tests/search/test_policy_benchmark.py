from __future__ import annotations

import importlib
import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from research_swarm.control.commands import PauseCampaign
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ResearchError
from tests.paths import REPO_ROOT


@pytest.fixture
def benchmark_modules(monkeypatch: pytest.MonkeyPatch):
    root = REPO_ROOT / "examples" / "policy_benchmark"
    monkeypatch.syspath_prepend(str(root))
    old = {name: sys.modules.pop(name, None) for name in ("contract", "session", "run")}
    try:
        yield importlib.import_module("contract"), importlib.import_module("run")
    finally:
        for name, module in old.items():
            sys.modules.pop(name, None)
            if module is not None:
                sys.modules[name] = module


def definition(contract):
    return contract.Definition(
        repeats=1,
        no_op_panels=1,
        cases=(
            contract.Case(name="dev", split="development", seed=101),
            contract.Case(name="holdout", split="held_out", seed=1001),
        ),
    )


def test_benchmark_freezes_disjoint_starting_states_and_preserves_incomplete_cells(
    benchmark_modules,
):
    contract, _ = benchmark_modules
    plan = definition(contract)
    with pytest.raises(ValidationError, match="overlap"):
        contract.Definition(cases=(plan.cases[0], plan.cases[1].model_copy(update={"seed": 102})))
    with pytest.raises(ValidationError, match="held-out"):
        contract.Definition(
            cases=(plan.cases[0], plan.cases[0].model_copy(update={"name": "dev2", "seed": 301}))
        )
    result = contract.summarize(
        [
            {"split": "held_out", "policy": "greedy", "complete": True, "improvement": 1.0},
            {"split": "held_out", "policy": "greedy", "complete": False, "improvement": None},
        ],
        [{"complete": True, "outcome": "reject"}, {"complete": False, "outcome": "blocked"}],
    )
    assert result["policies"][0]["mean_observed_improvement"] is None
    assert result["no_op"]["incomplete_panels"] == 1
    assert contract.frequency_interval(0, 3)["interval_95"][1] > 0.5
    assert contract.frequency_interval(0, 0)["rate"] is None


@pytest.mark.asyncio
async def test_real_benchmark_survives_lost_child_reply_and_reopens_without_new_trials(
    benchmark_modules,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    if os.environ.get("RESEARCH_BENCHMARK_TESTS") != "1":
        pytest.skip("Explicit empirical CPU qualification")
    contract, runner = benchmark_modules
    plan = (
        contract.Definition.model_validate(
            strict_json_loads((runner.HERE / "benchmark.json").read_bytes())
        )
        if os.environ.get("RESEARCH_BENCHMARK_FULL") == "1"
        else definition(contract)
    )
    root, registry = tmp_path / "benchmark", tmp_path / "machine" / "resources.sqlite"
    benchmark = runner.Benchmark(root, plan, machine_registry=registry)
    original = benchmark.client.command
    lost = False

    def lose(operation, request, **kwargs):
        nonlocal lost
        result = original(operation, request, **kwargs)
        if operation == "campaign.create_child" and not lost:
            lost = True
            raise TimeoutError("Fixture child acknowledgment lost after commit")
        return result

    monkeypatch.setattr(benchmark.client, "command", lose)
    with pytest.raises(TimeoutError):
        await benchmark.run()
    resumed = runner.Benchmark(root, plan, machine_registry=registry)
    result = await resumed.run()
    assert result["complete"], result
    assert len(result["cells"]) == len(plan.cells())
    assert len(result["no_op_panels"]) == plan.no_op_panels
    for cell in result["cells"]:
        assert len(cell["trial_outcomes"]) == 4
        assert cell["audit_evaluation_id"] != cell["frozen_selection"]["selection_evaluation_id"]
        assert all(item["eligible"] for item in cell["receipts"])
        assert all(
            Decimal(item["reserved"]) == Decimal(item["uncertain"]) == 0 for item in cell["budget"]
        )
    panel = result["no_op_panels"][0]
    assert len(panel["pairs"]) == 5
    assert all(set(pair["values"]) == {"candidate", "comparator"} for pair in panel["pairs"])
    digest = result.pop("result_digest")
    assert digest == record_digest("research.example-policy-benchmark-result", result)
    result["result_digest"] = digest
    count = len(resumed.client.list(RecordQuery(kind="trial", limit=100))["items"])
    assert count == len(plan.cells()) * 4 + plan.no_op_panels * 11
    replay = runner.Benchmark(root, plan, machine_registry=registry)
    assert await replay.run() == result
    assert len(replay.client.list(RecordQuery(kind="trial", limit=100))["items"]) == count
    with pytest.raises(ResearchError, match="Frozen benchmark"):
        runner.Benchmark(
            root, plan.model_copy(update={"repeats": plan.repeats + 1}), machine_registry=registry
        )
    print(
        {
            "cells": len(plan.cells()),
            "trials": count,
            "no_op": result["summary"]["no_op"],
            "policy_results": result["summary"]["policies"],
        }
    )


def test_interrupted_preparation_is_atomic_and_changed_source_is_rejected(
    benchmark_modules,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    contract, runner = benchmark_modules
    benchmark = runner.Benchmark(
        tmp_path / "run",
        definition(contract),
        machine_registry=tmp_path / "pool" / "resources.sqlite",
    )
    write = Path.write_bytes

    def fail(path, data):
        if path.name == "common.py":
            raise OSError("Fixture interrupted source preparation")
        return write(path, data)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "write_bytes", fail)
        with pytest.raises(OSError, match="interrupted"):
            benchmark.source(0.0)
    assert not (benchmark.root / "source-0.0").exists()
    source = benchmark.source(0.0)
    (source / "train.py").write_text("altered")
    with pytest.raises(ResearchError, match="source differs"):
        benchmark.source(0.0)


@pytest.mark.asyncio
async def test_failed_native_baseline_is_incomplete_and_report_refreshes_after_progress(
    benchmark_modules,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    contract, runner = benchmark_modules
    benchmark = runner.Benchmark(
        tmp_path / "run",
        definition(contract),
        machine_registry=tmp_path / "pool" / "resources.sqlite",
    )
    registered = benchmark.client.register_project(
        benchmark.source(0.35), idempotency_key="fixture-project"
    )["result"]

    def invalid_report(*args, **kwargs):
        raise ValueError("Fixture invalid workload report after native closure")

    with monkeypatch.context() as patch:
        patch.setattr("research_swarm.workers.local.read_report", invalid_report)
        result = await benchmark.no_op(0, registered)
    assert not result["complete"] and result["outcome"] == "baseline_failed"
    assert result["trial_outcomes"][0]["state"] == "failed"
    original = benchmark.client.get("report", result["report_id"])
    campaign = benchmark.client.get("campaign", result["campaign_id"])
    benchmark.client.command(
        "campaign.pause",
        PauseCampaign(campaign_id=campaign["id"]),
        idempotency_key="pause-after-report",
        expected_revision=campaign["revision"],
    )
    updated = benchmark.report("no-op-0", campaign["id"])
    assert updated["id"] != original["id"]
    assert updated["data"]["snapshot"]["campaign_state"] == "paused"
    assert original["data"]["snapshot"]["campaign_state"] == "running"
    assert all(
        Decimal(item["reserved"]) == Decimal(item["uncertain"]) == 0 for item in result["budget"]
    )
