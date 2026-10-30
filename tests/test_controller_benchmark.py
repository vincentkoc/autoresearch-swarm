import importlib
import os
import sys
from pathlib import Path

import pytest

from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ResearchError


@pytest.fixture
def benchmark_module(monkeypatch):
    root = Path(__file__).resolve().parents[1] / "examples" / "controller_benchmark"
    monkeypatch.syspath_prepend(str(root))
    prior = sys.modules.pop("controller_benchmark", None)
    try:
        yield importlib.import_module("controller_benchmark")
    finally:
        sys.modules.pop("controller_benchmark", None)
        if prior is not None:
            sys.modules["controller_benchmark"] = prior


def test_controller_traffic_and_budget_measurements_preserve_accounting(benchmark_module, tmp_path):
    module = benchmark_module
    traffic = module.traffic(tmp_path / "traffic", 2, 8)
    assert traffic["valid"]
    assert traffic["latency"]["heartbeat"]["count"] == 16
    assert traffic["latency"]["query"]["count"] == 4
    assert traffic["latency"]["replay"]["count"] == 2
    assert traffic["accepted_commands_per_second"] > 0
    budget = module.budget_race(tmp_path / "budget", 4)
    assert budget["valid"] and budget["winners"] == 1
    assert budget["latency"]["failures"] == 3
    assert module.restart_replay(tmp_path / "replay")["valid"]


def test_missing_pair_and_failed_samples_never_become_a_complete_mean(benchmark_module):
    module = benchmark_module
    measured = module.latency([{"elapsed_ns": 1_000_000, "success": False}])
    assert measured == {"count": 1, "failures": 1, "p50_ms": 1, "p95_ms": 1, "p99_ms": 1}
    partial = [{"kind": "cpu", "repeat": 0, "count": 1, "status": "failed"}]
    summary = module.summarize(partial)
    assert summary["median_speedup"] is None
    assert summary["pairs"] == [{"repeat": 0, "speedup": None, "parallel_efficiency": None}]
    invalid = {
        "name": "cpu-invalid",
        "kind": "cpu",
        "status": "complete",
        "result": {
            "valid": False,
            "eligible_trials": 1,
            "planned_trials": 2,
            "eligible_trials_per_hour": 10,
            "maximum_train_interval_overlap": 1,
        },
    }
    rendered = module.markdown({"cells": [invalid], "summary": {"pairs": []}})
    assert "| invalid |" in rendered and "eligible 1/2" in rendered


async def test_changed_workload_is_refused_before_native_admission(benchmark_module, tmp_path):
    module = benchmark_module
    with pytest.raises(ResearchError, match="Frozen workload changed"):
        await module.cpu_cell(
            tmp_path / "cell", module.Definition(), 1, 0, expected_workload="sha256:" + "0" * 64
        )
    assert not (tmp_path / "native-runtime").exists()


@pytest.mark.skipif((os.cpu_count() or 1) < 2, reason="Two observed CPU slots required")
async def test_native_benchmark_resume_keeps_original_trials_and_censors_interrupted_timing(
    benchmark_module, tmp_path, monkeypatch
):
    module = benchmark_module
    definition = module.Definition(connections=(1,), commands=8, trials=2, repeats=1)
    root = tmp_path / "benchmark"
    benchmark = module.Benchmark(root, definition)
    original = module.write_private

    def interrupt(path, content):
        if path.name == "cell-cpu-0-1.json" and strict_json_loads(content)["status"] == "complete":
            raise KeyboardInterrupt
        original(path, content)

    monkeypatch.setattr(module, "write_private", interrupt)
    with pytest.raises(KeyboardInterrupt):
        await benchmark.run()
    monkeypatch.setattr(module, "write_private", original)
    result = await module.Benchmark(root, definition).run()
    assert result["summary"]["complete_cells"] == 4
    assert all(cell["result"]["valid"] for cell in result["cells"])
    cpu = [cell for cell in result["cells"] if cell["kind"] == "cpu"]
    assert cpu[0]["result"]["resumed"]
    assert cpu[0]["result"]["elapsed_ns"] is None
    assert cpu[0]["result"]["eligible_trials_per_hour"] is None
    assert len(cpu[0]["result"]["stages"]) == 4
    assert len({stage["attempt_id"] for stage in cpu[0]["result"]["stages"]}) == 4
    assert [trial["seed"] for trial in cpu[0]["result"]["trials"]] == [1000, 1001]
    assert cpu[1]["result"]["maximum_train_interval_overlap"] == 2
    assert result["summary"]["median_speedup"] is None
    assert await module.Benchmark(root, definition).run() == result
    with pytest.raises(ResearchError, match="Frozen benchmark inputs differ"):
        module.Benchmark(root, definition.model_copy(update={"commands": 16}))
