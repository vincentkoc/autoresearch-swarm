"""Opt-in native Metal qualification; the default suite does not import MLX."""

from __future__ import annotations

import json
import math
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from research_swarm.engines.registration import register_environment
from research_swarm.projects.capture import capture_project
from research_swarm.projects.manifest import resolve_project
from research_swarm.swarm.command import run_campaign
from research_swarm.swarm.store import SQLiteStore
from research_swarm.workers.supervision import base_environment
from tests.paths import REPO_ROOT


def source_root() -> Path:
    return REPO_ROOT / "examples" / "cpu_bigram"


def test_mlx_project_reuses_the_reference_scorer_with_a_distinct_contract(tmp_path: Path) -> None:
    source = source_root()
    mlx_bundle, cpu_bundle = tmp_path / "mlx.zip", tmp_path / "cpu.zip"
    capture_project(source, mlx_bundle, manifest_path="research_mlx.toml")
    capture_project(source, cpu_bundle)
    mlx, cpu = (
        resolve_project(mlx_bundle, manifest_path="research_mlx.toml"),
        resolve_project(cpu_bundle),
    )
    assert mlx.data_manifest_digest == cpu.data_manifest_digest
    assert mlx.manifest.evaluation.metrics == cpu.manifest.evaluation.metrics
    assert mlx.manifest.evaluation.comparison_class != cpu.manifest.evaluation.comparison_class
    assert mlx.environment_contract_digest != cpu.environment_contract_digest
    assert mlx.manifest.mutation.allow == ("train_mlx.py",)
    assert {"common.py", "evaluate.py", "dataset.json", "mlx-macos-cp312.lock"} <= set(
        mlx.protected_paths
    )
    assert all(
        stage.resources.engine_profile == "mlx-metal" and stage.resources.accelerator_count == 1
        for stage in mlx.manifest.stages
    )


@pytest.fixture
def mlx_python() -> Path:
    if os.environ.get("RESEARCH_MLX_TESTS") != "1":
        pytest.skip("Native MLX qualification is explicitly enabled on an admitted Apple worker")
    selected = os.environ.get("RESEARCH_MLX_PYTHON")
    assert selected, "Set RESEARCH_MLX_PYTHON to the isolated hash-locked interpreter"
    path = Path(selected).absolute()
    assert path.is_file(), "The explicitly selected MLX interpreter is absent"
    return path


def test_mlx_materialized_sgd_matches_the_cpu_reference(mlx_python: Path, tmp_path: Path) -> None:
    program = r"""
import json, math, random, sys
sys.path.insert(0, sys.argv[1])
import mlx.core as mx
from common import ALPHABET
from train_mlx import step
assert mx.metal.is_available(), "Metal is required; no CPU fallback"
generator = random.Random(7)
size = len(ALPHABET)
reference = [[generator.gauss(0, 0.01) for _ in range(size)] for _ in range(size)]
with mx.stream(mx.gpu):
    weights = mx.array(reference, dtype=mx.float32)
    for _ in range(32):
        previous, target = generator.randrange(size), generator.randrange(size)
        row = reference[previous]
        maximum = max(row)
        probabilities = [math.exp(weight-maximum) for weight in row]
        total = sum(probabilities)
        for token in range(size):
            row[token] -= 0.35*(probabilities[token]/total-(token == target))
        weights, value = step(weights, previous, target)
        mx.eval(weights, value)
        mx.synchronize(mx.gpu)
    observed = weights.tolist()
error = max(abs(a-b) for row_a,row_b in zip(reference,observed) for a,b in zip(row_a,row_b))
print(json.dumps({"steps":32,"max_error":error,"stream":"gpu"}))
"""
    result = subprocess.run(
        [str(mlx_python), "-I", "-c", program, str(source_root())],
        cwd=tmp_path,
        env=base_environment(),
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=45,
    )
    assert result.returncode == 0, result.stderr[-4000:]
    evidence = json.loads(result.stdout)
    assert evidence["steps"] == 32 and evidence["stream"] == "gpu"
    assert evidence["max_error"] < 1e-6


@pytest.mark.parametrize("seed", [7, 8])
def test_registered_mlx_worker_trains_then_scores_independently(
    mlx_python: Path,
    tmp_path: Path,
    seed: int,
) -> None:
    source = tmp_path / "ordinary-project"
    shutil.copytree(source_root(), source)
    inspected = tmp_path / "source-inspection.zip"
    capture_project(source, inspected, manifest_path="research_mlx.toml")
    contract = resolve_project(
        inspected, manifest_path="research_mlx.toml"
    ).environment_contract_digest
    environment = register_environment(
        tmp_path / "registration" / "mlx.json",
        profile="mlx-metal",
        executable=mlx_python,
        environment_contracts=(contract,),
        memory_bytes=1024 * 1024 * 1024,
        probe_directory=tmp_path / "probe",
    )
    assert environment.descriptor.framework_version == "0.32.4"
    assert environment.descriptor.device_backend == "metal"
    assert (
        environment.descriptor.device is not None and environment.descriptor.device.unified_memory
    )
    fingerprint = environment.profile.execution_fingerprint
    stage_files = tmp_path / "outputs"
    config = tmp_path / "campaign.yaml"
    config.write_text(
        json.dumps(
            {
                "campaign_id": "mlx-baseline",
                "metric": "val_bpb",
                "direction": "minimize",
                "task_dir": str(source),
                "command": [
                    sys.executable,
                    "-m",
                    "research_swarm.swarm.native",
                    "--manifest",
                    "research_mlx.toml",
                    "--registration",
                    str(tmp_path / "registration" / "mlx.json"),
                    "--outputs",
                    str(stage_files),
                ],
                "resource": "gpu",
                "backend": "research_swarm.swarm.backends.local:from_spec",
                "options": {"backend": {"device": "mps"}},
                "evaluation_mode": "smoke",
                "max_workers": 1,
                "max_trials": 1,
                "max_attempts": 1,
                "seed": seed,
                "candidates": [{"candidate_id": "baseline"}],
            }
        )
    )
    queue = tmp_path / "queue.sqlite"
    _, code = run_campaign(config, queue)
    (result,) = SQLiteStore(queue).results("mlx-baseline")
    assert code == 0 and result.status == "ok", result.logs
    reports = {}
    for path in stage_files.glob("*/stage-*/context.json"):
        context = json.loads(path.read_bytes())
        reports[context["stage_id"]] = json.loads(Path(context["report_path"]).read_bytes())
    trained, evaluated = reports["train"], reports["evaluate"]
    diagnostics = trained["diagnostics"]
    assert diagnostics["materialized_gpu_updates"] == diagnostics["optimizer_steps"] > 0
    assert diagnostics["stream"] == "gpu" and diagnostics["precision"] == "float32"
    assert diagnostics["execution_fingerprint"] == fingerprint
    assert diagnostics["device_name"] == environment.descriptor.device.name
    assert trained["phase_timings_ms"]["active_training"] >= 1000
    assert trained["phase_timings_ms"]["warmup"] >= 0
    assert evaluated["observed_seed"] == seed
    score = result.metrics["val_bpb"]
    assert math.isfinite(score) and score < math.log2(30) - 0.05
    assert evaluated["measurements"][0]["value"] == score
    print(
        json.dumps(
            {
                "backend": "mlx-metal",
                "mlx": environment.descriptor.framework_version,
                "device": environment.descriptor.device.name,
                "seed": seed,
                "materialized_updates": diagnostics["optimizer_steps"],
                "val_bpb": score,
                "active_training_ms": trained["phase_timings_ms"]["active_training"],
                "warmup_ms": trained["phase_timings_ms"]["warmup"],
                "execution_fingerprint": fingerprint,
            }
        )
    )
