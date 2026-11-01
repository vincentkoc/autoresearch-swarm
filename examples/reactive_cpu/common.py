"""Protected data, model checks and workload file protocol for the CPU example."""

from __future__ import annotations

import json
import math
import os
from pathlib import Path

from research_swarm.domain.canonical import content_digest
from research_swarm.projects.workload import WorkloadContext, WorkloadReport

ROOT = Path(__file__).resolve().parent


def context() -> WorkloadContext:
    value = WorkloadContext.model_validate_json(Path(os.environ["RESEARCH_CONTEXT"]).read_bytes())
    value.verify()
    Path(value.output_root).mkdir(parents=True, exist_ok=True)
    return value


def data() -> tuple[dict, str]:
    payload = (ROOT / "dataset.json").read_bytes()
    return json.loads(payload), content_digest(payload)


def read_input(request: WorkloadContext, name: str) -> bytes:
    reference = request.inputs[name]
    with Path(reference.path).open("rb") as stream:
        payload = stream.read(8 * 1024 * 1024 + 1)
    if (
        len(payload) > 8 * 1024 * 1024
        or len(payload) != reference.size_bytes
        or content_digest(payload) != reference.digest
    ):
        raise ValueError("Input artifact differs from its declared bytes")
    return payload


def model(request: WorkloadContext, name: str, mode: str) -> dict:
    value = json.loads(read_input(request, name))
    configuration, digest = data()
    if (
        value["format"] != "reactive-linear-v1"
        or value["seed"] != request.seed
        or value["data_manifest_digest"] != digest
        or value["mode"] != mode
        or value["steps"] != configuration["steps"]
        or len(value["weights"]) != 2
        or any(
            type(item) not in (int, float) or not math.isfinite(item) for item in value["weights"]
        )
    ):
        raise ValueError("Model differs from the evaluator contract")
    return value


def mse(weights: list[float], inputs: list[float]) -> float:
    slope, bias = weights
    return sum((slope * x + bias - (2 * x + 1)) ** 2 for x in inputs) / len(inputs)


def report(request: WorkloadContext, **fields: object) -> None:
    value = WorkloadReport(
        context_digest=request.context_digest, observed_seed=request.seed, **fields
    )
    Path(request.report_path).write_text(value.model_dump_json(), encoding="utf-8")
