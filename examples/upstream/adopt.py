"""Create a private ordinary project from exact, locally available upstream blobs."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from research_swarm.projects.manifest import load_manifest
from research_swarm.projects.source import read_git_file

from .contract import (
    MAX_SOURCE,
    PANEL,
    REVISION,
    SOURCE_HASHES,
    bounded,
    digest,
    reference_prefix,
    selected_files,
    strict_json,
    validate_candidate,
    validate_data,
    validate_execution,
    verify_source,
)

ROOT = Path(__file__).resolve().parent
WRAPPERS = ("contract.py", "runtime.py", "train_adapter.py", "evaluate.py")


def inspect(checkout: Path) -> dict[str, bytes]:
    result = {}
    for name in SOURCE_HASHES:
        revision, content = read_git_file(checkout, REVISION, name, limit=MAX_SOURCE)
        if revision != REVISION:
            raise ValueError("Resolved upstream commit differs")
        verify_source(name, content)
        result[name] = content
    validate_candidate(result["train.py"], result["train.py"])
    reference_prefix(result["train.py"])
    return result


def manifest(
    data,
    *,
    memory_bytes: int,
    accelerator_memory_bytes: int,
    scratch_bytes: int,
    cpu_slots: int,
    timeout_ms: int,
) -> str:
    for value in (memory_bytes, accelerator_memory_bytes, scratch_bytes, cpu_slots, timeout_ms):
        if type(value) is not int or value <= 0:
            raise ValueError("Declare positive native resource capacities and timeout")
    if timeout_ms <= 300_000:
        raise ValueError("Native timeout must also cover startup, excluded warmup and evaluation")
    document = f'''schema_version = "1.0"

[project]
name = "Pinned Karpathy autoresearch learning-rate study"
slug = "karpathy-lr"
description = "Locally supplied upstream source with independent checkpoint evaluation."

[source]
mode = "directory"
paths = ["*.py", "*.json", "*.lock", "reference/*.py", "research.toml"]

[mutation]
allow = ["train.py"]

[environment]
profile = "pytorch-cuda"
lock_path = "environment.lock"
network_policy = "inherit"

[data]
manifest_path = "data-manifest.json"

[evaluation]
protected_paths = ["prepare.py", "reference/*.py", "contract.py", "runtime.py", "evaluate.py",
                   "train_adapter.py", "upstream.json", "execution.json"]
comparison_class = "karpathy-228791fb-lr-v1"
measurement_trust = "authenticated"
expected_members = ["{PANEL}"]

[[evaluation.metrics]]
name = "val_bpb"
unit = "bits_per_byte"
direction = "minimize"
aggregation = "validation_nats_over_non_special_target_bytes"

[timing]
budget_ms = 300000
scope = "active_training"
timer_authority = "project_reported"
'''
    for evaluation in (False, True):
        stage = "evaluate" if evaluation else "train"
        inputs = sorted(item["input"] for item in selected_files(data, evaluation))
        if evaluation:
            inputs += ["weights", "checkpoint"]
        document += f'''
[[stages]]
id = "{stage}"
kind = "{stage}"
argv = ["python", "{"evaluate.py" if evaluation else "train_adapter.py"}"]
timeout_ms = {timeout_ms}
required_artifacts = {json.dumps(inputs)}
'''
        document += (
            'dependencies = ["train"]\n'
            if evaluation
            else 'output_names = ["weights", "checkpoint"]\n'
        )
        document += f"""
[stages.resources]
engine_profile = "pytorch-cuda"
cpu_slots = {cpu_slots}
memory_bytes = {memory_bytes}
scratch_bytes = {scratch_bytes}
accelerator_count = 1
accelerator_memory_bytes = {accelerator_memory_bytes}
exclusive_device = true
required_features = ["registered-python-v1"]
"""
    return document


def adopt(
    checkout: Path,
    destination: Path,
    *,
    data_path: Path,
    execution_path: Path,
    lock_path: Path,
    memory_bytes: int,
    accelerator_memory_bytes: int,
    scratch_bytes: int,
    cpu_slots: int = 8,
    timeout_ms: int = 1_800_000,
) -> Path:
    sources = inspect(checkout)  # No upstream imports, downloads or destination writes.
    data_bytes, execution_bytes, lock = (
        bounded(data_path),
        bounded(execution_path),
        bounded(lock_path),
    )
    data = validate_data(strict_json(data_bytes))
    validate_execution(strict_json(execution_bytes))
    if not lock.strip():
        raise ValueError("An operator-supplied environment lock is required")
    document = manifest(
        data,
        memory_bytes=memory_bytes,
        accelerator_memory_bytes=accelerator_memory_bytes,
        scratch_bytes=scratch_bytes,
        cpu_slots=cpu_slots,
        timeout_ms=timeout_ms,
    )
    wrappers = {name: bounded(ROOT / name) for name in WRAPPERS}
    provenance = {
        "schema_version": "1.0",
        "repository": "https://github.com/karpathy/autoresearch",
        "revision": REVISION,
        "sources": SOURCE_HASHES,
        "wrappers": {name: digest(content) for name, content in wrappers.items()},
        "environment_lock_digest": digest(lock),
        "execution_digest": digest(execution_bytes),
        "qualification": "adoption-only; native CUDA execution and parity require qualification",
    }
    destination.mkdir(parents=False, exist_ok=False)
    try:
        (destination / "reference").mkdir()
        for name, content in {**sources, **wrappers}.items():
            (destination / name).write_bytes(content)
        (destination / "reference/train.py").write_bytes(sources["train.py"])
        (destination / "data-manifest.json").write_bytes(data_bytes)
        (destination / "execution.json").write_bytes(execution_bytes)
        (destination / "environment.lock").write_bytes(lock)
        (destination / "upstream.json").write_text(json.dumps(provenance, indent=2) + "\n")
        (destination / "research.toml").write_text(document)
        load_manifest(destination / "research.toml")
    except BaseException:
        shutil.rmtree(destination)  # Only the new directory owned by this invocation.
        raise
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkout", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--execution", type=Path, required=True)
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--memory-bytes", type=int, required=True)
    parser.add_argument("--accelerator-memory-bytes", type=int, required=True)
    parser.add_argument("--scratch-bytes", type=int, required=True)
    parser.add_argument("--cpu-slots", type=int, default=8)
    parser.add_argument("--timeout-ms", type=int, default=1_800_000)
    args = parser.parse_args()
    result = adopt(
        args.checkout,
        args.destination,
        data_path=args.data,
        execution_path=args.execution,
        lock_path=args.lock,
        memory_bytes=args.memory_bytes,
        accelerator_memory_bytes=args.accelerator_memory_bytes,
        scratch_bytes=args.scratch_bytes,
        cpu_slots=args.cpu_slots,
        timeout_ms=args.timeout_ms,
    )
    print(json.dumps({"project": str(result), "upstream_revision": REVISION}))


if __name__ == "__main__":
    main()
