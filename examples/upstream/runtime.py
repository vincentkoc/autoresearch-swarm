"""Protected runtime glue; heavy dependencies load only inside an experiment."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
from contextlib import contextmanager
from pathlib import Path

from contract import (
    MAX_INPUT,
    bounded,
    digest,
    module_from_tree,
    prepare_tree,
    selected_files,
    strict_json,
    validate_candidate,
    validate_context,
    validate_data,
    validate_execution,
    verify_source,
)

ROOT = Path(__file__).resolve().parent


def load(evaluation: bool):
    request = validate_context(
        strict_json(bounded(Path(os.environ["RESEARCH_CONTEXT"]))), evaluation=evaluation
    )
    reference = bounded(ROOT / "reference/train.py")
    preparation = bounded(ROOT / "prepare.py")
    verify_source("train.py", reference)
    verify_source("prepare.py", preparation)
    candidate = bounded(ROOT / "train.py")
    validate_candidate(candidate, reference)
    data_bytes = bounded(ROOT / "data-manifest.json")
    data = validate_data(strict_json(data_bytes))
    execution = validate_execution(strict_json(bounded(ROOT / "execution.json")))
    for package, version in execution["packages"].items():
        if importlib.metadata.version(package) != version:
            raise ValueError(f"Installed package differs from frozen execution: {package}")
    expected = {item["input"] for item in selected_files(data, evaluation)}
    if evaluation:
        expected.update({"weights", "checkpoint"})
    if set(request["inputs"]) != expected:
        raise ValueError("Declared inputs differ from the frozen data and checkpoint contract")
    Path(request["output_root"]).mkdir(parents=True, exist_ok=True)
    return request, data, digest(data_bytes), candidate, reference, preparation, execution


def verify_input(reference, destination: Path | None = None) -> Path:
    path = Path(reference["path"])
    size = reference["size_bytes"]
    if (
        type(size) is not int
        or not 0 < size <= MAX_INPUT
        or path.is_symlink()
        or not path.is_file()
    ):
        raise ValueError("Invalid input file or size")
    hasher, received = hashlib.sha256(), 0
    target = None
    try:
        if destination is not None:
            destination.parent.mkdir(parents=True, exist_ok=True)
            target = destination.open("xb")
        with path.open("rb") as source:
            while block := source.read(min(1024 * 1024, size - received + 1)):
                received += len(block)
                if received > size:
                    raise ValueError("Input exceeds its declared size")
                hasher.update(block)
                if target is not None:
                    target.write(block)
        if received != size or "sha256:" + hasher.hexdigest() != reference["digest"]:
            raise ValueError("Input differs from its declared artifact")
    finally:
        if target is not None:
            target.close()
    return destination or path


def stage_data(request, data, *, evaluation: bool) -> Path:
    cache = Path(request["output_root"]) / "input-cache"
    cache.mkdir(exist_ok=False)
    for item in selected_files(data, evaluation):
        reference = request["inputs"][item["input"]]
        if any(reference[key] != item[key] for key in ("digest", "size_bytes")):
            raise ValueError("Input differs from frozen data identity")
        verify_input(reference, cache / item["path"])
    return cache


@contextmanager
def fixed_imports(preparation: bytes, cache: Path, execution):
    import sys

    os.environ["PYTORCH_ALLOC_CONF"] = "expandable_segments:True"
    os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
    import kernels
    import torch

    if not torch.cuda.is_available() or torch.version.hip is not None:
        raise ValueError("Pinned upstream workload requires native CUDA")
    original = kernels.get_kernel
    previous = sys.modules.get("prepare")

    def pinned_kernel(repo):
        if repo not in execution["kernels"]:
            raise ValueError("Undeclared kernel repository")
        return original(repo, revision=execution["kernels"][repo])

    kernels.get_kernel = pinned_kernel
    try:
        prepared = module_from_tree(
            "prepare", ROOT / "prepare.py", prepare_tree(preparation, cache)
        )
        yield prepared
    finally:
        kernels.get_kernel = original
        if previous is None:
            sys.modules.pop("prepare", None)
        else:
            sys.modules["prepare"] = previous


def tensor_schema(tensors):
    return {
        name: {"shape": list(tensor.shape), "dtype": str(tensor.dtype)}
        for name, tensor in tensors.items()
    }


def report(request, **fields):
    value = {
        "schema_version": "1.0",
        "context_digest": request["context_digest"],
        "observed_seed": request["seed"],
        **fields,
    }
    destination = Path(request["report_path"])
    pending = destination.with_suffix(".pending")
    pending.write_text(json.dumps(value, allow_nan=False), encoding="utf-8")
    os.replace(pending, destination)


def file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        hasher = hashlib.file_digest(stream, "sha256")
    return "sha256:" + hasher.hexdigest()
