"""Original, standard-library contracts for a locally supplied upstream checkout."""

from __future__ import annotations

import ast
import hashlib
import json
import math
import re
from pathlib import Path
from types import ModuleType
from typing import Any

REVISION = "228791fb499afffb54b46200aca536f79142f117"
SOURCE_HASHES = {
    "train.py": "sha256:2954175f4ac42ad65164aef40910ef953789abcd05a5cc886ac9ba5a00814414",
    "prepare.py": "sha256:4f2ba9cbb8ba8c4a3d35be405a913e2f3be3af9aea103ed52ef7b2a662058150",
}
LEARNING_RATES = frozenset({"EMBEDDING_LR", "UNEMBEDDING_LR", "MATRIX_LR", "SCALAR_LR"})
KERNEL_REPOS = frozenset({"varunneal/flash-attention-3", "kernels-community/flash-attn3"})
PANEL = "climbmix-shard-06542-upstream-v1"
MAX_SOURCE = 1024 * 1024
MAX_INPUT = 4 * 1024**3
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


def digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def strict_json(content: bytes) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    def invalid(value: str) -> None:
        raise ValueError("Non-finite JSON value")

    return json.loads(content, object_pairs_hook=pairs, parse_constant=invalid)


def bounded(path: Path, limit: int = MAX_SOURCE) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError("Expected an ordinary file")
    with path.open("rb") as stream:
        content = stream.read(limit + 1)
    if len(content) > limit:
        raise ValueError("Input exceeds its size limit")
    return content


def verify_source(name: str, content: bytes) -> None:
    if len(content) > MAX_SOURCE or digest(content) != SOURCE_HASHES[name]:
        raise ValueError(f"Unsupported upstream {name}; exact pinned bytes required")


def validate_candidate(candidate: bytes, reference: bytes) -> dict[str, float]:
    """Only four finite scalar LR literals may differ; never execute either input."""
    if max(len(candidate), len(reference)) > MAX_SOURCE:
        raise ValueError("Source exceeds its size limit")

    def normalize(content: bytes) -> tuple[str, dict[str, float]]:
        tree = ast.parse(content)
        found = {}
        for node in tree.body:
            if not isinstance(node, ast.Assign) or len(node.targets) != 1:
                continue
            target = node.targets[0]
            if not isinstance(target, ast.Name) or target.id not in LEARNING_RATES:
                continue
            value = node.value
            if (
                target.id in found
                or not isinstance(value, ast.Constant)
                or type(value.value) not in (int, float)
                or not math.isfinite(value.value)
                or not 0 <= value.value <= 10
            ):
                raise ValueError("Learning rates must be unique literals in [0, 10]")
            found[target.id] = float(value.value)
            node.value = ast.Constant(value="allowed-learning-rate")
        if found.keys() != LEARNING_RATES:
            raise ValueError("Missing learning-rate assignments")
        return ast.dump(tree, include_attributes=False), found

    actual, rates = normalize(candidate)
    expected, _ = normalize(reference)
    if actual != expected:
        raise ValueError("Candidate changes the frozen model, timing, data or execution contract")
    return rates


def reference_prefix(content: bytes) -> ast.Module:
    tree = ast.parse(content)
    boundary = ast.dump(ast.parse("t_start = time.time()").body[0])
    indices = [index for index, node in enumerate(tree.body) if ast.dump(node) == boundary]
    if len(indices) != 1:
        raise ValueError("Expected one upstream setup boundary")
    return ast.Module(body=tree.body[: indices[0]], type_ignores=[])


def prepare_tree(content: bytes, cache: Path) -> ast.Module:
    tree = ast.parse(content)
    assignments = [
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == "CACHE_DIR"
    ]
    if len(assignments) != 1:
        raise ValueError("Expected one preparation cache assignment")
    assignments[0].value = ast.Constant(str(cache))
    return ast.fix_missing_locations(tree)


def module_from_tree(name: str, path: Path, tree: ast.Module) -> ModuleType:
    import sys

    module = ModuleType(name)
    module.__file__ = str(path)
    sys.modules[name] = module  # Dataclass annotation lookup requires this identity.
    try:
        exec(compile(tree, str(path), "exec"), module.__dict__)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


def validate_data(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"schema_version", "vocab_size", "files"}:
        raise ValueError("Invalid data manifest fields")
    if (
        value["schema_version"] != "1.0"
        or type(value["vocab_size"]) is not int
        or value["vocab_size"] != 8192
    ):
        raise ValueError("The pinned tokenizer must have 8192 entries")
    files = value["files"]
    if not isinstance(files, list) or not 4 <= len(files) <= 64:
        raise ValueError("Declare tokenizer, token bytes, validation and training shards")
    names, paths = set(), set()
    for item in files:
        if not isinstance(item, dict) or set(item) != {"input", "path", "digest", "size_bytes"}:
            raise ValueError("Invalid data file fields")
        name, path = item["input"], item["path"]
        if (
            not isinstance(name, str)
            or re.fullmatch(r"data_[a-z0-9_]{1,64}", name) is None
            or name in names
            or not isinstance(path, str)
            or re.fullmatch(
                r"data/shard_[0-9]{5}\.parquet|tokenizer/(tokenizer\.pkl|token_bytes\.pt)", path
            )
            is None
            or path in paths
            or not isinstance(item["digest"], str)
            or DIGEST.fullmatch(item["digest"]) is None
            or type(item["size_bytes"]) is not int
            or not 0 < item["size_bytes"] <= MAX_INPUT
        ):
            raise ValueError("Invalid or duplicate data identity")
        if path.startswith("data/") and int(path[11:16]) > 6542:
            raise ValueError("Shard lies outside the pinned dataset")
        names.add(name)
        paths.add(path)
    required = {"data/shard_06542.parquet", "tokenizer/tokenizer.pkl", "tokenizer/token_bytes.pt"}
    if not required < paths:
        raise ValueError("Missing required data or training shard")
    return value


def selected_files(data: dict[str, Any], evaluation: bool) -> list[dict[str, Any]]:
    return [
        item
        for item in data["files"]
        if not evaluation
        or item["path"].startswith("tokenizer/")
        or item["path"] == "data/shard_06542.parquet"
    ]


def validate_execution(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"schema_version", "packages", "kernels"}:
        raise ValueError("Invalid execution manifest")
    packages, kernels = value["packages"], value["kernels"]
    required = {
        "torch",
        "kernels",
        "safetensors",
        "numpy",
        "pyarrow",
        "rustbpe",
        "tiktoken",
        "requests",
    }
    if (
        value["schema_version"] != "1.0"
        or not isinstance(packages, dict)
        or not required <= packages.keys()
        or any(
            not isinstance(k, str) or not isinstance(v, str) or not v for k, v in packages.items()
        )
        or packages["torch"].split("+")[0] != "2.9.1"
        or not isinstance(kernels, dict)
        or set(kernels) != KERNEL_REPOS
        or any(
            not isinstance(v, str) or re.fullmatch(r"[0-9a-f]{40}", v) is None
            for v in kernels.values()
        )
    ):
        raise ValueError("Freeze installed package versions and both full kernel revisions")
    return value


def validate_context(value: Any, *, evaluation: bool) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != "1.0"
        or type(value.get("seed")) is not int
        or value["seed"] != 42
        or value.get("time_budget_ms") != 300_000
        or value.get("stage_id") != ("evaluate" if evaluation else "train")
        or not isinstance(value.get("context_digest"), str)
        or DIGEST.fullmatch(value["context_digest"]) is None
    ):
        raise ValueError(
            "Upstream LR mode requires seed 42, 300 seconds and no parameter overrides"
        )
    parameters = value.get("parameters")
    if not isinstance(parameters, dict) or set(parameters) != {"engine"}:
        raise ValueError("Only the registered engine envelope is accepted")
    engine = parameters["engine"]
    required = {
        "profile": "pytorch-cuda",
        "framework": "pytorch",
        "device_backend": "cuda",
        "logical_device": "cuda:0",
        "launch_mode": "isolated-verify-then-project-v1",
    }
    if not isinstance(engine, dict) or any(
        engine.get(key) != expected for key, expected in required.items()
    ):
        raise ValueError("Expected a registered native CUDA engine")
    for field in ("execution_fingerprint", "environment_digest", "launch_adapter_digest"):
        if not isinstance(engine.get(field), str) or DIGEST.fullmatch(engine[field]) is None:
            raise ValueError("Missing registered engine identity")
    for field in ("output_root", "report_path"):
        if not isinstance(value.get(field), str) or not Path(value[field]).is_absolute():
            raise ValueError("Context paths must be absolute")
    if not isinstance(value.get("inputs"), dict):
        raise ValueError("Missing declared inputs")
    return value


def expected_config() -> dict[str, Any]:
    return dict(
        sequence_len=2048,
        vocab_size=8192,
        n_layer=8,
        n_head=4,
        n_kv_head=4,
        n_embd=512,
        window_pattern="SSSL",
    )


def validate_checkpoint(value: Any, *, candidate: str, data: str, weights: str) -> dict[str, Any]:
    fields = {
        "format",
        "seed",
        "config",
        "candidate_digest",
        "reference_digest",
        "data_digest",
        "weights_digest",
        "optimizer_steps",
        "active_training_ms",
        "tensors",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value["format"] != "karpathy-lr-safetensors-v1"
        or type(value["seed"]) is not int
        or value["seed"] != 42
        or value["config"] != expected_config()
        or any(
            type(value["config"][name]) is not type(expected)
            for name, expected in expected_config().items()
        )
        or value["candidate_digest"] != candidate
        or value["reference_digest"] != SOURCE_HASHES["train.py"]
        or value["data_digest"] != data
        or value["weights_digest"] != weights
        or type(value["optimizer_steps"]) is not int
        or value["optimizer_steps"] < 12
        or type(value["active_training_ms"]) is not int
        or value["active_training_ms"] < 300_000
        or not isinstance(value["tensors"], dict)
        or not value["tensors"]
    ):
        raise ValueError("Checkpoint differs from the fixed experiment contract")
    for name, tensor in value["tensors"].items():
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(tensor, dict)
            or set(tensor) != {"shape", "dtype"}
            or tensor["dtype"] not in {"torch.float32", "torch.bfloat16"}
            or not isinstance(tensor["shape"], list)
            or not tensor["shape"]
            or any(type(n) is not int or n <= 0 for n in tensor["shape"])
        ):
            raise ValueError("Invalid checkpoint tensor schema")
    return value
