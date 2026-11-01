"""CPU contract proof with original synthetic source; no upstream code is bundled."""

from __future__ import annotations

import ast
import copy
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from examples.upstream import adopt, contract
from research_swarm.domain.experiments import ResourceRequirements, StageSpec
from research_swarm.engines.execution import context as engine_context
from research_swarm.engines.models import DeviceObservation
from research_swarm.projects.capture import capture_project
from research_swarm.projects.manifest import resolve_project
from research_swarm.projects.source import matches, validate_mutation
from tests.workers.test_engines import CONTRACT, descriptor
from tests.workers.test_engines import cpu_registration as cpu_registration
from tests.workers.test_engines import private_probe_directory as private_probe_directory

TRAIN = b"""import time
EMBEDDING_LR = 0.6
UNEMBEDDING_LR = 0.004
MATRIX_LR = 0.04
SCALAR_LR = 0.5
SEED = 42
MODEL = "original synthetic fixture"
def scoring():
    return "not a real score"
t_start = time.time()
raise RuntimeError("setup must never execute during adoption or prefix extraction")
"""
PREPARE = b"""CACHE_DIR = "ambient-cache-is-forbidden"
DATA_DIR = CACHE_DIR + "/data"
def default_path(path=DATA_DIR):
    return path
"""


def data_manifest():
    paths = (
        "data/shard_00000.parquet",
        "data/shard_06542.parquet",
        "tokenizer/tokenizer.pkl",
        "tokenizer/token_bytes.pt",
    )
    return {
        "schema_version": "1.0",
        "vocab_size": 8192,
        "files": [
            {
                "input": f"data_item_{index}",
                "path": path,
                "digest": contract.digest(path.encode()),
                "size_bytes": len(path),
            }
            for index, path in enumerate(paths)
        ],
    }


def execution_manifest():
    return {
        "schema_version": "1.0",
        "packages": {
            "torch": "2.9.1",
            "kernels": "0.11.7",
            "safetensors": "0.6.2",
            "numpy": "2.3.4",
            "pyarrow": "22.0.0",
            "rustbpe": "0.1.0",
            "tiktoken": "0.12.0",
            "requests": "2.32.5",
        },
        "kernels": dict.fromkeys(contract.KERNEL_REPOS, "a" * 40),
    }


@pytest.fixture
def checkout(tmp_path, monkeypatch):
    root = tmp_path / "checkout"
    root.mkdir()
    for name, content in {"train.py": TRAIN, "prepare.py": PREPARE}.items():
        (root / name).write_bytes(content)
        monkeypatch.setitem(contract.SOURCE_HASHES, name, contract.digest(content))

    def git(*args):
        return subprocess.check_output(["git", *args], cwd=root, text=True).strip()

    git("init", "-q")
    git("add", ".")
    git(
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-qm",
        "Synthetic source",
    )
    monkeypatch.setattr(adopt, "REVISION", git("rev-parse", "HEAD"))
    (root / "train.py").write_text("raise RuntimeError('dirty worktree must not be imported')")
    return root


@pytest.fixture
def inputs(tmp_path):
    data, execution, lock = tmp_path / "data.json", tmp_path / "execution.json", tmp_path / "lock"
    data.write_text(json.dumps(data_manifest()))
    execution.write_text(json.dumps(execution_manifest()))
    lock.write_text("# Synthetic frozen fixture; not an installable CUDA environment\n")
    return dict(
        data_path=data,
        execution_path=execution,
        lock_path=lock,
        memory_bytes=32 * 1024**3,
        accelerator_memory_bytes=80 * 1024**3,
        scratch_bytes=64 * 1024**3,
    )


def test_adoption_reads_exact_git_objects_and_protects_full_contract(checkout, inputs, tmp_path):
    before = (checkout / "train.py").read_bytes()
    destination = adopt.adopt(checkout, tmp_path / "adopted", **inputs)
    assert (checkout / "train.py").read_bytes() == before
    assert (destination / "train.py").read_bytes() == TRAIN
    assert (destination / "reference/train.py").read_bytes() == TRAIN
    provenance = json.loads((destination / "upstream.json").read_bytes())
    assert provenance["revision"] == adopt.REVISION
    assert str(checkout) not in json.dumps(provenance)
    bundle = tmp_path / "source.zip"
    original = capture_project(destination, bundle)
    resolved = resolve_project(bundle)
    protected = {
        "contract.py",
        "runtime.py",
        "evaluate.py",
        "train_adapter.py",
        "prepare.py",
        "reference/train.py",
        "upstream.json",
        "execution.json",
        "data-manifest.json",
        "environment.lock",
        "research.toml",
    }
    assert all(matches(path, resolved.protected_paths) for path in protected)
    assert resolved.manifest.stages[0].resources.accelerator_count == 1
    assert resolved.manifest.stages[0].required_artifacts == tuple(
        item["input"] for item in data_manifest()["files"]
    )
    assert "data_item_0" not in resolved.manifest.stages[1].required_artifacts
    (destination / "train.py").write_bytes(TRAIN.replace(b"MATRIX_LR = 0.04", b"MATRIX_LR = 0.05"))
    changed = capture_project(destination, tmp_path / "changed.zip")
    validate_mutation(
        original, changed, resolved.manifest.mutation, protected_paths=resolved.protected_paths
    )
    with pytest.raises(FileExistsError):
        adopt.adopt(checkout, destination, **inputs)
    assert (destination / "train.py").read_bytes() != TRAIN


def test_wrong_upstream_hash_fails_before_destination_creation(
    checkout, inputs, tmp_path, monkeypatch
):
    monkeypatch.setitem(contract.SOURCE_HASHES, "train.py", contract.digest(b"another revision"))
    destination = tmp_path / "absent"
    with pytest.raises(ValueError, match="exact pinned bytes"):
        adopt.adopt(checkout, destination, **inputs)
    assert not destination.exists()


@pytest.mark.parametrize(
    "replacement",
    [
        b"MATRIX_LR = True",
        b"MATRIX_LR = -1",
        b"MATRIX_LR = 11",
        b"MATRIX_LR = float('nan')",
        b"MATRIX_LR = 1e999",
        b"MATRIX_LR = 0.05\nMATRIX_LR = 0.06",
    ],
)
def test_disallowed_learning_rate_values(replacement):
    with pytest.raises(ValueError):
        contract.validate_candidate(TRAIN.replace(b"MATRIX_LR = 0.04", replacement), TRAIN)


@pytest.mark.parametrize(
    "before,after",
    [
        (b"SEED = 42", b"SEED = 43"),
        (b"import time", b"import time, os"),
        (b"t_start = time.time()", b"t_start = 0"),
        (b'return "not a real score"', b"return 0"),
        (b"MODEL =", b"OTHER_MODEL ="),
        (b"SCALAR_LR = 0.5", b"SCALAR_LR: float = 0.5"),
    ],
)
def test_changes_outside_literal_surface_rejected(before, after):
    with pytest.raises(ValueError):
        contract.validate_candidate(TRAIN.replace(before, after), TRAIN)


def test_lr_edit_and_prefix_extraction_never_execute_training():
    rates = contract.validate_candidate(TRAIN.replace(b"MATRIX_LR = 0.04", b"MATRIX_LR = 0"), TRAIN)
    assert rates["MATRIX_LR"] == 0
    namespace = {}
    exec(compile(contract.reference_prefix(TRAIN), "synthetic", "exec"), namespace)
    assert namespace["MODEL"] == "original synthetic fixture" and "t_start" not in namespace
    for source in (b"x = 1", TRAIN + b"\nt_start = time.time()\n"):
        with pytest.raises(ValueError, match="one upstream setup boundary"):
            contract.reference_prefix(source)


def test_cache_redirection_precedes_captured_defaults(tmp_path):
    namespace = {}
    tree = contract.prepare_tree(PREPARE, tmp_path)
    exec(compile(tree, "synthetic", "exec"), namespace)
    assert namespace["default_path"]() == str(tmp_path) + "/data"
    assert len([node for node in tree.body if isinstance(node, ast.Assign)]) == 2


def test_data_contract_rejects_undeclared_layouts_and_identities():
    original = data_manifest()
    for field, value in (
        ("path", "../tokenizer.pkl"),
        ("path", "data/shard_06543.parquet"),
        ("input", "weights"),
        ("size_bytes", True),
        ("digest", "sha256:no"),
    ):
        changed = copy.deepcopy(original)
        changed["files"][0][field] = value
        with pytest.raises(ValueError):
            contract.validate_data(changed)
    duplicate = copy.deepcopy(original)
    duplicate["files"].append(duplicate["files"][0])
    with pytest.raises(ValueError):
        contract.validate_data(duplicate)


def test_execution_requires_exact_kernel_commits_and_packages():
    valid = execution_manifest()
    assert contract.validate_execution(valid) == valid
    for mutate in (
        lambda v: v["kernels"].update({"varunneal/flash-attention-3": "main"}),
        lambda v: v["packages"].pop("safetensors"),
        lambda v: v["packages"].update(torch="2.10.0"),
    ):
        changed = copy.deepcopy(valid)
        mutate(changed)
        with pytest.raises(ValueError):
            contract.validate_execution(changed)


def test_context_and_checkpoint_reject_false_comparability(tmp_path, cpu_registration):
    registered = cpu_registration.model_copy(
        update={
            "descriptor": descriptor(
                profile="pytorch-cuda",
                framework="pytorch",
                device_backend="cuda",
                framework_version="fixture",
                device=DeviceObservation(
                    index=0,
                    name="Synthetic CUDA",
                    physical_id="fixture-cuda-0",
                    memory_bytes=4096,
                ),
            )
        }
    )
    stage = StageSpec(
        id="train",
        kind="train",
        argv=("python", "train_adapter.py"),
        timeout_ms=1_800_000,
        resources=ResourceRequirements(
            engine_profile="pytorch-cuda",
            accelerator_count=1,
        ),
    )
    engine = engine_context(registered, SimpleNamespace(environment_digest=CONTRACT), stage)
    request = {
        "schema_version": "1.0",
        "seed": 42,
        "time_budget_ms": 300_000,
        "stage_id": "train",
        "context_digest": contract.digest(b"context"),
        "inputs": {},
        "parameters": {"engine": engine},
        "output_root": str(tmp_path),
        "report_path": str(tmp_path / "report"),
    }
    assert contract.validate_context(request, evaluation=False) == request
    for changes in (
        {"profile": "python-cpu"},
        {"logical_device": "cuda:1"},
        {"device_backend": "rocm"},
        {"execution_fingerprint": "missing"},
    ):
        with pytest.raises(ValueError):
            contract.validate_context(
                {**request, "parameters": {"engine": {**engine, **changes}}}, evaluation=False
            )
    for field, value in (
        ("seed", 43),
        ("time_budget_ms", 300),
        ("stage_id", "evaluate"),
        ("parameters", {"batch": 1}),
        ("context_digest", None),
    ):
        with pytest.raises(ValueError):
            contract.validate_context({**request, field: value}, evaluation=False)
    identities = dict(
        candidate=contract.digest(TRAIN),
        data=contract.digest(b"data"),
        weights=contract.digest(b"weights"),
    )
    checkpoint = {
        "format": "karpathy-lr-safetensors-v1",
        "seed": 42,
        "config": contract.expected_config(),
        "candidate_digest": identities["candidate"],
        "reference_digest": contract.SOURCE_HASHES["train.py"],
        "data_digest": identities["data"],
        "weights_digest": identities["weights"],
        "optimizer_steps": 12,
        "active_training_ms": 300_001,
        "tensors": {"weight": {"shape": [2, 2], "dtype": "torch.float32"}},
    }
    assert contract.validate_checkpoint(checkpoint, **identities) == checkpoint
    for field, value in (
        ("seed", True),
        ("active_training_ms", 299_999),
        ("optimizer_steps", 11),
        ("weights_digest", contract.digest(b"changed")),
        ("config", {}),
    ):
        with pytest.raises(ValueError):
            contract.validate_checkpoint({**checkpoint, field: value}, **identities)
    with pytest.raises(ValueError, match="Duplicate"):
        contract.strict_json(b'{"seed": 42, "seed": 43}')


def test_verified_data_staging_copies_only_declared_bytes(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "contract", contract)
    specification = importlib.util.spec_from_file_location(
        "upstream_runtime", adopt.ROOT / "runtime.py"
    )
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    data = data_manifest()
    request = {"output_root": str(tmp_path / "output"), "inputs": {}}
    Path(request["output_root"]).mkdir()
    for item in data["files"]:
        path = tmp_path / item["input"]
        path.write_bytes(item["path"].encode())
        request["inputs"][item["input"]] = {
            "path": str(path),
            "digest": item["digest"],
            "size_bytes": item["size_bytes"],
        }
    cache = module.stage_data(request, data, evaluation=True)
    assert not (cache / "data/shard_00000.parquet").exists()
    assert (cache / "tokenizer/tokenizer.pkl").read_bytes() == b"tokenizer/tokenizer.pkl"
    first = request["inputs"][data["files"][0]["input"]]
    Path(first["path"]).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="differs"):
        module.verify_input(first, tmp_path / "verified-copy")


@pytest.mark.skipif(
    not os.environ.get("RESEARCH_UPSTREAM_CHECKOUT"), reason="User-supplied checkout only"
)
def test_optional_exact_upstream_static_contract():
    sources = adopt.inspect(Path(os.environ["RESEARCH_UPSTREAM_CHECKOUT"]))
    assert set(sources) == {"train.py", "prepare.py"}
