from __future__ import annotations

import copy
import tomllib
import zipfile
from pathlib import Path

import pytest

from research_swarm.domain.experiments import StageSpec
from research_swarm.projects.manifest import (
    EnvironmentContract,
    ProjectManifest,
    load_manifest,
    parse_manifest,
    resolve_project,
)
from research_swarm.projects.source import bundle_directory, validate_mutation

MANIFEST = """
schema_version = "1.0"

[project]
slug = "cpu-example"
name = "CPU research example"

[source]
mode = "directory"
paths = ["*.py", "*.json", "*.lock", "research.toml"]

[mutation]
allow = ["train.py"]

[environment]
profile = "python-cpu"
lock_path = "requirements.lock"
network_policy = "inherit"

[data]
manifest_path = "data-manifest.json"

[evaluation]
protected_paths = ["evaluate.py"]
comparison_class = "cpu-example-v1"

[[evaluation.metrics]]
name = "loss"
unit = "squared_error"
direction = "minimize"

[timing]
budget_ms = 200
scope = "active_training"
timer_authority = "project_reported"

[[stages]]
id = "train"
kind = "train"
argv = ["python", "train.py"]
timeout_ms = 1000

[[stages]]
id = "evaluate"
kind = "evaluate"
argv = ["python", "evaluate.py"]
dependencies = ["train"]
timeout_ms = 1000
"""


@pytest.fixture
def project(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    root.mkdir()
    (root / "research.toml").write_text(MANIFEST)
    (root / "train.py").write_text("rate = 0.1\n")
    (root / "evaluate.py").write_text("metric = 'loss'\n")
    (root / "data-manifest.json").write_text('{"version": 1}\n')
    (root / "requirements.lock").write_text("stdlib-only\n")
    return root


def capture(project: Path, path: Path) -> None:
    manifest = load_manifest(project / "research.toml")
    bundle_directory(project, path, paths=manifest.source.paths)


def test_resolution_uses_bundled_contract_and_protects_dependencies(
    project: Path, tmp_path: Path
) -> None:
    bundle = tmp_path / "source.zip"
    capture(project, bundle)
    resolved = resolve_project(bundle)
    assert resolved.manifest.project.slug == "cpu-example"
    assert set(resolved.protected_paths) == {
        "research.toml",
        "evaluate.py",
        "requirements.lock",
        "data-manifest.json",
    }
    (project / "research.toml").write_text(
        MANIFEST.replace('unit = "squared_error"', 'unit = "other"')
    )
    assert load_manifest(project / "research.toml").evaluation.metrics[0].unit == "other"
    assert resolve_project(bundle) == resolved


def test_train_changes_preserve_protocol_but_evaluator_changes_do_not(
    project: Path, tmp_path: Path
) -> None:
    first = tmp_path / "baseline.zip"
    capture(project, first)
    baseline = resolve_project(first)
    (project / "train.py").write_text("rate = 0.2\n")
    second = tmp_path / "candidate.zip"
    capture(project, second)
    candidate = resolve_project(second)
    assert candidate.source.tree_digest != baseline.source.tree_digest
    assert candidate.protocol_digest == baseline.protocol_digest
    assert validate_mutation(
        baseline.source,
        candidate.source,
        baseline.manifest.mutation,
        protected_paths=baseline.protected_paths,
    ) == ("train.py",)
    (project / "evaluate.py").write_text("metric = 'cheat'\n")
    third = tmp_path / "changed-evaluator.zip"
    capture(project, third)
    changed = resolve_project(third)
    assert changed.protocol_digest != baseline.protocol_digest


def test_missing_protected_pattern_fails_resolution(project: Path, tmp_path: Path) -> None:
    (project / "research.toml").write_text(
        MANIFEST.replace('protected_paths = ["evaluate.py"]', 'protected_paths = ["missing/**"]')
    )
    bundle = tmp_path / "source.zip"
    capture(project, bundle)
    with pytest.raises(ValueError, match="no entries"):
        resolve_project(bundle)


def test_protected_file_digest_is_checked_during_resolution(project: Path, tmp_path: Path) -> None:
    bundle = tmp_path / "source.zip"
    capture(project, bundle)
    tampered = tmp_path / "tampered.zip"
    with zipfile.ZipFile(bundle) as original, zipfile.ZipFile(tampered, "w") as output:
        for info in original.infolist():
            content = original.read(info)
            if info.filename == "evaluate.py":
                content = content.replace(b"loss", b"hack")
            output.writestr(info, content)
    with pytest.raises(ValueError, match="source identity"):
        resolve_project(tampered)


@pytest.mark.parametrize(
    "before,after,reason",
    [
        ('dependencies = ["train"]', 'dependencies = ["missing"]', "Unknown stage"),
        ('id = "evaluate"', 'id = "train"', "Duplicate stage"),
        ("budget_ms = 200", "budget_ms = 0", "positive"),
        ("budget_ms = 200", "budget_ms = 1.5", "valid integer"),
        ('schema_version = "1.0"', 'schema_version = "2.0"', "String should match"),
    ],
)
def test_manifest_rejects_invalid_contracts(before: str, after: str, reason: str) -> None:
    with pytest.raises(ValueError, match=reason):
        parse_manifest(MANIFEST.replace(before, after).encode())


def test_manifest_rejects_cycle_and_unknown_fields() -> None:
    cyclic = MANIFEST.replace(
        'id = "train"\nkind', 'id = "train"\ndependencies = ["evaluate"]\nkind'
    )
    with pytest.raises(ValueError, match="cycle"):
        parse_manifest(cyclic.encode())
    with pytest.raises(ValueError, match="Extra inputs"):
        parse_manifest(("unknown = true\n" + MANIFEST).encode())


def test_metadata_changes_preserve_scientific_identity(project: Path, tmp_path: Path) -> None:
    baseline_path = tmp_path / "baseline.zip"
    capture(project, baseline_path)
    baseline = resolve_project(baseline_path)
    updated = MANIFEST.replace(
        'name = "CPU research example"',
        'name = "CPU research example"\ndescription = "Updated explanation"',
    ).replace('mode = "directory"', 'mode = "directory"\nbase_revision = "main"')
    (project / "research.toml").write_text("# Comment\n" + updated)
    changed_path = tmp_path / "metadata.zip"
    capture(project, changed_path)
    changed = resolve_project(changed_path)
    assert changed.source.tree_digest != baseline.source.tree_digest
    assert changed.manifest_digest != baseline.manifest_digest
    assert changed.evaluator_contract_digest == baseline.evaluator_contract_digest
    assert changed.protocol_digest == baseline.protocol_digest


def test_evaluator_command_changes_scientific_identity(project: Path, tmp_path: Path) -> None:
    baseline_path = tmp_path / "baseline.zip"
    capture(project, baseline_path)
    baseline = resolve_project(baseline_path)
    (project / "research.toml").write_text(
        MANIFEST.replace('["python", "evaluate.py"]', '["python", "evaluate.py", "--mean"]')
    )
    changed_path = tmp_path / "changed.zip"
    capture(project, changed_path)
    changed = resolve_project(changed_path)
    assert changed.evaluator_contract_digest != baseline.evaluator_contract_digest
    assert changed.protocol_digest != baseline.protocol_digest


@pytest.mark.parametrize("typed_environment", [False, True])
@pytest.mark.parametrize("typed_stages", [False, True])
def test_profile_inheritance_preserves_overrides_and_inputs(
    typed_environment: bool, typed_stages: bool
) -> None:
    data = tomllib.loads(MANIFEST)
    data["environment"]["profile"] = "python-mlx"
    data["stages"][1]["resources"] = {"engine_profile": "python-cpu", "cpu_slots": 2}
    if typed_environment:
        data["environment"] = EnvironmentContract.model_validate(data["environment"])
    if typed_stages:
        data["stages"] = [StageSpec.model_validate(stage) for stage in data["stages"]]
    original = copy.deepcopy(data)
    manifest = ProjectManifest.model_validate(data)
    assert data == original
    assert manifest.stages[0].resources.engine_profile == "python-mlx"
    assert manifest.stages[1].resources.engine_profile == "python-cpu"
    assert manifest.stages[1].resources.cpu_slots == 2
    assert ProjectManifest.model_validate_json(manifest.model_dump_json()) == manifest
