"""Receipt validation and fixed-seed replay of an ordinary, project-owned workload."""

from __future__ import annotations

import copy
import importlib.util
import json
import math
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.types import new_id
from research_swarm.projects.manifest import load_manifest
from research_swarm.projects.workload import (
    WorkloadContext,
    WorkloadInput,
    WorkloadReport,
    read_report,
    verify_outputs,
)
from tests.paths import REPO_ROOT


@dataclass
class Archive:
    source: Path
    trial_id: str
    inputs: dict[str, WorkloadInput]
    endpoints: dict[str, Path]
    report: WorkloadReport


def execute(
    source: Path,
    root: Path,
    *,
    trial_id: str,
    stage: str,
    inputs: dict[str, WorkloadInput] | None = None,
    seed: int = 7,
) -> tuple[subprocess.CompletedProcess[str], WorkloadContext]:
    root.mkdir(parents=True)
    request = WorkloadContext(
        trial_id=trial_id,
        attempt_id=new_id(),
        stage_id=stage,
        spec_digest=content_digest(b"synthetic-archive-producer"),
        seed=seed,
        time_budget_ms=1000,
        inputs=inputs or {},
        output_root=str(root / "outputs"),
        report_path=str(root / "report.json"),
    ).sealed()
    context_path = root / "context.json"
    context_path.write_bytes(canonical_bytes(request.model_dump(mode="json")))
    allowed = ("PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "HOME", "USERPROFILE")
    env = {key: os.environ[key] for key in allowed if key in os.environ}
    result = subprocess.run(
        [sys.executable, stage + ".py"],
        cwd=source,
        env={**env, "RESEARCH_CONTEXT": str(context_path)},
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
    )
    return result, request


@pytest.fixture
def archive(tmp_path: Path) -> Archive:
    source = tmp_path / "ordinary-project"
    shutil.copytree(REPO_ROOT / "examples" / "synthetic_pose", source)
    return run_project(source, tmp_path / "archive")


def run_project(source: Path, root: Path) -> Archive:
    trial_id = new_id()
    manifest = load_manifest(source / "research.toml")
    trained, training = execute(source, root / "train", trial_id=trial_id, stage="train")
    assert trained.returncode == 0, trained.stderr
    train_report = read_report(training, manifest.stages[0])
    inputs = {
        output.output.name: WorkloadInput(
            path=str(Path(training.output_root) / output.output.path),
            digest=output.digest,
            size_bytes=output.size_bytes,
        )
        for output in verify_outputs(training, train_report, maximum_bytes=1024 * 1024)
    }
    evaluated, evaluation = execute(
        source, root / "evaluate", trial_id=trial_id, stage="evaluate", inputs=inputs
    )
    assert evaluated.returncode == 0, evaluated.stderr
    report = read_report(evaluation, manifest.stages[1])
    endpoints = {
        output.output.name: Path(evaluation.output_root) / output.output.path
        for output in verify_outputs(evaluation, report, maximum_bytes=1024 * 1024)
    }
    return Archive(source, trial_id, inputs, endpoints, report)


@pytest.fixture
def reader(archive: Archive, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    spec = importlib.util.spec_from_file_location("receipts", archive.source / "receipts.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "receipts", module)
    spec.loader.exec_module(module)
    return module


def expected(archive: Archive) -> list[str]:
    return json.loads((archive.source / "dataset.json").read_bytes())["partition"]["search"]


def identity(endpoint: dict[str, Any]) -> dict[str, str]:
    return {name: endpoint[name] for name in ("trial", "batch_sha256", "checkpoint_sha256")}


def test_fixed_seed_replays_checkpoint_and_both_endpoint_formats(
    archive: Archive, reader: ModuleType, tmp_path: Path
) -> None:
    replay = run_project(archive.source, tmp_path / "replay")
    assert replay.trial_id != archive.trial_id
    assert replay.report.observed_seed == archive.report.observed_seed == 7
    assert (
        Path(replay.inputs["model"].path).read_bytes()
        == Path(archive.inputs["model"].path).read_bytes()
    )
    original_fit = json.loads(Path(archive.inputs["fit_receipt"].path).read_bytes())
    replay_fit = json.loads(Path(replay.inputs["fit_receipt"].path).read_bytes())
    for name in ("checkpoint_sha256", "draw_order_sha256", "draws", "updates", "valid_frames"):
        assert replay_fit[name] == original_fit[name]
    assert replay_fit["trial"] == replay.trial_id

    panels = {}
    for name, schema in (
        ("endpoint_schema2", "campaign-schema-2"),
        ("endpoint_candidates_v2", "candidates-v2"),
    ):
        original = json.loads(archive.endpoints[name].read_bytes())
        fresh = json.loads(replay.endpoints[name].read_bytes())
        assert fresh == {**original, "trial": replay.trial_id}
        assert "seed" not in fresh
        panels[name] = reader.normalize(
            fresh,
            source_schema=schema,
            expected_people=expected(archive),
            expected_identity=identity(fresh),
        )
    full = panels["endpoint_schema2"]["full"]
    legacy = panels["endpoint_candidates_v2"]
    assert legacy["full"].values() == full.values()
    assert legacy["central"].valid_elements == 2 and full.valid_elements == 8

    rows = json.loads(replay.endpoints["endpoint_schema2"].read_bytes())["people"].values()
    macro = sum(math.degrees(row["error_sum"] / row["elements"]) for row in rows) / len(rows)
    pooled = math.degrees(
        sum(row["error_sum"] for row in rows) / sum(row["elements"] for row in rows)
    )
    assert not math.isclose(macro, pooled)
    measured = {metric.name: metric.value for metric in replay.report.measurements}
    assert measured == {"macro_mae_deg": macro, "pooled_mae_deg": pooled}


@pytest.mark.parametrize(
    "case,match",
    [
        ("coverage", "coverage"),
        ("units", "units"),
        ("count", "positive safe integer"),
        ("statistic", "finite"),
        ("aggregate", "aggregate"),
        ("panels", "both declared panels"),
        ("identity", "identity differs"),
        ("unknown", "Unknown endpoint schema"),
    ],
)
def test_project_reader_rejects_incompatible_or_inconsistent_receipts(
    archive: Archive, reader: ModuleType, case: str, match: str
) -> None:
    schema = "candidates-v2" if case in ("aggregate", "panels") else "campaign-schema-2"
    name = "endpoint_candidates_v2" if schema == "candidates-v2" else "endpoint_schema2"
    endpoint = json.loads(archive.endpoints[name].read_bytes())
    pins = identity(endpoint)
    if case == "coverage":
        endpoint["people"].pop(expected(archive)[0])
    elif case == "units":
        endpoint["units"] = "degrees"
    elif case == "count":
        endpoint["people"][expected(archive)[0]]["elements"] = True
    elif case == "statistic":
        endpoint["people"][expected(archive)[0]]["error_sum"] = float("nan")
    elif case == "aggregate":
        endpoint["scores"]["full"]["person_mae_deg"] += 1
    elif case == "panels":
        del endpoint["scores"]["central"]
    elif case == "identity":
        pins["checkpoint_sha256"] = "different-checkpoint"
    else:
        schema = "guessed-schema"
    with pytest.raises(ValueError, match=match):
        reader.normalize(
            endpoint,
            source_schema=schema,
            expected_people=expected(archive),
            expected_identity=pins,
        )


@pytest.mark.parametrize("case", ["seed", "units", "coverage", "partition", "checkpoint"])
def test_protected_evaluator_rejects_semantic_mismatch_even_with_updated_input_digest(
    archive: Archive, case: str, tmp_path: Path
) -> None:
    model = json.loads(Path(archive.inputs["model"].path).read_bytes())
    if case == "seed":
        model["seed"] = 8
    elif case == "units":
        model["units"] = "degrees"
    elif case == "coverage":
        model["fit_people"] = model["fit_people"][:1]
    elif case == "partition":
        model["partition_sha256"] = "changed"
    else:
        model["weight"] += 1
    inputs = copy.deepcopy(archive.inputs)
    path = tmp_path / "changed-model.json"
    path.write_bytes(canonical_bytes(model))
    inputs["model"] = WorkloadInput(
        path=str(path), digest=content_digest(path.read_bytes()), size_bytes=path.stat().st_size
    )
    if case == "partition":
        fit = json.loads(Path(inputs["fit_receipt"].path).read_bytes())
        fit["checkpoint_sha256"] = inputs["model"].digest.removeprefix("sha256:")
        fit_path = tmp_path / "changed-fit.json"
        fit_path.write_bytes(canonical_bytes(fit))
        inputs["fit_receipt"] = WorkloadInput(
            path=str(fit_path),
            digest=content_digest(fit_path.read_bytes()),
            size_bytes=fit_path.stat().st_size,
        )
    failed, context = execute(
        archive.source,
        tmp_path / "negative-evaluation",
        trial_id=archive.trial_id,
        stage="evaluate",
        inputs=inputs,
    )
    assert failed.returncode != 0 and "ValueError" in failed.stderr
    assert not Path(context.report_path).exists()
