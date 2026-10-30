from __future__ import annotations

import json
import math
import os
import subprocess
import sys
from pathlib import Path

import pytest

from research_swarm.domain.canonical import content_digest
from research_swarm.domain.experiments import StageSpec
from research_swarm.domain.types import new_id
from research_swarm.projects.manifest import load_manifest, resolve_project
from research_swarm.projects.source import bundle_directory, extract_bundle
from research_swarm.projects.workload import (
    WorkloadContext,
    WorkloadInput,
    WorkloadOutput,
    WorkloadReport,
    read_report,
    verify_outputs,
)
from research_swarm.workers.contracts import NativeCommand
from research_swarm.workers.supervision import base_environment, run_native


def context(tmp_path: Path, *, stage_id: str = "train", seed: int = 7) -> WorkloadContext:
    output = tmp_path / "outputs"
    output.mkdir(parents=True)
    return WorkloadContext(
        trial_id=new_id(),
        attempt_id=new_id(),
        stage_id=stage_id,
        spec_digest=content_digest(b"synthetic"),
        seed=seed,
        time_budget_ms=200,
        output_root=str(output),
        report_path=str(tmp_path / "report.json"),
    ).sealed()


def test_stale_report_wrong_seed_and_duplicate_names_are_rejected(tmp_path: Path) -> None:
    request = context(tmp_path)
    stage = StageSpec(id="train", kind="train", argv=("python", "train.py"))
    report = WorkloadReport(context_digest=request.context_digest, observed_seed=8)
    Path(request.report_path).write_text(report.model_dump_json())
    with pytest.raises(ValueError, match="context or seed"):
        read_report(request, stage)
    with pytest.raises(ValueError, match="unique"):
        WorkloadReport(
            context_digest=request.context_digest,
            observed_seed=7,
            outputs=(
                WorkloadOutput(name="model", path="a"),
                WorkloadOutput(name="model", path="b"),
            ),
        )
    Path(request.report_path).write_text('{"observed_seed":7,"observed_seed":7}')
    with pytest.raises(ValueError, match="Duplicate"):
        read_report(request, stage)


def test_missing_output_and_out_of_bounds_content_are_not_accepted(tmp_path: Path) -> None:
    request = context(tmp_path)
    stage = StageSpec(id="train", kind="train", argv=("python",), output_names=("model",))
    report = WorkloadReport(context_digest=request.context_digest, observed_seed=7)
    Path(request.report_path).write_text(report.model_dump_json())
    with pytest.raises(ValueError, match="output names"):
        read_report(request, stage)
    report = report.model_copy(update={"outputs": (WorkloadOutput(name="model", path="model"),)})
    (Path(request.output_root) / "model").write_bytes(b"weights")
    with pytest.raises(ValueError, match="byte allowance"):
        verify_outputs(request, report, maximum_bytes=2)
    output = verify_outputs(request, report, maximum_bytes=7)[0]
    assert output.digest == content_digest(b"weights") and output.size_bytes == 7


@pytest.mark.skipif(os.name != "posix", reason="Symlink creation may require Windows privileges")
def test_report_and_output_links_cannot_redirect_worker_reads(tmp_path: Path) -> None:
    request = context(tmp_path)
    target = tmp_path / "outside"
    target.write_text("unrelated")
    Path(request.report_path).symlink_to(target)
    stage = StageSpec(id="train", kind="train", argv=("python",))
    with pytest.raises((ValueError, OSError)):
        read_report(request, stage)
    (Path(request.output_root) / "linked").symlink_to(target)
    report = WorkloadReport(
        context_digest=request.context_digest,
        observed_seed=7,
        outputs=(WorkloadOutput(name="model", path="linked"),),
    )
    with pytest.raises((ValueError, OSError)):
        verify_outputs(request, report, maximum_bytes=100)


def test_original_cpu_workload_trains_and_evaluates_from_an_immutable_bundle(
    tmp_path: Path,
) -> None:
    source = Path(__file__).resolve().parents[1] / "examples" / "cpu_bigram"
    template = load_manifest(source / "research.toml")
    bundle = tmp_path / "source.zip"
    captured = bundle_directory(source, bundle, paths=template.source.paths)
    resolved = resolve_project(bundle)
    execution = tmp_path / "candidate"
    extract_bundle(bundle, execution, expected_tree_digest=captured.tree_digest)
    models = []
    for seed in (7, 8):
        request = context(tmp_path / f"seed-{seed}", seed=seed)
        file = Path(request.report_path).with_name("context.json")
        file.write_text(request.model_dump_json(), encoding="utf-8")
        environment = {**base_environment(), "RESEARCH_CONTEXT": str(file)}
        trained = run_native(
            NativeCommand(argv=(sys.executable, "train.py"), cwd=str(execution), timeout_ms=10000),
            tmp_path / f"train-logs-{seed}",
            environment=environment,
        )
        assert trained.exit_code == 0 and trained.closure.scope_closed
        report = read_report(request, resolved.manifest.stages[0])
        assert report.observed_seed == seed and report.diagnostics["optimizer_steps"] > 0
        artifact = verify_outputs(request, report, maximum_bytes=1024 * 1024)[0]
        model_path = Path(request.output_root) / artifact.output.path
        model = json.loads(model_path.read_text(encoding="utf-8"))
        assert model["seed"] == seed
        models.append(model["weights"])
        evaluation = context(tmp_path / f"eval-{seed}", stage_id="evaluate", seed=seed)
        evaluation = evaluation.model_copy(
            update={
                "inputs": {
                    "model": WorkloadInput(
                        path=str(model_path), digest=artifact.digest, size_bytes=artifact.size_bytes
                    )
                },
                "context_digest": None,
            }
        ).sealed()
        eval_file = Path(evaluation.report_path).with_name("context.json")
        eval_file.write_text(evaluation.model_dump_json(), encoding="utf-8")
        evaluated = run_native(
            NativeCommand(
                argv=(sys.executable, "evaluate.py"), cwd=str(execution), timeout_ms=10000
            ),
            tmp_path / f"eval-logs-{seed}",
            environment={**base_environment(), "RESEARCH_CONTEXT": str(eval_file)},
        )
        assert evaluated.exit_code == 0 and evaluated.closure.scope_closed
        score = read_report(evaluation, resolved.manifest.stages[1])
        assert score.observed_members == ("synthetic-validation-v1",)
        assert score.measurements[0].name == "val_bpb"
        assert score.measurements[0].value < math.log2(len(model["alphabet"])) - 0.05
    assert models[0] != models[1]


@pytest.mark.parametrize("offset", [0.0, 1e20])
def test_uniform_logits_have_the_same_score_under_a_large_common_offset(
    tmp_path: Path, offset: float
) -> None:
    source = Path(__file__).resolve().parents[1] / "examples" / "cpu_bigram"
    alphabet = " \n,.abcdefghijklmnopqrstuvwxyz"
    model = {
        "format": "cpu-bigram-v1",
        "alphabet": alphabet,
        "weights": [[offset] * len(alphabet) for _ in alphabet],
        "seed": 7,
        "data_manifest_digest": content_digest((source / "dataset.json").read_bytes()),
    }
    model_path = tmp_path / "model.json"
    model_path.write_text(json.dumps(model), encoding="utf-8")
    content = model_path.read_bytes()
    request = context(tmp_path / "evaluation", stage_id="evaluate")
    request = request.model_copy(
        update={
            "inputs": {
                "model": WorkloadInput(
                    path=str(model_path), digest=content_digest(content), size_bytes=len(content)
                )
            },
            "context_digest": None,
        }
    ).sealed()
    file = tmp_path / "context.json"
    file.write_text(request.model_dump_json(), encoding="utf-8")
    outcome = run_native(
        NativeCommand(argv=(sys.executable, "evaluate.py"), cwd=str(source), timeout_ms=10000),
        tmp_path / "logs",
        environment={**base_environment(), "RESEARCH_CONTEXT": str(file)},
    )
    assert outcome.exit_code == 0 and outcome.closure.scope_closed
    report = read_report(request, load_manifest(source / "research.toml").stages[1])
    assert report.measurements[0].value == pytest.approx(math.log2(len(alphabet)), abs=1e-10)


@pytest.mark.skipif(os.name != "posix", reason="POSIX FIFO fixture")
@pytest.mark.parametrize("kind", ["report", "output"])
def test_fifo_report_or_output_cannot_block_verification(tmp_path: Path, kind: str) -> None:
    request = context(tmp_path)
    fifo = Path(request.report_path) if kind == "report" else Path(request.output_root) / "fifo"
    os.mkfifo(fifo)
    code = """
import json,sys
from research_swarm.domain.experiments import StageSpec
from research_swarm.projects.workload import (
    WorkloadContext,WorkloadReport,WorkloadOutput,read_report,verify_outputs)
data=json.load(sys.stdin)
context=WorkloadContext.model_validate(data['context'])
try:
    if data['kind']=='report':
        read_report(context,StageSpec(id='train',kind='train',argv=('python',)))
    else:
        verify_outputs(context,WorkloadReport(context_digest=context.context_digest,observed_seed=7,
            outputs=(WorkloadOutput(name='model',path='fifo'),)),maximum_bytes=100)
except ValueError:
    print('rejected')
else:
    raise AssertionError('FIFO accepted')
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        input=json.dumps({"context": request.model_dump(mode="json"), "kind": kind}),
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    )
    assert result.stdout.strip() == "rejected"
