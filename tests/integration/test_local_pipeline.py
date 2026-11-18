from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest

from research_swarm.engines.registration import register_environment
from research_swarm.projects.capture import capture_project
from research_swarm.projects.manifest import resolve_project
from research_swarm.swarm.command import run_campaign
from research_swarm.swarm.interfaces import Result
from research_swarm.swarm.store import SQLiteStore
from tests.paths import REPO_ROOT


def native_campaign(example: str, metric: str, root: Path, *, registered: bool) -> Path:
    source = REPO_ROOT / "examples" / example
    argv = [sys.executable, "-m", "research_swarm.swarm.native", "--outputs", str(root / "outputs")]
    if registered:
        bundle = root / "source.zip"
        capture_project(source, bundle)
        contract = resolve_project(bundle)
        registration = root / "registration" / "engine.json"
        register_environment(
            registration,
            profile="python-cpu",
            executable=Path(sys.executable),
            environment_contracts=(contract.environment_contract_digest,),
            probe_directory=root / "probe",
        )
        argv += ["--registration", str(registration)]
    path = root / "campaign.yaml"
    path.write_text(
        json.dumps(
            {
                "campaign_id": "test",
                "metric": metric,
                "direction": "minimize",
                "command": argv,
                "task_dir": str(source),
                "max_workers": 1,
                "max_trials": 1,
                "max_attempts": 1,
                "trial_timeout_seconds": 25,
                "evaluation_mode": "smoke",
                "candidates": [{"candidate_id": "baseline"}],
            }
        )
    )
    return path


def run_native_example(example: str, metric: str, root: Path, *, registered: bool) -> Result:
    queue = root / "queue.sqlite"
    path = native_campaign(example, metric, root, registered=registered)
    _, code = run_campaign(path, queue)
    store = SQLiteStore(queue)
    (result,) = store.results("test")
    assert code == 0 and result.status == "ok", (result.error, result.logs)
    assert store.incumbent("test")[0] == result.attempt.trial.trial_id
    assert run_campaign(path, queue)[1] == 0
    assert store.results("test") == [result]
    return result


def test_native_stage_score_survives_campaign_resume(tmp_path: Path) -> None:
    result = run_native_example("cpu_bigram", "val_bpb", tmp_path, registered=False)
    assert result.metrics["val_bpb"] > 0
    root = next((tmp_path / "outputs").iterdir())
    training, evaluation = sorted(root.glob("stage-*/context.json"))
    train, score = (json.loads(path.read_text()) for path in (training, evaluation))
    assert train["stage_id"] == "train" and score["stage_id"] == "evaluate"
    assert Path(score["inputs"]["model"]["path"]).read_bytes()
    observed = json.loads(Path(score["report_path"]).read_text())["measurements"]
    assert result.metrics == {entry["name"]: entry["value"] for entry in observed}


def test_evaluation_member_mismatch_becomes_failed_attempt(tmp_path: Path) -> None:
    path = native_campaign("cpu_bigram", "val_bpb", tmp_path, registered=False)
    config = json.loads(path.read_text())
    project = tmp_path / "different-panel"
    shutil.copytree(config["task_dir"], project)
    evaluator = project / "evaluate.py"
    original = evaluator.read_text()
    evaluator.write_text(
        original.replace(
            'observed_members=[manifest["panel"]]', 'observed_members=["another-panel"]'
        )
    )
    config["task_dir"] = str(project)
    path.write_text(json.dumps(config))
    queue = tmp_path / "queue.sqlite"
    _, code = run_campaign(path, queue)
    store = SQLiteStore(queue)
    (result,) = store.results("test")
    assert code != 0 and result.status == "failed"
    assert any("different member set" in line for line in result.logs)
    assert store.incumbent("test") is None


def test_scoped_worker_runs_registered_project_and_publishes_the_stage_score(
    tmp_path: Path,
) -> None:
    from research_swarm.cli import main
    from research_swarm.swarm.interfaces import Trial
    from research_swarm.swarm.spec import load_spec
    from tests.swarm.test_worker_http import worker_http

    config = native_campaign("cpu_bigram", "val_bpb", tmp_path, registered=True)
    spec = load_spec(config)
    database = tmp_path / "queue.sqlite"
    store = SQLiteStore(database)
    store.create_campaign(spec)
    store.submit(
        Trial(
            trial_id="native",
            campaign_id=spec.campaign_id,
            candidate=spec.candidates[0],
            seed=7,
            fidelity=1,
        )
    )
    credential = tmp_path / "worker-token"
    credential.write_text("test-worker-token")
    credential.chmod(0o600)
    with worker_http(database) as http:
        code = main(
            [
                "--controller",
                str(http.base_url),
                "--token-file",
                str(credential),
                "swarm",
                "worker",
                str(config),
                "--worker-id",
                "worker-1",
                "--once",
            ]
        )
    (result,) = store.results(spec.campaign_id)
    assert code == 0 and result.attempt.worker_id == "worker-1"
    assert result.status == "ok", (result.error, result.logs)
    contexts = [
        json.loads(p.read_text()) for p in (tmp_path / "outputs").glob("*/stage-*/context.json")
    ]
    (evaluated,) = [context for context in contexts if context["stage_id"] == "evaluate"]
    observation = json.loads(Path(evaluated["report_path"]).read_text())
    assert evaluated["parameters"]["engine"]["profile"] == "python-cpu"
    assert observation["observed_seed"] == result.attempt.trial.seed == 7
    assert result.metrics == {"val_bpb": observation["measurements"][0]["value"]}


@pytest.mark.parametrize("second_producer", [False, True], ids=["missing", "ambiguous"])
def test_required_stage_inputs_are_checked_before_launch(
    tmp_path: Path, second_producer: bool
) -> None:
    config_path = native_campaign("cpu_bigram", "val_bpb", tmp_path, registered=False)
    config = json.loads(config_path.read_text())
    project = tmp_path / "invalid-stage-graph"
    shutil.copytree(config["task_dir"], project)
    manifest = project / "research.toml"
    source = manifest.read_text()
    if second_producer:
        source = source.replace('dependencies = ["train"]', 'dependencies = ["train", "duplicate"]')
        source += """
[[stages]]
id = "duplicate"
kind = "train"
argv = ["python", "train.py"]
output_names = ["model"]
"""
        missing = "model"
    else:
        source = source.replace(
            'required_artifacts = ["model"]', 'required_artifacts = ["missing-model"]'
        )
        missing = "missing-model"
    manifest.write_text(source)
    config["task_dir"] = str(project)
    config_path.write_text(json.dumps(config))
    queue = tmp_path / "queue.sqlite"
    _, code = run_campaign(config_path, queue)
    (result,) = SQLiteStore(queue).results("test")
    assert code != 0 and result.status == "failed"
    assert any(
        f"Stage evaluate requires exactly one direct producer of {missing}" in line
        for line in result.logs
    )
    assert not list((tmp_path / "outputs").glob("*/stage-*"))
