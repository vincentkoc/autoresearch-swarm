from __future__ import annotations

import json
import sys
from pathlib import Path

from research_swarm import ResearchClient
from research_swarm.control.queries import RecordQuery
from research_swarm.engines.registration import register_environment
from research_swarm.orchestration.bootstrap import RunOptions
from research_swarm.orchestration.run import run_project
from research_swarm.projects.manifest import load_manifest, resolve_project
from research_swarm.projects.source import bundle_directory
from research_swarm.reactive.transcript import ControlTranscript
from tests.paths import REPO_ROOT


async def test_registered_cpu_project_records_pending_progress_baseline_replay_and_evaluation(
    tmp_path: Path,
) -> None:
    source = REPO_ROOT / "examples" / "reactive_cpu"
    manifest = load_manifest(source / "research.toml")
    bundle = tmp_path / "source.zip"
    bundle_directory(source, bundle, paths=manifest.source.paths)
    resolved = resolve_project(bundle)
    registered = register_environment(
        tmp_path / "engine.json",
        profile="python-cpu",
        executable=Path(sys.executable),
        environment_contracts=(resolved.environment_contract_digest,),
        probe_directory=tmp_path / "probe",
    )
    state = tmp_path / "state"
    with ResearchClient.local(state) as client:
        result = await run_project(
            client,
            source,
            key="reactive-cpu",
            options=RunOptions(max_trials=1, primary_metric="mse"),
            machine_registry=tmp_path / "machine" / "resources.sqlite",
            registration=registered,
        )
        assert result.state == "awaiting_completion", result.model_dump()
        assert result.driver.executed_attempt_count == 4
        trial = client.get("trial", result.trial_id)
        assert trial["state"] == "succeeded" and trial["data"]["evaluation_status"] == "valid"
        assert client.get("campaign", result.campaign_id)["data"]["incumbent_candidate_id"] is None
        assert len(client.list(RecordQuery(kind="evaluation_receipt"))["items"]) == 1
    transcripts = list(state.rglob("transcript.json"))
    assert len(transcripts) == 1
    transcript = ControlTranscript.read(transcripts[0])
    receipt = transcript.applications()[0]
    assert receipt.observed_step == receipt.requested_step == 8
    assert 12 < receipt.applied_step < 48
    trajectory = json.loads(transcripts[0].with_name("trajectory.json").read_text())
    pending = [row for row in trajectory if row["boundary"] == "waiting"]
    assert len(pending) >= 3 and pending[0]["train_mse"] != pending[-1]["train_mse"]
    assert all(row["learning_rate"] == 0.01 for row in trajectory[: receipt.applied_step])
    assert all(row["learning_rate"] == 0.2 for row in trajectory[receipt.applied_step :])
    models = {
        model["mode"]: model
        for path in state.rglob("model.json")
        if (model := json.loads(path.read_text())).get("format") == "reactive-linear-v1"
    }
    assert models["reactive"]["weights"] == models["control_schedule_replay"]["weights"]
    assert models["reactive"]["weights"] != models["fixed_initial_controls"]["weights"]
    assert models["control_schedule_replay"]["source_transcript_digest"] == transcript.digest
    traces = list(state.rglob("reactive-replay.jsonl"))
    assert len(traces) == 1
    records = [json.loads(line) for line in traces[0].read_text().splitlines()]
    assert records[-1]["kind"] == "closed" and records[-1]["record"]["status"] == "complete"
    assert records[-1]["record"]["binding"] != transcript.session.binding.model_dump(mode="json")
