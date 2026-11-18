from __future__ import annotations

import json
from pathlib import Path

from research_swarm.reactive.transcript import ControlTranscript
from tests.integration.test_local_pipeline import run_native_example


def test_registered_cpu_project_records_pending_progress_baseline_replay_and_evaluation(
    tmp_path: Path,
) -> None:
    result = run_native_example("reactive_cpu", "mse", tmp_path, registered=True)
    assert result.metrics["mse"] >= 0
    state = tmp_path / "outputs"
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
