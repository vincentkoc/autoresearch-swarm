"""Synthetic native evidence never acquires evaluator authority."""

from __future__ import annotations

from copy import deepcopy
from threading import Thread
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from research_swarm.api.native_history import server
from research_swarm.control.native_history import NativeHistory, project_native_history
from research_swarm.control.report_view import render_native_history_html


def history(count: int = 90) -> NativeHistory:
    rows = [
        {
            "id": f"attempt-{index}:control:2026",
            "kind": "native_fit",
            "study": "synthetic",
            "cohort": "FIT synthetic",
            "campaign_id": f"campaign-{index}",
            "trial_id": f"trial-{index}",
            "attempt_id": f"attempt-{index}",
            "arm": "control",
            "seed": 2026,
            "status": "completed",
            "started_at": "2026-01-01T00:00:00Z",
            "finished_at": "2026-01-01T01:00:00Z",
            "timestamp_basis": "retained diagnostic event",
            "source_revision": "synthetic-revision",
            "source_sha256": "synthetic-sha256",
            "metrics": {"person_mae_deg": 10 + index / 100},
        }
        for index in range(count)
    ]
    return NativeHistory.model_validate(
        {
            "captured_at": "2026-01-01T02:00:00Z",
            "observations": rows,
            "inventory": {"completed_fits": count},
            "sources": [],
            "limitations": ["Synthetic fixture"],
        }
    )


def test_all_trials_beyond_original_eight_run_fifty_two_receipt_limits() -> None:
    native = history()
    before = deepcopy(native)
    run, _ = project_native_history(native, "FIT synthetic", "person_mae_deg")
    assert len(run.points) == 90
    assert run.points[0].hours == 1
    assert len({point.evaluation_id for point in run.points}) == 90
    html = render_native_history_html(native, {})
    assert html.count('data-evidence-kind="native_fit"') == 90
    assert "evaluation commit time" not in html
    assert native == before


def test_duplicate_identity_nonfinite_and_future_time_rejected() -> None:
    raw = history(1).model_dump()
    raw["observations"].append(deepcopy(raw["observations"][0]))
    with pytest.raises(ValueError, match="Duplicate"):
        NativeHistory.model_validate(raw)
    raw = history(1).model_dump()
    raw["observations"][0]["metrics"]["person_mae_deg"] = float("nan")
    with pytest.raises(ValueError, match="Nonfinite"):
        NativeHistory.model_validate(raw)
    raw = history(1).model_dump()
    raw["observations"][0]["finished_at"] = "2026-01-02T00:00:00Z"
    with pytest.raises(ValueError, match="later than capture"):
        NativeHistory.model_validate(raw)


def test_running_missing_time_and_other_cohort_are_never_scored() -> None:
    raw = history(3).model_dump()
    raw["observations"][0]["status"] = "running"
    raw["observations"][1]["finished_at"] = None
    raw["observations"][2]["cohort"] = "inner16 synthetic"
    native = NativeHistory.model_validate(raw)
    run, rows = project_native_history(native, "FIT synthetic", "person_mae_deg")
    assert not run.points and len(rows) == 2 and run.untimed == 1
    html = render_native_history_html(native, {})
    assert "running" in html and "finish unavailable" in html


def test_untrusted_text_is_escaped() -> None:
    raw = history(1).model_dump()
    raw["observations"][0]["study"] = "<script>alert(1)</script>"
    html = render_native_history_html(NativeHistory.model_validate(raw), {"phase": "<img src=x>"})
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert "<img src=x>" not in html


def test_readonly_routes_paginate_and_reload_manifest(tmp_path) -> None:
    manifest = tmp_path / "history.json"
    status = tmp_path / "status.json"
    manifest.write_text(history().model_dump_json())
    status.write_text("{}")
    listener = server(manifest, status, "127.0.0.1", 0)
    thread = Thread(target=listener.serve_forever)
    thread.start()
    base = f"http://127.0.0.1:{listener.server_port}"
    try:
        import json

        with urlopen(base + "/v1/native-history?offset=52&limit=38", timeout=5) as response:
            page = json.load(response)
        assert len(page["observations"]) == 38 and page["next_offset"] is None
        manifest.write_text(history(1).model_dump_json())
        with urlopen(base + "/healthz", timeout=5) as response:
            assert json.load(response)["total_observations"] == 1
        for path in ["/v1/commands", "/../status.json", "/?path=secrets", "/?bad"]:
            with pytest.raises(HTTPError):
                urlopen(base + path, timeout=5)
        with pytest.raises(HTTPError) as error:
            urlopen(Request(base + "/", method="POST", data=b"{}"), timeout=5)
        assert error.value.code == 501
    finally:
        listener.shutdown()
        thread.join(timeout=5)
        listener.server_close()


def test_absolute_level_timestamp_correction_preserves_inventory_and_geometry() -> None:
    import importlib.util
    from pathlib import Path

    producer_path = Path(__file__).resolve().parents[2] / "scripts/reconstruct_native_timeline.py"
    spec = importlib.util.spec_from_file_location("native_timeline_producer", producer_path)
    assert spec is not None and spec.loader is not None
    producer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(producer)
    previous = history().model_dump()
    for row in previous["observations"]:
        row["metrics"]["pooled_mae_deg"] = row["metrics"]["person_mae_deg"] + 0.1
    additions = []
    for seed in range(2026, 2036):
        for arm in ("control", "treatment"):
            row = deepcopy(previous["observations"][0])
            row.update(
                id=f"absolute-attempt-{seed}:{arm}:{seed}",
                study="absolute-level-context-v1",
                source_revision="4685c1abeab79a40f9d76fdeea40b9759aafe2f0",
                timestamp_basis="final_full_FIT_diagnostic heartbeat; start unavailable",
                arm=arm,
                seed=seed,
                trial_id=f"absolute-trial-{seed}",
                attempt_id=f"absolute-attempt-{seed}",
                campaign_id=f"absolute-campaign-{seed}",
                started_at=None,
                finished_at=None,
                observed_at=f"2026-01-01T01:30:{seed - 2026:02d}Z",
                fit_completed_at=f"2026-01-01T01:20:{seed - 2026:02d}Z",
            )
            additions.append(row)
    original = deepcopy(additions)
    corrected = [producer.absolute_level_timestamp(row) for row in additions]
    assert additions == original
    assert len(corrected) == 20
    assert all(
        row["fit_completed_at"] is None and row["finished_at"] is None and row["started_at"] is None
        for row in corrected
    )
    assert (
        len(
            {
                row["timestamp_basis"].split("paired_final_checkpoint_at=")[1].split(";")[0]
                for row in corrected
            }
        )
        == 10
    )
    for before, after in zip(original, corrected, strict=True):
        assert after["observed_at"] == before["observed_at"]
        assert "paired checkpoint is not an arm finish" in after["timestamp_basis"]
        assert {
            key: value
            for key, value in before.items()
            if key not in {"fit_completed_at", "timestamp_basis"}
        } == {
            key: value
            for key, value in after.items()
            if key not in {"fit_completed_at", "timestamp_basis"}
        }
        assert producer.absolute_level_timestamp(after) == after
    for field, value in (
        ("source_revision", "other-source"),
        ("timestamp_basis", "genuine individual arm finish"),
        ("study", "other-study"),
        ("kind", "evaluation"),
    ):
        outside_scope = {**original[0], field: value}
        before = deepcopy(outside_scope)
        assert producer.absolute_level_timestamp(outside_scope) == before
        assert outside_scope == before
    genuine_finish = {
        **original[0],
        "timestamp_basis": "genuine individual arm finish",
        "finished_at": "2026-01-01T01:19:59Z",
    }
    assert producer.absolute_level_timestamp(genuine_finish) == genuine_finish
    # Existing cohorts and records retain their original meaning and times.
    old_row = previous["observations"][0]
    assert producer.absolute_level_timestamp(old_row) == old_row
    for index in range(21):
        row = deepcopy(previous["observations"][0])
        row.update(
            id=f"historical-{index}",
            kind="historical_import",
            trial_id=None,
            attempt_id=None,
            cohort="historical synthetic",
            metrics={},
        )
        previous["observations"].append(row)
    baseline = NativeHistory.model_validate(
        {**previous, "observations": previous["observations"] + original}
    )
    actual = NativeHistory.model_validate(
        {**previous, "observations": previous["observations"] + corrected}
    )
    assert len(actual.observations) == 131
    for metric in ("person_mae_deg", "pooled_mae_deg"):
        before_run, _ = project_native_history(baseline, "FIT synthetic", metric)
        after_run, _ = project_native_history(actual, "FIT synthetic", metric)
        assert len(after_run.points) == 110
        assert [(p.evaluation_id, p.recorded_at, p.hours, p.value) for p in before_run.points] == [
            (p.evaluation_id, p.recorded_at, p.hours, p.value) for p in after_run.points
        ]
    html = render_native_history_html(actual, {})
    assert html.count('data-evidence-kind="native_fit"') == 220


def test_additional_history_actual_cli_keeps_merged_counts_limitations_and_timestamps(
    tmp_path, monkeypatch, capsys
) -> None:
    import importlib.util
    import json
    import sys
    from pathlib import Path

    script = Path(__file__).resolve().parents[2] / "scripts/reconstruct_native_timeline.py"
    spec = importlib.util.spec_from_file_location("timeline_merge_cli", script)
    assert spec is not None and spec.loader is not None
    producer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(producer)
    base = history().model_dump()
    for index, row in enumerate(base["observations"]):
        row["cohort"] = "FIT64 synthetic" if index < 60 else "inner16 synthetic"
        row["metrics"]["pooled_mae_deg"] = row["metrics"]["person_mae_deg"] + 0.1
    for index in range(21):
        row = deepcopy(base["observations"][0])
        row.update(
            id=f"unscored-{index}",
            kind="historical_import" if index < 7 else "attempt",
            attempt_id=f"unscored-attempt-{index}",
            trial_id=f"unscored-trial-{index}",
            cohort="unscored synthetic",
            metrics={},
            status="historical" if index < 7 else "failed" if index < 16 else "queued",
        )
        base["observations"].append(row)
    base["inventory"] = {
        "completed_fits": 90,
        "FIT64_completed_fits": 60,
        "scored_native_observations": 90,
        "evaluator_receipts": 4,
        "historical_imports": 7,
        "failed_attempts": 9,
        "queued_trials": 5,
    }
    base["limitations"] = ["Synthetic base limitation", "Synthetic shared limitation"]
    additional = {
        **deepcopy(base),
        "observations": [],
        "inventory": {"completed_fits": 20},
        "limitations": ["Synthetic shared limitation", "Absolute-level arm finish unknown"],
    }
    for seed in range(2026, 2036):
        for arm in ("control", "treatment"):
            row = deepcopy(base["observations"][0])
            row.update(
                id=f"latest-{seed}:{arm}",
                arm=arm,
                seed=seed,
                trial_id=f"latest-trial-{seed}",
                attempt_id=f"latest-attempt-{seed}",
                study="absolute-level-context-v1",
                cohort="FIT64 synthetic",
                source_revision="4685c1abeab79a40f9d76fdeea40b9759aafe2f0",
                timestamp_basis="final_full_FIT_diagnostic heartbeat; start unavailable",
                started_at=None,
                finished_at=None,
                observed_at=f"2026-01-01T01:30:{seed - 2026:02d}Z",
                fit_completed_at=f"2026-01-01T01:20:{seed - 2026:02d}Z",
            )
            additional["observations"].append(row)
    original_base, original_additional = deepcopy(base), deepcopy(additional)
    additional_path = tmp_path / "additional.json"
    additional_path.write_text(json.dumps(additional))
    output = tmp_path / "history.json"
    monkeypatch.setattr(producer, "reconstruct", lambda *args: deepcopy(base))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(script),
            "--evidence-repo",
            str(tmp_path),
            "--retained-dir",
            str(tmp_path),
            "--native-state",
            str(tmp_path),
            "--additional-native-history",
            str(additional_path),
            "--output",
            str(output),
            "--refresh",
        ],
    )
    producer.main()
    raw = output.read_bytes()
    result = NativeHistory.model_validate_json(raw)
    assert len(result.observations) == 131
    assert result.inventory["completed_fits"] == 110
    assert result.inventory["FIT64_completed_fits"] == 80
    assert result.inventory["scored_native_observations"] == 110
    for key in ("evaluator_receipts", "historical_imports", "failed_attempts", "queued_trials"):
        assert result.inventory[key] == original_base["inventory"][key]
    assert result.limitations == [
        "Synthetic base limitation",
        "Synthetic shared limitation",
        "Absolute-level arm finish unknown",
    ]
    before_fingerprint = {
        row["id"]: (
            row["trial_id"],
            row["attempt_id"],
            row["arm"],
            row["seed"],
            row["cohort"],
            row["metrics"],
        )
        for row in original_base["observations"] + original_additional["observations"]
    }
    assert before_fingerprint == {
        row.id: (row.trial_id, row.attempt_id, row.arm, row.seed, row.cohort, row.metrics)
        for row in result.observations
    }
    assert render_native_history_html(result, {}).count('data-evidence-kind="native_fit"') == 220
    latest = [row for row in result.observations if row.study == "absolute-level-context-v1"]
    assert len(latest) == 20
    for before, after in zip(original_additional["observations"], latest, strict=True):
        assert after.fit_completed_at is None
        assert after.observed_at == before["observed_at"]
        assert after.id == before["id"] and after.metrics == before["metrics"]
        assert f"paired_final_checkpoint_at={before['fit_completed_at']}" in after.timestamp_basis
    assert base == original_base and additional == original_additional
    producer.main()
    assert output.read_bytes() == raw
    assert '"unchanged": true' in capsys.readouterr().out
