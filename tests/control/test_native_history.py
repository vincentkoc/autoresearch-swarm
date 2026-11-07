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
