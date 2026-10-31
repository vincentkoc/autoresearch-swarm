"""Publication failure evidence stays useful, bounded and free of fixture authority."""

import json
from types import SimpleNamespace as NS

import pytest

from tests import test_hosted_research
from tests.test_hosted_result_publication import publication_diagnostic


@pytest.fixture
def diagnostic(tmp_path):
    root = tmp_path / "worker"
    output = root / "executions" / "fixture-attempt" / "output"
    output.mkdir(parents=True)
    secret = "rsw_" + "private_fixture_" * 3
    private = str(tmp_path / "private-project")
    detail = f"report failed at {private}: {secret}"
    case = NS(
        runner=NS(root=root),
        claimed={"result": {"id": "fixture-attempt"}},
        request={"payload": {"finding": detail, "report_artifact": None, "token": secret}},
        entry=NS(
            state="closed",
            finding=detail,
            command=NS(cwd=private, argv=("/fixture/python", secret), timeout_ms=10000),
            outcome=NS(
                reason="exited",
                exit_code=1,
                elapsed_ms=250,
                closure=NS(scope_closed=True, findings=(detail,) * 8),
                stdout=NS(name="stdout", retained_bytes=0, observed_bytes=0, eof=True),
                stderr=NS(name="stderr", retained_bytes=12000, observed_bytes=15000, eof=True),
                authority=secret,
            ),
        ),
    )
    attempt = {"state": "failed", "data": {"publication_status": "accepted", "token": secret}}
    return NS(case=case, attempt=attempt, output=output, secret=secret, private=private)


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_failure_evidence_selects_facts_and_redacts_before_bounding(diagnostic, newline):
    d = diagnostic
    (d.output / "stderr").write_bytes(
        newline.join(("x" * 12000, d.private, d.secret, "fixture error")).encode()
    )
    rendered = publication_diagnostic(d.case, d.attempt)
    assert d.secret not in rendered and d.private not in rendered
    assert str(d.case.runner.root.parent) not in rendered
    assert "authority" not in rendered and '"token"' not in rendered
    result = json.loads(rendered)
    assert result["native_reason"] == "exited" and result["exit_code"] == 1
    assert result["elapsed_ms"] == 250 and result["command_timeout_ms"] == 10000
    assert result["scope_closed"] is True and result["report_artifact_present"] is False
    assert len(result["closure_findings"]) == 4
    assert all(len(item) <= 512 for item in result["closure_findings"])
    assert len(result["stderr_tail"]) <= 2048
    assert result["stderr_tail"].endswith(
        newline.join(("<redacted>", "<redacted>", "fixture error"))
    )
    assert result["logs"]["stderr"] == {
        "retained_bytes": 12000,
        "observed_bytes": 15000,
        "eof": True,
    }


def test_missing_native_output_preserves_publication_finding(diagnostic):
    d = diagnostic
    d.case.entry.outcome = None
    result = json.loads(publication_diagnostic(d.case, d.attempt))
    assert result["stderr_unavailable"] == "FileNotFoundError"
    assert result["publication_finding"] == "report failed at <redacted>: <redacted>"
    assert "native_reason" not in result


def test_broken_diagnostic_does_not_replace_original_assertion(diagnostic):
    del diagnostic.case.entry.command
    assert json.loads(publication_diagnostic(diagnostic.case, diagnostic.attempt)) == {
        "diagnostic_unavailable": "AttributeError"
    }


@pytest.mark.parametrize("done", [False, True])
def test_bridge_timeout_keeps_original_error_and_excludes_query_headers_and_body(monkeypatch, done):
    error, calls = TimeoutError("Synthetic original timeout"), []

    class Pending:
        def result(self, *, timeout):
            assert timeout == 10
            raise error

        def done(self):
            return done

    def schedule(coroutine, loop):
        coroutine.close()
        calls.append(True)
        return Pending()

    monkeypatch.setattr(test_hosted_research.asyncio, "run_coroutine_threadsafe", schedule)
    with (
        test_hosted_research.sdk(NS(), object(), token="synthetic-header-secret") as client,
        pytest.raises(TimeoutError) as refused,
    ):
        client.transport.http.post(
            "/v1/commands/report.create?private=synthetic-query-secret",
            content="synthetic-body-secret",
        )
    assert refused.value is error and calls == [True]
    note = error.__notes__[0]
    assert "POST '/v1/commands/report.create'" in note
    assert "elapsed=" in note and "limit=10s" in note and f"future_done={done}" in note
    assert all(
        secret not in note
        for secret in ("synthetic-header-secret", "synthetic-query-secret", "synthetic-body-secret")
    )
