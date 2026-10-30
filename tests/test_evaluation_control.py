from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from research_swarm import ResearchClient
from research_swarm.client import LocalTransport
from research_swarm.control.dispatch import capabilities
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.projects.manifest import parse_manifest
from research_swarm.projects.workload import WorkloadContext, WorkloadReport, read_report
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline


def prepare_evaluation(pipeline: Pipeline) -> tuple[dict[str, Any], ResearchClient, dict[str, Any]]:
    train = pipeline.worker.claim(pipeline.ready())
    assert pipeline.worker.execute(train["id"])["state"] == "succeeded"
    evaluator = Principal(
        id=new_id(),
        workspace_id=pipeline.runtime.identity.workspace_id,
        scopes=frozenset({"read", "evaluation.publish"}),
    )
    credential = pipeline.runtime.credentials.issue(pipeline.runtime.principal(), evaluator)
    client = ResearchClient(
        LocalTransport(
            pipeline.runtime, lambda: pipeline.runtime.credentials.resolve(credential.token)
        )
    )
    job = pipeline.ready()
    intent = AuthorizeEvaluation(job_id=job["id"], evaluator_id=evaluator.id)
    requester = evaluator.model_copy(
        update={"id": new_id(), "scopes": frozenset({"read", "evaluation.request"})}
    )
    requester_credential = pipeline.runtime.credentials.issue(
        pipeline.runtime.principal(), requester
    )
    requester_client = ResearchClient(
        LocalTransport(
            pipeline.runtime,
            lambda: pipeline.runtime.credentials.resolve(requester_credential.token),
        )
    )
    with pytest.raises(ResearchError) as denied:
        requester_client.command(
            "evaluation.authorize",
            intent,
            idempotency_key="missing-artifact-scope",
            expected_revision=job["revision"],
        )
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert capabilities()["commands"]["evaluation.authorize"]["required_scopes"] == [
        "evaluation.request",
        "read",
        "artifact.upload",
    ]
    requester = requester.model_copy(
        update={
            "id": new_id(),
            "scopes": frozenset({"read", "evaluation.request", "artifact.upload"}),
        }
    )
    requester_credential = pipeline.runtime.credentials.issue(
        pipeline.runtime.principal(), requester
    )
    authorized = requester_client.command(
        "evaluation.authorize",
        intent,
        idempotency_key="authorize-evaluation",
        expected_revision=job["revision"],
    )
    assert (
        requester_client.command(
            "evaluation.authorize",
            intent,
            idempotency_key="authorize-evaluation",
            expected_revision=job["revision"],
        )
        == authorized
    )
    return authorized["result"], client, train


def test_only_authorized_evaluator_can_complete_exact_native_evidence(pipeline: Pipeline) -> None:
    authorized, evaluator, train = prepare_evaluation(pipeline)
    attempt = pipeline.worker.claim(pipeline.ready())
    assert pipeline.worker.execute(attempt["id"])["state"] == "succeeded"
    intent = CompleteEvaluation(request_id=authorized["id"], attempt_id=attempt["id"])
    with pytest.raises(ResearchError) as denied:
        pipeline.worker.client.command(
            "evaluation.complete", intent, idempotency_key="worker-forgery"
        )
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    with pytest.raises(ResearchError) as wrong_attempt:
        evaluator.command(
            "evaluation.complete",
            intent.model_copy(update={"attempt_id": train["id"]}),
            idempotency_key="training-is-not-evaluation",
        )
    assert wrong_attempt.value.code == ErrorCode.COMPARISON_INELIGIBLE
    result = evaluator.command("evaluation.complete", intent, idempotency_key="evaluate-once")
    assert (
        evaluator.command("evaluation.complete", intent, idempotency_key="evaluate-once") == result
    )
    receipt = result["result"]["data"]["receipt"]
    assert result["result"]["state"] == "valid"
    assert receipt["request_id"] == authorized["id"] and receipt["id"] != authorized["id"]
    assert receipt["observed_members"] == ["synthetic-validation-v1"]
    assert receipt["metrics"][0]["name"] == "val_bpb"
    assert (
        pipeline.client.get("trial", pipeline.trial["id"])["data"]["evaluation_status"] == "valid"
    )
    assert pipeline.client.list(RecordQuery(kind="promotion"))["items"] == []
    assert (
        evaluator.command("evaluation.complete", intent, idempotency_key="redelivered-evaluation")[
            "result"
        ]
        == result["result"]
    )


def test_evaluator_authority_cannot_be_added_after_execution(pipeline: Pipeline) -> None:
    for _ in range(2):
        attempt = pipeline.worker.claim(pipeline.ready())
        pipeline.worker.execute(attempt["id"])
    job = pipeline.client.get("job", attempt["data"]["dispatch"]["job_id"])
    with pytest.raises(ResearchError, match="before execution"):
        pipeline.client.command(
            "evaluation.authorize",
            AuthorizeEvaluation(job_id=job["id"], evaluator_id=new_id()),
            idempotency_key="retroactive-evaluation",
            expected_revision=job["revision"],
        )
    assert pipeline.client.list(RecordQuery(kind="evaluation_request"))["items"] == []
    assert (
        pipeline.client.get("trial", pipeline.trial["id"])["data"]["evaluation_status"] == "pending"
    )


@pytest.mark.parametrize("defect,expected", [("members", "incomplete"), ("units", "invalid")])
def test_ineligible_evaluation_is_recorded_without_becoming_valid(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
    defect: str,
    expected: str,
) -> None:
    authorized, evaluator, _ = prepare_evaluation(pipeline)

    def broken(context: WorkloadContext, stage: Any) -> WorkloadReport:
        report = read_report(context, stage)
        update = (
            {"observed_members": ()}
            if defect == "members"
            else {
                "measurements": (report.measurements[0].model_copy(update={"unit": "wrong-unit"}),)
            }
        )
        report = report.model_copy(update=update)
        Path(context.report_path).write_text(report.model_dump_json(), encoding="utf-8")
        return report

    monkeypatch.setattr("research_swarm.workers.local.read_report", broken)
    attempt = pipeline.worker.claim(pipeline.ready())
    assert pipeline.worker.execute(attempt["id"])["state"] == "succeeded"
    result = evaluator.command(
        "evaluation.complete",
        CompleteEvaluation(request_id=authorized["id"], attempt_id=attempt["id"]),
        idempotency_key="ineligible-evaluation",
    )["result"]
    assert result["state"] == expected
    assert result["data"]["receipt"]["findings"]
    assert (
        pipeline.client.get("trial", pipeline.trial["id"])["data"]["evaluation_status"] == expected
    )


def test_absent_legacy_panel_does_not_change_serialized_contract_shape() -> None:
    source = Path(__file__).resolve().parents[1] / "examples" / "cpu_bigram" / "research.toml"
    content = source.read_bytes().replace(b'expected_members = ["synthetic-validation-v1"]\n', b"")
    legacy = parse_manifest(content).evaluation
    assert legacy.expected_members is None
    assert "expected_members" not in legacy.model_dump(mode="json")
