from __future__ import annotations

import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import CampaignTarget
from research_swarm.control.models import SearchConfiguration, StoppingRule
from research_swarm.control.promotions import InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.search_models import AdmitProposalResponse, RespondToProposal
from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.paths import REPO_ROOT
from tests.search.test_proposals import PROPOSAL_LIMITS, baseline, proposer, request_proposal
from tests.workers.test_worker_sessions import advance_epoch


def admission_intent(pipeline: Pipeline, request_id: str) -> AdmitProposalResponse:
    context = pipeline.client.context(pipeline.campaign["id"])
    return AdmitProposalResponse(
        campaign_id=pipeline.campaign["id"],
        request_id=request_id,
        expected_incumbent_revision=context["campaign"]["data"]["incumbent_revision"],
        expected_controller_id=context["authority"]["controller_id"],
        expected_control_epoch=context["authority"]["control_epoch"],
    )


def changed_source(pipeline: Pipeline, tmp_path: Path, *, protected: bool = False) -> ArtifactRef:
    path = tmp_path / new_id()
    shutil.copytree(REPO_ROOT / "examples" / "cpu_bigram", path)
    edited = path / ("evaluate.py" if protected else "train.py")
    edited.write_text(
        edited.read_text(encoding="utf-8") + "\n# A source change for admission tests.\n",
        encoding="utf-8",
    )
    return pipeline.client.capture_project(path, idempotency_key=new_id())


def answered(
    pipeline: Pipeline, tmp_path: Path, *, protected: bool = False
) -> tuple[dict[str, Any], dict[str, Any]]:
    principal, agent, _ = proposer(pipeline)
    requested = request_proposal(pipeline, principal)
    response = agent.command(
        "search.respond",
        RespondToProposal(
            request_id=requested["request"]["id"],
            outcome="candidate",
            source_artifact=changed_source(pipeline, tmp_path, protected=protected),
            hypothesis="Check a candidate under the frozen experiment contract",
        ),
        idempotency_key=new_id(),
    )["result"]
    return requested, response


def admit(
    pipeline: Pipeline, request_id: str, revision: int = 1, key: str | None = None
) -> dict[str, Any]:
    return pipeline.client.command(
        "search.admit_response",
        admission_intent(pipeline, request_id),
        idempotency_key=key or new_id(),
        expected_revision=revision,
    )


@pytest.mark.parametrize("pipeline", [{"limits": PROPOSAL_LIMITS}], indirect=True)
class TestAdmission:
    def test_admission_atomically_queues_real_work_and_preserves_proposer(
        self, pipeline: Pipeline, tmp_path: Path
    ) -> None:
        from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation

        baseline(pipeline)
        requested, response = answered(pipeline, tmp_path)
        request_id = requested["request"]["id"]
        intent = admission_intent(pipeline, request_id)
        result = admit(pipeline, request_id, key="admit-original")
        output = result["result"]
        assert output["transition"]["data"]["outcome"] == "admitted"
        assert output["candidate"]["data"]["proposer_id"] == response["data"]["responder_id"]
        assert output["candidate"]["data"]["proposer_id"] != pipeline.runtime.principal().id
        assert output["candidate"]["data"]["parent_ids"] == [
            requested["request"]["data"]["parent_candidate_id"]
        ]
        assert output["search"]["revision"] == 2
        assert output["search"]["data"]["pending_request_ids"] == []
        assert output["search"]["data"]["active_trial_ids"] == [output["trial"]["id"]]
        assert output["trial"]["data"]["spec"]["seed"] == requested["request"]["data"]["seed"]
        assert pipeline.client.get("proposal_request", request_id)["state"] == "consumed"
        restarted = ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
        assert (
            restarted.command(
                "search.admit_response",
                intent,
                idempotency_key="admit-original",
                expected_revision=1,
            )
            == result
        )
        with pytest.raises(ResearchError) as consumed:
            admit(pipeline, request_id, 2)
        assert consumed.value.code == ErrorCode.INVALID_TRANSITION
        # Native execution and evaluator authority remain independent of policy admission.
        training = pipeline.worker.claim(pipeline.ready())
        pipeline.worker.execute(training["id"])
        job = pipeline.ready()
        authorization = pipeline.client.command(
            "evaluation.authorize",
            AuthorizeEvaluation(job_id=job["id"], evaluator_id=pipeline.runtime.principal().id),
            idempotency_key="search-evaluator",
            expected_revision=job["revision"],
        )["result"]
        attempt = pipeline.worker.claim(pipeline.ready())
        pipeline.worker.execute(attempt["id"])
        receipt = pipeline.client.command(
            "evaluation.complete",
            CompleteEvaluation(request_id=authorization["id"], attempt_id=attempt["id"]),
            idempotency_key="search-evaluation",
        )["result"]
        assert receipt["state"] == "valid"
        assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2
        assert (
            pipeline.client.get("campaign", pipeline.campaign["id"])["data"]["incumbent_revision"]
            == 1
        )
        # Scientific completion cannot implicitly free a policy slot before observation/selection.
        principal, _, _ = proposer(pipeline)
        with pytest.raises(ResearchError) as waiting:
            request_proposal(pipeline, principal, 2)
        assert waiting.value.code == ErrorCode.RESOURCE_UNAVAILABLE

    @pytest.mark.parametrize("outcome", ["invalid", "duplicate", "agent_failed"])
    def test_nonadmitted_outcomes_preserve_evidence_without_an_experiment(
        self, pipeline: Pipeline, tmp_path: Path, outcome: str
    ) -> None:
        baseline(pipeline)
        principal, agent, _ = proposer(pipeline)
        requested = request_proposal(pipeline, principal)
        row = requested["request"]
        if outcome == "agent_failed":
            answer = RespondToProposal(
                request_id=row["id"], outcome="failed", finding="No credible change"
            )
        else:
            source = (
                changed_source(pipeline, tmp_path, protected=True)
                if outcome == "invalid"
                else ArtifactRef.model_validate(row["data"]["parent_source"])
            )
            answer = RespondToProposal(
                request_id=row["id"],
                outcome="candidate",
                source_artifact=source,
                hypothesis="Preserve rejected or duplicate output",
            )
        recorded = agent.command("search.respond", answer, idempotency_key="answer")["result"]
        output = admit(pipeline, row["id"])["result"]
        assert output["transition"]["data"]["outcome"] == outcome
        assert output["trial"] is None and output["search"]["state"] == "ready"
        assert pipeline.client.get("proposal_response", row["id"]) == recorded
        assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 1
        assert len(pipeline.client.list(RecordQuery(kind="candidate"))["items"]) == (
            2 if outcome == "invalid" else 1
        )
        if outcome == "invalid":
            assert output["validation"]["data"]["changed_paths"] == ["evaluate.py"]
            assert output["validation"]["state"] == "invalid"
        elif outcome == "duplicate":
            assert (
                output["transition"]["data"]["candidate_id"] == row["data"]["parent_candidate_id"]
            )
        # A terminal response releases just its proposal slot.
        assert request_proposal(pipeline, principal, 2)["search"]["revision"] == 3

    def test_competing_admissions_commit_one_graph(
        self, pipeline: Pipeline, tmp_path: Path
    ) -> None:
        baseline(pipeline)
        requested, response = answered(pipeline, tmp_path)
        intent = admission_intent(pipeline, requested["request"]["id"])
        barrier = threading.Barrier(2)

        def commit(index: int) -> dict[str, Any] | ErrorCode:
            client = ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
            barrier.wait(timeout=5)
            try:
                return client.command(
                    "search.admit_response",
                    intent,
                    idempotency_key=f"competing-{index}",
                    expected_revision=1,
                )
            except ResearchError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(commit, range(2)))
        assert results.count(ErrorCode.REVISION_CONFLICT) == 1
        assert len(pipeline.client.list(RecordQuery(kind="policy_transition"))["items"]) == 1
        assert len(pipeline.client.list(RecordQuery(kind="candidate"))["items"]) == 2
        assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2
        assert pipeline.client.get("proposal_response", response["id"]) == response

    @pytest.mark.parametrize("change", ["pause", "epoch", "invalidate"])
    def test_changed_authority_between_preflight_and_commit_retains_response(
        self, pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
    ) -> None:
        from research_swarm.control import search_admission

        receipt = baseline(pipeline)
        requested, response = answered(pipeline, tmp_path)
        original = search_admission.verified_source

        @contextmanager
        def changed(*args: Any, **kwargs: Any) -> Any:
            with original(*args, **kwargs) as source:
                if change == "pause":
                    row = pipeline.client.get("campaign", pipeline.campaign["id"])
                    pipeline.client.command(
                        "campaign.pause",
                        CampaignTarget(campaign_id=row["id"]),
                        idempotency_key="pause-admission",
                        expected_revision=row["revision"],
                    )
                elif change == "epoch":
                    advance_epoch(pipeline)
                else:
                    pipeline.client.command(
                        "evidence.invalidate",
                        InvalidateEvidence(
                            target=EvidenceReference(kind="evaluation_receipt", id=receipt["id"]),
                            reason="Corrected during source verification",
                        ),
                        idempotency_key="correct-admission",
                    )
                yield source

        monkeypatch.setattr(search_admission, "verified_source", changed)
        with pytest.raises(ResearchError):
            admit(pipeline, requested["request"]["id"])
        assert pipeline.client.get("proposal_response", response["id"]) == response
        assert pipeline.client.get("search_state", pipeline.campaign["id"]) == requested["search"]
        assert len(pipeline.client.list(RecordQuery(kind="candidate"))["items"]) == 1
        assert pipeline.client.list(RecordQuery(kind="policy_transition"))["items"] == []

    def test_unverifiable_bundle_does_not_consume_response(
        self, pipeline: Pipeline, tmp_path: Path
    ) -> None:
        baseline(pipeline)
        principal, agent, _ = proposer(pipeline)
        requested = request_proposal(pipeline, principal)
        source = tmp_path / "bad.zip"
        source.write_bytes(b"not a ZIP source bundle")
        artifact = pipeline.client.upload_file(source, idempotency_key="bad-source")
        response = agent.command(
            "search.respond",
            RespondToProposal(
                request_id=requested["request"]["id"],
                outcome="candidate",
                source_artifact=artifact,
                hypothesis="Malformed output is still evidence",
            ),
            idempotency_key="bad-output",
        )["result"]
        with pytest.raises(ResearchError) as invalid:
            admit(pipeline, requested["request"]["id"])
        assert invalid.value.code == ErrorCode.ARTIFACT_UNVERIFIED
        assert pipeline.client.get("proposal_response", response["id"]) == response
        assert pipeline.client.get("search_state", pipeline.campaign["id"]) == requested["search"]
        assert pipeline.client.list(RecordQuery(kind="policy_transition"))["items"] == []

    def test_direct_service_requires_composed_admission_permissions(
        self, pipeline: Pipeline, tmp_path: Path
    ) -> None:
        baseline(pipeline)
        requested, _ = answered(pipeline, tmp_path)
        actor = pipeline.runtime.principal().model_copy(
            update={"id": new_id(), "scopes": frozenset({"search.manage", "read"})}
        )
        with pytest.raises(ResearchError) as denied:
            pipeline.runtime.service.admit_proposal_response(
                CommandContext(
                    principal=actor, idempotency_key="unprivileged-admit", expected_revision=1
                ),
                admission_intent(pipeline, requested["request"]["id"]),
            )
        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        assert pipeline.client.list(RecordQuery(kind="policy_transition"))["items"] == []


@pytest.mark.parametrize(
    "pipeline", [{"limits": PROPOSAL_LIMITS, "stopping": StoppingRule(max_trials=1)}], indirect=True
)
def test_trial_budget_failure_rolls_back_candidate_validation_and_policy_state(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    baseline(pipeline)
    requested, response = answered(pipeline, tmp_path)
    with pytest.raises(ResearchError) as exhausted:
        admit(pipeline, requested["request"]["id"])
    assert exhausted.value.code == ErrorCode.BUDGET_EXHAUSTED
    assert pipeline.client.get("search_state", pipeline.campaign["id"]) == requested["search"]
    assert pipeline.client.get("proposal_response", response["id"]) == response
    for kind in ("candidate", "candidate_validation", "trial"):
        assert len(pipeline.client.list(RecordQuery(kind=kind))["items"]) == 1
    assert pipeline.client.list(RecordQuery(kind="policy_transition"))["items"] == []


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            "limits": PROPOSAL_LIMITS,
            "search": SearchConfiguration(
                policy="external", version="external-v1", max_parallel_trials=2
            ),
        }
    ],
    indirect=True,
)
def test_parallel_proposals_duplicate_against_already_pending_candidate(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    baseline(pipeline)
    principal, agent, _ = proposer(pipeline)
    first = request_proposal(pipeline, principal)
    second = request_proposal(pipeline, principal, 1)
    artifact = changed_source(pipeline, tmp_path)
    for requested in (first, second):
        agent.command(
            "search.respond",
            RespondToProposal(
                request_id=requested["request"]["id"],
                outcome="candidate",
                source_artifact=artifact,
                hypothesis="Identical concurrent hypothesis",
            ),
            idempotency_key=new_id(),
        )
    requests = (first, second)
    barrier = threading.Barrier(2)

    def compete(index: int) -> dict[str, Any] | ErrorCode:
        barrier.wait(timeout=5)
        try:
            return admit(pipeline, requests[index]["request"]["id"], 2)["result"]
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(compete, range(2)))
    assert results.count(ErrorCode.REVISION_CONFLICT) == 1
    winner = next(index for index, result in enumerate(results) if isinstance(result, dict))
    admitted = results[winner]
    assert isinstance(admitted, dict)
    loser_id = requests[1 - winner]["request"]["id"]
    assert pipeline.client.get("proposal_response", loser_id)["data"]["request"][
        "source_artifact"
    ] == artifact.model_dump(mode="json")
    duplicate = admit(pipeline, loser_id, 3)["result"]
    assert duplicate["transition"]["data"]["outcome"] == "duplicate"
    assert duplicate["transition"]["data"]["candidate_id"] == admitted["candidate"]["id"]
    assert duplicate["search"]["data"]["active_trial_ids"] == [admitted["trial"]["id"]]
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2


@pytest.mark.parametrize("pipeline", [{"limits": PROPOSAL_LIMITS}], indirect=True)
def test_concurrent_same_key_acceptance_wins_over_later_preflight_failure(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from research_swarm.control import search_admission

    baseline(pipeline)
    requested, _ = answered(pipeline, tmp_path)
    request_id = requested["request"]["id"]
    original = search_admission.verified_source
    accepted = []

    @contextmanager
    def interrupted(*args: Any, **kwargs: Any) -> Any:
        # Another delivery commits while this one is outside the write transaction.
        with monkeypatch.context() as inner:
            inner.setattr(search_admission, "verified_source", original)
            accepted.append(admit(pipeline, request_id, key="uncertain-admission"))
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Later preflight failed")
        yield  # pragma: no cover

    monkeypatch.setattr(search_admission, "verified_source", interrupted)
    replay = admit(pipeline, request_id, key="uncertain-admission")
    assert replay == accepted[0]
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2
    assert len(pipeline.client.list(RecordQuery(kind="policy_transition"))["items"]) == 1


@pytest.mark.parametrize("pipeline", [{"limits": PROPOSAL_LIMITS}], indirect=True)
def test_new_controller_cannot_rebase_original_response_by_refreshing_expected_epoch(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    baseline(pipeline)
    requested, response = answered(pipeline, tmp_path)
    advance_epoch(pipeline)
    with pytest.raises(ResearchError) as stale:
        admit(pipeline, requested["request"]["id"])
    assert stale.value.code == ErrorCode.STALE_FENCE
    assert pipeline.client.get("proposal_response", response["id"]) == response
    assert pipeline.client.get("search_state", pipeline.campaign["id"]) == requested["search"]


@pytest.mark.parametrize("pipeline", [{"limits": PROPOSAL_LIMITS}], indirect=True)
def test_source_cleanup_io_failure_after_commit_recovers_accepted_acknowledgment(
    pipeline: Pipeline, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from research_swarm.control import search_admission

    baseline(pipeline)
    requested, _ = answered(pipeline, tmp_path)
    original = search_admission.verified_source

    @contextmanager
    def cleanup_failure(*args: Any, **kwargs: Any) -> Any:
        with original(*args, **kwargs) as source:
            yield source
        raise OSError("Fixture source cleanup failed after acceptance")

    monkeypatch.setattr(search_admission, "verified_source", cleanup_failure)
    accepted = admit(pipeline, requested["request"]["id"], key="cleanup-admission")
    assert accepted["disposition"] == "accepted"
    assert admit(pipeline, requested["request"]["id"], key="cleanup-admission") == accepted
    assert len(pipeline.client.list(RecordQuery(kind="policy_transition"))["items"]) == 1
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2


@pytest.mark.parametrize("pipeline", [{"limits": PROPOSAL_LIMITS}], indirect=True)
async def test_admission_cli_mcp_http_replay_one_policy_transition(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    from mcp import Client

    from research_swarm.api.http import create_app
    from research_swarm.runtime import read_private
    from tests.api.test_cli_mcp import cli, stdio
    from tests.orchestration.test_http_delivery import serving

    baseline(pipeline)
    requested, _ = answered(pipeline, tmp_path)
    intent = admission_intent(pipeline, requested["request"]["id"])
    accepted = cli(
        pipeline.runtime.root,
        "command",
        "search.admit_response",
        "--key",
        "transport-admission",
        "--revision",
        "1",
        payload=intent.model_dump(mode="json"),
    )
    async with Client(stdio(pipeline.runtime.root), read_timeout_seconds=20, cache=None) as agent:
        result = await agent.call_tool(
            "research_search_admit_response",
            {
                "request": intent.model_dump(mode="json"),
                "idempotency_key": "transport-admission",
                "expected_revision": 1,
            },
        )
        assert not result.is_error and result.structured_content is not None
        assert result.structured_content["command_id"] == accepted["command_id"]
        assert (
            result.structured_content["result"]["transition"]["id"]
            == accepted["result"]["transition"]["id"]
        )
    token = read_private(pipeline.runtime.root, "operator.token", limit=256).decode("ascii")
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        ResearchClient.connect(url, token) as client,
    ):
        assert (
            client.command(
                "search.admit_response",
                intent,
                idempotency_key="transport-admission",
                expected_revision=1,
            )
            == accepted
        )
    assert len(pipeline.client.list(RecordQuery(kind="policy_transition"))["items"]) == 1
