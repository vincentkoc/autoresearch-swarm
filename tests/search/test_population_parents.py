import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from research_swarm.control.learning_models import FreezeLearningPlan, PlanEpisode
from research_swarm.control.models import SearchConfiguration
from research_swarm.control.population_models import PopulationParentRef
from research_swarm.control.promotions import InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.search_models import (
    ProposalRequest,
    RespondToProposal,
    proposal_incumbent_id,
)
from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.experiments import Direction
from research_swarm.domain.learning import CategoricalPolicy, PolicyAction, RewardSpec
from research_swarm.domain.types import new_id
from tests.control.test_promotions import assess, second_evaluation
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.learning.test_learning_control import Prepared
from tests.learning.test_learning_datasets import finish_episode, qualify
from tests.search.test_population import retain, retention, retire
from tests.search.test_portfolio_control import THEMES, portfolio_options
from tests.search.test_proposals import PROPOSAL_LIMITS, baseline, proposal_intent, proposer
from tests.search.test_search_admission import admit
from tests.search.test_search_observations import evaluate
from tests.workers.test_worker_sessions import advance_epoch

EXTERNAL = {
    "limits": PROPOSAL_LIMITS,
    "search": SearchConfiguration(policy="external", version="external-v1", max_parallel_trials=2),
}


def setup_archive(pipeline: Pipeline, tmp_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    incumbent = baseline(pipeline)
    alternative = second_evaluation(pipeline, tmp_path)
    return incumbent, retain(pipeline, retention(pipeline, alternative))["result"]


def selected_request(
    pipeline: Pipeline, actor: Any, member: dict[str, Any], *, seed: int = 19
) -> Any:
    return proposal_intent(pipeline, actor).model_copy(
        update={
            "seed": seed,
            "parent_selection": PopulationParentRef(
                member_id=member["id"], revision=member["revision"]
            ),
        }
    )


def branch_source(pipeline: Pipeline, tmp_path: Path, rate: float) -> ArtifactRef:
    path = tmp_path / f"branch-{rate}"
    shutil.copytree(tmp_path / "zero-learning", path)
    training = path / "train.py"
    training.write_text(
        training.read_text().replace("LEARNING_RATE = 0.0", f"LEARNING_RATE = {rate}")
    )
    return pipeline.client.capture_project(path, idempotency_key=new_id())


@pytest.mark.parametrize("pipeline", [EXTERNAL], indirect=True)
def test_retired_parent_keeps_issued_lineage_while_promotion_uses_current_incumbent(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    incumbent, member = setup_archive(pipeline, tmp_path)
    actor, agent, _ = proposer(pipeline)
    legacy_intent = proposal_intent(pipeline, actor)
    assert "parent_selection" not in legacy_intent.model_dump(mode="json")
    intent = selected_request(pipeline, actor, member)
    issued = pipeline.client.command(
        "search.request_proposal", intent, idempotency_key="branch-request", expected_revision=0
    )["result"]
    request = issued["request"]
    frozen = request["data"]["parent_selection"]
    assert request["data"]["parent_candidate_id"] == member["id"]
    assert request["data"]["parent_source"] == member["data"]["source_artifact"]
    incumbent_id = pipeline.trial["data"]["candidate_id"]
    assert request["data"]["comparator_candidate_id"] == incumbent_id
    assert request["data"]["context"]["incumbent_evaluation_id"] == incumbent["id"]
    assert proposal_incumbent_id(ProposalRequest.model_validate(request["data"])) == incumbent_id
    retire(pipeline, member["id"], 1)
    before = pipeline.client.budget(pipeline.campaign["id"])
    retired_intent = intent.model_copy(
        update={"parent_selection": PopulationParentRef(member_id=member["id"], revision=2)}
    )
    with pytest.raises(ResearchError) as retired:
        pipeline.client.command(
            "search.request_proposal",
            retired_intent,
            idempotency_key="retired-parent",
            expected_revision=1,
        )
    assert retired.value.code == ErrorCode.COMPARISON_INELIGIBLE
    original_retention = retention(
        pipeline,
        pipeline.client.get("evaluation_receipt", member["data"]["request"]["evaluation_id"]),
    )
    retain(
        pipeline,
        original_retention.model_copy(update={"reason": "A later archive choice"}),
        revision=2,
    )
    with pytest.raises(ResearchError) as stale:
        pipeline.client.command(
            "search.request_proposal", intent, idempotency_key="stale-parent", expected_revision=1
        )
    assert stale.value.code == ErrorCode.REVISION_CONFLICT
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    agent.command(
        "search.respond",
        RespondToProposal(
            request_id=request["id"],
            outcome="candidate",
            source_artifact=branch_source(pipeline, tmp_path, 0.6),
            hypothesis="Revisit the retained branch",
        ),
        idempotency_key="branch-response",
    )
    accepted = admit(pipeline, request["id"], key="branch-admission")
    child = accepted["result"]["candidate"]
    assert child["data"]["parent_ids"] == [member["id"]]
    assert (
        pipeline.client.get("proposal_request", request["id"])["data"]["parent_selection"] == frozen
    )
    receipt = evaluate(pipeline, accepted["result"]["trial"])
    assessment = assess(pipeline, receipt, "branch-against-incumbent", 1)
    assert assessment["data"]["comparator_evaluation_id"] == incumbent["id"]
    assert assessment["data"]["decision"]["candidate_id"] == child["id"]
    pipeline.client.command(
        "evidence.invalidate",
        InvalidateEvidence(
            target=EvidenceReference(
                kind="evaluation_receipt", id=original_retention.evaluation_id
            ),
            reason="Later correction does not erase accepted command history",
        ),
        idempotency_key="after-admission",
    )
    assert admit(pipeline, request["id"], key="branch-admission") == accepted


@pytest.mark.parametrize("pipeline", [EXTERNAL], indirect=True)
@pytest.mark.parametrize("change", ["archive", "incumbent", "epoch"])
def test_parent_or_comparator_changes_between_preflight_and_commit_preserve_response(
    pipeline: Pipeline,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    from research_swarm.control import search_admission

    incumbent, member = setup_archive(pipeline, tmp_path)
    actor, agent, _ = proposer(pipeline)
    issued = pipeline.client.command(
        "search.request_proposal",
        selected_request(pipeline, actor, member),
        idempotency_key="branch-request",
        expected_revision=0,
    )["result"]
    request_id = issued["request"]["id"]
    response = agent.command(
        "search.respond",
        RespondToProposal(
            request_id=request_id,
            outcome="candidate",
            source_artifact=branch_source(pipeline, tmp_path, 0.6),
            hypothesis="An admitted branch needs current proof",
        ),
        idempotency_key="branch-response",
    )["result"]
    before = pipeline.client.budget(pipeline.campaign["id"])
    original = search_admission.verified_source

    @contextmanager
    def changed(*args: Any, **kwargs: Any) -> Any:
        with original(*args, **kwargs) as source:
            if change == "epoch":
                advance_epoch(pipeline)
            else:
                identifier = (
                    incumbent["id"]
                    if change == "incumbent"
                    else member["data"]["request"]["evaluation_id"]
                )
                pipeline.client.command(
                    "evidence.invalidate",
                    InvalidateEvidence(
                        target=EvidenceReference(kind="evaluation_receipt", id=identifier),
                        reason="Correction between byte preflight and admission",
                    ),
                    idempotency_key="racing-correction",
                )
            yield source

    monkeypatch.setattr(search_admission, "verified_source", changed)
    with pytest.raises(ResearchError):
        admit(pipeline, request_id)
    assert pipeline.client.get("proposal_response", request_id) == response
    assert pipeline.client.get("search_state", pipeline.campaign["id"]) == issued["search"]
    assert pipeline.client.budget(pipeline.campaign["id"])["accounts"] == before["accounts"]
    assert pipeline.client.list(RecordQuery(kind="policy_transition"))["items"] == []
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2


@pytest.mark.parametrize("pipeline", [EXTERNAL], indirect=True)
def test_learning_from_an_archived_parent_retains_the_incumbent_reward_comparator(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    incumbent, member = setup_archive(pipeline, tmp_path)
    actor, agent, token = proposer(pipeline)
    request = pipeline.client.command(
        "search.request_proposal",
        selected_request(pipeline, actor, member, seed=7),
        idempotency_key="learning-branch",
        expected_revision=0,
    )["result"]["request"]
    artifacts = {
        "fast": branch_source(pipeline, tmp_path, 0.6),
        "slow": branch_source(pipeline, tmp_path, 0.05),
    }
    intent = FreezeLearningPlan(
        plan_id=new_id(),
        action_artifacts=artifacts,
        policy=CategoricalPolicy(
            actions=tuple(
                PolicyAction(id=name, source_digest=ref.digest) for name, ref in artifacts.items()
            ),
            logits=(0.0, 0.0),
        ),
        reward_spec=RewardSpec(metric_name="val_bpb", direction=Direction.MINIMIZE, scale=5.0),
        episodes=(PlanEpisode(request_id=request["id"], split="development"),),
    )
    prepared = Prepared(intent, agent, token, [request])
    plan = prepared.freeze(pipeline.client)
    episode = plan["data"]["episodes"][0]
    assert episode["comparator_candidate_id"] == pipeline.trial["data"]["candidate_id"]
    assert episode["comparator_evaluation_id"] == incumbent["id"]
    admitted, _, _ = finish_episode(pipeline, prepared)
    assert admitted["candidate"]["data"]["parent_ids"] == [member["id"]]
    dataset = qualify(pipeline, prepared)
    trajectory = dataset["data"]["dataset"]["trajectories"][0]
    assert any(item["kind"] == "population_change" for item in trajectory["evidence"])
    assert dataset["data"]["dataset"]["trajectories"]
    pipeline.client.command(
        "evidence.invalidate",
        InvalidateEvidence(
            target=EvidenceReference(
                kind="evaluation_receipt", id=member["data"]["request"]["evaluation_id"]
            ),
            reason="Correct the observation that informed archive ancestry",
        ),
        idempotency_key="correct-learning-parent",
    )
    with pytest.raises(ResearchError):
        qualify(pipeline, prepared, key="requalify-corrected-parent")
    assert pipeline.client.get("learning_dataset", dataset["id"]) == dataset


@pytest.mark.parametrize("pipeline", [EXTERNAL], indirect=True)
def test_duplicate_response_keeps_its_proposed_parent_without_rewriting_canonical_lineage(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    _, member = setup_archive(pipeline, tmp_path)
    actor, agent, _ = proposer(pipeline)
    request = pipeline.client.command(
        "search.request_proposal",
        selected_request(pipeline, actor, member),
        idempotency_key="duplicate-branch",
        expected_revision=0,
    )["result"]["request"]
    canonical = pipeline.client.get("candidate", pipeline.trial["data"]["candidate_id"])
    before = pipeline.client.budget(pipeline.campaign["id"])
    agent.command(
        "search.respond",
        RespondToProposal(
            request_id=request["id"],
            outcome="candidate",
            source_artifact=ArtifactRef.model_validate(canonical["data"]["source_artifact"]),
            hypothesis="Return a previously measured source from another parent",
        ),
        idempotency_key="duplicate-response",
    )
    consumed = admit(pipeline, request["id"])["result"]
    assert consumed["transition"]["data"]["outcome"] == "duplicate"
    assert consumed["transition"]["data"]["candidate_id"] == canonical["id"]
    assert consumed["trial"] is None and consumed["candidate"] is None
    assert pipeline.client.get("candidate", canonical["id"]) == canonical
    assert pipeline.client.get("proposal_request", request["id"])["data"] == request["data"]
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2


@pytest.mark.parametrize("pipeline", [portfolio_options()], indirect=True)
def test_sealed_portfolio_theme_cannot_choose_an_archive_parent(pipeline: Pipeline) -> None:
    receipt = baseline(pipeline)
    member = retain(pipeline, retention(pipeline, receipt))["result"]
    actor, _, _ = proposer(pipeline)
    theme = THEMES[0]
    intent = selected_request(pipeline, actor, member).model_copy(
        update={
            "proposer_id": theme.proposer_id,
            "instructions": theme.instructions,
            "theme_id": theme.theme_id,
            "seed": 19,
        }
    )
    before = pipeline.client.budget(pipeline.campaign["id"])
    with pytest.raises(ResearchError) as denied:
        pipeline.client.command(
            "search.request_proposal",
            intent,
            idempotency_key="portfolio-archive",
            expected_revision=0,
        )
    assert denied.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert pipeline.client.budget(pipeline.campaign["id"]) == before


@pytest.mark.parametrize("pipeline", [{"limits": PROPOSAL_LIMITS}], indirect=True)
def test_greedy_refuses_archive_parent_without_charging_and_legacy_bytes_stay_stable(
    pipeline: Pipeline,
) -> None:
    receipt = baseline(pipeline)
    member = retain(pipeline, retention(pipeline, receipt))["result"]
    actor, _, _ = proposer(pipeline)
    before = pipeline.client.budget(pipeline.campaign["id"])
    with pytest.raises(ResearchError) as denied:
        pipeline.client.command(
            "search.request_proposal",
            selected_request(pipeline, actor, member),
            idempotency_key="greedy-archive",
            expected_revision=0,
        )
    assert denied.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    request = pipeline.client.command(
        "search.request_proposal",
        proposal_intent(pipeline, actor),
        idempotency_key="legacy-request",
        expected_revision=0,
    )["result"]["request"]["data"]
    assert "parent_selection" not in request and "comparator_candidate_id" not in request
    assert canonical_bytes(
        ProposalRequest.model_validate(request).model_dump(mode="json")
    ) == canonical_bytes(request)
