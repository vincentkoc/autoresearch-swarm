from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import CampaignTarget
from research_swarm.control.models import SearchConfiguration
from research_swarm.control.promotions import InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.search_models import (
    CancelProposal,
    ProposalRead,
    ReportedAgentUsage,
    RequestProposal,
    RespondToProposal,
)
from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from tests.control.test_promotions import assess, commit, evaluated
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.support import LIMITS
from tests.workers.test_worker_sessions import advance_epoch

PROPOSAL_LIMITS = (*LIMITS, BudgetLimit(dimension="proposal_requests", limit=2, unit="count"))


def proposer(pipeline: Pipeline) -> tuple[Principal, ResearchClient, str]:
    principal = Principal(
        id=new_id(),
        workspace_id=pipeline.runtime.identity.workspace_id,
        scopes=frozenset({"proposal.respond"}),
    )
    issued = pipeline.runtime.credentials.issue(pipeline.runtime.principal(), principal)
    client = ResearchClient(
        LocalTransport(pipeline.runtime, lambda: pipeline.runtime.credentials.resolve(issued.token))
    )
    return principal, client, issued.token


def baseline(pipeline: Pipeline) -> dict[str, Any]:
    receipt = evaluated(pipeline)
    commit(pipeline, assess(pipeline, receipt))
    return receipt


def proposal_intent(pipeline: Pipeline, principal: Principal) -> RequestProposal:
    view = pipeline.client.context(pipeline.campaign["id"])
    return RequestProposal(
        campaign_id=pipeline.campaign["id"],
        proposer_id=principal.id,
        seed=19,
        expected_incumbent_revision=view["campaign"]["data"]["incumbent_revision"],
        expected_controller_id=view["authority"]["controller_id"],
        expected_control_epoch=view["authority"]["control_epoch"],
        instructions="Investigate one optimizer change; preserve evaluator and data.",
    )


def request_proposal(pipeline: Pipeline, principal: Principal, revision: int = 0) -> dict[str, Any]:
    return pipeline.client.command(
        "search.request_proposal",
        proposal_intent(pipeline, principal),
        idempotency_key=new_id(),
        expected_revision=revision,
    )["result"]


def cancel(pipeline: Pipeline, row: dict[str, Any], revision: int = 1) -> dict[str, Any]:
    return pipeline.client.command(
        "search.cancel_proposal",
        CancelProposal(
            campaign_id=pipeline.campaign["id"],
            request_id=row["id"],
            reason="Change research theme",
        ),
        idempotency_key=new_id(),
        expected_revision=revision,
    )["result"]


def request_budget(pipeline: Pipeline) -> dict[str, Any]:
    return next(
        item
        for item in pipeline.client.budget(pipeline.campaign["id"])["accounts"]
        if item["dimension"] == "proposal_requests"
    )


@pytest.mark.parametrize("pipeline", [{"limits": PROPOSAL_LIMITS}], indirect=True)
class TestProposals:
    def test_response_survives_restart_before_candidate_publication(
        self, pipeline: Pipeline, tmp_path: Path
    ) -> None:
        baseline(pipeline)
        principal, client, token = proposer(pipeline)
        requested = request_proposal(pipeline, principal)
        row = requested["request"]
        frozen = row["data"]
        assert frozen["context_digest"] == record_digest(
            "research.proposal-context", frozen["context"]
        )
        assert len(canonical_bytes(frozen["context"])) <= 64 * 1024
        assert frozen["parent_candidate_id"] == pipeline.trial["data"]["candidate_id"]
        assert frozen["context"]["primary_metric"]["value"] > 0
        assert client.proposal_get(ProposalRead(request_id=row["id"]))["request"] == row
        view = pipeline.client.context(pipeline.campaign["id"])
        assert view["search_state"]["revision"] == 1
        assert view["pending_proposals"][0]["id"] == row["id"]
        assert "context" not in view["pending_proposals"][0]["data"]
        # Preserve output first, even if later source admission will reject this bundle.
        invalid_source = tmp_path / "unverified-source.zip"
        invalid_source.write_bytes(b"not a source bundle")
        artifact = pipeline.client.upload_file(invalid_source, idempotency_key="response-artifact")
        response = RespondToProposal(
            request_id=row["id"],
            outcome="candidate",
            source_artifact=artifact,
            hypothesis="An optimizer experiment awaiting validation",
            usage=ReportedAgentUsage(provider="fixture", model="external", amounts={"tokens": 37}),
        )
        acknowledgment = client.command("search.respond", response, idempotency_key="respond")
        assert acknowledgment["result"]["data"]["late_at_recording"] is False
        assert acknowledgment["result"]["data"]["request"]["usage"]["authority"] == "agent_reported"
        reopened = LocalRuntime.open(pipeline.runtime.root)
        restarted = ResearchClient(
            LocalTransport(reopened, lambda: reopened.credentials.resolve(token))
        )
        assert (
            restarted.command("search.respond", response, idempotency_key="respond")
            == acknowledgment
        )
        assert (
            restarted.command("search.respond", response, idempotency_key="new-delivery")["result"]
            == acknowledgment["result"]
        )
        assert (
            restarted.proposal_get(ProposalRead(request_id=row["id"]))["response"]
            == acknowledgment["result"]
        )
        assert pipeline.client.get("search_state", pipeline.campaign["id"]) == requested["search"]
        for kind in ("candidate", "trial"):
            assert len(pipeline.client.list(RecordQuery(kind=kind))["items"]) == 1
        assert pipeline.client.get("proposal_request", row["id"])["data"] == frozen
        assert request_budget(pipeline)["consumed"] == "1"
        with pytest.raises(ResearchError) as changed:
            restarted.command(
                "search.respond",
                response.model_copy(update={"hypothesis": "different"}),
                idempotency_key="changed-response",
            )
        assert changed.value.code == ErrorCode.IDEMPOTENCY_CONFLICT

    def test_assigned_agent_scope_and_artifact_identity_are_enforced(
        self, pipeline: Pipeline
    ) -> None:
        baseline(pipeline)
        principal, client, _ = proposer(pipeline)
        _, other, _ = proposer(pipeline)
        row = request_proposal(pipeline, principal)["request"]
        read = ProposalRead(request_id=row["id"])
        response = RespondToProposal(
            request_id=row["id"], outcome="failed", finding="No useful change"
        )
        for operation in (
            lambda: other.proposal_get(read),
            lambda: other.command("search.respond", response, idempotency_key=new_id()),
            lambda: client.get("campaign", pipeline.campaign["id"]),
        ):
            with pytest.raises(ResearchError) as denied:
                operation()
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        bad_ref = ArtifactRef.model_validate(row["data"]["parent_source"]).model_copy(
            update={"digest": "sha256:" + "0" * 64}
        )
        with pytest.raises(ResearchError) as mismatch:
            client.command(
                "search.respond",
                RespondToProposal(
                    request_id=row["id"],
                    outcome="candidate",
                    source_artifact=bad_ref,
                    hypothesis="Invalid artifact identity",
                ),
                idempotency_key="bad-artifact",
            )
        assert mismatch.value.code == ErrorCode.ARTIFACT_UNVERIFIED
        assert client.proposal_get(read)["response"] is None
        assert pipeline.client.get("proposal_request", row["id"])["state"] == "pending"
        assert (
            client.command("search.respond", response, idempotency_key="valid-failure")["result"][
                "data"
            ]["request"]["outcome"]
            == "failed"
        )

    @pytest.mark.parametrize("cause", ["cancel", "pause", "epoch", "invalidate"])
    def test_late_responses_are_retained_without_reopening_research(
        self, pipeline: Pipeline, cause: str
    ) -> None:
        receipt = baseline(pipeline)
        principal, client, _ = proposer(pipeline)
        row = request_proposal(pipeline, principal)["request"]
        if cause == "cancel":
            cancelled = cancel(pipeline, row)
            assert cancelled["search"]["data"]["pending_request_ids"] == []
            assert cancelled["cancellation"]["data"]["schema_version"] == "1.0"
        elif cause == "pause":
            campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
            pipeline.client.command(
                "campaign.pause",
                CampaignTarget(campaign_id=campaign["id"]),
                idempotency_key="pause",
                expected_revision=campaign["revision"],
            )
        elif cause == "epoch":
            advance_epoch(pipeline)
        else:
            pipeline.client.command(
                "evidence.invalidate",
                InvalidateEvidence(
                    target=EvidenceReference(kind="evaluation_receipt", id=receipt["id"]),
                    reason="Discovered invalid panel",
                ),
                idempotency_key="invalidate",
            )
        result = client.command(
            "search.respond",
            RespondToProposal(
                request_id=row["id"],
                outcome="failed",
                finding="Original agent finished after control changed",
            ),
            idempotency_key="late",
        )["result"]
        assert result["data"]["late_at_recording"] is True
        assert request_budget(pipeline)["consumed"] == "1"
        assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 1
        if cause == "cancel":
            assert pipeline.client.get("proposal_request", row["id"])["state"] == "cancelled"

    def test_search_revision_race_spends_one_request_and_preserves_rejected_command(
        self, pipeline: Pipeline
    ) -> None:
        baseline(pipeline)
        principal, _, _ = proposer(pipeline)
        intent = proposal_intent(pipeline, principal)
        barrier = threading.Barrier(2)

        def issue(index: int) -> dict[str, Any] | ErrorCode:
            client = ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
            barrier.wait(timeout=5)
            try:
                return client.command(
                    "search.request_proposal",
                    intent,
                    idempotency_key=f"race-{index}",
                    expected_revision=0,
                )
            except ResearchError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(issue, range(2)))
        assert results.count(ErrorCode.REVISION_CONFLICT) == 1
        assert len(pipeline.client.list(RecordQuery(kind="proposal_request"))["items"]) == 1
        assert request_budget(pipeline)["consumed"] == "1"
        winner = next(index for index, result in enumerate(results) if isinstance(result, dict))
        assert (
            pipeline.client.command(
                "search.request_proposal",
                intent,
                idempotency_key=f"race-{winner}",
                expected_revision=0,
            )
            == results[winner]
        )

    def test_greedy_capacity_cancellation_and_exhaustion_do_not_refund(
        self, pipeline: Pipeline
    ) -> None:
        baseline(pipeline)
        principal, client, _ = proposer(pipeline)
        first = request_proposal(pipeline, principal)
        with pytest.raises(ResearchError) as full:
            request_proposal(pipeline, principal, 1)
        assert full.value.code == ErrorCode.RESOURCE_UNAVAILABLE
        response = client.command(
            "search.respond",
            RespondToProposal(
                request_id=first["request"]["id"], outcome="failed", finding="No change"
            ),
            idempotency_key="failed",
        )["result"]
        cancelled = cancel(pipeline, first["request"])
        assert pipeline.client.get("proposal_response", response["id"]) == response
        second = request_proposal(pipeline, principal, cancelled["search"]["revision"])
        cancelled = cancel(pipeline, second["request"], second["search"]["revision"])
        with pytest.raises(ResearchError) as empty:
            request_proposal(pipeline, principal, cancelled["search"]["revision"])
        assert empty.value.code == ErrorCode.BUDGET_EXHAUSTED
        assert pipeline.client.get("search_state", pipeline.campaign["id"]) == cancelled["search"]
        assert request_budget(pipeline)["consumed"] == "2"
        assert request_budget(pipeline)["reserved"] == "0"

    def test_invalidated_incumbent_cannot_spend_proposal_budget(self, pipeline: Pipeline) -> None:
        receipt = baseline(pipeline)
        principal, _, _ = proposer(pipeline)
        pipeline.client.command(
            "evidence.invalidate",
            InvalidateEvidence(
                target=EvidenceReference(kind="evaluation_receipt", id=receipt["id"]),
                reason="Invalid baseline",
            ),
            idempotency_key="invalidate",
        )
        with pytest.raises(ResearchError) as suspended:
            request_proposal(pipeline, principal)
        assert suspended.value.code == ErrorCode.INVALID_TRANSITION
        assert request_budget(pipeline)["consumed"] == "0"
        assert pipeline.client.list(RecordQuery(kind="proposal_request"))["items"] == []


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
def test_external_policy_allows_bounded_parallel_requests(pipeline: Pipeline) -> None:
    baseline(pipeline)
    principal, _, _ = proposer(pipeline)
    first = request_proposal(pipeline, principal)
    second = request_proposal(pipeline, principal, 1)
    assert second["search"]["data"]["pending_request_ids"] == [
        first["request"]["id"],
        second["request"]["id"],
    ]
    with pytest.raises(ResearchError) as full:
        request_proposal(pipeline, principal, 2)
    assert full.value.code == ErrorCode.RESOURCE_UNAVAILABLE


def test_unallocated_request_budget_is_not_silently_added(pipeline: Pipeline) -> None:
    baseline(pipeline)
    principal, _, _ = proposer(pipeline)
    before = pipeline.client.budget(pipeline.campaign["id"])
    with pytest.raises(ResearchError) as unallocated:
        request_proposal(pipeline, principal)
    assert unallocated.value.code == ErrorCode.BUDGET_EXHAUSTED
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    assert pipeline.client.list(RecordQuery(kind="search_state"))["items"] == []


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            "limits": (
                *LIMITS,
                BudgetLimit(dimension="proposal_requests", limit=2, unit="count", kind=kind),
            )
        }
        for kind in ("capacity", "exposure")
    ],
    indirect=True,
)
def test_request_budget_rejects_wrong_accounting_kind_without_mutation(pipeline: Pipeline) -> None:
    baseline(pipeline)
    principal, _, _ = proposer(pipeline)
    before = pipeline.client.budget(pipeline.campaign["id"])
    with pytest.raises(ResearchError) as wrong_kind:
        request_proposal(pipeline, principal)
    assert wrong_kind.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
    assert pipeline.client.budget(pipeline.campaign["id"]) == before
    assert pipeline.client.list(RecordQuery(kind="proposal_request"))["items"] == []
    assert pipeline.client.list(RecordQuery(kind="search_state"))["items"] == []


@pytest.mark.parametrize("pipeline", [{"limits": PROPOSAL_LIMITS}], indirect=True)
async def test_assigned_agent_get_and_response_share_cli_mcp_http_sdk_contract(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    from mcp import Client

    from research_swarm.api.http import create_app
    from research_swarm.runtime import write_private
    from tests.api.test_cli_mcp import cli, stdio
    from tests.orchestration.test_http_delivery import serving

    baseline(pipeline)
    principal, _, token = proposer(pipeline)
    row = request_proposal(pipeline, principal)["request"]
    write_private(tmp_path / "proposer.token", token.encode("ascii"))
    token_path = tmp_path / "proposer.token"
    with serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url:
        flags = ("--controller", url, "--token-file", str(token_path))
        viewed = cli(pipeline.runtime.root, *flags, "proposal", "get", row["id"])
        assert viewed == {"request": row, "response": None}
        response = RespondToProposal(
            request_id=row["id"], outcome="failed", finding="No promising mutation found"
        )
        async with Client(
            stdio(pipeline.runtime.root, *flags), read_timeout_seconds=20, cache=None
        ) as agent:
            tools = {tool.name for tool in (await agent.list_tools()).tools}
            assert {
                "research_search_request_proposal",
                "research_search_respond",
                "research_proposal_get",
            } <= tools
            detail = await agent.call_tool(
                "research_proposal_get", {"request": {"request_id": row["id"]}}
            )
            assert not detail.is_error and detail.structured_content is not None
            assert detail.structured_content["result"] == viewed
            result = await agent.call_tool(
                "research_search_respond",
                {"request": response.model_dump(mode="json"), "idempotency_key": "agent-response"},
            )
            assert not result.is_error and result.structured_content is not None
            acknowledgment = result.structured_content
        with ResearchClient.connect(url, token) as client:
            replay = client.command("search.respond", response, idempotency_key="agent-response")
            assert replay["command_id"] == acknowledgment["command_id"]
            assert replay["result"]["id"] == acknowledgment["result"]["id"]
            assert (
                client.proposal_get(ProposalRead(request_id=row["id"]))["response"]
                == replay["result"]
            )
            with pytest.raises(ResearchError) as denied:
                client.context(pipeline.campaign["id"])
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
