"""An external MCP actor controls a real research cycle across process restart."""

from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from typing import Any

import pytest
from mcp import Client

from research_swarm.control.models import StoppingRule
from research_swarm.control.queries import RecordQuery
from research_swarm.orchestration.evaluation import EvaluationDriver
from research_swarm.runtime import write_private
from tests.test_cli_mcp import cli, stdio
from tests.test_evaluation_driver import role
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_proposals import PROPOSAL_LIMITS


async def tool(agent: Client, name: str, **arguments: Any) -> dict[str, Any]:
    response = await agent.call_tool("research_" + name, arguments)
    assert not response.is_error, response.structured_content
    assert response.structured_content is not None
    return dict(response.structured_content)


async def command(
    agent: Client, operation: str, request: dict[str, Any], key: str, revision: int | None = None
) -> dict[str, Any]:
    arguments: dict[str, Any] = {"request": request, "idempotency_key": key}
    if revision is not None:
        arguments["expected_revision"] = revision
    return await tool(agent, operation.replace(".", "_"), **arguments)


async def record(agent: Client, kind: str, identifier: str) -> dict[str, Any]:
    return (await tool(agent, "get_record", kind=kind, identifier=identifier, detail=True))[
        "result"
    ]


async def context(agent: Client, campaign_id: str) -> dict[str, Any]:
    return (await tool(agent, "get_context", campaign_id=campaign_id))["result"]


async def receipt(agent: Client, campaign_id: str, candidate_id: str) -> dict[str, Any]:
    found = (
        await tool(
            agent,
            "query_records",
            request={
                "kind": "evaluation_receipt",
                "campaign_id": campaign_id,
                "parent_id": candidate_id,
            },
        )
    )["result"]["items"]
    assert len(found) == 1
    return await record(agent, "evaluation_receipt", found[0]["id"])


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            "limits": PROPOSAL_LIMITS,
            "source_learning_rate": 0.0,
            "stopping": StoppingRule(max_trials=2, permit_manual_completion=False),
        }
    ],
    indirect=True,
)
async def test_mcp_actor_proposes_runs_selects_and_reports_after_process_restart(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    researcher, _, token = role(
        pipeline,
        "read",
        "campaign.manage",
        "source.capture",
        "artifact.upload",
        "search.manage",
        "search.observe",
        "proposal.respond",
        "candidate.propose",
        "candidate.validate",
        "trial.submit",
        "promotion.assess",
        "promotion.commit",
    )
    evaluator, evaluator_client, _ = role(pipeline, "read", "evaluation.publish")
    _, evaluation_controller, _ = role(pipeline, "read", "evaluation.request", "artifact.upload")
    evaluate = EvaluationDriver(
        evaluation_controller, evaluator_client, pipeline.campaign["id"], evaluator.id
    )
    token_path = pipeline.runtime.root / "researcher.token"
    write_private(token_path, token.encode("ascii"))
    connection = stdio(pipeline.runtime.root, "--token-file", str(token_path))
    campaign_id = pipeline.campaign["id"]

    def execute(trial: dict[str, Any]) -> None:
        training = pipeline.worker.claim(pipeline.client.get("job", trial["data"]["job_ids"][0]))
        assert pipeline.worker.execute(training["id"])["state"] == "succeeded"
        assert [step.operation for step in evaluate.advance().actions] == ["evaluation.authorize"]
        scoring = pipeline.worker.claim(pipeline.client.get("job", trial["data"]["job_ids"][1]))
        assert pipeline.worker.execute(scoring["id"])["state"] == "succeeded"
        assert [step.operation for step in evaluate.advance().actions] == ["evaluation.complete"]

    await asyncio.to_thread(execute, pipeline.trial)
    source = tmp_path / "agent-candidate"
    shutil.copytree(pipeline.source, source)
    train = source / "train.py"
    original = train.read_text(encoding="utf-8")
    assert original.count("LEARNING_RATE = 0.0") == 1
    train.write_text(
        original.replace("LEARNING_RATE = 0.0", "LEARNING_RATE = 0.35"), encoding="utf-8"
    )

    async with Client(connection, read_timeout_seconds=30, cache=None) as agent:
        capabilities = (await tool(agent, "capabilities"))["result"]
        assert capabilities["commands"]["search.admit_response"]["expected_revision"]
        baseline = await receipt(agent, campaign_id, pipeline.trial["data"]["candidate_id"])
        decision = await command(
            agent,
            "promotion.assess",
            {
                "campaign_id": campaign_id,
                "candidate_evaluation_id": baseline["id"],
                "expected_incumbent_revision": 0,
            },
            "baseline-assess",
        )
        await command(
            agent, "promotion.commit", {"decision_id": decision["result"]["id"]}, "baseline-select"
        )
        view = await context(agent, campaign_id)
        frozen_authority = {
            "expected_controller_id": view["authority"]["controller_id"],
            "expected_control_epoch": view["authority"]["control_epoch"],
        }
        proposal = await command(
            agent,
            "search.request_proposal",
            {
                "campaign_id": campaign_id,
                "proposer_id": researcher.id,
                "seed": 19,
                "expected_incumbent_revision": 1,
                **frozen_authority,
                "instructions": "Change the optimizer step size and preserve evaluation and data.",
            },
            "proposal-request",
            view["search_state"]["revision"] if view["search_state"] else 0,
        )
        request_id = proposal["result"]["request"]["id"]
        assigned = (await tool(agent, "proposal_get", request={"request_id": request_id}))["result"]
        assert (
            assigned["request"]["data"]["parent_candidate_id"]
            == pipeline.trial["data"]["candidate_id"]
        )
        captured = (
            await tool(agent, "capture_project", path=str(source), idempotency_key="agent-source")
        )["result"]
        await command(
            agent,
            "search.respond",
            {
                "request_id": request_id,
                "outcome": "candidate",
                "source_artifact": captured,
                "hypothesis": "A nonzero optimizer step improves the fixed baseline.",
            },
            "agent-response",
        )
        admission_request = {
            "campaign_id": campaign_id,
            "request_id": request_id,
            "expected_incumbent_revision": 1,
            **frozen_authority,
        }
        admission_revision = (await context(agent, campaign_id))["search_state"]["revision"]
        admitted = await command(
            agent, "search.admit_response", admission_request, "agent-admit", admission_revision
        )
        trial_id = admitted["result"]["trial"]["id"]
        trial = await record(agent, "trial", trial_id)
        candidate_id = trial["data"]["candidate_id"]
        candidate = await record(agent, "candidate", candidate_id)
        assert candidate["data"]["proposer_id"] == researcher.id
    # The MCP server process exits here. Native execution and independent evaluation
    # proceed without an agent connection or its in-memory plan.
    await asyncio.to_thread(execute, trial)

    async with Client(connection, read_timeout_seconds=30, cache=None) as agent:
        replay = await command(
            agent, "search.admit_response", admission_request, "agent-admit", admission_revision
        )
        assert replay == admitted
        trial = await record(agent, "trial", trial_id)
        result = await receipt(agent, campaign_id, candidate_id)
        measured = result["data"]["receipt"]
        assert measured["evaluator_id"] == evaluator.id
        assert measured["metrics"][0]["value"] < baseline["data"]["receipt"]["metrics"][0]["value"]
        attempted_forgery = await agent.call_tool(
            "research_evaluation_complete",
            {
                "request": {
                    "request_id": measured["request_id"],
                    "attempt_id": result["data"]["attempt_id"],
                },
                "idempotency_key": "researcher-cannot-score",
            },
        )
        assert attempted_forgery.is_error
        assert attempted_forgery.structured_content["error"]["code"] == "AUTHORIZATION_DENIED"
        observation = await command(
            agent,
            "search.observe_trial",
            {"campaign_id": campaign_id, "trial_id": trial_id},
            "agent-observe",
            trial["revision"],
        )
        observation_id = observation["result"]["id"]
        view = await context(agent, campaign_id)
        await command(
            agent,
            "search.select_observation",
            {"campaign_id": campaign_id, "observation_id": observation_id, **frozen_authority},
            "agent-pin",
            view["search_state"]["revision"],
        )
        decision = await command(
            agent,
            "promotion.assess",
            {
                "campaign_id": campaign_id,
                "candidate_evaluation_id": result["id"],
                "expected_incumbent_revision": 1,
            },
            "agent-assess",
        )
        await command(
            agent, "promotion.commit", {"decision_id": decision["result"]["id"]}, "agent-select"
        )
        view = await context(agent, campaign_id)
        assert view["campaign"]["data"]["incumbent_candidate_id"] == candidate_id
        await command(
            agent,
            "search.resolve_trial",
            {
                "campaign_id": campaign_id,
                "observation_id": observation_id,
                "decision_id": decision["result"]["id"],
                **frozen_authority,
            },
            "agent-resolve",
            view["search_state"]["revision"],
        )
        view = await context(agent, campaign_id)
        finish_revision = view["campaign"]["revision"]
        finish_request = {
            "campaign_id": campaign_id,
            "reason": "Declared two-trial research completed",
        }
        finished = await command(
            agent, "campaign.complete", finish_request, "agent-complete", finish_revision
        )
        report = await record(agent, "report", finished["result"]["report"]["id"])
        snapshot = report["data"]["snapshot"]
        assert snapshot["campaign_state"] == "completed"
        assert snapshot["incumbent"]["candidate_id"] == candidate_id
        assert snapshot["incumbent"]["evidence_level"] == "observed"
        assert snapshot["incumbent"]["revision"] == 2
        assert (await context(agent, campaign_id))["search_state"]["data"]["active_trial_ids"] == []

    replayed = await asyncio.to_thread(
        cli,
        pipeline.runtime.root,
        "--token-file",
        str(token_path),
        "command",
        "campaign.complete",
        "--key",
        "agent-complete",
        "--revision",
        str(finish_revision),
        payload=finish_request,
    )
    # CLI replay uses the same researcher credential, not the operator principal.
    assert replayed["result"]["report"]["id"] == report["id"]
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2
    assert len(pipeline.client.list(RecordQuery(kind="attempt"))["items"]) == 4
    assert len(pipeline.client.list(RecordQuery(kind="observation_consumption"))["items"]) == 1
    accounts = pipeline.client.budget(campaign_id)["accounts"]
    assert all(account["reserved"] == account["uncertain"] == "0" for account in accounts)
    assert (
        next(account for account in accounts if account["dimension"] == "attempts")["consumed"]
        == "4"
    )
