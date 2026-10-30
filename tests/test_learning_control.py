"""Native evidence, independent database races and agent transport for precommitment."""

import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.learning_models import (
    CommitPolicyDecision,
    FreezeLearningPlan,
    LearningPlan,
    PlanEpisode,
)
from research_swarm.control.models import SearchConfiguration
from research_swarm.control.promotions import InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.search_models import RespondToProposal
from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.experiments import Direction
from research_swarm.domain.learning import CategoricalPolicy, PolicyAction, RewardSpec
from research_swarm.domain.types import new_id
from research_swarm.learning.reinforce import decision
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_proposals import PROPOSAL_LIMITS, baseline, cancel, proposal_intent, proposer


@dataclass
class Prepared:
    intent: FreezeLearningPlan
    agent: ResearchClient
    token: str
    requests: list

    def freeze(self, client):
        return client.command("learning.freeze_plan", self.intent, idempotency_key="plan")["result"]

    def decide(self, index=0, key="decision"):
        return self.agent.command(
            "learning.commit_decision",
            CommitPolicyDecision(request_id=self.requests[index]["id"]),
            idempotency_key=key,
        )["result"]

    def response(self, index=0):
        row = self.requests[index]
        trace = decision(
            self.intent.policy,
            episode_id=row["id"],
            campaign_id=row["data"]["campaign_id"],
            proposal_request_id=row["id"],
            context_digest=row["data"]["context_digest"],
            seed=row["data"]["seed"],
        )
        return RespondToProposal(
            request_id=row["id"],
            outcome="candidate",
            source_artifact=self.intent.action_artifacts[trace.action_id],
            hypothesis="Evaluate the precommitted learning-rate action",
        )


def prepare(pipeline, tmp_path, count=1, seed=7):
    baseline(pipeline)
    principal, agent, token = proposer(pipeline)
    rows = []
    for ordinal in range(count):
        rows.append(
            pipeline.client.command(
                "search.request_proposal",
                proposal_intent(pipeline, principal).model_copy(update={"seed": seed}),
                idempotency_key=f"proposal-{ordinal}",
                expected_revision=ordinal,
            )["result"]["request"]
        )
    first = ArtifactRef.model_validate(rows[0]["data"]["parent_source"])
    source = tmp_path / "alternative"
    shutil.copytree(pipeline.source, source)
    training = source / "train.py"
    training.write_text(
        training.read_text().replace("LEARNING_RATE = 0.35", "LEARNING_RATE = 0.05")
    )
    revision = pipeline.client.register_project(source, idempotency_key="alternative")["result"]
    second = ArtifactRef.model_validate(revision["project_revision"]["data"]["source_artifact"])
    artifacts = {"fast": first, "slow": second}
    intent = FreezeLearningPlan(
        plan_id=new_id(),
        policy=CategoricalPolicy(
            actions=tuple(
                PolicyAction(id=name, source_digest=ref.digest) for name, ref in artifacts.items()
            ),
            logits=(0.0, 0.0),
        ),
        reward_spec=RewardSpec(metric_name="val_bpb", direction=Direction.MINIMIZE, scale=5.0),
        action_artifacts=artifacts,
        episodes=tuple(
            PlanEpisode(request_id=row["id"], split="development" if index == 0 else "held_out")
            for index, row in enumerate(rows)
        ),
    )
    return Prepared(intent, agent, token, rows)


def reopen_agent(pipeline, token):
    runtime = LocalRuntime.open(pipeline.runtime.root)
    return ResearchClient(LocalTransport(runtime, lambda: runtime.credentials.resolve(token)))


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
class TestLearningControl:
    def test_frozen_batch_replays_after_response_and_restart_without_new_cost(
        self, pipeline, tmp_path
    ):
        prepared = prepare(pipeline, tmp_path, count=2)
        before = pipeline.client.budget(pipeline.campaign["id"])
        plan_row = prepared.freeze(pipeline.client)
        plan = LearningPlan.model_validate(plan_row["data"])
        assert [row.split for row in plan.episodes] == ["development", "held_out"]
        assert [row.episode_id for row in plan.episodes] == [row["id"] for row in prepared.requests]
        assert all(row.seed == 7 for row in plan.episodes)
        first = prepared.decide()
        assert first["data"]["plan_digest"] == plan.digest
        assert first["data"]["decision"]["behavior_policy_digest"] == prepared.intent.policy.digest
        response = prepared.agent.command(
            "search.respond", prepared.response(), idempotency_key="candidate-response"
        )["result"]
        assert not response["data"]["late_at_recording"]
        prepared.agent = reopen_agent(pipeline, prepared.token)
        assert prepared.decide() == first
        assert prepared.decide(key="second-key") == first
        assert prepared.freeze(pipeline.client) == plan_row
        assert (
            pipeline.client.command(
                "learning.freeze_plan", prepared.intent, idempotency_key="plan-new-key"
            )["result"]
            == plan_row
        )
        assert pipeline.client.budget(pipeline.campaign["id"]) == before
        assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 1
        assert len(pipeline.client.list(RecordQuery(kind="learning_decision"))["items"]) == 1
        held_out = prepared.decide(index=1, key="held-out")
        assert held_out["data"]["decision"]["proposal_request_id"] == prepared.requests[1]["id"]

    def test_partial_batch_cannot_be_frozen_after_any_response(self, pipeline, tmp_path):
        prepared = prepare(pipeline, tmp_path, count=2)
        prepared.agent.command(
            "search.respond",
            RespondToProposal(
                request_id=prepared.requests[1]["id"],
                outcome="failed",
                finding="Failed before planning",
            ),
            idempotency_key="early-response",
        )
        with pytest.raises(ResearchError) as late:
            prepared.freeze(pipeline.client)
        assert late.value.code == ErrorCode.INVALID_TRANSITION
        assert pipeline.client.list(RecordQuery(kind="learning_episode"))["items"] == []
        assert pipeline.client.list(RecordQuery(kind="learning_plan"))["items"] == []

    def test_decision_event_failure_does_not_open_candidate_admission(
        self, pipeline, tmp_path, monkeypatch
    ):
        prepared = prepare(pipeline, tmp_path)
        prepared.freeze(pipeline.client)
        original = Transaction.event

        def fail(transaction, event_type, *args, **kwargs):
            if event_type == "learning.decision_committed":
                raise RuntimeError("decision event failed")
            return original(transaction, event_type, *args, **kwargs)

        monkeypatch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="decision event"):
            prepared.decide()
        assert pipeline.client.list(RecordQuery(kind="learning_decision"))["items"] == []
        with pytest.raises(ResearchError) as premature:
            prepared.agent.command(
                "search.respond", prepared.response(), idempotency_key="premature"
            )
        assert premature.value.code == ErrorCode.INVALID_TRANSITION
        monkeypatch.setattr(Transaction, "event", original)
        committed = prepared.decide()
        assert committed["data"][
            "source_artifact"
        ] == prepared.response().source_artifact.model_dump(mode="json")

    async def test_assigned_agent_commits_through_mcp_http_and_replays_via_sdk(
        self, pipeline, tmp_path
    ):
        from mcp import Client

        from research_swarm.api.http import create_app
        from research_swarm.runtime import read_private, write_private
        from tests.test_cli_mcp import stdio
        from tests.test_http_delivery import serving

        prepared = prepare(pipeline, tmp_path)
        token_file = tmp_path / "agent.token"
        write_private(token_file, prepared.token.encode("ascii"))
        with serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url:
            token = read_private(pipeline.runtime.root, "operator.token", limit=256).decode("ascii")
            with ResearchClient.connect(url, token) as operator:
                prepared.freeze(operator)
            flags = ("--controller", url, "--token-file", str(token_file))
            intent = CommitPolicyDecision(request_id=prepared.requests[0]["id"])
            async with Client(
                stdio(pipeline.runtime.root, *flags), read_timeout_seconds=20, cache=None
            ) as agent:
                names = {tool.name for tool in (await agent.list_tools()).tools}
                assert {
                    "research_learning_freeze_plan",
                    "research_learning_commit_decision",
                } <= names
                result = await agent.call_tool(
                    "research_learning_commit_decision",
                    {
                        "request": intent.model_dump(mode="json"),
                        "idempotency_key": "agent-decision",
                    },
                )
                assert not result.is_error and result.structured_content is not None
                acknowledgment = result.structured_content
            with ResearchClient.connect(url, prepared.token) as client:
                replay = client.command(
                    "learning.commit_decision", intent, idempotency_key="agent-decision"
                )
                assert replay["command_id"] == acknowledgment["command_id"]
                assert replay["result"] == acknowledgment["result"]
                source = ArtifactRef.model_validate(replay["result"]["data"]["source_artifact"])
                response = client.command(
                    "search.respond",
                    RespondToProposal(
                        request_id=intent.request_id,
                        outcome="candidate",
                        source_artifact=source,
                        hypothesis="Use the controller-selected source",
                    ),
                    idempotency_key="response",
                )["result"]
                assert (
                    response["data"]["request"]["source_artifact"]["digest"]
                    == replay["result"]["data"]["decision"]["source_digest"]
                )

    def test_candidate_requires_decision_and_exact_source_but_failure_survives(
        self, pipeline, tmp_path
    ):
        prepared = prepare(pipeline, tmp_path, count=2)
        prepared.freeze(pipeline.client)
        with pytest.raises(ResearchError) as missing:
            prepared.agent.command(
                "search.respond", prepared.response(), idempotency_key="too-soon"
            )
        assert missing.value.code == ErrorCode.INVALID_TRANSITION
        prepared.decide()
        correct = prepared.response()
        wrong = next(
            ref
            for ref in prepared.intent.action_artifacts.values()
            if ref != correct.source_artifact
        )
        with pytest.raises(ResearchError) as changed:
            prepared.agent.command(
                "search.respond",
                correct.model_copy(update={"source_artifact": wrong}),
                idempotency_key="wrong-source",
            )
        assert changed.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
        assert pipeline.client.list(RecordQuery(kind="proposal_response"))["items"] == []
        failed = RespondToProposal(
            request_id=prepared.requests[1]["id"],
            outcome="failed",
            finding="Agent failed before sampling",
        )
        prepared.agent.command("search.respond", failed, idempotency_key="failed")
        with pytest.raises(ResearchError) as late:
            prepared.decide(index=1, key="after-failure")
        assert late.value.code == ErrorCode.INVALID_TRANSITION
        assert (
            pipeline.client.get("learning_episode", prepared.requests[1]["id"])["data"]["episode"][
                "split"
            ]
            == "held_out"
        )

    def test_plan_cannot_rebind_relabel_or_change_reward_after_freeze(self, pipeline, tmp_path):
        prepared = prepare(pipeline, tmp_path)
        prepared.freeze(pipeline.client)
        with pytest.raises(ResearchError) as rebound:
            pipeline.client.command(
                "learning.freeze_plan",
                prepared.intent.model_copy(update={"plan_id": new_id()}),
                idempotency_key="rebind",
            )
        assert rebound.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
        for field, value in (
            ("reward_spec", prepared.intent.reward_spec.model_copy(update={"scale": 10.0})),
            ("episodes", (prepared.intent.episodes[0].model_copy(update={"split": "held_out"}),)),
        ):
            with pytest.raises(ResearchError) as changed:
                pipeline.client.command(
                    "learning.freeze_plan",
                    prepared.intent.model_copy(update={field: value}),
                    idempotency_key=field,
                )
            assert changed.value.code == ErrorCode.IDEMPOTENCY_CONFLICT

    def test_actor_ownership_and_freeze_scope(self, pipeline, tmp_path):
        prepared = prepare(pipeline, tmp_path)
        with pytest.raises(ResearchError) as denied:
            prepared.freeze(prepared.agent)
        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        prepared.freeze(pipeline.client)
        _, other, _ = proposer(pipeline)
        for client in (other, pipeline.client):
            with pytest.raises(ResearchError) as owner:
                client.command(
                    "learning.commit_decision",
                    CommitPolicyDecision(request_id=prepared.requests[0]["id"]),
                    idempotency_key=new_id(),
                )
            assert owner.value.code == ErrorCode.AUTHORIZATION_DENIED
        assert pipeline.client.list(RecordQuery(kind="learning_decision"))["items"] == []

    def test_same_seed_comparator_is_required(self, pipeline, tmp_path):
        prepared = prepare(pipeline, tmp_path, seed=19)
        with pytest.raises(ResearchError) as different:
            prepared.freeze(pipeline.client)
        assert different.value.code == ErrorCode.COMPARISON_INELIGIBLE
        assert pipeline.client.list(RecordQuery(kind="learning_plan"))["items"] == []

    @pytest.mark.parametrize("phase", ["before_freeze", "before_decision"])
    def test_corrections_block_new_learning_authority(self, pipeline, tmp_path, phase):
        prepared = prepare(pipeline, tmp_path)
        if phase == "before_decision":
            prepared.freeze(pipeline.client)
        evaluation_id = prepared.requests[0]["data"]["context"]["incumbent_evaluation_id"]
        pipeline.client.command(
            "evidence.invalidate",
            InvalidateEvidence(
                target=EvidenceReference(kind="evaluation_receipt", id=evaluation_id),
                reason="Comparator evidence invalidated",
            ),
            idempotency_key="correction",
        )
        with pytest.raises(ResearchError):
            prepared.freeze(pipeline.client) if phase == "before_freeze" else prepared.decide()
        assert pipeline.client.list(RecordQuery(kind="learning_decision"))["items"] == []

    def test_cancelled_episode_never_samples_but_keeps_failed_response(self, pipeline, tmp_path):
        prepared = prepare(pipeline, tmp_path)
        prepared.freeze(pipeline.client)
        cancel(pipeline, prepared.requests[0])
        with pytest.raises(ResearchError) as closed:
            prepared.decide()
        assert closed.value.code == ErrorCode.INVALID_TRANSITION
        response = prepared.agent.command(
            "search.respond",
            RespondToProposal(
                request_id=prepared.requests[0]["id"],
                outcome="failed",
                finding="Cancelled before decision",
            ),
            idempotency_key="late-failure",
        )["result"]
        assert response["data"]["late_at_recording"]

    def test_freeze_is_atomic_with_all_bindings_retentions_and_event(
        self, pipeline, tmp_path, monkeypatch
    ):
        prepared = prepare(pipeline, tmp_path, count=2)
        original = Transaction.event

        def fail(transaction, event_type, *args, **kwargs):
            if event_type == "learning.plan_frozen":
                raise RuntimeError("injected event failure")
            return original(transaction, event_type, *args, **kwargs)

        monkeypatch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="injected"):
            prepared.freeze(pipeline.client)
        for kind in ("learning_plan", "learning_episode"):
            assert pipeline.client.list(RecordQuery(kind=kind))["items"] == []
        assert (
            pipeline.runtime.database.store.read(
                lambda c: c.execute(
                    "SELECT COUNT(*) FROM records WHERE kind='artifact_reference' "
                    'AND payload LIKE \'%"owner_kind":"learning_plan"%\''
                ).one()[0]
            )
            == 0
        )
        monkeypatch.setattr(Transaction, "event", original)
        prepared.freeze(pipeline.client)
        assert len(pipeline.client.list(RecordQuery(kind="learning_episode"))["items"]) == 2

    def test_concurrent_decisions_reuse_one_committed_draw(self, pipeline, tmp_path):
        prepared = prepare(pipeline, tmp_path)
        prepared.freeze(pipeline.client)
        clients = [reopen_agent(pipeline, prepared.token) for _ in range(2)]
        barrier = threading.Barrier(2)

        def issue(index):
            barrier.wait(timeout=5)
            return clients[index].command(
                "learning.commit_decision",
                CommitPolicyDecision(request_id=prepared.requests[0]["id"]),
                idempotency_key=f"decision-{index}",
            )["result"]

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(issue, range(2)))
        assert results[0] == results[1]
        assert len(pipeline.client.list(RecordQuery(kind="learning_decision"))["items"]) == 1

    def test_response_and_plan_race_cannot_retroactively_freeze(self, pipeline, tmp_path):
        prepared = prepare(pipeline, tmp_path)
        operator = ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
        agent = reopen_agent(pipeline, prepared.token)
        barrier = threading.Barrier(2)

        def issue(index):
            barrier.wait(timeout=5)
            try:
                if index == 0:
                    prepared.freeze(operator)
                else:
                    agent.command("search.respond", prepared.response(), idempotency_key="response")
                return "accepted"
            except ResearchError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(issue, range(2)))
        assert results.count("accepted") == 1
        assert ErrorCode.INVALID_TRANSITION in results
        plans = pipeline.client.list(RecordQuery(kind="learning_plan"))["items"]
        responses = pipeline.client.list(RecordQuery(kind="proposal_response"))["items"]
        assert bool(plans) != bool(responses)

    def test_decision_and_candidate_response_race_keeps_commit_order(self, pipeline, tmp_path):
        prepared = prepare(pipeline, tmp_path)
        prepared.freeze(pipeline.client)
        clients = [reopen_agent(pipeline, prepared.token) for _ in range(2)]
        barrier = threading.Barrier(2)

        def issue(index):
            barrier.wait(timeout=5)
            try:
                return clients[index].command(
                    "learning.commit_decision" if index == 0 else "search.respond",
                    CommitPolicyDecision(request_id=prepared.requests[0]["id"])
                    if index == 0
                    else prepared.response(),
                    idempotency_key=f"race-{index}",
                )["result"]
            except ResearchError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(issue, range(2)))
        assert isinstance(results[0], dict)
        if results[1] != ErrorCode.INVALID_TRANSITION:
            assert (
                results[1]["data"]["request"]["source_artifact"]["digest"]
                == results[0]["data"]["decision"]["source_digest"]
            )
        assert len(pipeline.client.list(RecordQuery(kind="learning_decision"))["items"]) == 1
