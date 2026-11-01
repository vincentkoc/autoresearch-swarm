"""Analytic gradients and actual training/checking through ordinary native jobs."""

import math
import shutil
from uuid import UUID

import pytest
from pydantic import ValidationError

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import Direction
from research_swarm.domain.learning import (
    BudgetSettlementEvidence,
    CategoricalPolicy,
    LearningEvidence,
    PolicyAction,
    RewardSpec,
    Trajectory,
    TrajectoryDataset,
)
from research_swarm.learning.reinforce import decision, probabilities, reward, update
from research_swarm.orchestration.bootstrap import RunOptions, prepare_run
from research_swarm.runtime import LocalRuntime
from research_swarm.workers.local_enrollment import ensure_local_enrollment
from tests.paths import REPO_ROOT


def identifier(number):
    return str(UUID(int=number))


@pytest.fixture
def rollout():
    policy = CategoricalPolicy(
        actions=(
            PolicyAction(id="a", source_digest=content_digest(b"source-a")),
            PolicyAction(id="b", source_digest=content_digest(b"source-b")),
        ),
        logits=(0.0, 0.0),
    )
    spec = RewardSpec(metric_name="score", direction=Direction.MINIMIZE, scale=1.0)
    rows = []
    for ordinal in range(1, 9):
        trace = decision(
            policy,
            episode_id=identifier(ordinal),
            campaign_id=identifier(100 + ordinal),
            proposal_request_id=identifier(200 + ordinal),
            context_digest=content_digest(b"context"),
            seed=ordinal,
        )
        value = 1.0 if trace.action_id == "a" else 3.0
        rows.append(
            Trajectory(
                id=identifier(300 + ordinal),
                decision=trace,
                split="development",
                trial_id=identifier(400 + ordinal),
                outcome="measurement",
                metric_name="score",
                comparator_value=2.0,
                candidate_value=value,
                comparison_digest=content_digest(b"comparison"),
                comparator_evaluation_id=identifier(500 + ordinal),
                candidate_evaluation_id=identifier(600 + ordinal),
                consumed={"execution_ms": "100", "attempts": "2"},
                evidence=(
                    LearningEvidence(
                        kind="search_observation",
                        id=identifier(700 + ordinal),
                        revision=1,
                        digest=content_digest(b"synthetic-evidence"),
                    ),
                ),
                reward=2.0 - value,
            )
        )
    return policy, TrajectoryDataset(
        id=identifier(1000),
        plan_digest=content_digest(b"synthetic-plan"),
        behavior_policy_digest=policy.digest,
        reward_spec=spec,
        evidence_through=100,
        planned_episode_ids=tuple(row.decision.episode_id for row in rows),
        trajectories=tuple(rows),
    )


def test_analytic_gradient_changes_policy_and_preserves_inputs(rollout):
    policy, dataset = rollout
    before = canonical_bytes(
        {"policy": policy.model_dump(mode="json"), "dataset": dataset.model_dump(mode="json")}
    )
    trained = update(policy, dataset, learning_rate=0.5)
    assert trained.gradient == (0.5, -0.5)
    assert trained.policy.logits == (0.25, -0.25)
    assert probabilities(trained.policy)[0] == pytest.approx(0.6224593312018546)
    assert (
        trained.dataset_digest == dataset.digest and trained.policy.parent_digest == policy.digest
    )
    assert update(policy, dataset, learning_rate=0.5) == trained
    assert (
        canonical_bytes(
            {"policy": policy.model_dump(mode="json"), "dataset": dataset.model_dump(mode="json")}
        )
        == before
    )
    epsilon = 1e-6
    positive = CategoricalPolicy(actions=policy.actions, logits=(epsilon, 0.0))
    negative = CategoricalPolicy(actions=policy.actions, logits=(-epsilon, 0.0))

    # Independent finite difference of expected reward under the complete two-action task.
    def objective(value):
        return probabilities(value)[0] - probabilities(value)[1]

    assert (objective(positive) - objective(negative)) / (2 * epsilon) == pytest.approx(
        trained.gradient[0], abs=1e-9
    )
    with pytest.raises(ValueError, match="frozen behavior"):
        update(trained.policy, dataset, learning_rate=0.5)


def test_uniform_policy_identity_and_sampler_golden_vector(rollout):
    policy, dataset = rollout
    trace = dataset.trajectories[0].decision
    assert (
        policy.digest == "sha256:a906eb25b098e5b974bf7ff9fd5abb7cb872058207fe2607f1105ac00141ffe2"
    )
    assert trace.digest == "sha256:8bd01b2be861f8abba804f898fa9f7f1f5d9248636e37b49b52cdc82d886de8b"
    assert trace.draw == 0.399641782414875
    assert trace.action_id == "a" and trace.probabilities == (0.5, 0.5)


@pytest.mark.parametrize("field", ["trial_id", "candidate_evaluation_id"])
def test_repeated_research_evidence_cannot_count_as_another_episode(rollout, field):
    _, dataset = rollout
    data = dataset.model_dump(mode="json")
    data["trajectories"][1][field] = data["trajectories"][0][field]
    with pytest.raises(ValidationError, match="complete ordered"):
        TrajectoryDataset.model_validate(data)


def test_settlement_reference_uses_immutable_request_and_response_digests(rollout):
    _, dataset = rollout
    row = dataset.trajectories[0].model_dump(mode="json")
    reference = BudgetSettlementEvidence(
        id=identifier(900),
        request_digest=content_digest(b"settlement request"),
        response_digest=content_digest(b"settlement response"),
    )
    row["evidence"].append(reference.model_dump(mode="json"))
    parsed = Trajectory.model_validate(row)
    assert parsed.evidence[-1] == reference
    row["evidence"][-1]["revision"] = 1
    with pytest.raises(ValidationError):
        Trajectory.model_validate(row)


@pytest.mark.parametrize(
    "change", ["probability", "action", "source", "draw", "policy", "reward", "held_out", "unknown"]
)
def test_incompatible_or_censored_rollouts_cannot_train(rollout, change):
    policy, dataset = rollout
    data = dataset.model_dump(mode="json")
    row = data["trajectories"][0]
    trace = row["decision"]
    if change == "probability":
        trace["probabilities"] = [0.75, 0.25]
    elif change == "action":
        trace["action_id"] = "b" if trace["action_id"] == "a" else "a"
    elif change == "source":
        trace["source_digest"] = content_digest(b"undeclared source")
    elif change == "draw":
        trace["draw"] = (trace["draw"] + 0.3) % 1
    elif change == "policy":
        trace["behavior_policy_digest"] = content_digest(b"another policy")
    elif change == "reward":
        row["reward"] = 0.4
    elif change == "held_out":
        row["split"] = "held_out"
    else:
        row["outcome"] = "execution_unknown"
        for name in (
            "metric_name",
            "comparator_value",
            "candidate_value",
            "comparison_digest",
            "comparator_evaluation_id",
            "candidate_evaluation_id",
            "reward",
        ):
            row[name] = None
    malformed = TrajectoryDataset.model_validate(data)
    with pytest.raises(ValueError):
        update(policy, malformed, learning_rate=0.5)


def test_complete_batch_cannot_drop_or_duplicate_episode(rollout):
    _, dataset = rollout
    data = dataset.model_dump(mode="json")
    data["trajectories"].pop()
    with pytest.raises(ValidationError, match="complete ordered"):
        TrajectoryDataset.model_validate(data)
    data = dataset.model_dump(mode="json")
    data["trajectories"][1]["decision"]["proposal_request_id"] = data["trajectories"][0][
        "decision"
    ]["proposal_request_id"]
    with pytest.raises(ValidationError, match="complete ordered"):
        TrajectoryDataset.model_validate(data)


def test_cost_direction_and_clipping_are_declared(rollout):
    _, dataset = rollout
    row = next(row for row in dataset.trajectories if row.decision.action_id == "a")
    spec = dataset.reward_spec.model_copy(update={"execution_ms_penalty": 0.01})
    assert reward(spec, row) == 0
    assert reward(spec.model_copy(update={"execution_ms_penalty": 1.0}), row) == -1
    assert (
        reward(dataset.reward_spec.model_copy(update={"direction": Direction.MAXIMIZE}), row) == -1
    )
    with pytest.raises(ValueError, match="actual execution cost"):
        reward(spec, row.model_copy(update={"consumed": {}}))


def test_trajectory_preserves_large_exact_consumption(rollout):
    _, dataset = rollout
    row = dataset.trajectories[0].model_dump(mode="json")
    row["consumed"]["execution_ms"] = "10000000000000000000.000000001"
    parsed = Trajectory.model_validate(row)
    assert (
        parsed.model_dump(mode="json")["consumed"]["execution_ms"]
        == row["consumed"]["execution_ms"]
    )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf"), True])
def test_policy_parameters_refuse_nonfinite_or_boolean_values(value, rollout):
    policy, _ = rollout
    with pytest.raises(ValidationError):
        CategoricalPolicy(actions=policy.actions, logits=(value, 0.0))


def test_admitted_spread_keeps_each_action_above_sampler_resolution(rollout):
    policy, _ = rollout
    distribution = probabilities(CategoricalPolicy(actions=policy.actions, logits=(8.0, -8.0)))
    assert all(math.isfinite(value) and 2**-53 < value < 1 - 2**-53 for value in distribution)
    assert math.fsum(distribution) == pytest.approx(1.0)
    with pytest.raises(ValidationError, match="logit spread"):
        CategoricalPolicy(actions=policy.actions, logits=(40.0, -40.0))


def test_adjacent_binary64_probabilities_remain_compatible(rollout):
    policy, dataset = rollout
    row = dataset.trajectories[0]
    changed = row.model_copy(
        update={
            "decision": row.decision.model_copy(
                update={
                    "probabilities": (math.nextafter(0.5, 1), math.nextafter(0.5, 0)),
                }
            )
        }
    )
    compatible = dataset.model_copy(update={"trajectories": (changed, *dataset.trajectories[1:])})
    trained = update(policy, compatible, learning_rate=0.5)
    assert trained.gradient == pytest.approx((0.5, -0.5), abs=1e-15)


@pytest.mark.parametrize("tampered", [False, True])
def test_native_training_and_independent_checker(rollout, tmp_path, tampered):
    policy, dataset = rollout
    source = tmp_path / "policy-project"
    shutil.copytree(REPO_ROOT / "examples/cpu_policy_learning", source)
    if tampered:
        path = source / "train.py"
        content = path.read_text()
        marker = "    elapsed_ms = int((time.perf_counter() - started) * 1000)"
        assert marker in content
        path.write_text(
            content.replace(
                marker,
                "    trained = trained.model_copy(update={'policy': trained.policy.model_copy("
                "update={'logits': (1.0, -1.0)})})\n" + marker,
            )
        )
    runtime = LocalRuntime.open(tmp_path / "state")
    client = ResearchClient(LocalTransport(runtime))
    enrollment = ensure_local_enrollment(
        runtime, machine_registry=tmp_path / "machine" / "resources.sqlite"
    )
    inputs = {}
    for name, model in (("behavior", policy), ("trajectories", dataset)):
        path = tmp_path / (name + ".json")
        path.write_bytes(canonical_bytes(model.model_dump(mode="json")))
        inputs[name] = client.upload_file(path, idempotency_key=name)
    prepared = prepare_run(
        client,
        source,
        key="training",
        options=RunOptions(
            primary_metric="update_error",
            input_artifacts=inputs,
            max_trials=1,
        ),
        profile=enrollment.worker.profile,
    )
    campaign_id = prepared.plan.campaign_id
    completed = []
    authorization = None
    for _ in range(2):
        jobs = client.list(RecordQuery(kind="job", campaign_id=campaign_id, state="ready"))["items"]
        assert len(jobs) == 1
        job = client.get("job", jobs[0]["id"])
        if job["data"]["stage"]["kind"] == "evaluate":
            authorization = client.command(
                "evaluation.authorize",
                AuthorizeEvaluation(job_id=job["id"], evaluator_id=enrollment.evaluator_id),
                idempotency_key="authorize-checker",
                expected_revision=job["revision"],
            )["result"]
            job = client.get("job", job["id"])
        attempt = enrollment.worker.claim(job)
        completed.append(enrollment.worker.execute(attempt["id"]))
    assert completed[0]["state"] == "succeeded"
    if tampered:
        assert completed[1]["state"] == "failed"
        with pytest.raises(ResearchError) as refused:
            enrollment.evaluator.command(
                "evaluation.complete",
                CompleteEvaluation(request_id=authorization["id"], attempt_id=completed[1]["id"]),
                idempotency_key="reject-altered-checkpoint",
            )
        assert refused.value.code == ErrorCode.EVALUATION_INCOMPLETE
        assert not client.list(RecordQuery(kind="evaluation_receipt", campaign_id=campaign_id))[
            "items"
        ]
        return
    assert completed[1]["state"] == "succeeded"
    receipt = enrollment.evaluator.command(
        "evaluation.complete",
        CompleteEvaluation(request_id=authorization["id"], attempt_id=completed[1]["id"]),
        idempotency_key="complete-checker",
    )["result"]
    assert receipt["state"] == "valid"
    training = client.get("job", completed[0]["data"]["dispatch"]["job_id"])
    reference = training["data"]["outputs"]["policy"]
    path = tmp_path / "learned-policy.json"
    client.download(reference["id"], path)
    learned = CategoricalPolicy.model_validate_json(path.read_bytes())
    assert learned == update(policy, dataset, learning_rate=0.5).policy
    assert probabilities(learned)[0] > probabilities(policy)[0]
    assert (
        next(
            item
            for item in client.budget(campaign_id)["accounts"]
            if item["dimension"] == "attempts"
        )["consumed"]
        == "2"
    )
