"""One complete on-policy categorical batch, one reward-driven ascent update."""

from __future__ import annotations

import math

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.experiments import Direction
from research_swarm.domain.learning import (
    CategoricalPolicy,
    DecisionTrace,
    PolicyUpdate,
    RewardSpec,
    Trajectory,
    TrajectoryDataset,
)

PROBABILITY_REL_TOLERANCE = 1e-12
PROBABILITY_ABS_TOLERANCE = 1e-16


def _select(distribution: tuple[float, ...], draw: float) -> int:
    for index in range(len(distribution) - 1):
        if draw < math.fsum(distribution[: index + 1]):
            return index
    return len(distribution) - 1


def probabilities(policy: CategoricalPolicy) -> tuple[float, ...]:
    policy = CategoricalPolicy.model_validate(policy)
    maximum = max(policy.logits)
    weights = tuple(math.exp(value - maximum) for value in policy.logits)
    denominator = math.fsum(weights)
    return tuple(value / denominator for value in weights)


def decision(
    policy: CategoricalPolicy,
    *,
    episode_id: str,
    campaign_id: str,
    proposal_request_id: str,
    context_digest: str,
    seed: int,
) -> DecisionTrace:
    distribution = probabilities(policy)
    sample = record_digest(
        "research.policy-sample.v1",
        {
            "episode_id": episode_id,
            "campaign_id": campaign_id,
            "proposal_request_id": proposal_request_id,
            "context_digest": context_digest,
            "policy_digest": policy.digest,
            "seed": seed,
        },
    )
    # First 53 digest bits define a portable [0,1) draw, with no mutable RNG state.
    draw = (int(sample.removeprefix("sha256:"), 16) >> 203) / 2**53
    selected = _select(distribution, draw)
    action = policy.actions[selected]
    return DecisionTrace(
        episode_id=episode_id,
        campaign_id=campaign_id,
        proposal_request_id=proposal_request_id,
        context_digest=context_digest,
        behavior_policy_digest=policy.digest,
        seed=seed,
        draw=draw,
        probabilities=distribution,
        action_id=action.id,
        source_digest=action.source_digest,
    )


def reward(spec: RewardSpec, trajectory: Trajectory) -> float:
    spec = RewardSpec.model_validate(spec)
    trajectory = Trajectory.model_validate(trajectory)
    if trajectory.outcome != "measurement" or trajectory.metric_name != spec.metric_name:
        raise ValueError("Missing or invalid evidence blocks the complete training batch")
    if "execution_ms" not in trajectory.consumed:
        raise ValueError("Trajectory lacks actual execution cost")
    assert trajectory.comparator_value is not None and trajectory.candidate_value is not None
    delta = trajectory.comparator_value - trajectory.candidate_value
    if spec.direction == Direction.MAXIMIZE:
        delta = -delta
    value = delta / spec.scale - spec.execution_ms_penalty * float(
        trajectory.consumed["execution_ms"]
    )
    if not math.isfinite(value):
        raise ValueError("Derived reward is not finite")
    return max(-spec.clip_magnitude, min(spec.clip_magnitude, value))


def validate_batch(policy: CategoricalPolicy, dataset: TrajectoryDataset) -> None:
    policy = CategoricalPolicy.model_validate(policy)
    dataset = TrajectoryDataset.model_validate(dataset)
    if dataset.behavior_policy_digest != policy.digest:
        raise ValueError("Training requires the frozen behavior policy")
    for trajectory in dataset.trajectories:
        trace = trajectory.decision
        if trajectory.split != "development":
            raise ValueError("Held-out trajectories cannot train a policy")
        expected = decision(
            policy,
            episode_id=trace.episode_id,
            campaign_id=trace.campaign_id,
            proposal_request_id=trace.proposal_request_id,
            context_digest=trace.context_digest,
            seed=trace.seed,
        )
        distribution = trace.probabilities
        excluded = {"probabilities", "action_id", "source_digest"}
        if (
            trace.model_dump(exclude=excluded) != expected.model_dump(exclude=excluded)
            or len(distribution) != len(policy.actions)
            or not math.isclose(math.fsum(distribution), 1.0, rel_tol=0, abs_tol=1e-12)
            or any(
                not math.isclose(
                    left,
                    right,
                    rel_tol=PROBABILITY_REL_TOLERANCE,
                    abs_tol=PROBABILITY_ABS_TOLERANCE,
                )
                for left, right in zip(distribution, expected.probabilities, strict=True)
            )
        ):
            raise ValueError("Decision differs from its frozen policy, sampling or source contract")
        action = policy.actions[_select(distribution, trace.draw)]
        if (trace.action_id, trace.source_digest) != (action.id, action.source_digest):
            raise ValueError("Decision action differs from its recorded sampling distribution")
        expected_reward = reward(dataset.reward_spec, trajectory)
        if trajectory.reward is None or not math.isclose(
            trajectory.reward, expected_reward, rel_tol=1e-12, abs_tol=1e-12
        ):
            raise ValueError("Recorded reward differs from the declared raw outcome and cost")


def update(
    policy: CategoricalPolicy, dataset: TrajectoryDataset, *, learning_rate: float
) -> PolicyUpdate:
    """Compute parameters, without granting dataset qualification or policy adoption."""
    validate_batch(policy, dataset)
    gradient = tuple(
        math.fsum(
            reward(dataset.reward_spec, row)
            * ((row.decision.action_id == action.id) - row.decision.probabilities[index])
            for row in dataset.trajectories
        )
        / len(dataset.trajectories)
        for index, action in enumerate(policy.actions)
    )
    updated = CategoricalPolicy(
        actions=policy.actions,
        logits=tuple(
            weight + learning_rate * value
            for weight, value in zip(policy.logits, gradient, strict=True)
        ),
        parent_digest=policy.digest,
    )
    return PolicyUpdate(
        behavior_policy_digest=policy.digest,
        dataset_digest=dataset.digest,
        reward_spec_digest=dataset.reward_spec.digest,
        learning_rate=learning_rate,
        sample_count=len(dataset.trajectories),
        gradient=gradient,
        mean_reward=math.fsum(reward(dataset.reward_spec, row) for row in dataset.trajectories)
        / len(dataset.trajectories),
        policy=updated,
    )
