"""Protected independent calculation of the declared reward-driven parameter update."""

import math

from common import context, read_input, report, settings

from research_swarm.domain.learning import CategoricalPolicy, PolicyUpdate, TrajectoryDataset
from research_swarm.learning.reinforce import validate_batch


def main():
    request = context()
    behavior = CategoricalPolicy.model_validate_json(read_input(request, "behavior"))
    dataset = TrajectoryDataset.model_validate_json(read_input(request, "trajectories"))
    policy = CategoricalPolicy.model_validate_json(read_input(request, "policy"))
    trained = PolicyUpdate.model_validate_json(read_input(request, "update"))
    config = settings()
    validate_batch(behavior, dataset)
    if (
        trained.policy != policy
        or trained.dataset_digest != dataset.digest
        or trained.behavior_policy_digest != behavior.digest
        or trained.reward_spec_digest != dataset.reward_spec.digest
        or trained.learning_rate != config["learning_rate"]
        or trained.algorithm != config["algorithm"]
        or trained.epochs != config["epochs"]
        or trained.sample_count != len(dataset.trajectories)
        or policy.actions != behavior.actions
    ):
        raise ValueError("Training output provenance differs")
    # Deliberately independent of update(): calculate each sample's log-policy gradient.
    sums = [0.0] * len(behavior.actions)
    rewards = []
    for row in dataset.trajectories:
        spec = dataset.reward_spec
        sign = 1 if spec.direction.value == "minimize" else -1
        value = sign * (row.comparator_value - row.candidate_value) / spec.scale
        value -= spec.execution_ms_penalty * float(row.consumed["execution_ms"])
        value = min(spec.clip_magnitude, max(-spec.clip_magnitude, value))
        rewards.append(value)
        for index, action in enumerate(behavior.actions):
            sums[index] += value * (
                int(action.id == row.decision.action_id) - row.decision.probabilities[index]
            )
    gradients = [value / len(rewards) for value in sums]
    expected = [
        weight + config["learning_rate"] * gradient
        for weight, gradient in zip(behavior.logits, gradients, strict=True)
    ]
    residuals = [abs(left - right) for left, right in zip(expected, policy.logits, strict=True)]
    residuals.extend(
        abs(left - right) for left, right in zip(gradients, trained.gradient, strict=True)
    )
    residuals.append(abs(math.fsum(rewards) / len(rewards) - trained.mean_reward))
    error = max(residuals)
    if not math.isfinite(error) or error > config["maximum_update_error"]:
        raise ValueError("Policy checkpoint does not implement the declared update")
    report(
        request,
        measurements=[
            {
                "name": "update_error",
                "value": error,
                "unit": "absolute_parameter_error",
                "direction": "minimize",
            }
        ],
        observed_members=[config["panel"]],
        diagnostics={
            "algorithm": trained.algorithm,
            "trained_policy_digest": policy.digest,
            "qualification": "numerical_update_only",
        },
    )


if __name__ == "__main__":
    main()
