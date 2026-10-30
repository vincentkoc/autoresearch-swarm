"""Train the policy parameters; this does not train the underlying bigram model."""

import time
from pathlib import Path

from common import context, read_input, report, settings

from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.learning import CategoricalPolicy, TrajectoryDataset
from research_swarm.learning.reinforce import update


def main():
    request = context()
    behavior = CategoricalPolicy.model_validate_json(read_input(request, "behavior"))
    dataset = TrajectoryDataset.model_validate_json(read_input(request, "trajectories"))
    configuration = settings()
    started = time.perf_counter()
    trained = update(behavior, dataset, learning_rate=configuration["learning_rate"])
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    output = Path(request["output_root"])
    (output / "policy.json").write_bytes(canonical_bytes(trained.policy.model_dump(mode="json")))
    (output / "update.json").write_bytes(canonical_bytes(trained.model_dump(mode="json")))
    report(
        request,
        outputs=[
            {"name": "policy", "path": "policy.json", "media_type": "application/json"},
            {"name": "update", "path": "update.json", "media_type": "application/json"},
        ],
        phase_timings_ms={"active_training": elapsed_ms},
        diagnostics={"algorithm": trained.algorithm, "samples": trained.sample_count, "epochs": 1},
    )


if __name__ == "__main__":
    main()
