import json
import os
import subprocess
import sys

import pytest

from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.experiments import (
    Direction,
    ExperimentSpec,
    MetricDefinition,
    ResourceRequirements,
    SourceRef,
    StageSpec,
)
from research_swarm.domain.lifecycle import (
    AttemptState,
    CampaignState,
    attempt_transition,
    campaign_transition,
)
from research_swarm.domain.types import new_id


def example_spec() -> ExperimentSpec:
    digest = content_digest(b"synthetic")
    return ExperimentSpec(
        workspace_id=new_id(),
        project_id=new_id(),
        campaign_id=new_id(),
        candidate_id=new_id(),
        source=SourceRef(artifact_id=new_id(), tree_digest=digest),
        protocol_digest=digest,
        comparison_class="cpu-reference-v1",
        environment_digest=digest,
        evaluator_contract_digest=digest,
        data_manifest_digest=digest,
        metrics=(MetricDefinition(name="loss", unit="mse", direction=Direction.MINIMIZE),),
        stages=(StageSpec(id="train", kind="train", argv=("python", "train.py")),),
        seed=42,
        policy_version="greedy-v1",
    )


def test_seal_is_stable_and_referenced_digests_are_covered() -> None:
    spec = example_spec().sealed()
    assert spec.sealed() == spec
    spec.verify()
    changed = spec.model_copy(update={"data_manifest_digest": content_digest(b"other")})
    with pytest.raises(ValueError, match="digest mismatch"):
        changed.verify()


def test_unsealed_spec_is_not_executable() -> None:
    with pytest.raises(ValueError, match="not sealed"):
        example_spec().verify()


def test_sealing_revalidates_copied_models() -> None:
    invalid = example_spec().model_copy(update={"seed": -1})
    with pytest.raises(ValueError):
        invalid.sealed()


def test_unordered_fields_have_stable_cross_process_hashes() -> None:
    stage = (
        example_spec()
        .stages[0]
        .model_copy(
            update={
                "resources": ResourceRequirements(
                    required_features=frozenset({"alpha", "beta", "gamma", "delta", "epsilon"})
                )
            }
        )
    )
    payload = example_spec().model_copy(update={"stages": (stage,)}).model_dump(mode="json")
    code = (
        "import json,sys; "
        "from research_swarm.domain.experiments import ExperimentSpec; "
        "print(ExperimentSpec.model_validate(json.load(sys.stdin)).sealed().spec_digest)"
    )
    outputs = [
        subprocess.run(
            [sys.executable, "-c", code],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=True,
            env={**os.environ, "PYTHONHASHSEED": seed},
        ).stdout.strip()
        for seed in ("1", "2", "3")
    ]
    assert len(set(outputs)) == 1


def test_serialization_schema_preserves_typed_properties() -> None:
    schema = ResourceRequirements.model_json_schema(mode="serialization")
    assert schema["properties"]["cpu_slots"]["type"] == "integer"
    assert schema["properties"]["required_features"]["items"]["type"] == "string"
    assert schema["properties"]["required_features"]["x-ordering"] == "canonical-json-bytes"


def test_mutable_nested_values_cannot_evade_seal() -> None:
    spec = example_spec().model_copy(update={"extensions": {"example.setting": 1}}).sealed()
    spec.extensions["example.setting"] = 2
    with pytest.raises(ValueError, match="digest mismatch"):
        spec.verify()


def test_graph_rejects_cycle_or_missing_dependency() -> None:
    first = StageSpec(id="a", kind="train", argv=("python",), dependencies=("b",))
    second = StageSpec(id="b", kind="train", argv=("python",), dependencies=("a",))
    with pytest.raises(ValueError, match="cycle"):
        example_spec().model_copy(update={"stages": (first, second)}).sealed()
    with pytest.raises(ValueError, match="Unknown"):
        example_spec().model_copy(update={"stages": (first,)}).sealed()


def test_terminal_campaign_does_not_restart_itself() -> None:
    with pytest.raises(ResearchError):
        campaign_transition(CampaignState.COMPLETED, CampaignState.RUNNING)


def test_unknown_execution_requires_reconciliation_not_retry_transition() -> None:
    with pytest.raises(ResearchError):
        attempt_transition(AttemptState.EXECUTION_UNKNOWN, AttemptState.SUCCEEDED)
    attempt_transition(AttemptState.EXECUTION_UNKNOWN, AttemptState.SUCCEEDED, reconciliation=True)
    with pytest.raises(ResearchError):
        attempt_transition(
            AttemptState.EXECUTION_UNKNOWN, AttemptState.RUNNING, reconciliation=True
        )
