import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from research_swarm.domain.canonical import content_digest
from research_swarm.domain.experiments import ResourceRequirements, StageSpec, validate_stage_graph
from research_swarm.domain.types import new_id
from research_swarm.projects.workload import WorkloadContext


@pytest.fixture
def context(tmp_path: Path) -> WorkloadContext:
    return WorkloadContext(
        trial_id=new_id(),
        attempt_id=new_id(),
        stage_id="train",
        spec_digest=content_digest(b"synthetic"),
        seed=42,
        time_budget_ms=1000,
        report_path=str(tmp_path / "report.json"),
        output_root=str(tmp_path),
    )


def test_seal_is_stable_and_referenced_digests_are_covered(context: WorkloadContext) -> None:
    spec = context.sealed()
    assert spec.sealed() == spec
    spec.verify()
    with pytest.raises(ValueError, match="digest mismatch"):
        spec.model_copy(update={"spec_digest": content_digest(b"other")}).verify()


def test_unsealed_context_is_not_executable(context: WorkloadContext) -> None:
    with pytest.raises(ValueError, match="not sealed"):
        context.verify()


def test_sealing_revalidates_copied_models(context: WorkloadContext) -> None:
    with pytest.raises(ValueError):
        context.model_copy(update={"seed": -1}).sealed()


def test_unordered_fields_have_stable_cross_process_hashes() -> None:
    resources = ResourceRequirements(
        required_features=frozenset({"alpha", "beta", "gamma", "delta", "epsilon"})
    )
    payload = resources.model_dump(mode="json")
    code = (
        "import json,sys; "
        "from research_swarm.domain.experiments import ResourceRequirements; "
        "from research_swarm.domain.canonical import record_digest; "
        "resource=ResourceRequirements.model_validate(json.load(sys.stdin)); "
        "print(record_digest('resource', resource.model_dump(mode='json')))"
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


def test_mutable_nested_values_cannot_evade_seal(context: WorkloadContext) -> None:
    spec = context.model_copy(update={"parameters": {"example.setting": 1}}).sealed()
    spec.parameters["example.setting"] = 2
    with pytest.raises(ValueError, match="digest mismatch"):
        spec.verify()


def test_graph_rejects_cycle_or_missing_dependency() -> None:
    first = StageSpec(id="a", kind="train", argv=("python",), dependencies=("b",))
    second = StageSpec(id="b", kind="train", argv=("python",), dependencies=("a",))
    with pytest.raises(ValueError, match="cycle"):
        validate_stage_graph((first, second))
    with pytest.raises(ValueError, match="Unknown"):
        validate_stage_graph((first,))
