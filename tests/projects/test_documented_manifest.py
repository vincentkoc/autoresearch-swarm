from __future__ import annotations

from research_swarm.projects.manifest import parse_manifest
from tests.paths import REPO_ROOT


def test_documented_manifest_defines_a_complete_evaluation_panel() -> None:
    documentation = (REPO_ROOT / "docs/project-manifests.md").read_text(encoding="utf-8")
    template = documentation.split("~~~toml\n", 1)[1].split("~~~", 1)[0]
    manifest = parse_manifest(template.encode("utf-8"))
    assert manifest.evaluation.expected_members == frozenset({"validation-a", "validation-b"})
    assert manifest.evaluation.comparison_class == "example-v1"
    train, evaluate = manifest.stages
    assert train.id == "train" and "model" in train.output_names
    assert evaluate.id == "evaluate" and evaluate.dependencies == (train.id,)
    assert evaluate.required_artifacts == ("model",)
