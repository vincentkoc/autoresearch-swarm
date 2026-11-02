from __future__ import annotations

import pytest

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import EvaluationRequest
from research_swarm.evaluation.eligibility import validate_receipt
from research_swarm.projects.manifest import parse_manifest
from tests.control.test_evaluation import fixtures
from tests.paths import REPO_ROOT


def test_documented_manifest_defines_a_complete_evaluation_panel() -> None:
    documentation = (REPO_ROOT / "docs/project-manifests.md").read_text(encoding="utf-8")
    template = documentation.split("~~~toml\n", 1)[1].split("~~~", 1)[0]
    manifest = parse_manifest(template.encode("utf-8"))
    actor, request, receipt = fixtures()
    request = EvaluationRequest.model_validate(
        {
            **request.model_dump(),
            "comparison_class": manifest.evaluation.comparison_class,
            "expected_members": manifest.evaluation.expected_members,
        }
    )
    receipt = receipt.model_copy(
        update={"observed_members": frozenset({"validation-a", "validation-b"})}
    )
    validate_receipt(actor, request, receipt)
    with pytest.raises(ResearchError) as incomplete:
        validate_receipt(
            actor,
            request,
            receipt.model_copy(update={"observed_members": frozenset({"validation-a"})}),
        )
    assert incomplete.value.code == ErrorCode.EVALUATION_INCOMPLETE
