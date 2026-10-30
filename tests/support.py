from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from research_swarm.control.commands import (
    CampaignTarget,
    CreateCampaign,
    ProposeCandidate,
    RegisterProject,
    ValidateCandidate,
)
from research_swarm.control.models import StoppingRule
from research_swarm.control.service import ResearchService
from research_swarm.domain.artifacts import ArtifactRecord
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.projects.source import bundle_directory
from research_swarm.state.artifacts import ArtifactService, BeginUpload
from research_swarm.state.database import Database
from research_swarm.storage.filesystem import FilesystemBlobStore

LIMITS = (
    BudgetLimit(dimension="attempts", limit=20, unit="count"),
    BudgetLimit(dimension="execution_ms", limit=300_000, unit="ms"),
)


@dataclass
class ResearchFixture:
    service: ResearchService
    source: Path
    context: Callable[..., CommandContext]
    project: dict[str, Any]
    artifact: ArtifactRef

    def publish(self, content: bytes) -> ArtifactRef:
        artifacts = self.service.artifacts
        upload = artifacts.begin_upload(
            self.context(), BeginUpload(digest=content_digest(content), size_bytes=len(content))
        )["result"]
        artifacts.upload_part(self.context(), upload["id"], 0, content)
        record = artifacts.finalize_upload(self.context(), upload["id"])["result"]
        return ArtifactRecord.model_validate(record["data"]).reference

    def capture(self) -> ArtifactRef:
        bundle = self.source.parent / (new_id() + ".zip")
        bundle_directory(self.source, bundle, paths=("**",))
        return self.publish(bundle.read_bytes())

    def campaign(
        self, *, max_trials: int = 10, start: bool = True, **kwargs: Any
    ) -> dict[str, Any]:
        campaign = self.service.create_campaign(
            self.context(),
            CreateCampaign(
                project_id=self.project["id"],
                name="Research test",
                primary_metric="val_bpb",
                limits=LIMITS,
                stopping=StoppingRule(max_trials=max_trials),
                **kwargs,
            ),
        )["result"]
        if start:
            for command in (self.service.seal_campaign, self.service.start_campaign):
                campaign = command(
                    self.context(revision=campaign["revision"]),
                    CampaignTarget(campaign_id=campaign["id"]),
                )["result"]
        return campaign

    def candidate(
        self, campaign: dict[str, Any], artifact: ArtifactRef | None = None
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        candidate = self.service.propose_candidate(
            self.context(revision=campaign["revision"]),
            ProposeCandidate(
                campaign_id=campaign["id"],
                source_artifact=artifact or self.artifact,
                hypothesis="Measure the fixed baseline",
            ),
        )["result"]
        validation = self.service.validate_candidate(
            self.context(revision=candidate["revision"]),
            ValidateCandidate(candidate_id=candidate["id"]),
        )["result"]
        return candidate, validation


@pytest.fixture
def research(
    tmp_path: Path, database: Database, context: Callable[..., CommandContext]
) -> ResearchFixture:
    source = tmp_path / "source"
    shutil.copytree(Path(__file__).resolve().parents[1] / "examples" / "cpu_bigram", source)
    artifacts = ArtifactService(
        database, FilesystemBlobStore(tmp_path / "artifacts"), storage_limit_bytes=10_000_000
    )
    service = ResearchService(database, artifacts)
    fixture = ResearchFixture(
        service,
        source,
        context,
        {},
        ArtifactRef(id=new_id(), digest="sha256:" + "0" * 64, size_bytes=0),
    )
    artifact = fixture.capture()
    registered = service.register_project(context(), RegisterProject(source_artifact=artifact))[
        "result"
    ]
    fixture.artifact, fixture.project = artifact, registered["project"]
    return fixture
