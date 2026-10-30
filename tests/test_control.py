from __future__ import annotations

import shutil
import zipfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from research_swarm.control.commands import (
    CampaignTarget,
    CreateCampaign,
    ProposeCandidate,
    RegisterProject,
    SubmitTrial,
    ValidateCandidate,
)
from research_swarm.control.models import StoppingRule, TrialData
from research_swarm.control.service import ResearchService
from research_swarm.domain.artifacts import ArtifactRecord
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.projects.source import bundle_directory
from research_swarm.state.artifacts import ArtifactService, BeginUpload
from research_swarm.state.database import Database, Transaction
from research_swarm.state.ledger import BudgetLedger
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


def test_sealed_trial_graph_survives_controller_restart(research: ResearchFixture) -> None:
    campaign = research.campaign()
    candidate, validation = research.candidate(campaign)
    request = SubmitTrial(
        campaign_id=campaign["id"],
        candidate_id=candidate["id"],
        validation_id=validation["id"],
        seed=7,
    )
    context = research.context("one-trial", revision=campaign["revision"])
    response = research.service.submit_trial(context, request)
    trial = TrialData.model_validate(response["result"]["data"])
    trial.spec.verify()
    assert trial.spec.seed == 7
    assert trial.spec.source.tree_digest == candidate["data"]["source"]["tree_digest"]
    assert trial.execution_outcome == "pending" and trial.evaluation_status == "pending"
    restarted = ResearchService(
        Database(research.service.database.path), research.service.artifacts
    )
    assert restarted.submit_trial(context, request) == response
    jobs = [
        restarted.database.read_record(context, "job", identifier) for identifier in trial.job_ids
    ]
    assert [job["state"] for job in jobs] == ["ready", "blocked"]
    assert jobs[1]["data"]["input_sources"] == {"model": jobs[0]["id"]}
    assert jobs[1]["data"]["predecessor_ids"] == [jobs[0]["id"]]
    with restarted.database.connect() as connection:
        account = BudgetLedger.account(Transaction(connection, context), campaign["id"], "attempts")
        assert account.reserved == account.consumed == 0


def test_duplicate_submission_commits_one_trial_and_one_stage_graph(
    research: ResearchFixture,
) -> None:
    campaign = research.campaign()
    candidate, validation = research.candidate(campaign)
    request = SubmitTrial(
        campaign_id=campaign["id"],
        candidate_id=candidate["id"],
        validation_id=validation["id"],
        seed=11,
    )
    context = research.context("parallel-delivery", revision=campaign["revision"])
    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(
            pool.map(lambda _: research.service.submit_trial(context, request), range(4))
        )
    assert all(response == responses[0] for response in responses)
    records = research.service.database.snapshot(context)["records"]
    assert sum(row["kind"] == "trial" for row in records) == 1
    assert sum(row["kind"] == "job" for row in records) == 2
    with pytest.raises(ResearchError) as failure:
        research.service.submit_trial(context, request.model_copy(update={"seed": 12}))
    assert failure.value.code == ErrorCode.IDEMPOTENCY_CONFLICT


def test_original_campaign_keeps_its_project_revision(research: ResearchFixture) -> None:
    campaign = research.campaign()
    original_revision = campaign["data"]["contract"]["project_revision_id"]
    evaluator = research.source / "evaluate.py"
    evaluator.write_text(
        evaluator.read_text(encoding="utf-8") + "\n# New evaluator revision\n", encoding="utf-8"
    )
    changed = research.capture()
    updated = research.service.register_project(
        research.context(revision=research.project["revision"]),
        RegisterProject(source_artifact=changed, project_id=research.project["id"]),
    )["result"]
    assert updated["project_revision"]["id"] != original_revision
    candidate, validation = research.candidate(campaign, changed)
    assert validation["data"]["valid"] is False
    assert validation["data"]["changed_paths"] == ["evaluate.py"]
    with pytest.raises(ResearchError) as failure:
        research.service.submit_trial(
            research.context(revision=campaign["revision"]),
            SubmitTrial(
                campaign_id=campaign["id"],
                candidate_id=candidate["id"],
                validation_id=validation["id"],
                seed=1,
            ),
        )
    assert failure.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
    later = research.campaign()
    assert later["data"]["contract"]["project_revision_id"] == updated["project_revision"]["id"]
    _, accepted = research.candidate(later, changed)
    assert accepted["data"]["valid"] is True


def test_mutable_training_edits_do_not_change_protocol(research: ResearchFixture) -> None:
    campaign = research.campaign()
    train = research.source / "train.py"
    train.write_text(train.read_text(encoding="utf-8").replace("0.35", "0.25"), encoding="utf-8")
    source = research.capture()
    candidate, validation = research.candidate(campaign, source)
    assert validation["data"]["valid"] is True
    assert validation["data"]["changed_paths"] == ["train.py"]
    trial = research.service.submit_trial(
        research.context(revision=campaign["revision"]),
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=candidate["id"],
            validation_id=validation["id"],
            seed=1,
        ),
    )["result"]
    assert (
        trial["data"]["spec"]["protocol_digest"] == campaign["data"]["contract"]["protocol_digest"]
    )
    with pytest.raises(ResearchError):
        research.service.artifacts.delete(
            research.context(), source.id, "Still required by candidate"
        )


def test_candidate_and_validation_cannot_cross_campaigns(research: ResearchFixture) -> None:
    first, second = research.campaign(), research.campaign()
    candidate, validation = research.candidate(first)
    other, _ = research.candidate(second)
    with pytest.raises(ResearchError) as failure:
        research.service.submit_trial(
            research.context(revision=second["revision"]),
            SubmitTrial(
                campaign_id=second["id"],
                candidate_id=other["id"],
                validation_id=validation["id"],
                seed=1,
            ),
        )
    assert failure.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
    with pytest.raises(ResearchError, match="another campaign"):
        research.service.propose_candidate(
            research.context(revision=second["revision"]),
            ProposeCandidate(
                campaign_id=second["id"],
                source_artifact=research.artifact,
                parent_ids=(candidate["id"],),
                hypothesis="Invalid ancestry",
            ),
        )


def test_trial_limit_is_atomic_across_competing_submissions(research: ResearchFixture) -> None:
    campaign = research.campaign(max_trials=1)
    candidate, validation = research.candidate(campaign)

    def submit(seed: int) -> bool:
        try:
            research.service.submit_trial(
                research.context(revision=campaign["revision"]),
                SubmitTrial(
                    campaign_id=campaign["id"],
                    candidate_id=candidate["id"],
                    validation_id=validation["id"],
                    seed=seed,
                ),
            )
            return True
        except ResearchError as error:
            assert error.code == ErrorCode.BUDGET_EXHAUSTED
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(submit, (1, 2))) == [False, True]


def test_failed_job_insert_rolls_back_trial_jobs_and_events(
    research: ResearchFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    campaign = research.campaign()
    candidate, validation = research.candidate(campaign)
    before = research.service.database.snapshot(research.context())
    original = Transaction.insert

    def insert(self: Transaction, kind: str, identifier: str, data: Any, **kwargs: Any) -> Any:
        if kind == "job" and data["stage"]["kind"] == "evaluate":
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Injected transaction failure")
        return original(self, kind, identifier, data, **kwargs)

    monkeypatch.setattr(Transaction, "insert", insert)
    with pytest.raises(ResearchError, match="Injected"):
        research.service.submit_trial(
            research.context(revision=campaign["revision"]),
            SubmitTrial(
                campaign_id=campaign["id"],
                candidate_id=candidate["id"],
                validation_id=validation["id"],
                seed=1,
            ),
        )
    assert research.service.database.snapshot(research.context()) == before


def test_registration_replay_precedes_source_preflight(
    research: ResearchFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = research.context("register-replay")
    request = RegisterProject(source_artifact=research.artifact)
    response = research.service.register_project(context, request)

    def unavailable(*args: Any, **kwargs: Any) -> None:
        raise OSError("Store temporarily offline")

    monkeypatch.setattr(research.service.artifacts.store, "fetch", unavailable)
    assert research.service.register_project(context, request) == response
    with pytest.raises(OSError, match="offline"):
        research.service.register_project(research.context(), request)


def test_registration_verifies_mutable_file_content_not_only_manifest(
    research: ResearchFixture,
) -> None:
    bundle = research.source.parent / "original.zip"
    corrupt = research.source.parent / "wrong-content.zip"
    bundle_directory(research.source, bundle, paths=("**",))
    with zipfile.ZipFile(bundle) as source, zipfile.ZipFile(corrupt, "w") as target:
        for info in source.infolist():
            content = source.read(info)
            if info.filename == "train.py":
                content = b"!" + content[1:]
            target.writestr(info, content)
    artifact = research.publish(corrupt.read_bytes())
    with pytest.raises(ResearchError) as failure:
        research.service.register_project(
            research.context(), RegisterProject(source_artifact=artifact)
        )
    assert failure.value.code == ErrorCode.ARTIFACT_UNVERIFIED


def test_sealing_refuses_ambiguous_stage_input(research: ResearchFixture) -> None:
    campaign = research.campaign(start=False, input_artifacts={"model": research.artifact})
    with pytest.raises(ResearchError, match="exactly one"):
        research.service.seal_campaign(
            research.context(revision=campaign["revision"]),
            CampaignTarget(campaign_id=campaign["id"]),
        )
    assert (
        research.service.database.read_record(research.context(), "campaign", campaign["id"])[
            "state"
        ]
        == "draft"
    )


def test_commands_require_current_revision_and_scope(research: ResearchFixture) -> None:
    campaign = research.campaign(start=False)
    with pytest.raises(ResearchError) as failure:
        research.service.seal_campaign(
            research.context(), CampaignTarget(campaign_id=campaign["id"])
        )
    assert failure.value.code == ErrorCode.REVISION_CONFLICT
    context = research.context(revision=campaign["revision"])
    denied = context.model_copy(
        update={"principal": context.principal.model_copy(update={"scopes": frozenset({"read"})})}
    )
    with pytest.raises(ResearchError) as failure:
        research.service.seal_campaign(denied, CampaignTarget(campaign_id=campaign["id"]))
    assert failure.value.code == ErrorCode.AUTHORIZATION_DENIED
