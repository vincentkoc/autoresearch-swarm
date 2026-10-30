"""Freeze a local run's source and replay bootstrap without recapturing code."""

from __future__ import annotations

import hashlib
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from filelock import FileLock
from pydantic import Field, field_validator

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.commands import (
    CampaignTarget,
    CreateCampaign,
    ProposeCandidate,
    RegisterProject,
    SubmitTrial,
    ValidateCandidate,
)
from research_swarm.control.models import (
    ProjectRevisionData,
    SearchConfiguration,
    Seed,
    StoppingRule,
)
from research_swarm.control.workers import NATIVE_CLEANUP_MS, EngineCapabilities
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.canonical import canonical_bytes, content_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import ID, ByteCount, Digest, Name, VersionedRecord, new_id
from research_swarm.orchestration.intents import CommandIntents
from research_swarm.projects.capture import capture_project
from research_swarm.projects.manifest import resolve_project
from research_swarm.projects.native_files import SourceReader
from research_swarm.projects.source import portable_path
from research_swarm.runtime import private_state_directory, read_private, write_private


class RunOptions(VersionedRecord):
    manifest_path: str = "research.toml"
    source_revision: str | None = None
    primary_metric: Name | None = None
    seed: Seed = 0
    max_trials: int = Field(default=10, strict=True, ge=1, le=100_000)
    parallelism: int = Field(default=1, strict=True, ge=1, le=1024)
    input_artifacts: dict[Name, ArtifactRef] = Field(default_factory=dict)
    search_policy: Literal["external", "greedy"] = Field(
        default="external", exclude_if=lambda value: value == "external"
    )

    _manifest = field_validator("manifest_path")(portable_path)


class RunPlan(VersionedRecord):
    id: ID = Field(default_factory=new_id)
    workspace_id: ID
    controller_id: ID
    actor_id: ID
    source_root: str
    options: RunOptions
    capture_started: bool = False
    source_digest: Digest | None = None
    source_size_bytes: ByteCount | None = None
    source_tree_digest: Digest | None = None
    resolved_revision: str | None = None
    source_artifact: ArtifactRef | None = None
    campaign_id: ID | None = None
    candidate_id: ID | None = None
    trial_id: ID | None = None


@dataclass(frozen=True)
class PreparedRun:
    plan: RunPlan
    root: Path


def _save(root: Path, plan: RunPlan) -> None:
    write_private(root / "plan.json", canonical_bytes(plan.model_dump(mode="json")))


def _source(
    root: Path, plan: RunPlan, client: ResearchClient, profile: EngineCapabilities | None
) -> RunPlan:
    bundle = root / "source.zip"
    if not bundle.exists():
        if plan.source_artifact is not None:
            client.download(plan.source_artifact.id, bundle)
        elif plan.capture_started:
            raise ResearchError(
                ErrorCode.ARTIFACT_UNVERIFIED,
                "Interrupted capture has no source bundle; use a new run key or explicit recovery",
            )
        else:
            plan = plan.model_copy(update={"capture_started": True})
            _save(root, plan)
            capture_project(
                Path(plan.source_root),
                bundle,
                manifest_path=plan.options.manifest_path,
                revision=plan.options.source_revision,
            )
    try:
        resolved = resolve_project(bundle, manifest_path=plan.options.manifest_path)
    except (OSError, ValueError, KeyError, zipfile.BadZipFile) as error:
        raise ResearchError(
            ErrorCode.ARTIFACT_UNVERIFIED, "Frozen source contract is invalid"
        ) from error
    if profile is not None:
        for stage in resolved.manifest.stages:
            profile.require(stage.resources, resolved.manifest.environment.network_policy)
    digest, size = hashlib.sha256(), 0
    with SourceReader(root) as reader, reader.open(bundle.name) as stream:
        while block := stream.read(1024 * 1024):
            size += len(block)
            digest.update(block)
    observed = "sha256:" + digest.hexdigest()
    if plan.source_digest is not None and (
        plan.source_digest != observed
        or plan.source_size_bytes != size
        or plan.source_tree_digest != resolved.source.tree_digest
        or plan.resolved_revision != resolved.source.base_revision
    ):
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Frozen run source changed")
    if plan.source_digest is None:
        plan = plan.model_copy(
            update={
                "source_digest": observed,
                "source_size_bytes": size,
                "source_tree_digest": resolved.source.tree_digest,
                "resolved_revision": resolved.source.base_revision,
            }
        )
        _save(root, plan)
    if plan.source_artifact is None:
        reference = client.upload_file(
            bundle, idempotency_key="run-source:" + plan.id, media_type="application/zip"
        )
        plan = plan.model_copy(update={"source_artifact": reference})
        _save(root, plan)
    return plan


def _submit(plan: RunPlan, commands: CommandIntents) -> RunPlan:
    assert plan.source_artifact is not None
    registered = commands.command(
        "register",
        "controller",
        "project.register",
        RegisterProject(
            source_artifact=plan.source_artifact, manifest_path=plan.options.manifest_path
        ),
    )["result"]
    revision = ProjectRevisionData.model_validate(registered["project_revision"]["data"])
    manifest = revision.resolved.manifest
    metric = plan.options.primary_metric
    if metric is None:
        if len(manifest.evaluation.metrics) != 1:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Choose a primary metric for this project"
            )
        metric = manifest.evaluation.metrics[0].name
    campaign = commands.command(
        "create",
        "controller",
        "campaign.create",
        CreateCampaign(
            project_id=registered["project"]["id"],
            project_revision_id=registered["project_revision"]["id"],
            name=manifest.project.name,
            primary_metric=metric,
            search=SearchConfiguration(
                policy=plan.options.search_policy,
                version=plan.options.search_policy + "-v1",
                max_parallel_trials=1
                if plan.options.search_policy == "greedy"
                else plan.options.parallelism,
            ),
            stopping=StoppingRule(max_trials=plan.options.max_trials),
            limits=(
                BudgetLimit(
                    dimension="attempts",
                    unit="count",
                    limit=len(manifest.stages) * plan.options.max_trials,
                ),
                BudgetLimit(
                    dimension="execution_ms",
                    unit="ms",
                    limit=sum(stage.timeout_ms + NATIVE_CLEANUP_MS for stage in manifest.stages)
                    * plan.options.max_trials,
                ),
            )
            + (
                (
                    BudgetLimit(
                        dimension="proposal_requests",
                        unit="count",
                        limit=plan.options.max_trials - 1,
                    ),
                )
                if plan.options.search_policy == "greedy"
                else ()
            ),
            input_artifacts=plan.options.input_artifacts,
        ),
    )["result"]
    for operation in ("seal", "start"):
        campaign = commands.command(
            operation,
            "controller",
            "campaign." + operation,
            CampaignTarget(campaign_id=campaign["id"]),
            revision=campaign["revision"],
        )["result"]
    candidate = commands.command(
        "candidate",
        "controller",
        "candidate.propose",
        ProposeCandidate(
            campaign_id=campaign["id"],
            source_artifact=plan.source_artifact,
            hypothesis="Measure the frozen project baseline under its declared contract",
        ),
        revision=campaign["revision"],
    )["result"]
    validation = commands.command(
        "validate",
        "controller",
        "candidate.validate",
        ValidateCandidate(candidate_id=candidate["id"]),
        revision=candidate["revision"],
    )["result"]
    trial = commands.command(
        "submit",
        "controller",
        "trial.submit",
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=candidate["id"],
            validation_id=validation["id"],
            seed=plan.options.seed,
        ),
        revision=campaign["revision"],
    )["result"]
    return plan.model_copy(
        update={
            "campaign_id": campaign["id"],
            "candidate_id": candidate["id"],
            "trial_id": trial["id"],
        }
    )


def prepare_run(
    client: ResearchClient,
    source: Path,
    *,
    key: str | None = None,
    options: RunOptions | None = None,
    profile: EngineCapabilities | None = None,
) -> PreparedRun:
    if not isinstance(client.transport, LocalTransport):
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Local bootstrap requires local state"
        )
    actor = client.transport.resolve_principal()
    for scope in (
        "read",
        "source.capture",
        "artifact.upload",
        "project.manage",
        "campaign.manage",
        "candidate.propose",
        "candidate.validate",
        "trial.submit",
    ):
        actor.require(scope)
    options = RunOptions.model_validate(options or RunOptions())
    identity = key if key is not None else str(source.expanduser().resolve())
    if not identity or len(identity) > 4096:
        raise ValueError("Run key requires between 1 and 4096 characters")
    runtime = client.transport.runtime
    root = runtime.root / "runs" / content_digest(identity.encode("utf-8")).split(":")[1]
    with private_state_directory(root), FileLock(root / "bootstrap.lock"):
        path = root / "plan.json"
        if path.exists():
            plan = RunPlan.model_validate(
                strict_json_loads(read_private(root, path.name, limit=2 * 1024 * 1024))
            )
            if (plan.workspace_id, plan.controller_id, plan.actor_id) != (
                runtime.identity.workspace_id,
                runtime.identity.controller_id,
                actor.id,
            ):
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Run delivery authority differs"
                )
            if plan.options != options:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Run key already has different options"
                )
        else:
            if (root / "source.zip").exists() or (root / "commands").exists():
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED,
                    "Run plan is missing; explicit recovery is required",
                )
            plan = RunPlan(
                source_root=str(source.expanduser().resolve()),
                options=options,
                workspace_id=runtime.identity.workspace_id,
                controller_id=runtime.identity.controller_id,
                actor_id=actor.id,
            )
            _save(root, plan)
        commands = CommandIntents(root / "commands", {"controller": client})
        plan = _source(root, plan, client, profile)
        if plan.campaign_id is not None:
            client.get("campaign", plan.campaign_id)
            return PreparedRun(plan, root)
        commands.recover()
        plan = _submit(plan, commands)
        _save(root, plan)
        return PreparedRun(plan, root)
