"""A small declarative contract for an ordinary research repository."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from research_swarm.domain.canonical import content_digest, record_digest
from research_swarm.domain.experiments import MetricDefinition, StageSpec, validate_stage_graph
from research_swarm.domain.types import Digest, Milliseconds, Name, StringSet, VersionedRecord
from research_swarm.projects.source import (
    MutationPolicy,
    SourceManifest,
    inspect_bundle,
    matches,
    open_bundle,
    path_pattern,
    portable_path,
)

MAX_PROJECT_MANIFEST_BYTES = 1024 * 1024


class ProjectIdentity(VersionedRecord):
    name: Name
    slug: str = Field(pattern=r"^[a-z][a-z0-9-]{0,63}$")
    description: str = Field(default="", max_length=4096)


class SourceConfiguration(VersionedRecord):
    mode: Literal["git", "directory"] = "git"
    paths: tuple[str, ...] = Field(min_length=1)
    base_revision: str = "HEAD"

    @field_validator("paths")
    @classmethod
    def selected_paths(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(path_pattern(value) for value in values)


class EnvironmentContract(VersionedRecord):
    profile: Name = "python-cpu"
    lock_path: str | None = None
    network_policy: Literal["inherit", "denied"] = "inherit"

    @field_validator("lock_path")
    @classmethod
    def lock(cls, value: str | None) -> str | None:
        return portable_path(value) if value is not None else None


class DatasetContract(VersionedRecord):
    manifest_path: str
    _manifest = field_validator("manifest_path")(portable_path)


class EvaluatorContract(VersionedRecord):
    protected_paths: tuple[str, ...] = Field(min_length=1)
    metrics: tuple[MetricDefinition, ...] = Field(min_length=1)
    comparison_class: Name
    measurement_trust: Literal["self_reported", "authenticated", "protected"] = "authenticated"
    expected_members: StringSet | None = Field(default=None, exclude_if=lambda value: value is None)

    @field_validator("protected_paths")
    @classmethod
    def paths(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(path_pattern(value) for value in values)

    @model_validator(mode="after")
    def unique_metrics(self) -> EvaluatorContract:
        if len({metric.name for metric in self.metrics}) != len(self.metrics):
            raise ValueError("Evaluator metric names must be unique")
        return self


class TimingContract(VersionedRecord):
    budget_ms: Milliseconds
    scope: Literal["active_training", "execution"] = "execution"
    timer_authority: Literal["project_reported", "trusted_harness"] = "project_reported"

    @model_validator(mode="after")
    def positive_budget(self) -> TimingContract:
        if self.budget_ms == 0:
            raise ValueError("Experiment timing budget must be positive")
        return self


class ProjectManifest(VersionedRecord):
    project: ProjectIdentity
    source: SourceConfiguration
    mutation: MutationPolicy
    environment: EnvironmentContract = Field(default_factory=EnvironmentContract)
    data: DatasetContract
    evaluation: EvaluatorContract
    timing: TimingContract
    stages: tuple[StageSpec, ...] = Field(min_length=1, max_length=256)

    @model_validator(mode="before")
    @classmethod
    def inherit_profiles(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        environment = value.get("environment", {})
        stages = value.get("stages")
        if isinstance(environment, EnvironmentContract):
            environment = environment.model_dump()
        if not isinstance(environment, dict) or not isinstance(stages, (list, tuple)):
            return value
        profile = environment.get("profile", "python-cpu")
        resolved = []
        for stage in stages:
            if isinstance(stage, StageSpec):
                stage = stage.model_dump(exclude_unset=True)
            if not isinstance(stage, dict) or not isinstance(stage.get("resources", {}), dict):
                resolved.append(stage)
                continue
            resolved.append(
                {
                    **stage,
                    "resources": {"engine_profile": profile, **stage.get("resources", {})},
                }
            )
        return {**value, "stages": resolved}

    @model_validator(mode="after")
    def valid_stages(self) -> ProjectManifest:
        validate_stage_graph(self.stages)
        if not any(stage.kind == "evaluate" for stage in self.stages):
            raise ValueError("A research project requires an evaluation stage")
        if self.timing.scope == "active_training" and not any(
            stage.kind in {"train", "policy_train"} for stage in self.stages
        ):
            raise ValueError("Active-training timing requires a training stage")
        return self


class ResolvedProject(VersionedRecord):
    manifest: ProjectManifest
    source: SourceManifest
    manifest_path: str
    manifest_digest: Digest
    data_manifest_digest: Digest
    evaluator_contract_digest: Digest
    environment_contract_digest: Digest
    protocol_digest: Digest
    protected_paths: tuple[str, ...]


def load_manifest(path: Path) -> ProjectManifest:
    with path.open("rb") as source:
        content = source.read(MAX_PROJECT_MANIFEST_BYTES + 1)
    return parse_manifest(content)


def parse_manifest(content: bytes) -> ProjectManifest:
    if len(content) > MAX_PROJECT_MANIFEST_BYTES:
        raise ValueError("Project manifest exceeds its size limit")
    return ProjectManifest.model_validate(tomllib.loads(content.decode("utf-8")))


def resolve_project(
    bundle: Path | bytes, *, manifest_path: str = "research.toml"
) -> ResolvedProject:
    """Resolve only the immutable bundle; later working-tree edits cannot affect it."""
    portable_path(manifest_path)
    source = inspect_bundle(bundle)
    entries = {entry.path: entry for entry in source.entries}

    def read(path: str, limit: int = MAX_PROJECT_MANIFEST_BYTES) -> bytes:
        entry = entries.get(path)
        if entry is None or entry.size_bytes > limit:
            raise ValueError("A required project contract file is absent or too large")
        with open_bundle(bundle) as archive:
            content = archive.read(path)
        if len(content) != entry.size_bytes or content_digest(content) != entry.digest:
            raise ValueError("Project contract content does not match its source identity")
        return content

    manifest = parse_manifest(read(manifest_path))
    protected = [manifest_path, manifest.data.manifest_path, *manifest.evaluation.protected_paths]
    if manifest.environment.lock_path is not None:
        protected.append(manifest.environment.lock_path)
    protected_paths = tuple(sorted(set(protected)))
    for pattern in protected_paths:
        if not any(matches(path, (pattern,)) for path in entries):
            raise ValueError(f"Protected source pattern has no entries: {pattern}")
    for path in entries:
        if matches(path, protected_paths):
            read(path, limit=64 * 1024 * 1024)
    data_digest = content_digest(read(manifest.data.manifest_path))
    evaluator_digest = record_digest(
        "research.evaluator-contract",
        {
            "contract": manifest.evaluation.model_dump(mode="json"),
            "stages": [
                stage.model_dump(mode="json", exclude={"resources"})
                for stage in sorted(manifest.stages, key=lambda stage: stage.id)
                if stage.kind == "evaluate"
            ],
            "protected_source": [
                entries[path].model_dump(mode="json")
                for path in sorted(entries)
                if matches(path, manifest.evaluation.protected_paths)
            ],
        },
    )
    environment_digest = record_digest(
        "research.environment-contract",
        {
            "contract": manifest.environment.model_dump(mode="json"),
            "lock_digest": (
                entries[manifest.environment.lock_path].digest
                if manifest.environment.lock_path is not None
                else None
            ),
        },
    )
    protocol_digest = record_digest(
        "research.protocol",
        {
            "data_manifest_digest": data_digest,
            "evaluator_contract_digest": evaluator_digest,
            "timing": manifest.timing.model_dump(mode="json"),
        },
    )
    return ResolvedProject(
        manifest=manifest,
        source=source,
        manifest_path=manifest_path,
        manifest_digest=record_digest(
            "research.project-manifest", manifest.model_dump(mode="json")
        ),
        data_manifest_digest=data_digest,
        evaluator_contract_digest=evaluator_digest,
        environment_contract_digest=environment_digest,
        protocol_digest=protocol_digest,
        protected_paths=protected_paths,
    )
