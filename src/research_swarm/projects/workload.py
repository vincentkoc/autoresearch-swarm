"""A language-neutral file protocol for ordinary project commands."""

from __future__ import annotations

import hashlib
from pathlib import Path, PurePosixPath, PureWindowsPath

from pydantic import Field, JsonValue, field_validator, model_validator

from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.experiments import Metric, StageSpec
from research_swarm.domain.types import ID, ByteCount, Digest, Milliseconds, Name, VersionedRecord
from research_swarm.projects.native_files import SourceReader
from research_swarm.projects.source import portable_path


def absolute_path(value: str) -> str:
    if "\0" in value or not (
        PurePosixPath(value).is_absolute() or PureWindowsPath(value).is_absolute()
    ):
        raise ValueError("Execution context paths must be absolute and NUL-free")
    return value


class WorkloadInput(VersionedRecord):
    path: str
    digest: Digest
    size_bytes: ByteCount
    _path = field_validator("path")(absolute_path)


class WorkloadContext(VersionedRecord):
    trial_id: ID
    attempt_id: ID
    stage_id: Name
    spec_digest: Digest
    seed: int = Field(strict=True, ge=0, le=9_007_199_254_740_991)
    time_budget_ms: Milliseconds
    inputs: dict[Name, WorkloadInput] = Field(default_factory=dict)
    output_root: str
    report_path: str
    parameters: dict[str, JsonValue] = Field(default_factory=dict)
    context_digest: Digest | None = None
    _paths = field_validator("output_root", "report_path")(absolute_path)

    def sealed(self) -> WorkloadContext:
        validated = WorkloadContext.model_validate(self)
        digest = record_digest(
            "research.workload-context",
            validated.model_dump(mode="json", exclude={"context_digest"}),
        )
        if self.context_digest is not None and self.context_digest != digest:
            raise ValueError("Workload context digest mismatch")
        return validated.model_copy(update={"context_digest": digest})

    def verify(self) -> None:
        if self.context_digest is None:
            raise ValueError("Workload context is not sealed")
        self.sealed()


class WorkloadOutput(VersionedRecord):
    name: Name
    path: str
    media_type: str = "application/octet-stream"
    _path = field_validator("path")(portable_path)


class WorkloadReport(VersionedRecord):
    context_digest: Digest
    observed_seed: int = Field(strict=True, ge=0, le=9_007_199_254_740_991)
    outputs: tuple[WorkloadOutput, ...] = ()
    measurements: tuple[Metric, ...] = ()
    observed_members: tuple[str, ...] = ()
    phase_timings_ms: dict[Name, Milliseconds] = Field(default_factory=dict)
    diagnostics: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def unique_names(self) -> WorkloadReport:
        for values in (
            [output.name for output in self.outputs],
            [metric.name for metric in self.measurements],
            list(self.observed_members),
        ):
            if len(values) != len(set(values)):
                raise ValueError("Workload report names and panel members must be unique")
        return self


class VerifiedOutput(VersionedRecord):
    output: WorkloadOutput
    digest: Digest
    size_bytes: ByteCount


def read_report(context: WorkloadContext, stage: StageSpec) -> WorkloadReport:
    context.verify()
    if context.stage_id != stage.id:
        raise ValueError("Workload context identifies another stage")
    report_path = Path(context.report_path)
    with (
        SourceReader(report_path.parent) as reader,
        reader.open(portable_path(report_path.name)) as stream,
    ):
        content = stream.read(1024 * 1024 + 1)
    if len(content) > 1024 * 1024:
        raise ValueError("Workload report exceeds its size limit")
    report = WorkloadReport.model_validate(strict_json_loads(content))
    if report.context_digest != context.context_digest or report.observed_seed != context.seed:
        raise ValueError("Workload report does not match its context or seed")
    if {output.name for output in report.outputs} != set(stage.output_names):
        raise ValueError("Workload output names do not match the stage contract")
    return report


def verify_outputs(
    context: WorkloadContext, report: WorkloadReport, *, maximum_bytes: int
) -> tuple[VerifiedOutput, ...]:
    """Verify bounded file contents before upload; final publication verifies them again."""
    context.verify()
    report = WorkloadReport.model_validate(report)
    if (
        report.context_digest != context.context_digest
        or report.observed_seed != context.seed
        or maximum_bytes < 0
    ):
        raise ValueError("Output verification requires the original context and a valid bound")
    remaining = maximum_bytes
    results = []
    with SourceReader(Path(context.output_root)) as reader:
        for output in report.outputs:
            digest = hashlib.sha256()
            size = 0
            with reader.open(output.path) as stream:
                while block := stream.read(min(1024 * 1024, remaining + 1)):
                    size += len(block)
                    remaining -= len(block)
                    if remaining < 0:
                        raise ValueError("Workload output exceeds its byte allowance")
                    digest.update(block)
            results.append(
                VerifiedOutput(
                    output=output,
                    digest="sha256:" + digest.hexdigest(),
                    size_bytes=size,
                )
            )
    return tuple(results)
