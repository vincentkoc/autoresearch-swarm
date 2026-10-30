"""Durable local bindings for a sealed workload and its admitted native resources."""

from __future__ import annotations

from pathlib import Path

from pydantic import field_validator

from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import DispatchEnvelope, ExperimentSpec
from research_swarm.domain.types import ID, Digest, VersionedRecord
from research_swarm.projects.native_files import SourceReader
from research_swarm.projects.workload import WorkloadContext, absolute_path
from research_swarm.workers.contracts import NativeCommand


class PreparedExecution(VersionedRecord):
    context: WorkloadContext
    context_path: str
    executable: str
    profile_digest: Digest
    resource_registry: str
    machine_claim_id: ID
    working_directory: str | None = None
    _paths = field_validator("context_path", "executable", "resource_registry")(absolute_path)

    @field_validator("working_directory")
    @classmethod
    def directory(cls, value: str | None) -> str | None:
        return absolute_path(value) if value is not None else None

    def verify(self, dispatch: DispatchEnvelope, spec: ExperimentSpec) -> None:
        self.context.verify()
        expected = (
            dispatch.attempt_id,
            dispatch.trial_id,
            dispatch.stage_id,
            dispatch.spec_digest,
            spec.seed,
        )
        actual = (
            self.context.attempt_id,
            self.context.trial_id,
            self.context.stage_id,
            self.context.spec_digest,
            self.context.seed,
        )
        if actual != expected:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Prepared workload identity differs"
            )

    def verify_context_file(self) -> None:
        path = Path(self.context_path)
        with SourceReader(path.parent) as reader, reader.open(path.name) as stream:
            payload = stream.read(1024 * 1024 + 1)
        if len(payload) > 1024 * 1024:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Prepared context exceeds its limit")
        context = WorkloadContext.model_validate(strict_json_loads(payload))
        context.verify()
        if context != self.context:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Prepared context content changed")

    def command(self, declared: NativeCommand) -> NativeCommand:
        """Resolve only the executable through the admitted profile; preserve stage arguments."""
        if self.working_directory is not None and declared.cwd != self.working_directory:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Prepared directory differs")
        return NativeCommand.model_validate(
            declared.model_copy(update={"argv": (self.executable, *declared.argv[1:])})
        )
