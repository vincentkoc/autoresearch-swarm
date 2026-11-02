"""Durable local bindings for a sealed workload and its admitted native resources."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, field_validator

from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import DispatchEnvelope, ExperimentSpec
from research_swarm.domain.types import ID, Digest, VersionedRecord
from research_swarm.projects.workload import WorkloadContext, absolute_path
from research_swarm.reactive.admission import verify_parameters
from research_swarm.workers.contracts import NativeCommand
from research_swarm.workers.engine_binding import EngineBinding


class PreparedExecution(VersionedRecord):
    context: WorkloadContext
    context_path: str
    executable: str
    profile_digest: Digest
    resource_registry: str
    machine_claim_id: ID
    working_directory: str | None = None
    engine: EngineBinding | None = Field(default=None, exclude_if=lambda value: value is None)
    _paths = field_validator("context_path", "executable", "resource_registry")(absolute_path)

    @field_validator("working_directory")
    @classmethod
    def directory(cls, value: str | None) -> str | None:
        return absolute_path(value) if value is not None else None

    def verify(self, dispatch: DispatchEnvelope, spec: ExperimentSpec) -> None:
        self.context.verify()
        stage = next(stage for stage in spec.stages if stage.id == dispatch.stage_id)
        try:
            verify_parameters(self.context, spec, stage)
        except ValueError:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Reactive workload context differs"
            ) from None
        if (self.engine is None) != ("engine" not in self.context.parameters):
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED,
                "Registered context and launch binding must be present together",
            )
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
        if self.engine is not None:
            declared = self.context.parameters.get("engine")
            if not isinstance(declared, dict) or (
                declared.get("execution_fingerprint") != self.profile_digest
                or declared.get("environment_digest") != spec.environment_digest
                or declared.get("profile") != stage.resources.engine_profile
                or declared.get("launch_mode") != "isolated-verify-then-project-v1"
            ):
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Registered workload context differs"
                )

    def verify_launch(self, dispatch: DispatchEnvelope, spec: ExperimentSpec) -> None:
        """Native-only checks. Publication verification must never open worker paths."""
        self.verify(dispatch, spec)
        if self.engine is not None:
            from research_swarm.engines.execution import context, resolve_engine

            registration = resolve_engine(self.engine)
            stage = next(stage for stage in spec.stages if stage.id == dispatch.stage_id)
            if (
                self.executable != registration.executable
                or self.profile_digest != registration.profile.execution_fingerprint
                or self.context.parameters.get("engine") != context(registration, spec, stage)
            ):
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Registered workload binding differs"
                )

    def resource_amounts(self, spec: ExperimentSpec) -> dict[str, int]:
        from research_swarm.engines.execution import demand, resolve_engine

        stage = next(stage for stage in spec.stages if stage.id == self.context.stage_id)
        registration = resolve_engine(self.engine) if self.engine is not None else None
        return demand(stage.resources, registration)

    def environment(self, spec: ExperimentSpec) -> dict[str, str]:
        if self.engine is None:
            return {}
        from research_swarm.engines.execution import environment, resolve_engine

        stage = next(stage for stage in spec.stages if stage.id == self.context.stage_id)
        return environment(resolve_engine(self.engine), stage)

    def verify_context_file(self) -> None:
        from research_swarm.projects.native_files import SourceReader

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
        prefix = (
            (self.executable, "-I", self.engine.bootstrap.path, self.engine.payload.path)
            if self.engine is not None
            else (self.executable,)
        )
        return NativeCommand.model_validate(
            declared.model_copy(update={"argv": (*prefix, *declared.argv[1:])})
        )
