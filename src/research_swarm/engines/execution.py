"""Derive registered native admission and frozen launch files from one contract."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from research_swarm.domain.canonical import canonical_bytes, content_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import ExperimentSpec, ResourceRequirements, StageSpec
from research_swarm.engines.adapter import launch_adapter_digest
from research_swarm.engines.registration import EnvironmentRegistration, read_registration
from research_swarm.projects.source import portable_path
from research_swarm.runtime import private_state_directory, read_private, write_private
from research_swarm.workers.engine_binding import EngineBinding, EngineFile


def capacities(registration: EnvironmentRegistration) -> dict[str, int]:
    result = {"cpu": registration.cpu_slots}
    if registration.memory_bytes:
        result["memory"] = registration.memory_bytes
    if registration.scratch_bytes:
        result["scratch"] = registration.scratch_bytes
    if registration.device_pool is not None:
        result[registration.device_pool] = 1
    return result


def demand(
    resources: ResourceRequirements, registration: EnvironmentRegistration | None
) -> dict[str, int]:
    result = {"cpu": resources.cpu_slots}
    memory = resources.memory_bytes
    if registration is not None:
        registration.profile.require(resources, "inherit")
        device = registration.descriptor.device
        if resources.accelerator_count != (1 if device else 0):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "Registered stage must reserve its complete selected device",
            )
        if device is not None:
            if not resources.exclusive_device:
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE,
                    "Registered accelerator stages require exclusive admission",
                )
            pool = registration.device_pool
            assert pool is not None
            result[pool] = 1
            if device.unified_memory:
                memory += resources.accelerator_memory_bytes
        if memory > registration.memory_bytes:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "Combined unified memory exceeds the admitted host capacity",
            )
    if memory:
        result["memory"] = memory
    if resources.scratch_bytes:
        result["scratch"] = resources.scratch_bytes
    return result


def validate_stage(
    registration: EnvironmentRegistration, spec: ExperimentSpec, stage: StageSpec
) -> None:
    registration.require_environment(spec.environment_digest)
    if registration.launch_adapter_digest is None:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE,
            "Registration lacks a qualified launch adapter binding",
        )
    demand(stage.resources, registration)
    if (
        registration.policy.precision != "project_declared"
        or registration.policy.compilation != "project_declared"
    ):
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE,
            "Explicit precision or compilation requires a qualified workload policy",
        )
    arguments = stage.argv
    if len(arguments) < 2 or arguments[0] not in {"python", "python3", registration.executable}:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE,
            "Registered stage requires its selected Python interpreter",
        )
    if arguments[1] in {"-m", "-c"}:
        if len(arguments) < 3 or not arguments[2]:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Registered Python invocation is incomplete"
            )
    elif arguments[1].startswith("-"):
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Registered Python option is not supported"
        )
    else:
        portable_path(arguments[1])


def context(
    registration: EnvironmentRegistration, spec: ExperimentSpec, stage: StageSpec
) -> dict[str, Any]:
    validate_stage(registration, spec, stage)
    backend = registration.descriptor.device_backend
    return {
        "profile": registration.descriptor.profile,
        "framework": registration.descriptor.framework,
        "device_backend": backend,
        "logical_device": "cuda:0"
        if backend in {"cuda", "rocm"}
        else "gpu"
        if backend == "metal"
        else backend,
        "execution_fingerprint": registration.profile.execution_fingerprint,
        "environment_digest": spec.environment_digest,
        "policy": registration.policy.model_dump(mode="json"),
        "launch_adapter_digest": registration.launch_adapter_digest,
        "launch_mode": "isolated-verify-then-project-v1",
        "resources": demand(stage.resources, registration),
    }


def environment(registration: EnvironmentRegistration, stage: StageSpec) -> dict[str, str]:
    result = {"OMP_NUM_THREADS": str(stage.resources.cpu_slots)}
    backend, device = registration.descriptor.device_backend, registration.descriptor.device
    if backend in {"cuda", "rocm"}:
        assert device is not None
        result["CUDA_VISIBLE_DEVICES"] = str(device.index)
        if backend == "rocm":
            result["HIP_VISIBLE_DEVICES"] = str(device.index)
    if backend == "mps":
        result["PYTORCH_ENABLE_MPS_FALLBACK"] = "0"
    return result


def _payload(registration: EnvironmentRegistration, probe: EngineFile) -> dict[str, Any]:
    return {
        "profile": registration.descriptor.profile,
        "descriptor": registration.descriptor.model_dump(mode="json", exclude_defaults=True),
        "interpreter": registration.interpreter.model_dump(mode="json", exclude_defaults=True),
        "executable_digest": registration.executable_digest,
        "logical_device_index": 0,
        "probe": probe.model_dump(mode="json", exclude_defaults=True),
    }


def _freeze(path: Path, payload: bytes) -> EngineFile:
    if path.exists():
        if read_private(path.parent, path.name, limit=1024 * 1024) != payload:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Frozen engine input changed")
    else:
        write_private(path, payload)
    return EngineFile(path=str(path), digest=content_digest(payload))


def prepare_engine(root: Path, registration: EnvironmentRegistration) -> EngineBinding:
    root = root.absolute()
    with private_state_directory(root):
        frozen_registration = _freeze(
            root / "registration.json", canonical_bytes(registration.model_dump(mode="json"))
        )
        probe = _freeze(root / "probe.py", Path(__file__).with_name("_probe.py").read_bytes())
        bootstrap = _freeze(root / "launch.py", Path(__file__).with_name("_launch.py").read_bytes())
        if (
            launch_adapter_digest(bootstrap.digest, probe.digest)
            != registration.launch_adapter_digest
        ):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "Registered launch adapter differs from installed code",
            )
        payload = _freeze(root / "launch.json", canonical_bytes(_payload(registration, probe)))
    return EngineBinding(
        registration=frozen_registration, bootstrap=bootstrap, probe=probe, payload=payload
    )


def resolve_engine(binding: EngineBinding) -> EnvironmentRegistration:
    for entry in (binding.registration, binding.bootstrap, binding.probe, binding.payload):
        path = Path(entry.path)
        if content_digest(read_private(path.parent, path.name, limit=1024 * 1024)) != entry.digest:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Frozen engine input differs")
    registration = read_registration(Path(binding.registration.path))
    if (
        launch_adapter_digest(binding.bootstrap.digest, binding.probe.digest)
        != registration.launch_adapter_digest
    ):
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Frozen launch adapter differs from registration"
        )
    path = Path(binding.payload.path)
    payload = strict_json_loads(read_private(path.parent, path.name, limit=1024 * 1024))
    if payload != _payload(registration, binding.probe):
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Engine launch payload differs from registration"
        )
    return registration
