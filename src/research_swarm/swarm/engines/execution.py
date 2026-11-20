"""Derive registered native admission and frozen launch files from one contract."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.swarm.engines.adapter import launch_adapter_digest, script_bytes
from research_swarm.swarm.engines.registration import EnvironmentRegistration
from research_swarm.swarm.private_files import private_state_directory, read_private, write_private
from research_swarm.swarm.projects.source import portable_path
from research_swarm.swarm.records import ResourceRequirements, StageSpec


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
    registration: EnvironmentRegistration, environment_digest: str, stage: StageSpec
) -> None:
    registration.require_environment(environment_digest)
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
    registration: EnvironmentRegistration, environment_digest: str, stage: StageSpec
) -> dict[str, Any]:
    validate_stage(registration, environment_digest, stage)
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
        "environment_digest": environment_digest,
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


def _freeze(path: Path, content: bytes) -> str:
    if path.exists():
        if read_private(path.parent, path.name, limit=1024 * 1024) != content:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Frozen engine input changed")
    else:
        write_private(path, content)
    return content_digest(content)


def prepare_engine(root: Path, registration: EnvironmentRegistration) -> tuple[str, ...]:
    """Return the registered interpreter and its isolated bootstrap argv prefix."""
    root = root.absolute()
    with private_state_directory(root):
        launch, probe, payload = (root / name for name in ("launch.py", "probe.py", "launch.json"))
        launch_digest = _freeze(launch, script_bytes("_launch.py"))
        probe_digest = _freeze(probe, script_bytes("_probe.py"))
        if launch_adapter_digest(launch_digest, probe_digest) != registration.launch_adapter_digest:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "Registered launch adapter differs from installed code",
            )
        _freeze(
            payload,
            canonical_bytes(
                {
                    "profile": registration.descriptor.profile,
                    "descriptor": registration.descriptor.model_dump(
                        mode="json", exclude_defaults=True
                    ),
                    "interpreter": registration.interpreter.model_dump(
                        mode="json", exclude_defaults=True
                    ),
                    "executable_digest": registration.executable_digest,
                    "logical_device_index": 0,
                    "probe": {"path": str(probe), "digest": probe_digest},
                }
            ),
        )
    return registration.executable, "-I", str(launch), str(payload)
