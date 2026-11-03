from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.experiments import ResourceRequirements
from research_swarm.engines.execution import capacities, demand
from research_swarm.engines.models import DeviceObservation
from research_swarm.engines.probe import InterpreterIdentity
from research_swarm.engines.registration import EnvironmentRegistration, ExecutionPolicy
from tests.workers.test_engines import descriptor

GIB = 1024**3


def registered(tmp_path: Path, slots: int = 2) -> EnvironmentRegistration:
    return EnvironmentRegistration(
        executable=str(tmp_path / "python"),
        probe_directory=str(tmp_path / "probe"),
        executable_digest=content_digest(b"fixture interpreter"),
        launch_adapter_digest=content_digest(b"fixture adapter"),
        interpreter=InterpreterIdentity(
            executable=str(tmp_path / "python"),
            prefix=str(tmp_path),
            base_prefix=str(tmp_path),
        ),
        descriptor=descriptor(
            profile="pytorch-mps",
            framework="pytorch",
            device_backend="mps",
            framework_version="fixture",
            device=DeviceObservation(
                index=0, name="Fixture MPS", physical_id="fixture-physical-0", unified_memory=True
            ),
        ),
        environment_contracts=(content_digest(b"fixture environment"),),
        cpu_slots=4,
        memory_bytes=128 * GIB,
        scratch_bytes=512 * 1024**2,
        policy=ExecutionPolicy(device_admission="mps_shared", mps_shared_slots=slots),
    )


def test_shared_registration_is_opt_in_and_keeps_one_physical_gpu(tmp_path: Path) -> None:
    value = registered(tmp_path)
    default = value.model_copy(update={"policy": ExecutionPolicy()})
    assert "device_admission" not in default.policy.model_dump()
    assert "mps_shared_slots" not in default.policy.model_dump()
    assert "shared_execution_slots" not in default.profile.model_dump()
    assert value.profile.accelerator_count == default.profile.accelerator_count == 1
    assert value.device_pool == default.device_pool
    assert capacities(value)[value.device_pool] == 1
    assert capacities(value)[value.shared_device_pool] == 2
    assert value.profile.execution_fingerprint != default.profile.execution_fingerprint
    assert value.max_concurrency == 2 and default.max_concurrency == 1
    resources = ResourceRequirements(
        engine_profile="pytorch-mps",
        cpu_slots=2,
        memory_bytes=64 * GIB,
        scratch_bytes=256 * 1024**2,
        accelerator_count=1,
        exclusive_device=False,
        required_features=frozenset({"mps-shared-execution-slots"}),
    )
    amounts = demand(resources, value)
    assert amounts["memory"] == 64 * GIB
    assert amounts[value.shared_device_pool] == 1 and value.device_pool not in amounts
    with pytest.raises(ResearchError):
        demand(resources, default)
    with pytest.raises(ResearchError):
        demand(resources.model_copy(update={"required_features": frozenset()}), value)
    exclusive = demand(resources.model_copy(update={"exclusive_device": True}), value)
    assert exclusive[value.device_pool] == 1 and value.shared_device_pool not in exclusive


@pytest.mark.parametrize("slots", [0, 9, True])
def test_slot_bounds(slots: int) -> None:
    with pytest.raises(ValidationError):
        ExecutionPolicy(device_admission="mps_shared", mps_shared_slots=slots)


def test_non_mps_sharing_rejected(tmp_path: Path) -> None:
    value = registered(tmp_path).model_dump()
    value["descriptor"] = descriptor().model_dump()
    with pytest.raises(ValidationError, match="physical MPS"):
        EnvironmentRegistration.model_validate(value)
