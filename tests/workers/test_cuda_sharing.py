from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.experiments import ResourceRequirements
from research_swarm.engines.execution import capacities, demand
from research_swarm.engines.models import DeviceObservation
from research_swarm.engines.registration import EnvironmentRegistration, ExecutionPolicy
from research_swarm.workers import remote_enrollment
from research_swarm.workers.remote_enrollment import WorkerDescription, describe_worker
from tests.workers.test_engines import descriptor
from tests.workers.test_mps_sharing import registered as mps_registration

GIB = 1024**3


def registered(tmp_path: Path) -> EnvironmentRegistration:
    value = mps_registration(tmp_path).model_dump()
    value["descriptor"] = descriptor(
        profile="pytorch-cuda",
        framework="pytorch",
        device_backend="cuda",
        framework_version="fixture",
        device=DeviceObservation(
            index=0, name="Fixture CUDA", physical_id="fixture-cuda-0", memory_bytes=24 * GIB
        ),
    ).model_dump()
    value["policy"] = ExecutionPolicy(
        device_admission="cuda_shared", cuda_shared_slots=20, cuda_memory_bytes=20 * GIB
    ).model_dump()
    return EnvironmentRegistration.model_validate(value)


def resources() -> ResourceRequirements:
    return ResourceRequirements(
        engine_profile="pytorch-cuda",
        cpu_slots=1,
        memory_bytes=GIB,
        accelerator_count=1,
        accelerator_memory_bytes=GIB,
        exclusive_device=False,
        required_features=frozenset({"cuda-shared-execution-slots"}),
    )


def test_cuda_capacity_retains_one_physical_gpu_and_separate_vram(tmp_path: Path) -> None:
    value = registered(tmp_path)
    pools = capacities(value)
    assert value.profile.accelerator_count == 1
    assert value.max_concurrency == value.profile.shared_execution_slots == 20
    assert pools[value.device_pool] == 1
    assert pools[value.shared_device_pool] == 20
    assert pools[value.accelerator_memory_pool] == 20 * GIB
    amounts = demand(resources(), value)
    assert amounts["memory"] == GIB
    assert amounts[value.accelerator_memory_pool] == GIB
    assert amounts[value.shared_device_pool] == 1
    assert value.device_pool not in amounts
    exclusive = demand(resources().model_copy(update={"exclusive_device": True}), value)
    assert exclusive[value.device_pool] == 1
    assert value.shared_device_pool not in exclusive
    assert value.accelerator_memory_pool not in exclusive


@pytest.mark.parametrize(
    "change",
    [
        {"accelerator_memory_bytes": 0},
        {"accelerator_memory_bytes": 21 * GIB},
        {"required_features": frozenset()},
        {"accelerator_count": 0},
        {"engine_profile": "python-cpu"},
    ],
)
def test_cuda_requires_honest_device_and_positive_bounded_vram(
    tmp_path: Path, change: dict
) -> None:
    with pytest.raises(ResearchError):
        demand(resources().model_copy(update=change), registered(tmp_path))


@pytest.mark.parametrize("slots", [0, 21, True])
def test_cuda_slot_bounds(slots: int) -> None:
    with pytest.raises(ValidationError):
        ExecutionPolicy(
            device_admission="cuda_shared", cuda_shared_slots=slots, cuda_memory_bytes=GIB
        )


@pytest.mark.parametrize("change", [{"cuda_memory_bytes": 0}, {"mps_shared_slots": 2}])
def test_cuda_policy_does_not_borrow_mps_capacity(change: dict) -> None:
    with pytest.raises(ValidationError):
        ExecutionPolicy.model_validate(
            {"device_admission": "cuda_shared", "cuda_memory_bytes": GIB, **change}
        )


@pytest.mark.parametrize("capacity", [0, 25 * GIB])
def test_cuda_registration_refuses_missing_or_excessive_vram(tmp_path: Path, capacity: int) -> None:
    value = registered(tmp_path).model_dump()
    value["policy"]["cuda_memory_bytes"] = capacity
    with pytest.raises(ValidationError):
        EnvironmentRegistration.model_validate(value)


def test_cuda_registration_requires_resolved_discrete_cuda_device(tmp_path: Path) -> None:
    value = registered(tmp_path).model_dump()
    for change in ({"physical_id": None}, {"unified_memory": True}):
        altered = {**value, "descriptor": {**value["descriptor"]}}
        altered["descriptor"]["device"] = {**value["descriptor"]["device"], **change}
        with pytest.raises(ValidationError):
            EnvironmentRegistration.model_validate(altered)
    value["descriptor"]["device_backend"] = "rocm"
    with pytest.raises(ValidationError):
        EnvironmentRegistration.model_validate(value)


def test_default_and_mps_records_omit_cuda_extensions(tmp_path: Path) -> None:
    for value in (ExecutionPolicy(), mps_registration(tmp_path).policy):
        assert "cuda_shared_slots" not in value.model_dump()
        assert "cuda_memory_bytes" not in value.model_dump()
    profile = mps_registration(tmp_path).profile.model_dump()
    profile["shared_execution_slots"] = 9
    with pytest.raises(ValidationError):
        type(mps_registration(tmp_path).profile).model_validate(profile)
    value = mps_registration(tmp_path)
    assert value.profile.execution_fingerprint == (
        "sha256:1687adeeb291e83d293e53cbd3221d4def15565b09dcd22df481525b3b4c0afb"
    )
    exclusive = value.model_copy(update={"policy": ExecutionPolicy()})
    assert exclusive.profile.execution_fingerprint == (
        "sha256:2418be4a89113fd50c5419ba6ed6ff54b87d85de962ac183d56b21828e7b3820"
    )


def test_remote_cap_is_independent_of_machine_slots_and_pinned_on_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(remote_enrollment, "verify_registration", lambda _: None)
    monkeypatch.setattr(remote_enrollment.os, "cpu_count", lambda: 8)
    registration = registered(tmp_path)
    args = {
        "cpu_slots": registration.cpu_slots,
        "registration": registration,
        "machine_registry": tmp_path / "registry.sqlite",
        "concurrency_limit": 1,
    }
    first = describe_worker(tmp_path / "worker", **args)
    assert first.max_concurrency == 1
    assert first.profile.shared_execution_slots == 20
    assert describe_worker(tmp_path / "worker", **args) == first
    with pytest.raises(ResearchError, match="inputs changed"):
        describe_worker(tmp_path / "worker", **{**args, "concurrency_limit": 2})
    unlimited = first.model_dump()
    unlimited.pop("concurrency_limit")
    assert WorkerDescription.model_validate(unlimited).max_concurrency == 20
    with pytest.raises(ValidationError):
        WorkerDescription.model_validate({**first.model_dump(), "concurrency_limit": 21})
    assert first.profile.execution_fingerprint == registration.profile.execution_fingerprint
    assert first.environment_contracts == (content_digest(b"fixture environment"),)
