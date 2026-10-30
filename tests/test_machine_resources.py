from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.workers.authority import (
    DispatchVerifier,
    VerifiedDispatch,
    WorkerBinding,
    authorize_dispatch,
)
from research_swarm.workers.journal import WorkerJournal
from research_swarm.workers.resources import MachineResources, ResourceDemand
from research_swarm.workers.supervisor import run_attempt
from tests.worker_support import dispatch


def binding(dispatch: VerifiedDispatch) -> WorkerBinding:
    return WorkerBinding.model_validate(
        {key: getattr(dispatch.envelope, key) for key in WorkerBinding.model_fields}
    )


def another_worker(first: VerifiedDispatch) -> VerifiedDispatch:
    envelope = first.envelope.model_copy(
        update={
            "id": new_id(),
            "attempt_id": new_id(),
            "job_id": new_id(),
            "worker_id": new_id(),
            "worker_session_id": new_id(),
        }
    )
    key = b"synthetic-test-authority-key-32-bytes"
    envelope = authorize_dispatch(envelope, key)
    bound = WorkerBinding.model_validate(
        {field: getattr(envelope, field) for field in WorkerBinding.model_fields}
    )
    return DispatchVerifier(bound, key).verify(envelope, first.spec)


def test_one_machine_pool_cannot_be_enrolled_under_two_controllers(tmp_path: Path) -> None:
    verified, _ = dispatch(tmp_path)
    machine = MachineResources(tmp_path / "native.sqlite")
    capacity = ResourceDemand(amounts={"cpu": 4, "device-0": 1})
    machine.enroll(binding(verified), capacity)
    MachineResources(machine.path).enroll(binding(verified), capacity)
    with pytest.raises(ResearchError, match="another enrollment"):
        machine.enroll(binding(verified).model_copy(update={"controller_id": new_id()}), capacity)
    with pytest.raises(ResearchError, match="another enrollment"):
        machine.enroll(binding(verified), ResourceDemand(amounts={"cpu": 8}))
    assert machine.availability() == {"cpu": 4, "device-0": 1}


def test_native_allocation_is_atomic_across_pools_and_survives_restart(tmp_path: Path) -> None:
    first, _ = dispatch(tmp_path)
    machine = MachineResources(tmp_path / "native.sqlite")
    machine.enroll(binding(first), ResourceDemand(amounts={"cpu": 4, "device-0": 1}))
    demand = ResourceDemand(amounts={"cpu": 2, "device-0": 1})
    claim = machine.reserve(first, demand)
    assert machine.reserve(first, demand) == claim
    with pytest.raises(ResearchError, match="occupied"):
        MachineResources(machine.path).reserve(another_worker(first), demand)
    assert machine.availability() == {"cpu": 2, "device-0": 0}
    with pytest.raises(ResearchError, match="changed"):
        machine.reserve(first, ResourceDemand(amounts={"cpu": 1}))


def test_unknown_execution_keeps_capacity_and_only_native_closure_releases_it(
    tmp_path: Path,
) -> None:
    verified, command = dispatch(tmp_path)
    machine = MachineResources(tmp_path / "native.sqlite")
    machine.enroll(binding(verified), ResourceDemand(amounts={"cpu": 1}))
    claim = machine.reserve(verified, ResourceDemand(amounts={"cpu": 1}))
    journal = WorkerJournal(tmp_path / "worker" / "journal.sqlite")
    entry = journal.receive(verified, command)
    machine.quarantine(claim, "Controller lease expired")
    with pytest.raises(ResearchError) as error:
        machine.release(entry)
    assert error.value.code == ErrorCode.EXECUTION_UNKNOWN
    assert MachineResources(machine.path).availability() == {"cpu": 0}
    outcome = run_attempt(journal, entry.dispatch.attempt_id)
    machine.release(outcome)
    machine.release(outcome)
    assert machine.availability() == {"cpu": 1}
    with pytest.raises(ResearchError, match="already closed"):
        machine.reserve(verified, ResourceDemand(amounts={"cpu": 1}))


def test_cancelled_unstarted_execution_can_release_without_a_fake_native_result(
    tmp_path: Path,
) -> None:
    verified, command = dispatch(tmp_path)
    machine = MachineResources(tmp_path / "native.sqlite")
    machine.enroll(binding(verified), ResourceDemand(amounts={"cpu": 1}))
    machine.reserve(verified, ResourceDemand(amounts={"cpu": 1}))
    journal = WorkerJournal(tmp_path / "worker" / "journal.sqlite")
    entry = journal.receive(verified, command)
    journal.request_cancel(entry.dispatch.attempt_id, 1)
    closed = run_attempt(journal, entry.dispatch.attempt_id)
    assert closed.outcome is None
    altered = closed.model_copy(
        update={"dispatch": closed.dispatch.model_copy(update={"fencing_generation": 2})}
    )
    with pytest.raises(ResearchError, match="another native grant"):
        machine.release(altered)
    assert machine.availability() == {"cpu": 0}
    machine.release(closed)
    assert machine.availability() == {"cpu": 1}


def test_independent_processes_cannot_reserve_one_exclusive_device(tmp_path: Path) -> None:
    first, _ = dispatch(tmp_path)
    second = another_worker(first)
    machine = MachineResources(tmp_path / "native.sqlite")
    machine.enroll(binding(first), ResourceDemand(amounts={"device-0": 1}))
    code = """
import json,sys
from pathlib import Path
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.experiments import DispatchEnvelope, ExperimentSpec
from research_swarm.workers.authority import DispatchVerifier,WorkerBinding
from research_swarm.workers.resources import MachineResources,ResourceDemand
data=json.load(sys.stdin)
envelope=DispatchEnvelope.model_validate(data['envelope'])
bound=WorkerBinding.model_validate(data['binding'])
verified=DispatchVerifier(bound,b'synthetic-test-authority-key-32-bytes').verify(
    envelope,ExperimentSpec.model_validate(data['spec']))
try:
    MachineResources(Path(data['path'])).reserve(verified,ResourceDemand(amounts={'device-0':1}))
    print('reserved')
except ResearchError as error:
    print(error.code.value)
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", code],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    for process, request in zip(processes, (first, second), strict=True):
        assert process.stdin is not None
        process.stdin.write(
            json.dumps(
                {
                    "envelope": request.envelope.model_dump(mode="json"),
                    "spec": request.spec.model_dump(mode="json"),
                    "binding": binding(request).model_dump(mode="json"),
                    "path": str(machine.path),
                }
            )
        )
        process.stdin.close()
        process.stdin = None
    outputs = [process.communicate(timeout=10) for process in processes]
    assert all(process.returncode == 0 for process in processes), outputs
    assert sorted(out.strip() for out, _ in outputs) == ["RESOURCE_UNAVAILABLE", "reserved"]
    assert machine.availability() == {"device-0": 0}
