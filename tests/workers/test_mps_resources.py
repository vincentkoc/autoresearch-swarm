from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest

from research_swarm.domain.errors import ResearchError
from research_swarm.domain.types import new_id
from research_swarm.workers.authority import (
    DispatchVerifier,
    VerifiedDispatch,
    WorkerBinding,
    authorize_dispatch,
)
from research_swarm.workers.resources import MachineResources, ResourceDemand
from tests.worker_support import dispatch

PHYSICAL = "device:" + "a" * 64
SHARED = "mps-shared:" + "a" * 64


def binding(verified: VerifiedDispatch) -> WorkerBinding:
    return WorkerBinding.model_validate(
        {k: getattr(verified.envelope, k) for k in WorkerBinding.model_fields}
    )


def another_worker(first: VerifiedDispatch) -> VerifiedDispatch:
    envelope = first.envelope.model_copy(
        update={
            k: new_id() for k in ("id", "attempt_id", "job_id", "worker_id", "worker_session_id")
        }
    )
    key = b"synthetic-test-authority-key-32-bytes"
    envelope = authorize_dispatch(envelope, key)
    worker = WorkerBinding.model_validate(
        {k: getattr(envelope, k) for k in WorkerBinding.model_fields}
    )
    return DispatchVerifier(worker, key).verify(envelope, first.spec)


def shared_machine(tmp_path: Path):
    first, _ = dispatch(tmp_path)
    machine = MachineResources(tmp_path / "machine.sqlite")
    machine.enroll(
        binding(first),
        ResourceDemand(amounts={"cpu": 4, "memory": 128, "scratch": 512, PHYSICAL: 1, SHARED: 2}),
    )
    return machine, first


def test_two_shared_claims_admit_third_refuses_and_unknown_is_retained(tmp_path: Path) -> None:
    machine, first = shared_machine(tmp_path)
    request = ResourceDemand(amounts={"cpu": 2, "memory": 64, "scratch": 256, SHARED: 1})
    claim = machine.reserve(first, request)
    machine.quarantine(claim, "Fixture unresolved native closure")
    machine.reserve(another_worker(first), request)
    with pytest.raises(ResearchError):
        machine.reserve(another_worker(first), request)
    assert machine.availability()[SHARED] == 0
    with machine._connect() as sql:
        assert (
            sql.execute(
                "SELECT state FROM claims WHERE attempt_id=?", (claim.attempt_id,)
            ).fetchone()[0]
            == "unknown"
        )


@pytest.mark.parametrize("first_pool,second_pool", [(PHYSICAL, SHARED), (SHARED, PHYSICAL)])
def test_exclusive_shared_exclusion_in_both_directions(
    tmp_path: Path, first_pool: str, second_pool: str
) -> None:
    machine, first = shared_machine(tmp_path)
    claim = machine.reserve(first, ResourceDemand(amounts={first_pool: 1}))
    machine.quarantine(claim, "Fixture unknown")
    assert machine.availability()[second_pool] == 0
    with pytest.raises(ResearchError, match="admission conflict"):
        machine.reserve(another_worker(first), ResourceDemand(amounts={second_pool: 1}))


def test_one_claim_cannot_mix_exclusive_and_shared_device_modes(tmp_path: Path) -> None:
    machine, first = shared_machine(tmp_path)
    with pytest.raises(ResearchError, match="admission conflict"):
        machine.reserve(first, ResourceDemand(amounts={PHYSICAL: 1, SHARED: 1}))
    assert machine.availability()[PHYSICAL] == 1
    assert machine.availability()[SHARED] == 2


def test_independent_connections_race_exclusive_against_shared(tmp_path: Path) -> None:
    machine, first = shared_machine(tmp_path)
    barrier = Barrier(2)

    def reserve(args):
        verified, pool = args
        registry = MachineResources.open_existing(machine.path, machine.identity)
        barrier.wait(timeout=5)
        try:
            registry.reserve(verified, ResourceDemand(amounts={pool: 1}))
            return "held"
        except ResearchError:
            return "refused"

    with ThreadPoolExecutor(max_workers=2) as threads:
        results = list(threads.map(reserve, [(first, PHYSICAL), (another_worker(first), SHARED)]))
    assert sorted(results) == ["held", "refused"]


def test_legacy_writer_races_shared_admission_without_bypassing_trigger(tmp_path: Path) -> None:
    machine, first = shared_machine(tmp_path)
    barrier = Barrier(2)

    def legacy() -> str:
        barrier.wait(timeout=5)
        try:
            with machine._transaction() as sql:
                sql.execute(
                    "INSERT INTO claims(attempt_id,payload,state) VALUES (?,?,'held')",
                    (first.envelope.attempt_id, "{}"),
                )
                sql.execute(
                    "INSERT INTO allocations VALUES (?,?,1)",
                    (first.envelope.attempt_id, PHYSICAL),
                )
            return "held"
        except sqlite3.IntegrityError:
            return "refused"

    def shared() -> str:
        registry = MachineResources.open_existing(machine.path, machine.identity)
        barrier.wait(timeout=5)
        try:
            registry.reserve(another_worker(first), ResourceDemand(amounts={SHARED: 1}))
            return "held"
        except ResearchError:
            return "refused"

    with ThreadPoolExecutor(max_workers=2) as threads:
        old, new = threads.submit(legacy), threads.submit(shared)
        assert sorted([old.result(), new.result()]) == ["held", "refused"]


@pytest.mark.parametrize("held_pool,new_pool", [(PHYSICAL, SHARED), (SHARED, PHYSICAL)])
def test_legacy_sql_allocation_boundary_rolls_back_every_pool(
    tmp_path: Path, held_pool: str, new_pool: str
) -> None:
    machine, first = shared_machine(tmp_path)
    machine.reserve(first, ResourceDemand(amounts={held_pool: 1}))
    new = another_worker(first).envelope.attempt_id
    # This is the old writer's transaction: no cross-mode Python predicate.
    with (
        pytest.raises(sqlite3.IntegrityError, match="admission conflict"),
        machine._transaction() as sql,
    ):
        sql.execute(
            "INSERT INTO claims(attempt_id,payload,state) VALUES (?,?,'held')",
            (new, json.dumps({})),
        )
        sql.execute("INSERT INTO allocations VALUES (?,?,?)", (new, "cpu", 2))
        sql.execute("INSERT INTO allocations VALUES (?,?,?)", (new, new_pool, 1))
    with machine._connect() as sql:
        assert (
            sql.execute("SELECT count(*) FROM claims WHERE attempt_id=?", (new,)).fetchone()[0] == 0
        )
    assert machine.availability()["cpu"] == 4


@pytest.mark.parametrize("resource,amount", [("cpu", 5), ("memory", 129), ("scratch", 513)])
def test_shared_slots_do_not_bypass_aggregate_host_limits(
    tmp_path: Path, resource: str, amount: int
) -> None:
    machine, first = shared_machine(tmp_path)
    with pytest.raises(ResearchError):
        machine.reserve(first, ResourceDemand(amounts={SHARED: 1, resource: amount}))
    assert machine.availability()[SHARED] == 2


def test_explicit_resize_preserves_holds_and_rolls_back(tmp_path: Path) -> None:
    machine, first = shared_machine(tmp_path)
    machine.reserve(first, ResourceDemand(amounts={"cpu": 3, SHARED: 1}))
    with pytest.raises(ResearchError, match="another enrollment"):
        machine.enroll(binding(first), ResourceDemand(amounts={"cpu": 8}))
    with pytest.raises(ResearchError, match="discard held"):
        machine.enroll(
            binding(first), ResourceDemand(amounts={"scratch": 1024, "cpu": 2}), resize=True
        )
    assert machine.availability()["scratch"] == 512
    machine.enroll(binding(first), ResourceDemand(amounts={"cpu": 8}), resize=True)
    assert machine.availability()["cpu"] == 5
    with pytest.raises(ResearchError, match="must remain one"):
        machine.enroll(binding(first), ResourceDemand(amounts={PHYSICAL: 2}), resize=True)
