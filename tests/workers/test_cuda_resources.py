from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest

from research_swarm.domain.errors import ResearchError
from research_swarm.workers.journal_records import JournalEntry, JournalState
from research_swarm.workers.resources import MachineResources, ResourceDemand
from tests.worker_support import dispatch
from tests.workers.test_mps_resources import another_worker, binding

PHYSICAL = "device:" + "c" * 64
SHARED = "cuda-shared:" + "c" * 64
VRAM = "cuda-memory:" + "c" * 64


def machine(tmp_path: Path):
    first, _ = dispatch(tmp_path)
    registry = MachineResources(tmp_path / "machine.sqlite")
    registry.enroll(
        binding(first),
        ResourceDemand(amounts={"cpu": 20, "memory": 80, PHYSICAL: 1, SHARED: 20, VRAM: 40}),
    )
    return registry, first


def shared_demand(**extra):
    return ResourceDemand(amounts={SHARED: 1, VRAM: 2, "cpu": 1, "memory": 4, **extra})


def test_twenty_real_bindings_reserve_atomically_and_twenty_first_refuses(tmp_path: Path) -> None:
    registry, first = machine(tmp_path)
    workers = [first, *(another_worker(first) for _ in range(19))]
    barrier = Barrier(20)

    def reserve(verified):
        independent = MachineResources.open_existing(registry.path, registry.identity)
        barrier.wait(timeout=10)
        return independent.reserve(verified, shared_demand())

    with ThreadPoolExecutor(max_workers=20) as pool:
        claims = list(pool.map(reserve, workers))
    assert len({claim.worker_id for claim in claims}) == 20
    assert len({claim.worker_session_id for claim in claims}) == 20
    assert len({claim.attempt_id for claim in claims}) == 20
    assert registry.availability()[SHARED] == registry.availability()[VRAM] == 0
    with pytest.raises(ResearchError):
        registry.reserve(another_worker(first), shared_demand())
    registry.quarantine(claims[0], "Synthetic unresolved closure")
    with pytest.raises(ResearchError):
        registry.reserve(another_worker(first), shared_demand())


def test_vram_exhaustion_stops_admission_even_with_unused_slots(tmp_path: Path) -> None:
    registry, first = machine(tmp_path)
    registry.reserve(first, shared_demand(**{VRAM: 39}))
    with pytest.raises(ResearchError, match="occupied"):
        registry.reserve(another_worker(first), shared_demand())
    assert registry.availability()[SHARED] == 19
    assert registry.availability()[VRAM] == 1


@pytest.mark.parametrize("first_shared", [False, True])
def test_cuda_exclusive_conflicts_with_shared_including_unknown(
    tmp_path: Path, first_shared: bool
) -> None:
    registry, first = machine(tmp_path)
    exclusive = ResourceDemand(amounts={PHYSICAL: 1})
    one, two = (shared_demand(), exclusive) if first_shared else (exclusive, shared_demand())
    claim = registry.reserve(first, one)
    registry.quarantine(claim, "Synthetic unresolved closure")
    with pytest.raises(ResearchError, match="admission conflict"):
        registry.reserve(another_worker(first), two)
    assert registry.availability()[PHYSICAL if first_shared else SHARED] == 0


def test_independent_connections_race_cuda_exclusive_against_shared(tmp_path: Path) -> None:
    registry, first = machine(tmp_path)
    barrier = Barrier(2)

    def reserve(args):
        verified, demand = args
        independent = MachineResources.open_existing(registry.path, registry.identity)
        barrier.wait(timeout=5)
        try:
            independent.reserve(verified, demand)
            return "held"
        except ResearchError:
            return "refused"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                reserve,
                [
                    (first, ResourceDemand(amounts={PHYSICAL: 1})),
                    (another_worker(first), shared_demand()),
                ],
            )
        )
    assert sorted(results) == ["held", "refused"]


@pytest.mark.parametrize("held_shared", [False, True])
def test_legacy_writer_cannot_bypass_cuda_conflict_trigger(
    tmp_path: Path, held_shared: bool
) -> None:
    registry, first = machine(tmp_path)
    registry.reserve(
        first, shared_demand() if held_shared else ResourceDemand(amounts={PHYSICAL: 1})
    )
    new = another_worker(first).envelope.attempt_id
    with (
        pytest.raises(sqlite3.IntegrityError, match="admission conflict"),
        registry._transaction() as sql,
    ):
        sql.execute("INSERT INTO claims(attempt_id,payload,state) VALUES (?,?,'held')", (new, "{}"))
        sql.execute("INSERT INTO allocations VALUES (?,?,1)", (new, "cpu"))
        sql.execute(
            "INSERT INTO allocations VALUES (?,?,1)", (new, PHYSICAL if held_shared else SHARED)
        )
    with registry._connect() as sql:
        assert (
            sql.execute("SELECT count(*) FROM claims WHERE attempt_id=?", (new,)).fetchone()[0] == 0
        )


@pytest.mark.parametrize("pool", [SHARED, VRAM])
def test_shared_claim_requires_both_slot_and_vram(tmp_path: Path, pool: str) -> None:
    registry, first = machine(tmp_path)
    with pytest.raises(ResearchError, match="slots and VRAM"):
        registry.reserve(first, ResourceDemand(amounts={pool: 1}))


def test_confirmed_unstarted_release_returns_cuda_capacity(tmp_path: Path) -> None:
    registry, first = machine(tmp_path)
    registry.reserve(first, shared_demand())
    registry.release(
        JournalEntry(
            dispatch=first.envelope,
            spec=first.spec,
            command=dispatch(tmp_path)[1],
            state=JournalState.NOT_STARTED,
        )
    )
    assert registry.availability()[VRAM] == 40
    assert registry.availability()[SHARED] == 20


@pytest.mark.parametrize("amounts", [{PHYSICAL: 2}, {SHARED: 21, VRAM: 40}, {SHARED: 20}])
def test_invalid_cuda_capacity_cannot_enroll(tmp_path: Path, amounts: dict) -> None:
    first, _ = dispatch(tmp_path)
    registry = MachineResources(tmp_path / "machine.sqlite")
    with pytest.raises(ResearchError):
        registry.enroll(binding(first), ResourceDemand(amounts={PHYSICAL: 1, **amounts}))
    assert registry.availability() == {}
