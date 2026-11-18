from concurrent.futures import ThreadPoolExecutor

import pytest

from research_swarm.swarm.resources import Demand, ResourcePool


def test_mixed_slots_and_rolling_token_window() -> None:
    now = [100.0]
    pool = ResourcePool(cpu=2, llm=2, tokens_per_minute=20, clock=lambda: now[0])
    demand = Demand(cpu=1, llm=1, llm_tokens=10)
    abandoned = pool.acquire(demand)
    assert abandoned is not None
    pool.release(abandoned, dispatched=False)
    first = pool.acquire(demand, max_fits=8)
    assert first is not None and first.fits == 2
    assert pool.acquire(Demand(cpu=1)) is None
    pool.release(first)
    assert pool.acquire(demand) is None
    assert pool.acquire(Demand(cpu=2)) is not None
    now[0] += 59.9
    assert pool.acquire(Demand(llm=1, llm_tokens=1)) is None
    now[0] += 0.1
    assert pool.acquire(Demand(llm=1, llm_tokens=20)) is not None


def test_gpu_calibration_packs_to_target_without_partial_cpu_claims() -> None:
    utilisation = [0.2]
    pool = ResourcePool(
        cpu=12, devices_mib={"gpu-0": 1000}, target=0.8, gpu_utilisation=lambda _: utilisation[0]
    )
    demand = Demand(cpu=1, gpu_profile="small-model")
    calibration = pool.acquire(demand, max_fits=12)
    assert calibration is not None and calibration.fits == 1 and calibration.vram_mib == 800
    assert pool.acquire(demand) is None
    pool.release(calibration, peak_vram_mib=200)
    batch = pool.acquire(demand, max_fits=10)
    assert batch is not None and (batch.device, batch.fits, batch.vram_mib) == ("gpu-0", 4, 800)
    assert pool.acquire(demand) is None
    assert pool.acquire(Demand(cpu=8)) is not None
    pool.release(batch, peak_vram_mib=240)
    utilisation[0] = 0.8
    assert pool.acquire(demand) is None
    utilisation[0] = 0.4
    smaller_batch = pool.acquire(demand, max_fits=5)
    assert smaller_batch is not None and smaller_batch.fits == 3
    assert pool.acquire(Demand(gpu_profile="unknown-shape")) is None


def test_atomic_concurrent_admission_across_devices() -> None:
    pool = ResourcePool(cpu=20, devices_mib={"device-b": 500, "device-a": 500})
    with ThreadPoolExecutor(max_workers=16) as executor:
        claims = list(
            executor.map(lambda _: pool.acquire(Demand(cpu=1, gpu_profile="p")), range(20))
        )
    accepted = [a for a in claims if a is not None]
    assert sorted(a.device for a in accepted if a.device) == ["device-a", "device-b"]
    for a in accepted:
        pool.release(a, peak_vram_mib=500)
        with pytest.raises(ValueError, match="not active"):
            pool.release(a)
    # An observed peak above the target cannot be packed onto either device.
    assert pool.acquire(Demand(gpu_profile="p")) is None


def test_invalid_requests_and_measurements() -> None:
    with pytest.raises(ValueError):
        Demand()
    with pytest.raises(ValueError):
        Demand(llm_tokens=10)
    with pytest.raises(ValueError):
        ResourcePool(target=float("nan"))
    pool = ResourcePool(cpu=1, llm=1, tokens_per_minute=2)
    with pytest.raises(ValueError, match="token allowance"):
        pool.acquire(Demand(llm=1))
    allocation = pool.acquire(Demand(cpu=1))
    assert allocation is not None
    with pytest.raises(ValueError, match="GPU measurement"):
        pool.release(allocation, peak_vram_mib=1)
    pool.release(allocation)
