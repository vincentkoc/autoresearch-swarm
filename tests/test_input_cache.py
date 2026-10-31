from __future__ import annotations

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event

import pytest
from filelock import FileLock

from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import new_id
from research_swarm.state.private_files import read_private
from research_swarm.workers.input_cache import CacheEntry, CacheIndex, InputCache, InputCacheLimits


class Fixture:
    def __init__(self, root: Path, limits: InputCacheLimits | None = None):
        self.cache = InputCache(root, new_id(), limits or InputCacheLimits())
        self.content = b"synthetic shared input"
        self.reference = self.ref(self.content)
        self.authorizations = 0
        self.downloads = 0
        self.allowed = True

    @staticmethod
    def ref(content: bytes) -> ArtifactRef:
        return ArtifactRef(id=new_id(), digest=content_digest(content), size_bytes=len(content))

    def authorize(self):
        self.authorizations += 1
        if not self.allowed:
            raise ResearchError(ErrorCode.STALE_FENCE, "Synthetic grant expired")
        return self.reference

    def fetch(self, target):
        self.downloads += 1
        with target.open("xb") as stream:
            stream.write(self.content)
        return self.reference

    def materialize(self, destination):
        return self.cache.materialize(
            self.reference, destination, authorize=self.authorize, fetch=self.fetch
        )

    def blob(self):
        entry = CacheEntry(workspace_id=self.cache.workspace_id, reference=self.reference)
        return self.cache.root / (entry.key + ".blob")


def test_warm_cache_reauthorizes_and_copies_independent_bytes(tmp_path):
    fixture = Fixture(tmp_path / "cache")
    first, second, third = (tmp_path / name for name in ("first", "second", "third"))
    assert fixture.materialize(first).origin == "download"
    before = fixture.authorizations
    warm = fixture.materialize(second)
    assert warm.origin == "cache" and warm.avoided_transfer_bytes == len(fixture.content)
    assert fixture.authorizations > before and fixture.downloads == 1
    assert first.stat().st_nlink == second.stat().st_nlink == fixture.blob().stat().st_nlink == 1
    first.write_bytes(b"workload changed its own copy")
    assert fixture.materialize(third).origin == "cache"
    assert second.read_bytes() == third.read_bytes() == fixture.content
    with pytest.raises(ResearchError) as corrupt:
        fixture.materialize(first)
    assert corrupt.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    assert first.read_bytes() == b"workload changed its own copy"
    fixture.allowed = False
    with pytest.raises(ResearchError) as revoked:
        fixture.materialize(second)
    assert revoked.value.code == ErrorCode.STALE_FENCE
    assert fixture.downloads == 1


def test_full_reference_and_workspace_identity_are_required(tmp_path):
    fixture = Fixture(tmp_path / "cache")
    fixture.materialize(tmp_path / "first")
    changed = fixture.reference.model_copy(update={"media_type": "text/plain"})
    with pytest.raises(ResearchError) as mismatch:
        fixture.cache.materialize(
            changed, tmp_path / "denied", authorize=fixture.authorize, fetch=fixture.fetch
        )
    assert mismatch.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    assert not (tmp_path / "denied").exists()
    fixture.cache = InputCache(fixture.cache.root, new_id(), fixture.cache.limits)
    assert fixture.materialize(tmp_path / "different-workspace").origin == "download"
    assert fixture.downloads == 2


def test_waiting_cache_reader_rechecks_authority_after_lock_acquisition(tmp_path):
    fixture = Fixture(tmp_path / "cache")
    fixture.materialize(tmp_path / "first")
    entered = Event()

    def authorize():
        result = fixture.authorize()
        entered.set()
        return result

    with ThreadPoolExecutor(max_workers=1) as pool:
        with FileLock(fixture.cache.root / "cache.lock", timeout=5):
            future = pool.submit(
                fixture.cache.materialize,
                fixture.reference,
                tmp_path / "revoked",
                authorize=authorize,
                fetch=fixture.fetch,
            )
            assert entered.wait(5)
            fixture.allowed = False
        with pytest.raises(ResearchError) as revoked:
            future.result(timeout=5)
    assert revoked.value.code == ErrorCode.STALE_FENCE
    assert not (tmp_path / "revoked").exists()
    assert fixture.downloads == 1


def test_transport_failure_never_falls_back_to_cached_bytes(tmp_path):
    fixture = Fixture(tmp_path / "cache")
    fixture.materialize(tmp_path / "first")

    def unavailable():
        raise TimeoutError("Synthetic controller outage")

    with pytest.raises(TimeoutError):
        fixture.cache.materialize(
            fixture.reference, tmp_path / "denied", authorize=unavailable, fetch=fixture.fetch
        )
    assert not (tmp_path / "denied").exists()
    assert fixture.downloads == 1


def test_corrupt_shared_bytes_are_refetched_but_bad_origin_bytes_fail(tmp_path):
    fixture = Fixture(tmp_path / "cache")
    fixture.materialize(tmp_path / "first")
    fixture.blob().write_bytes(b"x" * len(fixture.content))
    assert fixture.materialize(tmp_path / "repaired").origin == "download"
    assert fixture.downloads == 2
    assert fixture.blob().read_bytes() == fixture.content
    fixture.blob().write_bytes(b"y" * len(fixture.content))
    fixture.content = b"z" * len(fixture.content)
    with pytest.raises(ResearchError) as failed:
        fixture.materialize(tmp_path / "bad-origin")
    assert failed.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    assert not (tmp_path / "bad-origin").exists()
    assert not fixture.blob().exists()


@pytest.mark.parametrize("limit", ["bytes", "entries"])
def test_lru_eviction_obeys_both_capacity_bounds(tmp_path, limit):
    limits = InputCacheLimits(
        max_bytes=44 if limit == "bytes" else 1000,
        max_entries=10 if limit == "bytes" else 2,
    )
    fixture = Fixture(tmp_path / "cache", limits)
    references = [Fixture.ref(fixture.content) for _ in range(3)]
    for index in (0, 1, 0, 2):
        fixture.reference = references[index]
        fixture.materialize(tmp_path / new_id())
        blobs = list(fixture.cache.root.glob("*.blob"))
        assert len(blobs) <= limits.max_entries
        assert sum(path.stat().st_size for path in blobs) <= limits.max_bytes
        assert not list(fixture.cache.root.glob("*.pending"))
    catalog = CacheIndex.model_validate_json(
        read_private(fixture.cache.root, "index.json", limit=4096)
    )
    assert [entry.reference for entry in catalog.entries] == [references[0], references[2]]
    fixture.reference = references[1]
    assert fixture.materialize(tmp_path / "evicted").origin == "download"


@pytest.mark.parametrize(
    "limits", [InputCacheLimits(max_bytes=0), InputCacheLimits(max_entry_bytes=1)]
)
def test_disabled_and_oversized_entries_bypass_cache_creation(tmp_path, limits):
    fixture = Fixture(tmp_path / "cache", limits)
    assert fixture.materialize(tmp_path / "first").origin == "download"
    assert fixture.materialize(tmp_path / "second").origin == "download"
    assert fixture.authorizations >= 4 and fixture.downloads == 2
    assert not fixture.cache.root.exists()


def test_concurrent_cold_transfers_do_not_hold_the_cache_lock(tmp_path):
    fixture = Fixture(tmp_path / "cache")
    barrier = Barrier(2, timeout=5)

    def fetch(target):
        barrier.wait()
        return fixture.fetch(target)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(
                fixture.cache.materialize,
                fixture.reference,
                tmp_path / name,
                authorize=fixture.authorize,
                fetch=fetch,
            )
            for name in ("first", "second")
        ]
        assert [future.result(timeout=10).origin for future in futures] == ["download", "download"]
    assert len(list(fixture.cache.root.glob("*.blob"))) == 1
    assert fixture.materialize(tmp_path / "warm").origin == "cache"


PROCESS_SCRIPT = """
import os, sys, time
from pathlib import Path
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.workers import input_cache as module
root, target, workspace, raw, phase = sys.argv[1:]
reference = ArtifactRef.model_validate_json(raw)
cache = module.InputCache(Path(root), workspace, module.InputCacheLimits())
original = module.replace_private
def interrupt(source, destination):
    if destination.suffix == '.blob' and phase == 'stage':
        os._exit(74)
    original(source, destination)
    if destination.suffix == '.blob' and phase == 'published':
        os._exit(75)
module.replace_private = interrupt
def fetch(destination):
    if phase == 'concurrent':
        Path(target + '.ready').touch()
        deadline = time.monotonic() + 10
        while not Path(target).parent.joinpath('release').exists():
            if time.monotonic() >= deadline:
                raise TimeoutError('Owned concurrent fill did not release')
            time.sleep(0.01)
    destination.write_bytes(b'synthetic shared input')
result = cache.materialize(reference, Path(target), authorize=lambda: reference,
    fetch=lambda destination: (fetch(destination), reference)[1])
print(result.origin)
"""


def child(fixture, destination, phase):
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            PROCESS_SCRIPT,
            str(fixture.cache.root),
            str(destination),
            fixture.cache.workspace_id,
            fixture.reference.model_dump_json(),
            phase,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


@pytest.mark.parametrize("phase,code", [("stage", 74), ("published", 75)])
def test_process_exit_during_cache_publication_recovers_disposable_bytes(tmp_path, phase, code):
    fixture = Fixture(tmp_path / "cache")
    process = child(fixture, tmp_path / "interrupted", phase)
    try:
        _, stderr = process.communicate(timeout=15)
        assert process.returncode == code, stderr
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
    assert fixture.materialize(tmp_path / "recovered").origin == "download"
    assert fixture.blob().read_bytes() == fixture.content
    assert not list(fixture.cache.root.glob("*.pending"))
    assert len(list(fixture.cache.root.glob("*.blob"))) == 1


def test_independent_processes_share_one_cache_without_copy_aliases(tmp_path):
    import time

    fixture = Fixture(tmp_path / "cache")
    destinations = [tmp_path / name for name in ("first", "second")]
    processes = [child(fixture, destination, "concurrent") for destination in destinations]
    try:
        deadline = time.monotonic() + 10
        while not all(Path(str(destination) + ".ready").exists() for destination in destinations):
            assert time.monotonic() < deadline, "Owned cache processes did not enter transfer"
            time.sleep(0.01)
        (tmp_path / "release").touch()
        for process in processes:
            stdout, stderr = process.communicate(timeout=15)
            assert process.returncode == 0, stderr
            assert stdout.strip() == "download"
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)
    assert all(destination.read_bytes() == fixture.content for destination in destinations)
    assert len(list(fixture.cache.root.glob("*.blob"))) == 1
    assert fixture.materialize(tmp_path / "warm").origin == "cache"


def test_cache_bypasses_linked_entries_without_repair(tmp_path):
    fixture = Fixture(tmp_path / "cache")
    fixture.materialize(tmp_path / "first")
    alias = tmp_path / "external-alias"
    os.link(fixture.blob(), alias)
    result = fixture.materialize(tmp_path / "fresh")
    assert result.origin == "download" and result.cache_unavailable
    assert (tmp_path / "fresh").read_bytes() == fixture.content
    assert alias.read_bytes() == fixture.content
    assert fixture.blob().stat().st_nlink == 2


@pytest.mark.parametrize(
    "content", [b"{", b"{}", b"x" * (2 * 1024 * 1024 + 1), b"[" * 5000 + b"]" * 5000]
)
def test_invalid_disposable_index_bypasses_without_repair(tmp_path, content):
    fixture = Fixture(tmp_path / "cache")
    fixture.materialize(tmp_path / "first")
    index = fixture.cache.root / "index.json"
    index.write_bytes(content)
    result = fixture.materialize(tmp_path / "fresh")
    # An empty valid model is a cold catalog; invalid models/bytes bypass storage.
    assert result.origin == "download"
    if content != b"{}":
        assert result.cache_unavailable
        assert index.read_bytes() == content
    assert (tmp_path / "fresh").read_bytes() == fixture.content


def test_failed_cache_publication_keeps_verified_authorized_attempt_copy(tmp_path, monkeypatch):
    fixture = Fixture(tmp_path / "cache")

    def disk_full(*_):
        raise OSError(28, "Synthetic cache storage full")

    monkeypatch.setattr(fixture.cache, "_publish", disk_full)
    result = fixture.materialize(tmp_path / "valid")
    assert result.origin == "download" and result.cache_unavailable
    assert (tmp_path / "valid").read_bytes() == fixture.content
    assert fixture.authorizations >= 4


def test_failed_cache_recency_write_keeps_the_independent_warm_copy(tmp_path, monkeypatch):
    fixture = Fixture(tmp_path / "cache")
    fixture.materialize(tmp_path / "first")
    original, writes = fixture.cache._save, []

    def fail_recency(entries):
        writes.append(True)
        if len(writes) == 2:
            raise OSError(28, "Synthetic recency write failure")
        return original(entries)

    monkeypatch.setattr(fixture.cache, "_save", fail_recency)
    result = fixture.materialize(tmp_path / "warm")
    assert result.origin == "cache" and result.cache_unavailable
    assert (tmp_path / "warm").read_bytes() == fixture.content
    assert fixture.downloads == 1


def test_authorization_io_failure_inside_cache_lock_is_not_bypassed(tmp_path):
    fixture = Fixture(tmp_path / "cache")
    fixture.materialize(tmp_path / "first")
    calls = []

    def authorize():
        calls.append(True)
        if len(calls) == 2:
            raise TimeoutError("Synthetic authorization transport outage")
        return fixture.reference

    with pytest.raises(TimeoutError):
        fixture.cache.materialize(
            fixture.reference, tmp_path / "denied", authorize=authorize, fetch=fixture.fetch
        )
    assert not (tmp_path / "denied").exists()
    assert fixture.downloads == 1
