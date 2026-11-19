from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event

import pytest

from research_swarm.swarm import campaign_gc, data_stage
from research_swarm.swarm.data_stage import hydrate, mount_readonly, prepare, stage_path


@pytest.mark.skipif(os.name != "posix", reason="worker shared volume read locks")
def test_workers_copy_together_while_gc_waits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, volume = tmp_path / "source", tmp_path / "volume"
    source.mkdir()
    (source / "sample").write_bytes(b"42")
    stage = prepare(source, volume, "demo", "v1")
    path = stage_path(volume, "demo", "v1")
    readers, release = Barrier(3), Event()
    copy = data_stage._hash_tree

    def gated_copy(source: Path, target: Path, threads: int) -> tuple[str, int, int]:
        readers.wait(timeout=10)
        assert release.wait(timeout=10)
        return copy(source, target, threads)

    monkeypatch.setattr(data_stage, "_hash_tree", gated_copy)
    with ThreadPoolExecutor(max_workers=3) as clients:
        copies = [
            clients.submit(hydrate, volume, tmp_path / str(i), "demo", "v1") for i in range(2)
        ]
        try:
            readers.wait(timeout=10)
            collection = clients.submit(campaign_gc._remove_stage, path, stage)
            with pytest.raises(TimeoutError):
                collection.result(timeout=0.2)
        finally:
            release.set()
        assert all((result.result() / "sample").read_bytes() == b"42" for result in copies)
        assert collection.result(timeout=10) == "removed"


def test_prepare_once_per_location_and_parallel_boot_copy(tmp_path: Path) -> None:
    source = tmp_path / "inputs"
    (source / "empty").mkdir(parents=True)
    (source / "batch").mkdir()
    for i in range(12):
        (source / "batch" / str(i)).write_bytes(bytes([i]) * 500)
    volume = tmp_path / "region"
    with ThreadPoolExecutor(max_workers=4) as clients:
        stages = list(clients.map(lambda _: prepare(source, volume, "demo", "v1"), range(4)))
    stage = stages[0]
    assert all(item == stage for item in stages)
    assert (stage.files, stage.size_bytes) == (12, 6000)
    (source / "batch" / "0").write_bytes(b"changed")
    assert prepare(source, volume, "demo", "v1") == stage
    assert prepare(source, tmp_path / "other-region", "demo", "v1").sha256 != stage.sha256
    nvme = tmp_path / "nvme"
    worker_data = hydrate(volume, nvme, "demo", "v1", threads=4)
    assert (worker_data / "empty").is_dir()
    assert (worker_data / "batch" / "0").read_bytes() == bytes(500)
    assert (worker_data / "batch" / "0").stat().st_mode & 0o222 == 0
    assert hydrate(volume, nvme, "demo", "v1") == worker_data


def test_failed_copy_can_retry_without_publishing_partial_data(tmp_path: Path) -> None:
    source = tmp_path / "input"
    source.mkdir()
    link = source / "bad"
    link.symlink_to(tmp_path)
    volume = tmp_path / "volume"
    with pytest.raises(ValueError, match="symbolic link"):
        prepare(source, volume, "demo", "v1")
    assert not stage_path(volume, "demo", "v1").exists()
    link.unlink()
    (source / "good").write_bytes(b"first")
    prepare(source, volume, "demo", "v1")
    damaged = stage_path(volume, "demo", "v1") / "data" / "good"
    damaged.chmod(0o644)
    damaged.write_bytes(b"wrong")
    with pytest.raises(ValueError, match="differs"):
        hydrate(volume, tmp_path / "nvme", "demo", "v1")
    assert not stage_path(tmp_path / "nvme", "demo", "v1").exists()


def test_cached_worker_bytes_must_match_the_published_digest(tmp_path: Path) -> None:
    source, volume, nvme = (tmp_path / name for name in ("source", "region", "nvme"))
    source.mkdir()
    (source / "points").write_bytes(b"13")
    prepare(source, volume, "demo", "v1")
    cached = hydrate(volume, nvme, "demo", "v1") / "points"
    cached.chmod(0o644)
    cached.write_bytes(b"99")
    cached.chmod(0o444)
    with pytest.raises(ValueError, match="differs"):
        hydrate(volume, nvme, "demo", "v1")
    assert cached.read_bytes() == b"99"


def test_staged_directories_and_files_are_read_only(tmp_path: Path) -> None:
    source, volume, nvme = (tmp_path / name for name in ("source", "region", "nvme"))
    (source / "batch").mkdir(parents=True)
    (source / "batch/points").write_bytes(b"13")
    prepare(source, volume, "demo", "v1")
    for data in (volume / "demo/v1/data", hydrate(volume, nvme, "demo", "v1")):
        for entry in (data, data / "batch", data / "batch/points"):
            assert entry.stat().st_mode & 0o222 == 0
        if os.geteuid() != 0:
            with pytest.raises(PermissionError):
                (data / "batch/extra").touch()
            with pytest.raises(PermissionError):
                (data / "batch/points").write_bytes(b"99")

    # Valid caches created before directory sealing can still be reused.
    cached = nvme / "demo/v1/data"
    (cached / "batch").chmod(0o755)
    assert hydrate(volume, nvme, "demo", "v1") == cached
    assert (cached / "batch").stat().st_mode & 0o222 == 0


def test_source_and_stage_are_disjoint_and_scoped(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="overlap"):
        prepare(tmp_path, tmp_path / "stage", "demo", "v1")
    for campaign in ("", ".", "..", "../escape", "/escape"):
        with pytest.raises(ValueError, match="single path"):
            stage_path(tmp_path, campaign, "v1")


def test_worker_boot_propagates_mount_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "input"
    source.mkdir()
    volume = tmp_path / "volume"
    cli = [sys.executable, "-m", "research_swarm.swarm.data_stage"]
    args = ["--volume", str(volume), "--campaign", "demo", "--revision", "r1"]
    output = subprocess.check_output([*cli, "prepare", "--source", str(source), *args])
    assert json.loads(output)["campaign"] == "demo"

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    mount = fake_bin / "mount"
    trace = tmp_path / "trace"
    mount.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$TRACE"\nexit 7\n')
    mount.chmod(0o755)
    findmnt = fake_bin / "findmnt"
    findmnt.write_text("#!/bin/sh\nexit 1\n")
    findmnt.chmod(0o755)
    monkeypatch.setenv("TRACE", str(trace))
    monkeypatch.setenv("PATH", str(fake_bin) + os.pathsep + os.environ["PATH"])
    nvme, target = tmp_path / "nvme", tmp_path / "mountpoint"
    boot = subprocess.run([*cli, "boot", "--nvme", str(nvme), "--mountpoint", str(target), *args])
    assert boot.returncode != 0
    data = stage_path(nvme, "demo", "r1") / "data"
    assert trace.read_text().splitlines() == ["-o", "bind,ro", "--", str(data), str(target)]
    (target / "someone-elses-file").touch()
    trace.unlink()
    with pytest.raises(ValueError, match="in use"):
        mount_readonly(data, target)
    assert not trace.exists()


def test_existing_bind_must_match_data_and_be_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data, target = tmp_path / "data", tmp_path / "target"
    data.mkdir()
    target.mkdir()
    monkeypatch.setattr(os.path, "samefile", lambda a, b: True)
    monkeypatch.setattr(os, "statvfs", lambda path: os.statvfs_result((0,) * 10))
    with pytest.raises(RuntimeError, match="read-only"):
        mount_readonly(data, target)
    stats = os.statvfs_result((0,) * 8 + (os.ST_RDONLY, 0))
    monkeypatch.setattr(os, "statvfs", lambda path: stats)
    mount_readonly(data, target)
