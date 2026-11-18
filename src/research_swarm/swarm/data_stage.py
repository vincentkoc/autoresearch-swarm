"""Prepare a dataset revision on a location volume and hydrate it on worker boot.

The caller supplies mounted storage roots (one per location, plus worker-local NVMe).
A revision names a fixed snapshot. Choose a new revision to publish changed inputs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path

from filelock import FileLock


@dataclass(frozen=True)
class Stage:
    campaign: str
    revision: str
    sha256: str
    files: int
    size_bytes: int
    owner: str


def stage_path(root: Path, campaign: str, revision: str) -> Path:
    for name in (campaign, revision):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,126}", name):
            raise ValueError("campaign and revision must be single path components")
    parent = root.resolve() / campaign
    target = parent / revision
    if parent.is_symlink() or target.is_symlink():
        raise ValueError("stage path contains a symbolic link")
    return target


def read_stage(path: Path, *, pending_gc: bool = False) -> Stage:
    marker = path.with_name(f".gc.{path.name}.json") if pending_gc else path / "stage.json"
    if marker.is_symlink() or (
        not pending_gc and (not (path / "data").is_dir() or (path / "data").is_symlink())
    ):
        raise ValueError(f"invalid data stage at {path}")
    stage = Stage(**json.loads(marker.read_bytes()))
    if (stage.owner, stage.campaign, stage.revision) != (
        "autoresearch-swarm",
        path.parent.name,
        path.name,
    ) or not re.fullmatch("[0-9a-f]{64}", stage.sha256):
        raise ValueError(f"invalid stage identity at {path}")
    return stage


def _copy_tree(source: Path, target: Path, threads: int) -> tuple[str, int, int]:
    if not source.is_dir() or source.is_symlink():
        raise ValueError("dataset source must be a directory")
    records: list[tuple[str, str]] = []
    files: list[Path] = []
    # iterdir propagates read failures, so a partial traversal cannot publish.
    directories = [Path(".")]
    while directories:
        relative = directories.pop()
        (target / relative).mkdir()
        records.append((relative.as_posix(), "directory"))
        for entry in (source / relative).iterdir():
            name = relative / entry.name
            if entry.is_symlink():
                raise ValueError(f"dataset contains a symbolic link: {name}")
            if entry.is_dir():
                directories.append(name)
            elif entry.is_file():
                files.append(name)
            else:
                raise ValueError(f"dataset contains a special file: {name}")

    def copy(name: Path) -> tuple[str, str, int]:
        digest = hashlib.sha256()
        size = 0
        with (source / name).open("rb") as reader, (target / name).open("xb") as writer:
            while chunk := reader.read(1024 * 1024):
                writer.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        (target / name).chmod(0o444)
        return name.as_posix(), digest.hexdigest(), size

    total = 0
    with ThreadPoolExecutor(max_workers=threads) as pool:
        for filename, file_hash, size in pool.map(copy, files):
            records.append((filename, file_hash))
            total += size
    encoded = json.dumps(sorted(records), separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest(), len(files), total


def _publish(source: Path, target: Path, threads: int, *, expected: Stage | None = None) -> Stage:
    if threads < 1 or threads > 256:
        raise ValueError("copy threads must be between 1 and 256")
    source_path = source.resolve()
    if target.is_relative_to(source_path) or source_path.is_relative_to(target):
        raise ValueError("source and destination overlap")
    target.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(target.with_name(f".{target.name}.lock")):
        if target.with_name(f".gc.{target.name}.json").exists():
            raise ValueError("data stage cleanup is incomplete; retry campaign gc")
        if target.exists():
            stage = read_stage(target)
        else:
            with tempfile.TemporaryDirectory(prefix=".copy-", dir=target.parent) as temp:
                snapshot = Path(temp) / "snapshot"
                snapshot.mkdir()
                digest, files, size = _copy_tree(source, snapshot / "data", threads)
                stage = Stage(
                    target.parent.name, target.name, digest, files, size, "autoresearch-swarm"
                )
                if expected is not None and stage != expected:
                    raise ValueError("local copy differs from the published data stage")
                (snapshot / "stage.json").write_text(json.dumps(asdict(stage)), encoding="utf-8")
                snapshot.rename(target)
        if expected is not None and stage != expected:
            raise ValueError("cached revision differs from the published data stage")
        return stage


def prepare(
    source: Path, volume: Path, campaign: str, revision: str, *, threads: int = 16
) -> Stage:
    """Build once at this location. Concurrent callers share the published snapshot."""
    return _publish(source, stage_path(volume, campaign, revision), threads)


def hydrate(volume: Path, nvme: Path, campaign: str, revision: str, *, threads: int = 16) -> Path:
    """Copy and verify the snapshot once on this host, before a worker starts."""
    staged = stage_path(volume, campaign, revision)
    target = stage_path(nvme, campaign, revision)
    if staged == target:
        raise ValueError("the staging volume and worker cache overlap")
    with FileLock(staged.with_name(f".{staged.name}.lock")):
        _publish(staged / "data", target, threads, expected=read_stage(staged))
    return target / "data"


def mount_readonly(data: Path, mountpoint: Path) -> None:
    """Bind the hydrated tree on Linux. Run with mount privileges at boot."""
    data = data.resolve(strict=True)
    mountpoint = mountpoint.absolute()
    if mountpoint.is_symlink() or mountpoint.resolve() != mountpoint:
        raise ValueError("mountpoint must be a physical path")
    if data.is_relative_to(mountpoint) or mountpoint.is_relative_to(data):
        raise ValueError("mountpoint and hydrated data overlap")
    mountpoint.mkdir(parents=True, exist_ok=True)
    stage = data.parent
    with (
        FileLock(stage.with_name(f".{stage.name}.lock")),
        FileLock(mountpoint.with_name(f".{mountpoint.name}.mount.lock")),
    ):
        if not data.is_dir():
            raise ValueError("hydrated data was collected before mount")
        namespace = Path("/proc/self/ns/mnt")
        if namespace.exists() and (stage / "stage.json").exists():
            marker = stage / "mount-namespace"
            identity = os.readlink(namespace)
            if marker.exists() and marker.read_text() != identity:
                raise ValueError("data stage is exposed in another mount namespace")
            marker.write_text(identity)
        if not os.path.samefile(data, mountpoint):
            mounted = subprocess.run(
                ["findmnt", "--mountpoint", str(mountpoint)], stdout=subprocess.DEVNULL
            )
            if mounted.returncode != 1 or any(mountpoint.iterdir()):
                raise ValueError("mountpoint is already in use")
            subprocess.run(["mount", "-o", "bind,ro", "--", str(data), str(mountpoint)], check=True)
        if (
            not os.path.samefile(data, mountpoint)
            or not os.statvfs(mountpoint).f_flag & os.ST_RDONLY
        ):
            raise RuntimeError("read-only data mount did not become active")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    build = commands.add_parser("prepare")
    build.add_argument("--source", type=Path, required=True)
    boot = commands.add_parser("boot")
    boot.add_argument("--nvme", type=Path, required=True)
    boot.add_argument("--mountpoint", type=Path, required=True)
    for command in (build, boot):
        command.add_argument("--volume", required=True, type=Path)
        command.add_argument("--campaign", required=True)
        command.add_argument("--revision", required=True)
        command.add_argument("--threads", default=16, type=int)
    args = parser.parse_args()
    if args.action == "prepare":
        stage = prepare(
            args.source, args.volume, args.campaign, args.revision, threads=args.threads
        )
        print(json.dumps(asdict(stage)))
    else:
        data = hydrate(args.volume, args.nvme, args.campaign, args.revision, threads=args.threads)
        mount_readonly(data, args.mountpoint)


if __name__ == "__main__":
    main()
