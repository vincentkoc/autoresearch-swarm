"""Private native files with bounded reads and durable atomic replacement."""

from __future__ import annotations

import hashlib
import os
import stat
import unicodedata
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from research_swarm.swarm.errors import ErrorCode, ResearchError
from research_swarm.swarm.projects.native_files import SourceReader
from research_swarm.swarm.records import new_id


def default_state_directory() -> Path:
    return Path.home() / ".local" / "state" / "autoresearch-swarm"


def restore_admission_path(root: Path) -> Path:
    """Publish the native restore admission barrier before creating its root."""
    target = root.expanduser().resolve()
    name = target.name
    if target.exists():
        current = target.stat()
        with os.scandir(target.parent) as entries:
            for entry in entries:
                try:
                    candidate = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if (candidate.st_dev, candidate.st_ino) == (current.st_dev, current.st_ino):
                    name = entry.name
                    break
    # The containing directory supplies parent identity. Do not persist device
    # numbers, which can change after reboot. Coalesce case/normalization aliases
    # conservatively, including on case-sensitive volumes.
    normalized = unicodedata.normalize("NFD", name).casefold()
    identity = hashlib.sha256(os.fsencode(normalized)).hexdigest()
    return target.parent / (".research-restore-" + identity + ".pending")


def campaign_database(root: Path, *, project_directory: bool = False) -> Path:
    """Select a campaign workspace without touching retired controller state."""
    root = root.expanduser().absolute()
    markers: tuple[Path, ...] = (
        root / "research.sqlite",
        root / "recovery.pending",
        restore_admission_path(root),
    )
    # A project directory can contain its own identity.json alongside the YAML spec.
    if not project_directory:
        markers += (root / "identity.json",)
    if any(marker.exists() or marker.is_symlink() for marker in markers):
        raise ResearchError(
            ErrorCode.SCHEMA_UNSUPPORTED,
            "Legacy controller workspace. Read it with its original release "
            "and select a fresh directory for the campaign.",
        )
    return root / "swarm.sqlite"


def read_private(root: Path, name: str, *, limit: int) -> bytes:
    with SourceReader(root) as reader, reader.open(name) as stream:
        if stat.S_IMODE(os.fstat(stream.fileno()).st_mode) & 0o077:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Private runtime file has broad permissions"
            )
        content = stream.read(limit + 1)
    if len(content) > limit:
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Runtime file exceeds its limit")
    return content


def sync_directory(path: Path) -> None:
    directory = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def replace_private(source: Path, destination: Path) -> None:
    os.replace(source, destination)
    sync_directory(destination.parent)


def write_private(path: Path, content: bytes) -> None:
    staged = path.with_name(path.name + "." + new_id() + ".pending")
    descriptor = os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        replace_private(staged, path)
    finally:
        staged.unlink(missing_ok=True)


@contextmanager
def private_state_directory(root: Path, *, exclusive: bool = False) -> Iterator[None]:
    root.mkdir(mode=0o700, parents=True, exist_ok=not exclusive)
    if stat.S_IMODE(root.stat().st_mode) & 0o077:
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Runtime directory requires owner-only access"
        )
    with SourceReader(root):
        yield
