"""Private native files with bounded reads and durable atomic replacement."""

from __future__ import annotations

import hashlib
import os
import stat
import sys
import unicodedata
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.projects.native_files import SourceReader


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


def read_private(root: Path, name: str, *, limit: int) -> bytes:
    with SourceReader(root) as reader, reader.open(name) as stream:
        if sys.platform != "win32" and stat.S_IMODE(os.fstat(stream.fileno()).st_mode) & 0o077:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Private runtime file has broad permissions"
            )
        if sys.platform == "win32":
            from research_swarm.state.windows_privacy import verify_descriptor

            verify_descriptor(stream.fileno())
        content = stream.read(limit + 1)
    if len(content) > limit:
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Runtime file exceeds its limit")
    return content


def sync_directory(path: Path) -> None:
    if sys.platform != "win32":
        directory = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


def write_private(path: Path, content: bytes) -> None:
    staged = path.with_name(path.name + "." + new_id() + ".pending")
    if sys.platform == "win32":
        from research_swarm.state.windows_privacy import create_private_descriptor

        descriptor = create_private_descriptor(staged)
    else:
        descriptor = os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(staged, path)
        sync_directory(path.parent)
    finally:
        staged.unlink(missing_ok=True)


@contextmanager
def private_state_directory(root: Path, *, exclusive: bool = False) -> Iterator[None]:
    if sys.platform == "win32":
        from research_swarm.state.windows_privacy import private_directory

        with private_directory(root, exclusive=exclusive):
            yield
    else:
        root.mkdir(mode=0o700, parents=True, exist_ok=not exclusive)
        if stat.S_IMODE(root.stat().st_mode) & 0o077:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Runtime directory requires owner-only access"
            )
        with SourceReader(root):
            yield
