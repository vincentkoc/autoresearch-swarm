"""Bounded private checkpoint files; no archive paths or extraction semantics."""

from __future__ import annotations

import hashlib
import os
import stat
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

from research_swarm.domain.backup import BackupFile
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.projects.native_files import SourceReader
from research_swarm.state.private_files import sync_directory

CHUNK_BYTES = 1024 * 1024
DATABASE_LIMIT = 1024**3


@contextmanager
def private_reader(root: Path, name: str) -> Iterator[BinaryIO]:
    with SourceReader(root) as reader, reader.open(name) as stream:
        info = os.fstat(stream.fileno())
        if info.st_nlink != 1:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup file has multiple links")
        if sys.platform == "win32":
            from research_swarm.state.windows_privacy import verify_descriptor

            verify_descriptor(stream.fileno())
        elif stat.S_IMODE(info.st_mode) & 0o077:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Backup file is not private")
        yield stream


def measure(stream: BinaryIO, *, limit: int) -> BackupFile:
    digest, size = hashlib.sha256(), 0
    while content := stream.read(CHUNK_BYTES):
        size += len(content)
        if size > limit:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup file exceeds its bound")
        digest.update(content)
    return BackupFile(digest="sha256:" + digest.hexdigest(), size_bytes=size)


def measure_file(root: Path, name: str, *, limit: int) -> BackupFile:
    with private_reader(root, name) as stream:
        return measure(stream, limit=limit)


def copy_private(stream: BinaryIO, destination: Path, expected: BackupFile) -> None:
    staged = destination.with_name(destination.name + "." + new_id() + ".pending")
    if sys.platform == "win32":
        from research_swarm.state.windows_privacy import create_private_descriptor

        descriptor = create_private_descriptor(staged)
    else:
        descriptor = os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    digest, size = hashlib.sha256(), 0
    try:
        with os.fdopen(descriptor, "wb") as target:
            while content := stream.read(CHUNK_BYTES):
                size += len(content)
                if size > expected.size_bytes:
                    raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup file grew")
                digest.update(content)
                target.write(content)
            if (size, "sha256:" + digest.hexdigest()) != (expected.size_bytes, expected.digest):
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Backup file differs")
            target.flush()
            os.fsync(target.fileno())
        os.replace(staged, destination)
        sync_directory(destination.parent)
    finally:
        staged.unlink(missing_ok=True)
