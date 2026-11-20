"""Read source through native identities before inspecting any file content."""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, cast

from research_swarm.swarm.canonical import CHUNK_SIZE as CHUNK_SIZE
from research_swarm.swarm.canonical import hash_stream as hash_stream


class SourceReader:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.descriptor: int | None = None

    def __enter__(self) -> SourceReader:
        self.descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        return self

    def close(self) -> None:
        if self.descriptor is not None:
            os.close(self.descriptor)
            self.descriptor = None

    def __exit__(self, *_: object) -> None:
        self.close()

    @contextmanager
    def open(self, relative: str) -> Iterator[BinaryIO]:
        if self.descriptor is None:
            raise RuntimeError("Source reader is not open")
        parent = os.dup(self.descriptor)
        try:
            *directories, name = relative.split("/")
            for part in directories:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                os.close(parent)
                parent = child
            descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        finally:
            os.close(parent)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError("Source entries must be private regular files")
            with os.fdopen(descriptor, "rb") as stream:
                descriptor = -1
                yield cast(BinaryIO, stream)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
