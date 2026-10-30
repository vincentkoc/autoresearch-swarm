"""Blocking byte operations run outside domain transactions and async event loops."""

from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path
from typing import BinaryIO, Protocol

from research_swarm.domain.artifacts import StoredBlob, UploadSpec


class BlobStore(Protocol):
    def begin(self, upload: UploadSpec) -> int: ...

    def append(self, upload: UploadSpec, offset: int, content: bytes) -> int: ...

    def finalize(self, upload: UploadSpec) -> StoredBlob: ...

    def verify(self, workspace_id: str, blob: StoredBlob) -> None: ...

    def reader(self, workspace_id: str, blob: StoredBlob) -> AbstractContextManager[BinaryIO]: ...

    def fetch(self, workspace_id: str, blob: StoredBlob, destination: Path) -> None: ...

    def discard_upload(self, upload: UploadSpec) -> None: ...

    def delete(self, workspace_id: str, blob: StoredBlob) -> None: ...
