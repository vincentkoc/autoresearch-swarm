"""Disposable input bytes; assignment authority is checked outside this catalog."""

from __future__ import annotations

import hashlib
import os
import re
import sys
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Literal

from filelock import FileLock
from pydantic import Field, model_validator

from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import ID, VersionedRecord
from research_swarm.projects.native_files import SourceReader
from research_swarm.state.private_files import (
    private_state_directory,
    read_private,
    replace_private,
    sync_directory,
)

INDEX_LIMIT = 2 * 1024 * 1024
Authorize = Callable[[], ArtifactRef]
Fetch = Callable[[Path], ArtifactRef]


class InputCacheLimits(VersionedRecord):
    """Installation policy, independent of signed experiment/environment identities."""

    max_bytes: int = Field(default=1024**3, strict=True, ge=0, le=64 * 1024**3)
    max_entry_bytes: int = Field(default=256 * 1024**2, strict=True, ge=0, le=64 * 1024**3)
    max_entries: int = Field(default=128, strict=True, ge=0, le=1024)


class CacheEntry(VersionedRecord):
    workspace_id: ID
    reference: ArtifactRef

    @property
    def key(self) -> str:
        return record_digest("research.input-cache.v1", self.model_dump(mode="json"))[7:]


class CacheIndex(VersionedRecord):
    # Ordered least recently used first. One native lock owns this entire index.
    entries: tuple[CacheEntry, ...] = Field(default=(), max_length=1024)

    @model_validator(mode="after")
    def unique(self) -> CacheIndex:
        if len({entry.key for entry in self.entries}) != len(self.entries):
            raise ValueError("Repeated input cache identity")
        return self


@dataclass(frozen=True)
class MaterializedInput:
    origin: Literal["download", "cache", "attempt"]
    avoided_transfer_bytes: int
    cache_unavailable: bool = False


@dataclass(frozen=True)
class CacheSnapshot:
    references: tuple[ArtifactRef, ...]
    examined_bytes: int
    truncated: bool
    cache_unavailable: bool = False


class _CacheUnavailable(Exception):
    pass


@contextmanager
def _cache_io() -> Iterator[None]:
    """Only disposable cache operations may enter this failure boundary."""
    try:
        yield
    except ResearchError as error:
        if error.code == ErrorCode.ARTIFACT_UNVERIFIED:
            raise
        raise _CacheUnavailable from error
    except (OSError, ValueError) as error:
        raise _CacheUnavailable from error


def _different() -> ResearchError:
    return ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Worker input bytes or identity differ")


def _authorize(reference: ArtifactRef, authorize: Authorize) -> None:
    if authorize() != reference:
        raise _different()


@contextmanager
def _create(path: Path) -> Iterator[BinaryIO]:
    if sys.platform == "win32":
        from research_swarm.state.windows_privacy import create_private_descriptor

        descriptor = create_private_descriptor(path)
    else:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            yield stream
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def verify_input(source: Path, reference: ArtifactRef, output: BinaryIO | None = None) -> None:
    digest, size = hashlib.sha256(), 0
    with SourceReader(source.parent) as reader, reader.open(source.name) as stream:
        while chunk := stream.read(1024 * 1024):
            size += len(chunk)
            if size > reference.size_bytes:
                raise _different()
            digest.update(chunk)
            if output is not None:
                output.write(chunk)
    if size != reference.size_bytes or "sha256:" + digest.hexdigest() != reference.digest:
        raise _different()


def _copy(source: Path, destination: Path, reference: ArtifactRef) -> None:
    with _create(destination) as output:
        verify_input(source, reference, output)


class InputCache:
    def __init__(self, root: Path, workspace_id: str, limits: InputCacheLimits) -> None:
        self.root, self.workspace_id = root.absolute(), workspace_id
        self.limits = InputCacheLimits.model_validate(limits)

    def _eligible(self, reference: ArtifactRef) -> bool:
        return (
            self.limits.max_entries > 0
            and self.limits.max_bytes > 0
            and reference.size_bytes <= min(self.limits.max_entry_bytes, self.limits.max_bytes)
        )

    def _path(self, entry: CacheEntry) -> Path:
        return self.root / (entry.key + ".blob")

    def _save(self, entries: list[CacheEntry]) -> None:
        content = canonical_bytes(CacheIndex(entries=tuple(entries)).model_dump(mode="json"))
        if len(content) > INDEX_LIMIT:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Input cache index is too large"
            )
        temporary = self.root / "index.pending"
        with _create(temporary) as output:
            output.write(content)
        replace_private(temporary, self.root / "index.json")

    def _unlink(self, path: Path) -> None:
        # Refuse links and nonregular entries even when disposing of cache data.
        with SourceReader(self.root) as reader, reader.open(path.name):
            pass
        path.unlink()

    def _load(self) -> list[CacheEntry]:
        try:
            try:
                payload = strict_json_loads(
                    read_private(self.root, "index.json", limit=INDEX_LIMIT)
                )
            except RecursionError as error:
                raise ValueError("Input cache index nesting exceeds its limit") from error
            index = CacheIndex.model_validate(payload)
        except FileNotFoundError:
            index = CacheIndex()
        known = {entry.key + ".blob" for entry in index.entries}
        with os.scandir(self.root) as directory:
            for count, item in enumerate(directory, 1):
                if count > 1030:
                    raise ResearchError(
                        ErrorCode.PROJECT_CONTRACT_INVALID, "Input cache is oversized"
                    )
                if item.name in {"cache.lock", "index.json"}:
                    continue
                if item.name in {"entry.pending", "index.pending"} or (
                    re.fullmatch(r"[0-9a-f]{64}\.blob", item.name) and item.name not in known
                ):
                    self._unlink(self.root / item.name)
                elif item.name not in known:
                    raise ResearchError(
                        ErrorCode.PROJECT_CONTRACT_INVALID, "Unexpected input cache entry"
                    )
        entries = []
        for entry in index.entries:
            path = self._path(entry)
            try:
                with SourceReader(self.root) as reader, reader.open(path.name) as stream:
                    size = os.fstat(stream.fileno()).st_size
            except FileNotFoundError:
                continue
            if size != entry.reference.size_bytes or not self._eligible(entry.reference):
                self._unlink(path)
                continue
            entries.append(entry)
        self._make_room(entries, 0, 0)
        self._save(entries)
        return entries

    def _make_room(self, entries: list[CacheEntry], byte_count: int, entry_count: int) -> None:
        total = sum(entry.reference.size_bytes for entry in entries)
        while entries and (
            total + byte_count > self.limits.max_bytes
            or len(entries) + entry_count > self.limits.max_entries
        ):
            old = entries.pop(0)
            self._unlink(self._path(old))
            total -= old.reference.size_bytes

    @contextmanager
    def _locked(self) -> Iterator[list[CacheEntry]]:
        with ExitStack() as stack:
            with _cache_io():
                stack.enter_context(private_state_directory(self.root))
                stack.enter_context(FileLock(self.root / "cache.lock", timeout=10, mode=0o600))
                entries = self._load()
            yield entries

    def _publish(self, entries: list[CacheEntry], entry: CacheEntry, source: Path) -> None:
        if entry in entries:
            self._unlink(self._path(entry))
            entries.remove(entry)
        # Reserve payload space before creating the one staging copy.
        self._make_room(entries, entry.reference.size_bytes, 1)
        self._save(entries)
        temporary = self.root / "entry.pending"
        _copy(source, temporary, entry.reference)
        replace_private(temporary, self._path(entry))
        entries.append(entry)
        self._save(entries)
        sync_directory(self.root)

    def verified_snapshot(
        self, *, byte_limit: int = 256 * 1024**2, entry_limit: int = 128
    ) -> CacheSnapshot:
        """Bounded worker observations for advisory scheduling, never input authority.

        Rehash recent resident bytes; omit oversized entries rather than claiming
        their residency from an index. Call outside heartbeat/control executors.
        """
        if type(byte_limit) is not int or not 0 <= byte_limit <= 64 * 1024**3:
            raise ValueError("Invalid cache observation byte bound")
        if type(entry_limit) is not int or not 0 <= entry_limit <= 1024:
            raise ValueError("Invalid cache observation entry bound")
        if not self.root.exists() or not self.limits.max_bytes or not self.limits.max_entries:
            return CacheSnapshot((), 0, False)
        observed: list[ArtifactRef] = []
        examined, checked, truncated = 0, 0, False
        try:
            with self._locked() as entries, _cache_io():
                for entry in reversed(tuple(entries)):
                    if entry.workspace_id != self.workspace_id:
                        continue
                    if checked >= entry_limit or examined + entry.reference.size_bytes > byte_limit:
                        truncated = True
                        continue
                    examined += entry.reference.size_bytes
                    checked += 1
                    try:
                        verify_input(self._path(entry), entry.reference)
                    except ResearchError as error:
                        if error.code != ErrorCode.ARTIFACT_UNVERIFIED:
                            raise
                        self._unlink(self._path(entry))
                        entries.remove(entry)
                    else:
                        observed.append(entry.reference)
                self._save(entries)
        except _CacheUnavailable:
            return CacheSnapshot((), examined, True, True)
        return CacheSnapshot(tuple(observed), examined, truncated)

    def materialize(
        self,
        reference: ArtifactRef,
        destination: Path,
        *,
        authorize: Authorize,
        fetch: Fetch,
    ) -> MaterializedInput:
        _authorize(reference, authorize)
        if destination.exists():
            verify_input(destination, reference)
            _authorize(reference, authorize)
            return MaterializedInput("attempt", reference.size_bytes)
        entry = CacheEntry(workspace_id=self.workspace_id, reference=reference)
        unavailable, copied = False, False
        if self._eligible(reference):
            try:
                with self._locked() as entries:
                    _authorize(reference, authorize)
                    if entry in entries:
                        try:
                            with _cache_io():
                                _copy(self._path(entry), destination, reference)
                        except ResearchError as error:
                            if error.code != ErrorCode.ARTIFACT_UNVERIFIED:
                                raise
                            with _cache_io():
                                self._unlink(self._path(entry))
                                entries.remove(entry)
                                self._save(entries)
                        else:
                            copied = True
                            _authorize(reference, authorize)
                            with _cache_io():
                                entries.remove(entry)
                                entries.append(entry)
                                self._save(entries)
            except _CacheUnavailable:
                unavailable = True
            except BaseException:
                if copied:
                    destination.unlink(missing_ok=True)
                raise
            if copied:
                try:
                    _authorize(reference, authorize)
                except BaseException:
                    destination.unlink(missing_ok=True)
                    raise
                return MaterializedInput("cache", reference.size_bytes, unavailable)
        # Content transfer is outside the cache lock. Concurrent cold misses may
        # transfer twice; each holds an independent, authorized attempt copy.
        observed = fetch(destination)
        try:
            if observed != reference:
                raise _different()
            verify_input(destination, reference)
            _authorize(reference, authorize)
            if self._eligible(reference) and not unavailable:
                try:
                    with self._locked() as entries:
                        _authorize(reference, authorize)
                        with _cache_io():
                            self._publish(entries, entry, destination)
                except _CacheUnavailable:
                    unavailable = True
                    verify_input(destination, reference)
            _authorize(reference, authorize)
        except BaseException:
            destination.unlink(missing_ok=True)
            raise
        return MaterializedInput("download", 0, unavailable)
