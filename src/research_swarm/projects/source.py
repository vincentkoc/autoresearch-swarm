"""Portable source bundles without shared checkouts or archive extraction shortcuts."""

from __future__ import annotations

import fnmatch
import hashlib
import io
import os
import shutil
import signal
import stat
import sys
import tempfile
import time
import unicodedata
import zipfile
from collections.abc import Iterable
from contextlib import suppress
from functools import cache
from pathlib import Path, PurePosixPath
from typing import IO

from pydantic import Field, field_validator, model_validator

from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ByteCount, Digest, VersionedRecord

MANIFEST_NAME = ".research-source.json"
CHUNK_SIZE = 1024 * 1024
MAX_MANIFEST_BYTES = 4 * CHUNK_SIZE
MAX_SOURCE_BYTES = 512 * CHUNK_SIZE
MAX_SOURCE_FILES = 10_000
_WINDOWS_RESERVED = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


def portable_path(value: str) -> str:
    """Use one conservative path vocabulary on every supported operating system."""
    parts = value.split("/")
    if (
        not value
        or len(value) > 1024
        or len(parts) > 64
        or unicodedata.normalize("NFC", value) != value
        or any(
            not part
            or part in {".", ".."}
            or part.endswith((".", " "))
            or part.split(".")[0].upper() in _WINDOWS_RESERVED
            or any(ord(character) < 32 or character in '\\:*?"<>|' for character in part)
            for part in parts
        )
    ):
        raise ValueError("Source paths must be portable relative file names")
    if value == MANIFEST_NAME or any(part.casefold() == ".git" for part in parts):
        raise ValueError("Reserved source path")
    return value


def path_pattern(value: str) -> str:
    if not value or len(value) > 1024 or value.startswith("/") or "\\" in value or "\x00" in value:
        raise ValueError("Mutation patterns must be relative POSIX patterns")
    if len(value.split("/")) > 64 or any(part in {"", ".", ".."} for part in value.split("/")):
        raise ValueError("Mutation patterns must not traverse directories")
    return value


def matches(path: str, patterns: tuple[str, ...]) -> bool:
    parts = tuple(path.split("/"))

    @cache
    def match(names: tuple[str, ...], pattern: tuple[str, ...]) -> bool:
        if not pattern:
            return not names
        if pattern[0] == "**":
            return match(names, pattern[1:]) or bool(names and match(names[1:], pattern))
        return bool(
            names and fnmatch.fnmatchcase(names[0], pattern[0]) and match(names[1:], pattern[1:])
        )

    return any(match(parts, tuple(pattern.split("/"))) for pattern in patterns)


class SourceEntry(VersionedRecord):
    path: str
    digest: Digest
    size_bytes: ByteCount
    executable: bool = False

    _path = field_validator("path")(portable_path)


class SourceManifest(VersionedRecord):
    entries: tuple[SourceEntry, ...] = Field(min_length=1, max_length=MAX_SOURCE_FILES)
    tree_digest: Digest
    base_revision: str | None = None

    @model_validator(mode="after")
    def valid_tree(self) -> SourceManifest:
        paths = [entry.path for entry in self.entries]
        if paths != sorted(paths) or len({path.casefold() for path in paths}) != len(paths):
            raise ValueError("Source paths must be sorted, unique, and portable across case rules")
        files = set(paths)
        prefixes: dict[str, str] = {}
        for path in paths:
            pieces = path.split("/")
            for end in range(1, len(pieces) + 1):
                prefix = "/".join(pieces[:end])
                previous = prefixes.setdefault(prefix.casefold(), prefix)
                if previous != prefix:
                    raise ValueError("Source directory names collide across platform case rules")
            if any(
                str(parent) in files for parent in PurePosixPath(path).parents if str(parent) != "."
            ):
                raise ValueError("A source file cannot also be a directory")
        if sum(entry.size_bytes for entry in self.entries) > MAX_SOURCE_BYTES:
            raise ValueError("Source bundle exceeds its size limit")
        expected = record_digest(
            "research.source-tree",
            {"entries": [entry.model_dump(mode="json") for entry in self.entries]},
        )
        if self.tree_digest != expected:
            raise ValueError("Source tree digest mismatch")
        return self


class MutationPolicy(VersionedRecord):
    allow: tuple[str, ...] = ()
    deny: tuple[str, ...] = ()

    @field_validator("allow", "deny")
    @classmethod
    def patterns(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(path_pattern(value) for value in values)


def validate_mutation(
    base: SourceManifest,
    candidate: SourceManifest,
    policy: MutationPolicy,
    *,
    protected_paths: tuple[str, ...] = (),
) -> tuple[str, ...]:
    base = SourceManifest.model_validate(base)
    candidate = SourceManifest.model_validate(candidate)
    policy = MutationPolicy.model_validate(policy)
    for pattern in protected_paths:
        path_pattern(pattern)
    before = {entry.path: entry for entry in base.entries}
    after = {entry.path: entry for entry in candidate.entries}
    changed = tuple(
        sorted(path for path in before.keys() | after.keys() if before.get(path) != after.get(path))
    )
    forbidden = tuple(
        path
        for path in changed
        if matches(path, policy.deny + protected_paths) or not matches(path, policy.allow)
    )
    if forbidden:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID,
            "Candidate changes protected or undeclared source paths",
            details={"paths": list(forbidden)},
        )
    return changed


class _BundleWriter:
    def __init__(self, archive: zipfile.ZipFile) -> None:
        self.archive = archive
        self.entries: list[SourceEntry] = []
        self.total = 0

    def add(self, path: str, source: IO[bytes], size: int, executable: bool) -> None:
        portable_path(path)
        if (
            size < 0
            or self.total + size > MAX_SOURCE_BYTES
            or len(self.entries) >= MAX_SOURCE_FILES
        ):
            raise ValueError("Source bundle exceeds its size limit")
        if any(previous.path.casefold() == path.casefold() for previous in self.entries):
            raise ValueError("Duplicate source path")
        info = zipfile.ZipInfo(path, date_time=(1980, 1, 1, 0, 0, 0))
        info.create_system = 3
        info.external_attr = (stat.S_IFREG | (0o755 if executable else 0o644)) << 16
        digest = hashlib.sha256()
        copied = 0
        with self.archive.open(info, "w") as target:
            while block := source.read(CHUNK_SIZE):
                copied += len(block)
                if copied > size:
                    raise ValueError("Source changed while creating its bundle")
                digest.update(block)
                target.write(block)
        if copied != size:
            raise ValueError("Source changed while creating its bundle")
        self.entries.append(
            SourceEntry(
                path=path,
                digest="sha256:" + digest.hexdigest(),
                size_bytes=size,
                executable=executable,
            )
        )
        self.total += size

    def finish(self, revision: str | None) -> SourceManifest:
        entries = tuple(sorted(self.entries, key=lambda entry: entry.path))
        manifest = SourceManifest(
            entries=entries,
            base_revision=revision,
            tree_digest=record_digest(
                "research.source-tree",
                {"entries": [entry.model_dump(mode="json") for entry in entries]},
            ),
        )
        content = canonical_bytes(manifest.model_dump(mode="json"))
        if len(content) > MAX_MANIFEST_BYTES:
            raise ValueError("Source manifest exceeds its size limit")
        info = zipfile.ZipInfo(MANIFEST_NAME, date_time=(1980, 1, 1, 0, 0, 0))
        info.create_system = 3
        info.external_attr = (stat.S_IFREG | 0o644) << 16
        self.archive.writestr(info, content)
        return manifest


def _selected_files(root: Path, patterns: tuple[str, ...]) -> Iterable[tuple[str, Path]]:
    for pattern in patterns:
        path_pattern(pattern)
    for directory, children, files in os.walk(root, followlinks=False):
        children[:] = sorted(name for name in children if name != ".git")
        for name in children:
            path = Path(directory) / name
            if path.is_symlink():
                raise ValueError("Source trees must not contain directory symlinks")
        for name in sorted(files):
            path = Path(directory) / name
            relative = path.relative_to(root).as_posix()
            if matches(relative, patterns):
                yield relative, path


def bundle_directory(root: Path, destination: Path, *, paths: tuple[str, ...]) -> SourceManifest:
    """Capture explicitly selected working files; no Git revision is inferred."""
    from research_swarm.projects.native_files import SourceReader

    root = root.resolve(strict=True)
    if not root.is_dir() or destination.resolve().is_relative_to(root):
        raise ValueError("Bundle destination must be outside the source directory")
    created = False
    try:
        with destination.open("xb") as output:
            created = True
            with (
                SourceReader(root) as reader,
                zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive,
            ):
                writer = _BundleWriter(archive)
                for relative, path in sorted(_selected_files(root, paths)):
                    before = path.lstat()
                    if not stat.S_ISREG(before.st_mode):
                        raise ValueError("Source entries must be regular files")
                    with reader.open(relative) as source:
                        opened = os.fstat(source.fileno())
                        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                            raise ValueError("Source changed before it was read")
                        writer.add(relative, source, opened.st_size, bool(opened.st_mode & 0o111))
                        after = os.fstat(source.fileno())
                        if (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns) != (
                            after.st_size,
                            after.st_mtime_ns,
                            after.st_ctime_ns,
                        ):
                            raise ValueError("Source changed while it was read")
                manifest = writer.finish(None)
            output.flush()
            os.fsync(output.fileno())
        return manifest
    except BaseException:
        if created:
            destination.unlink(missing_ok=True)
        raise


def _git_read(root: Path, arguments: list[str], *, limit: int, deadline: float) -> bytes:
    import subprocess

    import psutil

    if deadline <= time.monotonic():
        raise TimeoutError("Source capture deadline expired")
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(
        {
            "GIT_NO_LAZY_FETCH": "1",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_TERMINAL_PROMPT": "0",
        }
    )
    # Regular files keep inherited output handles from blocking an EOF read forever.
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        process = subprocess.Popen(
            ["git", "-C", str(root), *arguments],
            stdout=output,
            stderr=errors,
            env=environment,
            start_new_session=os.name == "posix",
        )
        descendants: dict[int, psutil.Process] = {}
        try:
            while process.poll() is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Source capture deadline expired")
                if os.fstat(output.fileno()).st_size > limit:
                    raise ValueError("Git output exceeds its declared source bound")
                with suppress(psutil.NoSuchProcess):
                    for child in psutil.Process(process.pid).children(recursive=True):
                        descendants[child.pid] = child
                with suppress(subprocess.TimeoutExpired):
                    process.wait(timeout=min(0.02, remaining))
            if process.returncode:
                raise ValueError("Git could not read the selected committed source")
            output.seek(0)
            content = output.read(limit + 1)
            if len(content) > limit:
                raise ValueError("Git output exceeds its declared source bound")
            return content
        finally:
            # Only this invocation's processes are eligible. psutil fences PID reuse.
            for child in descendants.values():
                with suppress(psutil.NoSuchProcess):
                    child.kill()
            if process.poll() is None:
                if sys.platform != "win32":
                    with suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
                process.wait(timeout=10)


def read_git_file(
    root: Path, revision: str, path: str, *, limit: int, timeout_seconds: float = 30
) -> tuple[str, bytes]:
    """Resolve once, then read a contract file from that exact local commit."""
    portable_path(path)
    if not 1 <= limit <= MAX_SOURCE_BYTES or not 0 < timeout_seconds <= 3600:
        raise ValueError("Git contract read exceeds its bounds")
    deadline = time.monotonic() + timeout_seconds
    resolved = (
        _git_read(
            root,
            ["rev-parse", "--verify", "--end-of-options", revision + "^{commit}"],
            limit=128,
            deadline=deadline,
        )
        .decode("ascii")
        .strip()
    )
    return resolved, _git_read(
        root, ["cat-file", "blob", resolved + ":" + path], limit=limit, deadline=deadline
    )


def bundle_git(
    root: Path,
    revision: str,
    destination: Path,
    *,
    paths: tuple[str, ...],
    timeout_seconds: float = 30,
) -> SourceManifest:
    """Read literal tree/blob objects, unaffected by archive/export attributes."""
    if not 0 < timeout_seconds <= 3600:
        raise ValueError("Source capture timeout must be within (0, 3600] seconds")
    for pattern in paths:
        path_pattern(pattern)
    deadline = time.monotonic() + timeout_seconds
    resolved = (
        _git_read(
            root,
            ["rev-parse", "--verify", "--end-of-options", revision + "^{commit}"],
            limit=128,
            deadline=deadline,
        )
        .decode("ascii")
        .strip()
    )
    if len(resolved) not in {40, 64} or any(
        character not in "0123456789abcdef" for character in resolved
    ):
        raise ValueError("Git did not resolve an immutable commit")
    tree = _git_read(
        root, ["ls-tree", "-r", "-l", "-z", resolved], limit=32 * CHUNK_SIZE, deadline=deadline
    )
    selected: list[tuple[str, str, int, bool]] = []
    total = 0
    for line in tree.split(b"\0"):
        if not line:
            continue
        metadata, raw_path = line.split(b"\t", 1)
        name = raw_path.decode("utf-8")
        if not matches(name, paths):
            continue
        portable_path(name)
        mode, kind, raw_identifier, raw_size = metadata.split()
        if kind != b"blob" or mode not in {b"100644", b"100755"}:
            raise ValueError("Selected Git entries must be regular files")
        size = int(raw_size)
        total += size
        if size > 64 * CHUNK_SIZE or total > MAX_SOURCE_BYTES or len(selected) >= MAX_SOURCE_FILES:
            raise ValueError("Selected Git source exceeds its size or file-count limit")
        selected.append((name, raw_identifier.decode("ascii"), size, mode == b"100755"))
    created = False
    try:
        with destination.open("xb") as output:
            created = True
            with zipfile.ZipFile(output, "w") as archive:
                writer = _BundleWriter(archive)
                for name, identifier, size, executable in sorted(selected):
                    content = _git_read(
                        root, ["cat-file", "blob", identifier], limit=size, deadline=deadline
                    )
                    writer.add(name, io.BytesIO(content), size, executable)
                manifest = writer.finish(resolved)
            output.flush()
            os.fsync(output.fileno())
        return manifest
    except BaseException:
        if created:
            destination.unlink(missing_ok=True)
        raise


def inspect_bundle(path: Path) -> SourceManifest:
    if path.stat().st_size > MAX_SOURCE_BYTES + 2 * MAX_MANIFEST_BYTES:
        raise ValueError("Source archive exceeds its size limit")
    with zipfile.ZipFile(path) as archive:
        info = archive.getinfo(MANIFEST_NAME)
        if info.file_size > MAX_MANIFEST_BYTES or info.compress_type != zipfile.ZIP_STORED:
            raise ValueError("Invalid source manifest encoding or size")
        manifest = SourceManifest.model_validate(strict_json_loads(archive.read(info)))
        expected = {entry.path: entry for entry in manifest.entries}
        members = archive.infolist()
        if len(members) != len(expected) + 1 or len({item.filename for item in members}) != len(
            members
        ):
            raise ValueError("Source archive membership does not match the manifest")
        for item in members:
            if item.filename == MANIFEST_NAME:
                continue
            portable_path(item.filename)
            entry = expected.get(item.filename)
            mode = (item.external_attr >> 16) & 0xFFFF
            if (
                entry is None
                or item.file_size != entry.size_bytes
                or item.compress_type != zipfile.ZIP_STORED
                or item.flag_bits & 1
                or (stat.S_IFMT(mode) not in {0, stat.S_IFREG})
            ):
                raise ValueError("Source archive entry violates its contract")
        return manifest


def extract_bundle(path: Path, destination: Path, *, expected_tree_digest: str) -> SourceManifest:
    manifest = inspect_bundle(path)
    if manifest.tree_digest != expected_tree_digest:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Source tree identity changed")
    destination.mkdir(mode=0o700, parents=False, exist_ok=False)
    try:
        with zipfile.ZipFile(path) as archive:
            for entry in manifest.entries:
                target = destination.joinpath(*entry.path.split("/"))
                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                digest = hashlib.sha256()
                copied = 0
                with archive.open(entry.path) as source, target.open("xb") as output:
                    while block := source.read(CHUNK_SIZE):
                        copied += len(block)
                        if copied > entry.size_bytes:
                            raise ValueError("Source entry exceeds its declared size")
                        digest.update(block)
                        output.write(block)
                if copied != entry.size_bytes or "sha256:" + digest.hexdigest() != entry.digest:
                    raise ResearchError(
                        ErrorCode.ARTIFACT_UNVERIFIED, "Source entry digest mismatch"
                    )
                target.chmod(0o700 if entry.executable else 0o600)
        return manifest
    except BaseException:
        shutil.rmtree(destination)
        raise
