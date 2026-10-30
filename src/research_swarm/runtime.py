"""Local durable state and private operator identity, without network provisioning."""

from __future__ import annotations

import os
import secrets
import stat
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from filelock import FileLock
from pydantic import Field

from research_swarm.control.auth import CredentialStore
from research_swarm.control.service import ResearchService
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import ID, ByteCount, Timestamp, VersionedRecord, new_id, utc_now
from research_swarm.projects.native_files import SourceReader
from research_swarm.state.artifacts import ArtifactService
from research_swarm.state.database import Database
from research_swarm.storage.filesystem import FilesystemBlobStore


class RuntimeIdentity(VersionedRecord):
    workspace_id: ID
    controller_id: ID
    operator_id: ID
    storage_limit_bytes: ByteCount = Field(default=10 * 1024**3, gt=0)
    initialized_at: Timestamp | None = None


def default_state_directory() -> Path:
    return Path.home() / ".local" / "state" / "autoresearch-swarm"


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
        if sys.platform != "win32":
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        staged.unlink(missing_ok=True)


@contextmanager
def private_state_directory(root: Path) -> Iterator[None]:
    if sys.platform == "win32":
        from research_swarm.state.windows_privacy import private_directory

        with private_directory(root):
            yield
    else:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if stat.S_IMODE(root.stat().st_mode) & 0o077:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Runtime directory requires owner-only access"
            )
        with SourceReader(root):
            yield


@dataclass(frozen=True)
class LocalRuntime:
    root: Path
    identity: RuntimeIdentity
    database: Database
    artifacts: ArtifactService
    service: ResearchService
    credentials: CredentialStore

    @classmethod
    def open(cls, root: Path | None = None) -> LocalRuntime:
        root = (root or default_state_directory()).expanduser().absolute()
        with private_state_directory(root), FileLock(root / "initialize.lock"):
            identity_path = root / "identity.json"
            if identity_path.exists():
                identity = RuntimeIdentity.model_validate(
                    strict_json_loads(read_private(root, "identity.json", limit=16_384))
                )
            else:
                existing = {
                    "research.sqlite",
                    "research.sqlite-wal",
                    "research.sqlite-shm",
                    "operator.token",
                    "dispatch.key",
                    "artifacts",
                }
                if any((root / name).exists() for name in existing):
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED,
                        "Runtime identity is missing from existing state. "
                        "Explicit recovery is required",
                    )
                identity = RuntimeIdentity(
                    workspace_id=new_id(), controller_id=new_id(), operator_id=new_id()
                )
                write_private(identity_path, canonical_bytes(identity.model_dump(mode="json")))
            if identity.initialized_at is not None and any(
                not (root / name).is_file()
                for name in ("research.sqlite", "operator.token", "dispatch.key")
            ):
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED,
                    "Initialized runtime state is missing. Explicit recovery is required",
                )
            for name, limit in (("operator.token", 256), ("dispatch.key", 32)):
                if (root / name).exists():
                    content = read_private(root, name, limit=limit)
                    if name == "dispatch.key" and len(content) != 32:
                        raise ResearchError(
                            ErrorCode.AUTHORIZATION_DENIED, "Invalid local dispatch key"
                        )
            if sys.platform == "win32":
                from research_swarm.state.windows_privacy import verified_path

                for name in ("research.sqlite", "research.sqlite-wal", "research.sqlite-shm"):
                    path = root / name
                    if path.exists():
                        try:
                            with verified_path(path, directory=False):
                                pass
                        except FileNotFoundError:
                            if name == "research.sqlite":
                                raise
            if not (root / "research.sqlite").exists():
                write_private(root / "research.sqlite", b"")
            database = Database(root / "research.sqlite")
            database.bootstrap(identity.workspace_id, identity.controller_id)
            credentials = CredentialStore(database)
            operator = Principal(
                id=identity.operator_id, workspace_id=identity.workspace_id, scopes=frozenset({"*"})
            )
            token_path = root / "operator.token"
            if not token_path.exists():
                issued = credentials.issue(operator, operator)
                write_private(token_path, issued.token.encode("ascii"))
            key_path = root / "dispatch.key"
            if not key_path.exists():
                write_private(key_path, secrets.token_bytes(32))
            if len(read_private(root, "dispatch.key", limit=32)) != 32:
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Invalid local dispatch key")
            artifacts = ArtifactService(
                database,
                FilesystemBlobStore(root / "artifacts"),
                storage_limit_bytes=identity.storage_limit_bytes,
            )
            if identity.initialized_at is None:
                identity = identity.model_copy(update={"initialized_at": utc_now()})
                write_private(identity_path, canonical_bytes(identity.model_dump(mode="json")))
            return cls(
                root,
                identity,
                database,
                artifacts,
                ResearchService(
                    database, artifacts, dispatch_key=read_private(root, "dispatch.key", limit=32)
                ),
                credentials,
            )

    def principal(self, token_path: Path | None = None) -> Principal:
        path = token_path or self.root / "operator.token"
        token = read_private(path.parent, path.name, limit=256).decode("ascii").strip()
        actor = self.credentials.resolve(token)
        if actor.workspace_id != self.identity.workspace_id:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Credential belongs to another workspace"
            )
        return actor
