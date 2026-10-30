"""Local durable state and private operator identity, without network provisioning."""

from __future__ import annotations

import secrets
import sys
from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from filelock import FileLock
from pydantic import Field

from research_swarm.control.auth import CredentialStore
from research_swarm.control.authority_clock import NativeAuthorityTimeProvider
from research_swarm.control.service import ResearchService
from research_swarm.domain.assignments import signing_key_id
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import (
    ID,
    ByteCount,
    Digest,
    Timestamp,
    VersionedRecord,
    new_id,
    utc_now,
)
from research_swarm.state.artifacts import ArtifactService
from research_swarm.state.database import Database
from research_swarm.state.private_files import (
    private_state_directory as private_state_directory,
)
from research_swarm.state.private_files import (
    read_private as read_private,
)
from research_swarm.state.private_files import (
    write_private as write_private,
)
from research_swarm.storage.filesystem import FilesystemBlobStore


class RuntimeIdentity(VersionedRecord):
    workspace_id: ID
    controller_id: ID
    operator_id: ID
    storage_limit_bytes: ByteCount = Field(default=10 * 1024**3, gt=0)
    initialized_at: Timestamp | None = None
    assignment_key_id: Digest | None = None


def default_state_directory() -> Path:
    return Path.home() / ".local" / "state" / "autoresearch-swarm"


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
                    "assignment.key",
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
            signing_path = root / "assignment.key"
            if identity.assignment_key_id is not None and not signing_path.is_file():
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED,
                    "Assignment key is missing. Explicit recovery is required",
                )
            if signing_path.exists():
                signing_key = read_private(root, signing_path.name, limit=32)
                if len(signing_key) != 32:
                    raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Invalid assignment key")
                key_id = signing_key_id(
                    Ed25519PrivateKey.from_private_bytes(signing_key)
                    .public_key()
                    .public_bytes_raw()
                    .hex()
                )
                if identity.assignment_key_id not in (None, key_id):
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED,
                        "Assignment key differs from pinned identity",
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
            if identity.assignment_key_id is None:
                with database.connect() as connection:
                    signed_workers = connection.execute(
                        "SELECT COUNT(*) FROM records WHERE kind='worker' "
                        "AND json_type(payload,'$.assignment_authority')='object'"
                    ).one()[0]
                if signed_workers:
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED,
                        "Assignment identity is absent from signed state. "
                        "Explicit recovery is required",
                    )
                if not signing_path.exists():
                    write_private(signing_path, secrets.token_bytes(32))
                signing_key = read_private(root, signing_path.name, limit=32)
                key_id = signing_key_id(
                    Ed25519PrivateKey.from_private_bytes(signing_key)
                    .public_key()
                    .public_bytes_raw()
                    .hex()
                )
                identity = identity.model_copy(update={"assignment_key_id": key_id})
                write_private(identity_path, canonical_bytes(identity.model_dump(mode="json")))
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
                    database,
                    artifacts,
                    dispatch_key=read_private(root, "dispatch.key", limit=32),
                    assignment_key=signing_key,
                    authority_time_provider=NativeAuthorityTimeProvider(
                        database.authority_identity
                    ),
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
