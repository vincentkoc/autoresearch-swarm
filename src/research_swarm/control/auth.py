"""Opaque credentials resolve to server-owned actor and workspace authority."""

from __future__ import annotations

import hashlib
import re
import secrets
from dataclasses import dataclass, field

from pydantic import TypeAdapter

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import ID, Timestamp, new_id, utc_now
from research_swarm.state.database import Database, encode
from research_swarm.state.schema_contracts import require_credentials_schema
from research_swarm.state.sql import SqlSession
from research_swarm.state.workspace_gate import require_workspace_mutation


@dataclass(frozen=True)
class IssuedCredential:
    id: str
    token: str = field(repr=False)
    principal: Principal


class CredentialStore:
    def __init__(self, database: Database) -> None:
        self.database = database

        database.store.write(self.initialize_in)

    @staticmethod
    def initialize_in(connection: SqlSession) -> None:
        """Initialize credential storage inside the caller's authorized write transaction."""
        connection.execute("""CREATE TABLE IF NOT EXISTS credentials (
            id TEXT PRIMARY KEY, token_digest TEXT NOT NULL UNIQUE,
            workspace_id TEXT NOT NULL REFERENCES workspaces(id), principal TEXT NOT NULL,
            created_at TEXT NOT NULL, expires_at TEXT, revoked INTEGER NOT NULL DEFAULT 0
        )""")

    @classmethod
    def open_existing(cls, database: Database) -> CredentialStore:
        """Open provisioned credentials without creating, replacing or repairing state."""
        database.store.read(require_credentials_schema)
        instance = cls.__new__(cls)
        instance.database = database
        return instance

    def issue(
        self, issuer: Principal, principal: Principal, *, expires_at: str | None = None
    ) -> IssuedCredential:
        principal = self._authorize(issuer, principal)
        if expires_at is not None:
            expires_at = TypeAdapter(Timestamp).validate_python(expires_at)
            if expires_at <= utc_now():
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Credential expiry is in the past"
                )
        token = "rsw_" + secrets.token_urlsafe(32)
        identifier = new_id()

        def persist(connection: SqlSession) -> None:
            require_workspace_mutation(connection, principal.workspace_id, "credential.issue")
            connection.execute(
                "INSERT INTO credentials"
                "(id,token_digest,workspace_id,principal,created_at,expires_at) "
                "VALUES (?,?,?,?,?,?)",
                (
                    identifier,
                    self._digest(token),
                    principal.workspace_id,
                    encode(principal.model_dump(mode="json")),
                    utc_now(),
                    expires_at,
                ),
            )

        self.database.store.write(persist)
        return IssuedCredential(identifier, token, principal)

    @staticmethod
    def _authorize(issuer: Principal, principal: Principal) -> Principal:
        issuer.require("auth.manage")
        principal = Principal.model_validate(principal)
        if issuer.workspace_id != principal.workspace_id:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Credential workspace differs")
        if "*" not in issuer.scopes and not principal.scopes <= issuer.scopes:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Credential exceeds issuer scopes")
        return principal

    def install(
        self, issuer: Principal, principal: Principal, *, identifier: str, token: str
    ) -> IssuedCredential:
        """Resume local provisioning from a secret persisted before its first delivery.

        This is not credential rotation. Conflicting, revoked, or expiring credentials
        require an explicit operator action and are never replaced here.
        """
        self._authorize(issuer, principal)
        return self.database.store.write(
            lambda connection: self.install_in(
                connection, issuer, principal, identifier=identifier, token=token
            )
        )

    @classmethod
    def install_in(
        cls,
        connection: SqlSession,
        issuer: Principal,
        principal: Principal,
        *,
        identifier: str,
        token: str,
    ) -> IssuedCredential:
        """Install one credential without committing or opening another transaction."""
        principal = cls._authorize(issuer, principal)
        TypeAdapter(ID).validate_python(identifier)
        if re.fullmatch(r"rsw_[A-Za-z0-9_-]{43}", token) is None:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Invalid enrollment credential")
        digest = cls._digest(token)
        rows = connection.execute(
            "SELECT * FROM credentials WHERE id=? OR token_digest=?", (identifier, digest)
        ).fetchall()
        if rows:
            row = rows[0]
            if (
                len(rows) != 1
                or row["id"] != identifier
                or row["token_digest"] != digest
                or row["workspace_id"] != principal.workspace_id
                or Principal.model_validate_json(row["principal"]) != principal
                or row["revoked"]
                or row["expires_at"] is not None
            ):
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED,
                    "Enrollment credential differs or is no longer active",
                )
        else:
            require_workspace_mutation(connection, principal.workspace_id, "credential.install")
            connection.execute(
                "INSERT INTO credentials"
                "(id,token_digest,workspace_id,principal,created_at) VALUES (?,?,?,?,?)",
                (
                    identifier,
                    digest,
                    principal.workspace_id,
                    encode(principal.model_dump(mode="json")),
                    utc_now(),
                ),
            )
        return IssuedCredential(identifier, token, principal)

    @staticmethod
    def _digest(token: str) -> str:
        return hashlib.sha256(b"research.credential-v1\x00" + token.encode("utf-8")).hexdigest()

    def resolve(self, token: str, *, allow_expiring: bool = True) -> Principal:
        """Resolve current credentials; runtimes without qualified UTC can refuse expiry."""
        if not token.startswith("rsw_") or len(token) > 256:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Invalid credential")
        row = self.database.store.read(
            lambda connection: connection.execute(
                "SELECT principal,expires_at,revoked FROM credentials WHERE token_digest=?",
                (self._digest(token),),
            ).fetchone()
        )
        if row is None or row["revoked"]:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Invalid credential")
        if row["expires_at"] is not None:
            if not allow_expiring:
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Invalid credential")
            try:
                expires_at = TypeAdapter(Timestamp).validate_python(row["expires_at"])
            except ValueError as error:
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Invalid credential") from error
            if expires_at <= utc_now():
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Invalid credential")
        return Principal.model_validate_json(row["principal"])

    def revoke(self, issuer: Principal, identifier: str) -> None:
        issuer.require("auth.manage")

        def revoke(connection: SqlSession) -> None:
            require_workspace_mutation(connection, issuer.workspace_id, "credential.revoke")
            changed = connection.execute(
                "UPDATE credentials SET revoked=1 WHERE workspace_id=? AND id=? RETURNING id",
                (issuer.workspace_id, identifier),
            )
            if len(changed.fetchall()) != 1:
                raise ResearchError(ErrorCode.NOT_FOUND, "Credential not found")

        self.database.store.write(revoke)
