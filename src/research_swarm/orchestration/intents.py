"""Persist command envelopes before delivery and replay uncertain acknowledgments."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.domain.canonical import canonical_bytes, content_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Revision, VersionedRecord, new_id
from research_swarm.runtime import private_state_directory, read_private, write_private
from research_swarm.state.database import JsonObject

Route = Literal["controller", "worker", "evaluator"]


class CommandIntent(VersionedRecord):
    name: str = Field(min_length=1, max_length=512)
    route: Route
    operation: str
    payload: JsonObject
    expected_revision: Revision | None
    key: ID = Field(default_factory=new_id)
    state: Literal["pending", "accepted", "rejected"] = "pending"
    error: ErrorCode | None = None


class CommandIntents:
    """Caller holds one driver lock across recovery, scheduling, and drain."""

    def __init__(self, root: Path, clients: dict[Route, ResearchClient]) -> None:
        if any(not isinstance(client.transport, LocalTransport) for client in clients.values()):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Local intents require local command authority"
            )
        self.root, self.clients = root, clients
        with private_state_directory(root):
            identity = {}
            for route, client in clients.items():
                transport = client.transport
                assert isinstance(transport, LocalTransport)
                principal = transport.resolve_principal()
                identity[route] = {
                    "workspace_id": principal.workspace_id,
                    "controller_id": transport.runtime.identity.controller_id,
                    "actor_id": principal.id,
                }
            path = root / "identity.json"
            if path.exists():
                if strict_json_loads(read_private(root, path.name, limit=65536)) != identity:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Command route authority changed")
            else:
                if any(root.glob("*.json")):
                    raise ResearchError(ErrorCode.STALE_FENCE, "Command route identity is missing")
                write_private(path, canonical_bytes(identity))

    def _path(self, name: str) -> Path:
        return self.root / (content_digest(name.encode("utf-8")).split(":")[1] + ".json")

    def _read(self, path: Path) -> CommandIntent:
        intent = CommandIntent.model_validate(
            strict_json_loads(read_private(self.root, path.name, limit=2 * 1024 * 1024))
        )
        if self._path(intent.name) != path:
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Command intent name differs")
        return intent

    def _write(self, intent: CommandIntent) -> None:
        content = canonical_bytes(intent.model_dump(mode="json"))
        if len(content) > 2 * 1024 * 1024:
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Command intent is too large")
        write_private(self._path(intent.name), content)

    def _deliver(self, intent: CommandIntent) -> JsonObject:
        try:
            reply = self.clients[intent.route].command(
                intent.operation,
                intent.payload,
                idempotency_key=intent.key,
                expected_revision=intent.expected_revision,
            )
        except ResearchError as error:
            if intent.state != "accepted" and error.code in {
                ErrorCode.RESOURCE_UNAVAILABLE,
                ErrorCode.REVISION_CONFLICT,
            }:
                self._write(intent.model_copy(update={"state": "rejected", "error": error.code}))
            raise
        self._write(intent.model_copy(update={"state": "accepted"}))
        return reply

    def recover(self) -> None:
        for path in sorted(self.root.glob("*.json")):
            if path.name == "identity.json":
                continue
            intent = self._read(path)
            if intent.state == "pending":
                try:
                    self._deliver(intent)
                except ResearchError as error:
                    if error.code not in {
                        ErrorCode.RESOURCE_UNAVAILABLE,
                        ErrorCode.REVISION_CONFLICT,
                    }:
                        raise

    def command(
        self,
        name: str,
        route: Route,
        operation: str,
        request: VersionedRecord,
        *,
        revision: int | None = None,
        retry_rejected: bool = False,
    ) -> JsonObject:
        path = self._path(name)
        intent = self._read(path) if path.exists() else None
        if intent is not None and intent.state == "rejected":
            if not retry_rejected:
                raise ResearchError(intent.error or ErrorCode.INVALID_TRANSITION, "Intent rejected")
            intent = None
        if intent is None:
            intent = CommandIntent(
                name=name,
                route=route,
                operation=operation,
                payload=request.model_dump(mode="json"),
                expected_revision=revision,
            )
            self._write(intent)
        elif (intent.route, intent.operation, intent.payload, intent.expected_revision) != (
            route,
            operation,
            request.model_dump(mode="json"),
            revision,
        ):
            raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Persisted command intent differs")
        return self._deliver(intent)
