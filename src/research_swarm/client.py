"""Small synchronous and asynchronous clients for the shared research contract."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Protocol
from urllib.parse import quote, urlsplit

import httpx
from pydantic import TypeAdapter

from research_swarm.control.commands import RegisterProject
from research_swarm.control.dispatch import capabilities, dispatch_command
from research_swarm.control.queries import RecordKind, RecordQuery, ResearchQueries
from research_swarm.domain.artifacts import ArtifactRecord
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import ID, VersionedRecord, new_id
from research_swarm.projects.capture import capture_project
from research_swarm.projects.native_files import SourceReader
from research_swarm.runtime import LocalRuntime
from research_swarm.state.artifacts import BeginUpload
from research_swarm.state.database import JsonObject

PART_BYTES = 4 * 1024 * 1024
MAX_RESPONSE_BYTES = 32 * 1024 * 1024


class ClientTransport(Protocol):
    def command(
        self, operation: str, payload: JsonObject, key: str, revision: int | None
    ) -> JsonObject: ...
    def query(self, operation: str, payload: JsonObject) -> JsonObject: ...
    def begin_upload(self, request: BeginUpload, key: str) -> JsonObject: ...
    def upload_status(self, identifier: str) -> JsonObject: ...
    def append_upload(self, identifier: str, offset: int, content: bytes) -> None: ...
    def finish_upload(self, identifier: str, key: str) -> ArtifactRef: ...
    def download(self, identifier: str, destination: Path) -> ArtifactRef: ...
    def close(self) -> None: ...


class LocalTransport:
    def __init__(
        self, runtime: LocalRuntime, principal: Callable[[], Principal] | None = None
    ) -> None:
        self.runtime = runtime
        self.resolve_principal = principal or runtime.principal
        self.queries = ResearchQueries(runtime.database)

    def _context(self, key: str | None = None, revision: int | None = None) -> CommandContext:
        return CommandContext(
            principal=self.resolve_principal(),
            idempotency_key=new_id() if key is None else key,
            expected_revision=revision,
        )

    def command(
        self, operation: str, payload: JsonObject, key: str, revision: int | None
    ) -> JsonObject:
        return dispatch_command(
            self.runtime.service, self._context(key, revision), operation, payload
        )

    def query(self, operation: str, payload: JsonObject) -> JsonObject:
        context = self._context()
        context.principal.require("read")
        if operation == "capabilities":
            return capabilities()
        if operation == "get":
            return self.queries.get(
                context,
                RecordQuery(kind=payload["kind"]).kind,
                TypeAdapter(ID).validate_python(payload["id"]),
            )
        if operation == "list":
            return self.queries.list(context, RecordQuery.model_validate(payload))
        if operation == "context":
            return self.queries.context(
                context,
                TypeAdapter(ID).validate_python(payload["campaign_id"]),
                limit=payload.get("limit", 10),
            )
        if operation == "budget":
            return self.queries.budget(
                context, TypeAdapter(ID).validate_python(payload["campaign_id"])
            )
        if operation == "events":
            after = payload.get("after", 0)
            events = self.runtime.database.events(
                context, after=after, limit=payload.get("limit", 100)
            )
            return {
                "events": [event.model_dump(mode="json") for event in events],
                "cursor": events[-1].sequence if events else after,
            }
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Unknown research query")

    def begin_upload(self, request: BeginUpload, key: str) -> JsonObject:
        return dict(self.runtime.artifacts.begin_upload(self._context(key), request)["result"])

    def upload_status(self, identifier: str) -> JsonObject:
        return self.runtime.artifacts.upload_status(self._context(), identifier)

    def append_upload(self, identifier: str, offset: int, content: bytes) -> None:
        self.runtime.artifacts.upload_part(self._context(), identifier, offset, content)

    def finish_upload(self, identifier: str, key: str) -> ArtifactRef:
        row = self.runtime.artifacts.finalize_upload(self._context(key), identifier)["result"]
        return ArtifactRecord.model_validate(row["data"]).reference

    def download(self, identifier: str, destination: Path) -> ArtifactRef:
        context = self._context()
        reference = self.runtime.artifacts.get(context, identifier).reference
        self.runtime.artifacts.fetch(context, reference, destination)
        return reference

    def close(self) -> None:
        pass


class HTTPTransport:
    def __init__(self, url: str, token: str, *, timeout: float = 30) -> None:
        parsed = urlsplit(url)
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "Controller URL must be HTTP(S), without credentials, query, or fragment"
            )
        self.http = httpx.Client(
            base_url=url.rstrip("/"),
            headers={"Authorization": "Bearer " + token},
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
        )

    @staticmethod
    def _error(status: int, payload: JsonObject) -> None:
        if 200 <= status < 300:
            return
        error = payload.get("error", {})
        try:
            code = ErrorCode(error.get("code", "RESOURCE_UNAVAILABLE"))
        except ValueError:
            code = ErrorCode.RESOURCE_UNAVAILABLE
        raise ResearchError(
            code,
            error.get("message", "Controller request failed"),
            retryable=error.get("retryable", False),
            details=error.get("details", {"http_status": status}),
        )

    def _request(self, method: str, path: str, **kwargs: Any) -> JsonObject:
        with self.http.stream(method, path, **kwargs) as response:
            content = bytearray()
            for block in response.iter_bytes(64 * 1024):
                if len(content) + len(block) > MAX_RESPONSE_BYTES:
                    raise ResearchError(
                        ErrorCode.CAPABILITY_UNAVAILABLE, "Controller response exceeds its bound"
                    )
                content.extend(block)
            try:
                payload = strict_json_loads(bytes(content))
                if not isinstance(payload, dict):
                    raise ValueError("Expected object response")
            except ValueError as error:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "Controller returned an invalid response"
                ) from error
            self._error(response.status_code, payload)
            return dict(payload)

    @staticmethod
    def _headers(key: str, revision: int | None = None) -> dict[str, str]:
        headers = {"Idempotency-Key": key}
        if revision is not None:
            headers["X-Expected-Revision"] = str(revision)
        return headers

    def command(
        self, operation: str, payload: JsonObject, key: str, revision: int | None
    ) -> JsonObject:
        return self._request(
            "POST",
            "/v1/commands/" + quote(operation, safe=""),
            json=payload,
            headers=self._headers(key, revision),
        )

    def query(self, operation: str, payload: JsonObject) -> JsonObject:
        if operation == "capabilities":
            return self._request("GET", "/v1/capabilities")
        if operation == "get":
            kind = RecordQuery(kind=payload["kind"]).kind
            identifier = TypeAdapter(ID).validate_python(payload["id"])
            return self._request("GET", f"/v1/records/{kind}/{identifier}")
        if operation == "list":
            return self._request("POST", "/v1/query", json=payload)
        if operation in {"context", "budget"}:
            identifier = TypeAdapter(ID).validate_python(payload["campaign_id"])
            params = {"limit": payload.get("limit", 10)} if operation == "context" else {}
            return self._request("GET", f"/v1/campaigns/{identifier}/{operation}", params=params)
        if operation == "events":
            return self._request("GET", "/v1/events", params=payload)
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Unknown research query")

    def begin_upload(self, request: BeginUpload, key: str) -> JsonObject:
        return dict(
            self._request(
                "POST",
                "/v1/artifact-uploads",
                json=request.model_dump(mode="json"),
                headers=self._headers(key),
            )["result"]
        )

    def upload_status(self, identifier: str) -> JsonObject:
        return self._request(
            "GET", f"/v1/artifact-uploads/{TypeAdapter(ID).validate_python(identifier)}"
        )

    def append_upload(self, identifier: str, offset: int, content: bytes) -> None:
        self._request(
            "PUT",
            f"/v1/artifact-uploads/{TypeAdapter(ID).validate_python(identifier)}/parts",
            params={"offset": offset},
            content=content,
        )

    def finish_upload(self, identifier: str, key: str) -> ArtifactRef:
        row = self._request(
            "POST",
            f"/v1/artifact-uploads/{TypeAdapter(ID).validate_python(identifier)}/finalize",
            headers=self._headers(key),
        )["result"]
        return ArtifactRecord.model_validate(row["data"]).reference

    def download(self, identifier: str, destination: Path) -> ArtifactRef:
        identifier = TypeAdapter(ID).validate_python(identifier)
        record = ArtifactRecord.model_validate(self._request("GET", f"/v1/artifacts/{identifier}"))
        digest = hashlib.sha256()
        size = 0
        created = False
        try:
            with self.http.stream("GET", f"/v1/artifacts/{identifier}/content") as response:
                if response.status_code != 200:
                    raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Artifact download failed")
                with destination.open("xb") as output:
                    created = True
                    for block in response.iter_bytes(64 * 1024):
                        size += len(block)
                        if size > record.reference.size_bytes:
                            raise ResearchError(
                                ErrorCode.ARTIFACT_UNVERIFIED, "Artifact exceeds its declared size"
                            )
                        digest.update(block)
                        output.write(block)
            if (
                size != record.reference.size_bytes
                or "sha256:" + digest.hexdigest() != record.reference.digest
            ):
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Artifact download digest mismatch"
                )
        except BaseException:
            if created:
                destination.unlink(missing_ok=True)
            raise
        return record.reference

    def close(self) -> None:
        self.http.close()


class ResearchClient:
    def __init__(self, transport: ClientTransport) -> None:
        self.transport = transport

    @classmethod
    def local(
        cls, state_dir: Path | None = None, *, token_path: Path | None = None
    ) -> ResearchClient:
        runtime = LocalRuntime.open(state_dir)
        return cls(LocalTransport(runtime, lambda: runtime.principal(token_path)))

    @classmethod
    def connect(cls, url: str, token: str) -> ResearchClient:
        return cls(HTTPTransport(url, token))

    def command(
        self,
        operation: str,
        request: VersionedRecord | JsonObject,
        *,
        idempotency_key: str,
        expected_revision: int | None = None,
    ) -> JsonObject:
        payload = (
            request.model_dump(mode="json") if isinstance(request, VersionedRecord) else request
        )
        return self.transport.command(operation, payload, idempotency_key, expected_revision)

    def capabilities(self) -> JsonObject:
        return self.transport.query("capabilities", {})

    def get(self, kind: RecordKind, identifier: str) -> JsonObject:
        return self.transport.query("get", {"kind": kind, "id": identifier})

    def list(self, request: RecordQuery) -> JsonObject:
        return self.transport.query("list", request.model_dump(mode="json"))

    def context(self, campaign_id: str, *, limit: int = 10) -> JsonObject:
        return self.transport.query("context", {"campaign_id": campaign_id, "limit": limit})

    def budget(self, campaign_id: str) -> JsonObject:
        return self.transport.query("budget", {"campaign_id": campaign_id})

    def events(self, *, after: int = 0, limit: int = 100) -> JsonObject:
        return self.transport.query("events", {"after": after, "limit": limit})

    def upload_file(
        self, path: Path, *, idempotency_key: str, media_type: str = "application/octet-stream"
    ) -> ArtifactRef:
        digest = hashlib.sha256()
        size = 0
        with SourceReader(path.parent.resolve()) as reader, reader.open(path.name) as source:
            while block := source.read(PART_BYTES):
                digest.update(block)
                size += len(block)
            request = BeginUpload(
                digest="sha256:" + digest.hexdigest(), size_bytes=size, media_type=media_type
            )
            key = record_digest("research.client-transfer", {"key": idempotency_key}).removeprefix(
                "sha256:"
            )
            upload = self.transport.begin_upload(request, "begin:" + key)
            status = self.transport.upload_status(upload["id"])
            if status["state"] == "pending" and not status["transfer"]["published"]:
                offset = status["transfer"]["received_bytes"]
                source.seek(offset)
                while block := source.read(PART_BYTES):
                    try:
                        self.transport.append_upload(upload["id"], offset, block)
                    except ResearchError as error:
                        if error.code not in {ErrorCode.NOT_FOUND, ErrorCode.INVALID_TRANSITION}:
                            raise
                        current = self.transport.upload_status(upload["id"])
                        if current["state"] not in {"finalized", "cleanup"} and not (
                            current["state"] == "pending" and current["transfer"]["published"]
                        ):
                            raise
                        break
                    offset += len(block)
        return self.transport.finish_upload(upload["id"], "finish:" + key)

    def capture_project(
        self,
        root: Path,
        *,
        idempotency_key: str,
        manifest_path: str = "research.toml",
        revision: str | None = None,
    ) -> ArtifactRef:
        with TemporaryDirectory(prefix="research-capture-") as temporary:
            bundle = Path(temporary) / "source.zip"
            capture_project(root, bundle, manifest_path=manifest_path, revision=revision)
            return self.upload_file(
                bundle, idempotency_key=idempotency_key, media_type="application/zip"
            )

    def register_project(
        self,
        root: Path,
        *,
        idempotency_key: str,
        manifest_path: str = "research.toml",
        revision: str | None = None,
    ) -> JsonObject:
        reference = self.capture_project(
            root, idempotency_key=idempotency_key, manifest_path=manifest_path, revision=revision
        )
        return self.command(
            "project.register",
            RegisterProject(source_artifact=reference, manifest_path=manifest_path),
            idempotency_key=idempotency_key,
        )

    def download(self, identifier: str, destination: Path) -> ArtifactRef:
        return self.transport.download(identifier, destination)

    def close(self) -> None:
        self.transport.close()

    def __enter__(self) -> ResearchClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class AsyncResearchClient:
    """Async I/O convenience uses the same synchronous, replay-safe transport contract."""

    def __init__(self, client: ResearchClient) -> None:
        self.client = client

    @classmethod
    def local(
        cls, state_dir: Path | None = None, *, token_path: Path | None = None
    ) -> AsyncResearchClient:
        return cls(ResearchClient.local(state_dir, token_path=token_path))

    @classmethod
    def connect(cls, url: str, token: str) -> AsyncResearchClient:
        return cls(ResearchClient.connect(url, token))

    async def capabilities(self) -> JsonObject:
        return await asyncio.to_thread(self.client.capabilities)

    async def command(
        self,
        operation: str,
        request: VersionedRecord | JsonObject,
        *,
        idempotency_key: str,
        expected_revision: int | None = None,
    ) -> JsonObject:
        return await asyncio.to_thread(
            self.client.command,
            operation,
            request,
            idempotency_key=idempotency_key,
            expected_revision=expected_revision,
        )

    async def context(self, campaign_id: str, *, limit: int = 10) -> JsonObject:
        return await asyncio.to_thread(self.client.context, campaign_id, limit=limit)

    async def get(self, kind: RecordKind, identifier: str) -> JsonObject:
        return await asyncio.to_thread(self.client.get, kind, identifier)

    async def list(self, request: RecordQuery) -> JsonObject:
        return await asyncio.to_thread(self.client.list, request)

    async def budget(self, campaign_id: str) -> JsonObject:
        return await asyncio.to_thread(self.client.budget, campaign_id)

    async def events(self, *, after: int = 0, limit: int = 100) -> JsonObject:
        return await asyncio.to_thread(self.client.events, after=after, limit=limit)

    async def upload_file(
        self, path: Path, *, idempotency_key: str, media_type: str = "application/octet-stream"
    ) -> ArtifactRef:
        return await asyncio.to_thread(
            self.client.upload_file, path, idempotency_key=idempotency_key, media_type=media_type
        )

    async def capture_project(
        self,
        root: Path,
        *,
        idempotency_key: str,
        manifest_path: str = "research.toml",
        revision: str | None = None,
    ) -> ArtifactRef:
        return await asyncio.to_thread(
            self.client.capture_project,
            root,
            idempotency_key=idempotency_key,
            manifest_path=manifest_path,
            revision=revision,
        )

    async def register_project(
        self,
        root: Path,
        *,
        idempotency_key: str,
        manifest_path: str = "research.toml",
        revision: str | None = None,
    ) -> JsonObject:
        return await asyncio.to_thread(
            self.client.register_project,
            root,
            idempotency_key=idempotency_key,
            manifest_path=manifest_path,
            revision=revision,
        )

    async def download(self, identifier: str, destination: Path) -> ArtifactRef:
        return await asyncio.to_thread(self.client.download, identifier, destination)

    async def close(self) -> None:
        await asyncio.to_thread(self.client.close)

    async def __aenter__(self) -> AsyncResearchClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
