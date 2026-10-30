"""Small synchronous and asynchronous clients for the shared research contract."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import ssl
from collections.abc import Callable, Iterable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal, Protocol
from urllib.parse import quote, urlsplit

import httpx
from pydantic import TypeAdapter

from research_swarm.control.commands import RegisterProject
from research_swarm.control.dispatch import capabilities, dispatch_command
from research_swarm.control.queries import RecordKind, RecordQuery, ResearchQueries
from research_swarm.control.search_models import ProposalRead
from research_swarm.control.worker_artifacts import WorkerArtifactRead, WorkerArtifacts
from research_swarm.control.worker_outputs import (
    BeginWorkerUpload,
    WorkerOutputIntent,
    WorkerUploadTarget,
)
from research_swarm.control.worker_queries import WorkerQueries, WorkerQuery, WorkerRead
from research_swarm.domain.acknowledgments import (
    CommandAccepted,
    CommandRejected,
    RequestRejected,
    UploadPartAccepted,
)
from research_swarm.domain.artifacts import ArtifactRecord
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.clocks import ClockChallenge, SignedClockReply
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
RequestMode = Literal["query", "command", "upload_part"]


def write_download(
    reference: ArtifactRef, chunks: Iterable[bytes], destination: Path
) -> ArtifactRef:
    digest, size, created = hashlib.sha256(), 0, False
    try:
        with destination.open("xb") as output:
            created = True
            for block in chunks:
                size += len(block)
                if size > reference.size_bytes:
                    raise ResearchError(
                        ErrorCode.ARTIFACT_UNVERIFIED, "Artifact exceeds its declared size"
                    )
                digest.update(block)
                output.write(block)
        if size != reference.size_bytes or "sha256:" + digest.hexdigest() != reference.digest:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Artifact download digest mismatch")
    except BaseException:
        if created:
            destination.unlink(missing_ok=True)
        raise
    return reference


class UploadSink(Protocol):
    def begin_upload(self, request: BeginUpload, key: str) -> JsonObject: ...
    def upload_status(self, identifier: str) -> JsonObject: ...
    def append_upload(self, identifier: str, offset: int, content: bytes) -> None: ...
    def finish_upload(self, identifier: str, key: str) -> ArtifactRef: ...


class ClientTransport(UploadSink, Protocol):
    def command(
        self, operation: str, payload: JsonObject, key: str, revision: int | None
    ) -> JsonObject: ...
    def query(self, operation: str, payload: JsonObject) -> JsonObject: ...
    def download(self, identifier: str, destination: Path) -> ArtifactRef: ...
    def worker_download(self, request: WorkerArtifactRead, destination: Path) -> ArtifactRef: ...
    def worker_append(self, target: WorkerUploadTarget, offset: int, content: bytes) -> None: ...
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
        if operation == "worker.clock":
            return self.runtime.service.worker_clock(
                context, ClockChallenge.model_validate(payload)
            ).model_dump(mode="json")
        if operation == "worker.output":
            return self.runtime.service.worker_outputs.status(
                context, WorkerUploadTarget.model_validate(payload)
            )
        if operation == "worker.input":
            return (
                WorkerArtifacts(
                    self.runtime.database, self.runtime.artifacts, self.runtime.service.workers
                )
                .input_metadata(context, WorkerArtifactRead.model_validate(payload))
                .model_dump(mode="json")
            )
        if operation == "proposal.get":
            return self.runtime.service.proposals.get(context, ProposalRead.model_validate(payload))
        if operation == "worker.get":
            return WorkerQueries(self.runtime.database).get(
                context, WorkerRead.model_validate(payload)
            )
        if operation == "worker.list":
            return WorkerQueries(self.runtime.database).list(
                context, WorkerQuery.model_validate(payload)
            )
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

    def worker_append(self, target: WorkerUploadTarget, offset: int, content: bytes) -> None:
        self.runtime.service.worker_outputs.append(self._context(), target, offset, content)

    def finish_upload(self, identifier: str, key: str) -> ArtifactRef:
        row = self.runtime.artifacts.finalize_upload(self._context(key), identifier)["result"]
        return ArtifactRecord.model_validate(row["data"]).reference

    def download(self, identifier: str, destination: Path) -> ArtifactRef:
        context = self._context()
        reference = self.runtime.artifacts.get(context, identifier).reference
        self.runtime.artifacts.fetch(context, reference, destination)
        return reference

    def worker_download(self, request: WorkerArtifactRead, destination: Path) -> ArtifactRef:
        context = self._context()
        artifacts = WorkerArtifacts(
            self.runtime.database, self.runtime.artifacts, self.runtime.service.workers
        )
        reference = artifacts.input_metadata(context, request)
        with artifacts.input_reader(context, request) as source:
            return write_download(reference, iter(lambda: source.read(64 * 1024), b""), destination)

    def close(self) -> None:
        pass


def controller_url(url: str) -> str:
    """Validate a controller destination without opening a client or doing network I/O."""
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or not parsed.hostname
        or (parsed.port is not None and not 1 <= parsed.port <= 65535)
        or any(character.isspace() for character in url)
    ):
        raise ValueError("Controller URL must be HTTP(S), without credentials, query, or fragment")
    if parsed.scheme == "http":
        try:
            loopback = ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            loopback = parsed.hostname == "localhost"
        if not loopback or "%" in parsed.hostname:
            raise ValueError("Non-loopback controller connections require HTTPS")
    return url.rstrip("/")


class HTTPTransport:
    def __init__(
        self, url: str, token: str, *, timeout: float = 30, tls: ssl.SSLContext | None = None
    ) -> None:
        url = controller_url(url)
        if tls is not None and (
            not isinstance(tls, ssl.SSLContext)
            or tls.verify_mode != ssl.CERT_REQUIRED
            or not tls.check_hostname
        ):
            raise ValueError("TLS context must verify certificates and hostnames")
        self.http = httpx.Client(
            base_url=url,
            headers={"Authorization": "Bearer " + token},
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
            verify=tls if tls is not None else True,
        )

    @staticmethod
    def _error(status: int, payload: JsonObject) -> None:
        if not 400 <= status < 600:
            raise ValueError("Unexpected HTTP status")
        rejected = (
            CommandRejected.model_validate(payload)
            if "command_id" in payload
            else RequestRejected.model_validate(payload)
        )
        error = rejected.error
        raise ResearchError(
            error.code,
            error.message,
            retryable=error.retryable,
            details=error.details,
            command_id=rejected.command_id if isinstance(rejected, CommandRejected) else None,
        )

    def _request(
        self, method: str, path: str, *, mode: RequestMode = "query", **kwargs: Any
    ) -> JsonObject:
        try:
            with self.http.stream(method, path, **kwargs) as response:
                content = bytearray()
                for block in response.iter_bytes(64 * 1024):
                    if len(content) + len(block) > MAX_RESPONSE_BYTES:
                        raise ValueError("Controller response exceeds its bound")
                    content.extend(block)
                payload = strict_json_loads(bytes(content))
                if not isinstance(payload, dict):
                    raise ValueError("Expected object response")
                if not 200 <= response.status_code < 300:
                    self._error(response.status_code, payload)
                if mode == "command":
                    return dict(CommandAccepted.model_validate(payload).model_dump(mode="json"))
                if mode == "upload_part":
                    return dict(UploadPartAccepted.model_validate(payload).model_dump(mode="json"))
                if "error" in payload:
                    raise ValueError("Error response has a success status")
                return dict(payload)
        except (httpx.RequestError, ValueError) as error:
            mutating = mode != "query"
            raise ResearchError(
                ErrorCode.DELIVERY_UNCERTAIN if mutating else ErrorCode.TRANSPORT_UNAVAILABLE,
                "Controller acknowledgment is unavailable"
                if mutating
                else "Controller query failed",
                retryable=True,
                details={
                    "recovery": (
                        "replay_exact_command"
                        if mode == "command"
                        else "inspect_upload_then_replay_exact_range"
                        if mode == "upload_part"
                        else "retry_query"
                    ),
                },
            ) from error

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
            mode="command",
            json=payload,
            headers=self._headers(key, revision),
        )

    def query(self, operation: str, payload: JsonObject) -> JsonObject:
        if operation in {
            "proposal.get",
            "worker.get",
            "worker.list",
            "worker.input",
            "worker.output",
            "worker.clock",
        }:
            return self._request("POST", "/v1/" + operation.replace(".", "/"), json=payload)
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
                mode="command",
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
            mode="upload_part",
            params={"offset": offset},
            content=content,
        )

    def finish_upload(self, identifier: str, key: str) -> ArtifactRef:
        row = self._request(
            "POST",
            f"/v1/artifact-uploads/{TypeAdapter(ID).validate_python(identifier)}/finalize",
            mode="command",
            headers=self._headers(key),
        )["result"]
        return ArtifactRecord.model_validate(row["data"]).reference

    def worker_append(self, target: WorkerUploadTarget, offset: int, content: bytes) -> None:
        self._request(
            "PUT",
            "/v1/worker/output/parts",
            mode="upload_part",
            params={**target.model_dump(exclude={"schema_version"}), "offset": offset},
            content=content,
        )

    def download(self, identifier: str, destination: Path) -> ArtifactRef:
        identifier = TypeAdapter(ID).validate_python(identifier)
        record = ArtifactRecord.model_validate(self._request("GET", f"/v1/artifacts/{identifier}"))
        with self.http.stream("GET", f"/v1/artifacts/{identifier}/content") as response:
            if response.status_code != 200:
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Artifact download failed")
            return write_download(record.reference, response.iter_bytes(64 * 1024), destination)

    def worker_download(self, request: WorkerArtifactRead, destination: Path) -> ArtifactRef:
        payload = WorkerArtifactRead.model_validate(request).model_dump(mode="json")
        reference = ArtifactRef.model_validate(
            self._request("POST", "/v1/worker/input", json=payload)
        )
        with self.http.stream("POST", "/v1/worker/input/content", json=payload) as response:
            if response.status_code != 200:
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Worker input download failed")
            return write_download(reference, response.iter_bytes(64 * 1024), destination)

    def close(self) -> None:
        self.http.close()


class WorkerUploadSink:
    def __init__(self, transport: ClientTransport, intent: WorkerOutputIntent) -> None:
        self.transport, self.intent = transport, intent

    def target(self, identifier: str) -> WorkerUploadTarget:
        return WorkerUploadTarget(
            worker_id=self.intent.worker_id,
            session_id=self.intent.session_id,
            attempt_id=self.intent.attempt_id,
            upload_id=identifier,
        )

    def begin_upload(self, request: BeginUpload, key: str) -> JsonObject:
        payload = BeginWorkerUpload(
            **self.intent.model_dump(exclude={"schema_version"}), content=request
        )
        return dict(
            self.transport.command(
                "worker.begin_upload", payload.model_dump(mode="json"), key, None
            )["result"]
        )

    def upload_status(self, identifier: str) -> JsonObject:
        return self.transport.query(
            "worker.output", self.target(identifier).model_dump(mode="json")
        )

    def append_upload(self, identifier: str, offset: int, content: bytes) -> None:
        self.transport.worker_append(self.target(identifier), offset, content)

    def finish_upload(self, identifier: str, key: str) -> ArtifactRef:
        result = self.transport.command(
            "worker.finalize_upload", self.target(identifier).model_dump(mode="json"), key, None
        )["result"]
        return ArtifactRef.model_validate(result["artifact"])


def upload_file(
    sink: UploadSink, path: Path, *, idempotency_key: str, media_type: str
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
        upload = sink.begin_upload(request, "begin:" + key)
        status = sink.upload_status(upload["id"])
        if status["state"] == "pending" and not status["transfer"]["published"]:
            offset = status["transfer"]["received_bytes"]
            source.seek(offset)
            while block := source.read(PART_BYTES):
                try:
                    sink.append_upload(upload["id"], offset, block)
                except ResearchError as error:
                    if error.code not in {ErrorCode.NOT_FOUND, ErrorCode.INVALID_TRANSITION}:
                        raise
                    current = sink.upload_status(upload["id"])
                    if current["state"] not in {"finalized", "cleanup"} and not (
                        current["state"] == "pending" and current["transfer"]["published"]
                    ):
                        raise
                    break
                offset += len(block)
    return sink.finish_upload(upload["id"], "finish:" + key)


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
    def connect(
        cls, url: str, token: str, *, timeout: float = 30, tls: ssl.SSLContext | None = None
    ) -> ResearchClient:
        return cls(HTTPTransport(url, token, timeout=timeout, tls=tls))

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

    def proposal_get(self, request: ProposalRead) -> JsonObject:
        return self.transport.query("proposal.get", request.model_dump(mode="json"))

    def worker_get(self, request: WorkerRead) -> JsonObject:
        return self.transport.query("worker.get", request.model_dump(mode="json"))

    def worker_clock(self, request: ClockChallenge) -> SignedClockReply:
        return SignedClockReply.model_validate(
            self.transport.query("worker.clock", request.model_dump(mode="json"))
        )

    def worker_list(self, request: WorkerQuery) -> JsonObject:
        return self.transport.query("worker.list", request.model_dump(mode="json"))

    def worker_download(self, request: WorkerArtifactRead, destination: Path) -> ArtifactRef:
        return self.transport.worker_download(request, destination)

    def worker_input(self, request: WorkerArtifactRead) -> ArtifactRef:
        return ArtifactRef.model_validate(
            self.transport.query("worker.input", request.model_dump(mode="json"))
        )

    def worker_output(self, request: WorkerUploadTarget) -> JsonObject:
        return self.transport.query("worker.output", request.model_dump(mode="json"))

    def context(self, campaign_id: str, *, limit: int = 10) -> JsonObject:
        return self.transport.query("context", {"campaign_id": campaign_id, "limit": limit})

    def budget(self, campaign_id: str) -> JsonObject:
        return self.transport.query("budget", {"campaign_id": campaign_id})

    def events(self, *, after: int = 0, limit: int = 100) -> JsonObject:
        return self.transport.query("events", {"after": after, "limit": limit})

    def upload_file(
        self, path: Path, *, idempotency_key: str, media_type: str = "application/octet-stream"
    ) -> ArtifactRef:
        return upload_file(
            self.transport, path, idempotency_key=idempotency_key, media_type=media_type
        )

    def worker_upload_file(
        self,
        intent: WorkerOutputIntent,
        path: Path,
        *,
        idempotency_key: str,
        media_type: str = "application/octet-stream",
    ) -> ArtifactRef:
        return upload_file(
            WorkerUploadSink(self.transport, intent),
            path,
            idempotency_key=idempotency_key,
            media_type=media_type,
        )

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

    async def proposal_get(self, request: ProposalRead) -> JsonObject:
        return await asyncio.to_thread(self.client.proposal_get, request)

    async def worker_get(self, request: WorkerRead) -> JsonObject:
        return await asyncio.to_thread(self.client.worker_get, request)

    async def worker_clock(self, request: ClockChallenge) -> SignedClockReply:
        return await asyncio.to_thread(self.client.worker_clock, request)

    async def worker_list(self, request: WorkerQuery) -> JsonObject:
        return await asyncio.to_thread(self.client.worker_list, request)

    async def worker_download(self, request: WorkerArtifactRead, destination: Path) -> ArtifactRef:
        return await asyncio.to_thread(self.client.worker_download, request, destination)

    async def worker_input(self, request: WorkerArtifactRead) -> ArtifactRef:
        return await asyncio.to_thread(self.client.worker_input, request)

    async def worker_output(self, request: WorkerUploadTarget) -> JsonObject:
        return await asyncio.to_thread(self.client.worker_output, request)

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

    async def worker_upload_file(
        self,
        intent: WorkerOutputIntent,
        path: Path,
        *,
        idempotency_key: str,
        media_type: str = "application/octet-stream",
    ) -> ArtifactRef:
        return await asyncio.to_thread(
            self.client.worker_upload_file,
            intent,
            path,
            idempotency_key=idempotency_key,
            media_type=media_type,
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
