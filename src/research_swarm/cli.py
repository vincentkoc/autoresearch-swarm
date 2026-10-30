"""JSON-first local, remote, and agent entry points."""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.runtime import LocalRuntime, read_private


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="research", description=__doc__)
    root.add_argument("--state-dir", type=Path)
    root.add_argument("--controller", help="Use an HTTP research controller")
    root.add_argument("--token-file", type=Path, help="Private credential file, never a raw token")
    commands = root.add_subparsers(dest="action", required=True)
    commands.add_parser("init", help="Initialize private local state")
    serve = commands.add_parser("serve", help="Serve the authenticated control API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    commands.add_parser("mcp", help="Expose typed agent tools over stdio")
    commands.add_parser("capabilities")
    worker = commands.add_parser("worker")
    worker_actions = worker.add_subparsers(dest="worker_action", required=True)
    local = worker_actions.add_parser("run", help="Drain admitted campaign work on the local CPU")
    local.add_argument("campaign_id")
    local.add_argument("--cpu-slots", type=int, default=1)
    local.add_argument("--machine-registry", type=Path)
    command = commands.add_parser("command", help="Send a typed domain command")
    command.add_argument("operation")
    command.add_argument("--file", type=Path, help="JSON request file, otherwise read stdin")
    command.add_argument("--key", required=True, help="Stable idempotency key")
    command.add_argument("--revision", type=int)
    project = commands.add_parser("project")
    project_actions = project.add_subparsers(dest="project_action", required=True)
    for action in ("capture", "register"):
        item = project_actions.add_parser(action)
        item.add_argument("path", type=Path)
        item.add_argument("--manifest", default="research.toml")
        item.add_argument("--source-revision")
        item.add_argument("--key", required=True)
    get = commands.add_parser("get")
    get.add_argument("kind")
    get.add_argument("identifier")
    query = commands.add_parser("query")
    query.add_argument("kind")
    query.add_argument("--campaign")
    query.add_argument("--state")
    query.add_argument("--limit", type=int, default=25)
    query.add_argument("--cursor")
    context = commands.add_parser("context")
    context.add_argument("campaign_id")
    context.add_argument("--limit", type=int, default=10)
    budget = commands.add_parser("budget")
    budget.add_argument("campaign_id")
    events = commands.add_parser("events")
    events.add_argument("--after", type=int, default=0)
    events.add_argument("--limit", type=int, default=100)
    artifact = commands.add_parser("artifact")
    artifact_actions = artifact.add_subparsers(dest="artifact_action", required=True)
    upload = artifact_actions.add_parser("upload")
    upload.add_argument("path", type=Path)
    upload.add_argument("--key", required=True)
    download = artifact_actions.add_parser("download")
    download.add_argument("identifier")
    download.add_argument("destination", type=Path)
    return root


def _client(args: argparse.Namespace) -> ResearchClient:
    if args.controller:
        if args.token_file is None:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Remote control requires --token-file"
            )
        path = args.token_file.expanduser().absolute()
        token = read_private(path.parent, path.name, limit=256).decode("ascii").strip()
        return ResearchClient.connect(args.controller, token)
    return ResearchClient.local(args.state_dir, token_path=args.token_file)


def _request(path: Path | None) -> dict[str, Any]:
    if path is None:
        content = sys.stdin.buffer.read(1024 * 1024 + 1)
    else:
        with path.open("rb") as stream:
            content = stream.read(1024 * 1024 + 1)
    if len(content) > 1024 * 1024:
        raise ValueError("Command body exceeds its limit")
    value = strict_json_loads(content)
    if not isinstance(value, dict):
        raise ValueError("Command request must be an object")
    return dict(value)


def _execute(client: ResearchClient, args: argparse.Namespace) -> Any:
    if args.action == "worker":
        from research_swarm.orchestration.local import LocalDriver
        from research_swarm.workers.local_enrollment import ensure_local_enrollment

        if not isinstance(client.transport, LocalTransport):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Local driver requires local state"
            )
        enrollment = ensure_local_enrollment(
            client.transport.runtime,
            cpu_slots=args.cpu_slots,
            machine_registry=args.machine_registry,
            principal=client.transport.resolve_principal(),
        )
        return asyncio.run(LocalDriver(client, enrollment).drain(args.campaign_id)).model_dump(
            mode="json"
        )
    if args.action == "capabilities":
        return client.capabilities()
    if args.action == "command":
        return client.command(
            args.operation,
            _request(args.file),
            idempotency_key=args.key,
            expected_revision=args.revision,
        )
    if args.action == "project":
        if args.project_action == "capture":
            return client.capture_project(
                args.path,
                idempotency_key=args.key,
                manifest_path=args.manifest,
                revision=args.source_revision,
            ).model_dump(mode="json")
        return client.register_project(
            args.path,
            idempotency_key=args.key,
            manifest_path=args.manifest,
            revision=args.source_revision,
        )
    if args.action == "get":
        return client.get(RecordQuery(kind=args.kind).kind, args.identifier)
    if args.action == "query":
        return client.list(
            RecordQuery(
                kind=args.kind,
                campaign_id=args.campaign,
                state=args.state,
                limit=args.limit,
                cursor=args.cursor,
            )
        )
    if args.action == "context":
        return client.context(args.campaign_id, limit=args.limit)
    if args.action == "budget":
        return client.budget(args.campaign_id)
    if args.action == "events":
        return client.events(after=args.after, limit=args.limit)
    if args.action == "artifact":
        if args.artifact_action == "upload":
            return client.upload_file(args.path, idempotency_key=args.key).model_dump(mode="json")
        return client.download(args.identifier, args.destination).model_dump(mode="json")
    raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Unknown CLI operation")


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.action in {"init", "serve"}:
            if args.controller:
                raise ValueError("Local service commands cannot use a remote controller")
            runtime = LocalRuntime.open(args.state_dir)
            runtime.principal(args.token_file).require("service.manage")
            if args.action == "init":
                print(canonical_bytes(runtime.identity.model_dump(mode="json")).decode("utf-8"))
                return 0
            import uvicorn

            from research_swarm.api.http import create_app

            uvicorn.run(
                create_app(runtime.service, runtime.credentials),
                host=args.host,
                port=args.port,
                access_log=False,
                limit_concurrency=64,
            )
            return 0
        with _client(args) as client:
            if args.action == "mcp":
                from research_swarm.api.mcp import make_mcp

                capture: Callable[[], None] | None = None
                if isinstance(client.transport, LocalTransport):
                    local = client.transport

                    def authorize_capture() -> None:
                        local.resolve_principal().require("source.capture")

                    capture = authorize_capture
                make_mcp(client, authorize_local_capture=capture).run(transport="stdio")
                return 0
            result = _execute(client, args)
            print(canonical_bytes(result).decode("utf-8"))
            if args.action == "worker" and result["state"] == "blocked":
                return 3
        return 0
    except ResearchError as error:
        print(canonical_bytes(error.as_dict()).decode("utf-8"), file=sys.stderr)
        return 2
    except (ValueError, OSError, httpx.RequestError) as error:
        failure = ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Operation failed: " + type(error).__name__
        )
        print(canonical_bytes(failure.as_dict()).decode("utf-8"), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
