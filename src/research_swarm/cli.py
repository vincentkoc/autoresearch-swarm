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
    run = commands.add_parser(
        "run", help="Freeze a project, execute its baseline, and expose agent control"
    )
    run.add_argument("path", type=Path)
    run.add_argument("--key", help="Stable run identity; defaults to the source path")
    run.add_argument("--manifest", default="research.toml")
    run.add_argument("--source-revision")
    run.add_argument("--primary-metric")
    run.add_argument("--seed", type=int, default=0)
    run.add_argument("--max-trials", type=int, default=10)
    run.add_argument("--cpu-slots", type=int, default=1)
    run.add_argument("--machine-registry", type=Path)
    run.add_argument(
        "--inputs-file", type=Path, help="JSON mapping of input names to artifact references"
    )
    worker = commands.add_parser("worker")
    worker_actions = worker.add_subparsers(dest="worker_action", required=True)
    local = worker_actions.add_parser("run", help="Drain admitted campaign work on the local CPU")
    local.add_argument("campaign_id")
    local.add_argument("--cpu-slots", type=int, default=1)
    local.add_argument("--machine-registry", type=Path)
    describe = worker_actions.add_parser("describe", help="Describe a persistent remote CPU worker")
    describe.add_argument("--worker-dir", type=Path, required=True)
    describe.add_argument("--cpu-slots", type=int, default=1)
    describe.add_argument("--machine-registry", type=Path)
    provision = worker_actions.add_parser("provision", help="Write a private scoped worker bundle")
    provision.add_argument("--description", type=Path, required=True)
    provision.add_argument("--url", required=True)
    provision.add_argument("--project", action="append", required=True)
    provision.add_argument("--bundle", type=Path, required=True)
    provision.add_argument("--name", default="Remote CPU worker")
    connect = worker_actions.add_parser(
        "connect", help="Import or verify a pinned worker connection"
    )
    connect.add_argument("--worker-dir", type=Path, required=True)
    connect.add_argument("--bundle", type=Path)
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
    if args.action == "run":
        from research_swarm.orchestration.bootstrap import RunOptions
        from research_swarm.orchestration.run import run_project

        options = RunOptions(
            manifest_path=args.manifest,
            source_revision=args.source_revision,
            primary_metric=args.primary_metric,
            seed=args.seed,
            max_trials=args.max_trials,
            parallelism=args.cpu_slots,
            input_artifacts=_request(args.inputs_file) if args.inputs_file else {},
        )
        return asyncio.run(
            run_project(
                client,
                args.path,
                key=args.key,
                options=options,
                cpu_slots=args.cpu_slots,
                machine_registry=args.machine_registry,
            )
        ).model_dump(mode="json")
    if args.action == "worker":
        if args.worker_action == "provision":
            from research_swarm.workers.remote_enrollment import WorkerDescription, provision_worker

            bundle = provision_worker(
                client,
                WorkerDescription.model_validate(_request(args.description)),
                url=args.url,
                project_ids=tuple(args.project),
                destination=args.bundle,
                name=args.name,
            )
            return {"state": "provisioned", "worker": bundle.worker.model_dump(mode="json")}
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
        if args.action == "worker" and args.worker_action in {"describe", "connect"}:
            from research_swarm.workers.remote_enrollment import connect_worker, describe_worker

            if args.controller or args.token_file or args.state_dir:
                raise ValueError("Worker installation uses --worker-dir and its provisioned bundle")
            if args.worker_action == "describe":
                description = describe_worker(
                    args.worker_dir,
                    cpu_slots=args.cpu_slots,
                    machine_registry=args.machine_registry,
                )
                print(canonical_bytes(description.model_dump(mode="json")).decode("utf-8"))
            else:
                with connect_worker(args.worker_dir, bundle_path=args.bundle) as enrollment:
                    print(
                        canonical_bytes(
                            {
                                "state": "connected",
                                "worker": enrollment.worker.worker.model_dump(mode="json"),
                            }
                        ).decode("utf-8")
                    )
            return 0
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
            if args.action in {"worker", "run"} and result["state"] in {"blocked", "needs_review"}:
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
