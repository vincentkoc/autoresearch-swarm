"""JSON-first local, remote, and agent entry points."""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, TextIO

import httpx

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.runtime import LocalRuntime, read_private


def _emit_json(value: Any, *, stream: TextIO | None = None) -> None:
    """Emit canonical UTF-8 JSON regardless of the terminal or pipe's text encoding."""
    target = stream if stream is not None else sys.stdout
    payload = canonical_bytes(value) + b"\n"
    buffer = getattr(target, "buffer", None)
    if buffer is None:
        target.write(payload.decode("utf-8"))
    else:
        target.flush()
        buffer.write(payload)
        buffer.flush()
    target.flush()


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="research", description=__doc__)
    root.add_argument("--state-dir", type=Path)
    root.add_argument("--controller", help="Use an HTTP research controller")
    root.add_argument("--token-file", type=Path, help="Private credential file, never a raw token")
    commands = root.add_subparsers(dest="action", required=True)
    swarm = commands.add_parser("swarm", help="Run a capacity-driven research campaign")
    swarm_commands = swarm.add_subparsers(dest="swarm_action", required=True)
    swarm_run = swarm_commands.add_parser("run", help="Run or resume a YAML campaign")
    swarm_run.add_argument("spec", type=Path, help="Campaign YAML file")
    swarm_run.add_argument(
        "--db", type=Path, help="SQLite queue (default: swarm.sqlite beside spec)"
    )
    initialize = commands.add_parser("init", help="Initialize private local state")
    initialize.add_argument("--storage-config", type=Path, help="Immutable storage JSON selection")
    serve = commands.add_parser("serve", help="Serve the authenticated control API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    commands.add_parser("mcp", help="Expose typed agent tools over stdio")
    reactive = commands.add_parser(
        "reactive", help="Operate one explicitly granted local decision session"
    )
    reactive.add_argument("--session", type=Path, required=True)
    reactive_actions = reactive.add_subparsers(dest="reactive_action", required=True)
    for operation in ("inspect", "pending", "mcp"):
        reactive_actions.add_parser(operation)
    reactive_respond = reactive_actions.add_parser("respond")
    reactive_respond.add_argument(
        "--file", type=Path, required=True, help="Bounded decision proposal JSON"
    )
    reactive_export = reactive_actions.add_parser("export")
    reactive_export.add_argument("--after", type=int, default=-1)
    reactive_export.add_argument("--limit", type=int, default=20)
    reactive_export.add_argument("--max-bytes", type=int, default=65536)
    reports = commands.add_parser("report", help="Render immutable research reports")
    report_actions = reports.add_subparsers(dest="report_action", required=True)
    for operation in ("plot", "view"):
        plot = report_actions.add_parser(operation, help="Export bounded captured evidence")
        source = plot.add_mutually_exclusive_group(required=True)
        source.add_argument("--report-id", help="Existing immutable report ID")
        source.add_argument("--file", type=Path, help="Offline report record or snapshot JSON")
        plot.add_argument(
            "--compare-report", action="append", default=[], help="Comparable report ID"
        )
        plot.add_argument("--compare-file", action="append", type=Path, default=[])
        plot.add_argument("--comparison-digest", required=True)
        plot.add_argument("--metric", required=True)
        plot.add_argument("--unit", required=True)
        plot.add_argument("--direction", choices=("minimize", "maximize"), required=True)
        plot.add_argument(
            "--output", type=Path, required=True, help="New SVG/HTML path; never overwrite"
        )
        plot.add_argument("--title", default="Swarm · campaign")
        plot.add_argument(
            "--label", action="append", help="One display label per report, in input order"
        )
        if operation == "view":
            plot.add_argument(
                "--project-repository", help="Optional HTTPS GitHub project repository for About"
            )
    commands.add_parser("capabilities")
    from research_swarm.swarm.campaign_gc import register as register_campaign_gc

    register_campaign_gc(commands)
    engine = commands.add_parser("engine", help="Inspect and register local execution environments")
    engines = engine.add_subparsers(dest="engine_action", required=True)
    engines.add_parser("list")
    for operation in ("probe", "register"):
        entry = engines.add_parser(operation)
        entry.add_argument("profile")
        entry.add_argument("--python", type=Path)
        entry.add_argument("--device-index", type=int, default=0)
        entry.add_argument("--probe-dir", type=Path)
    probe = engines.choices["probe"]
    probe.add_argument("--timeout-ms", type=int, default=15000)
    registration = engines.choices["register"]
    registration.add_argument("--file", type=Path, required=True)
    registration.add_argument("--environment-contract", action="append", required=True)
    registration.add_argument("--cpu-slots", type=int, default=1)
    registration.add_argument("--memory-bytes", type=int, default=0)
    registration.add_argument("--scratch-bytes", type=int, default=0)
    registration.add_argument(
        "--precision",
        choices=("project_declared", "fp32", "fp16", "bf16"),
        default="project_declared",
    )
    registration.add_argument(
        "--compilation",
        choices=("project_declared", "eager", "cold", "warm"),
        default="project_declared",
    )
    verification = engines.add_parser("verify")
    verification.add_argument("--file", type=Path, required=True)
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
    run.add_argument("--registration", type=Path, help="Private registered execution environment")
    run.add_argument(
        "--inputs-file", type=Path, help="JSON mapping of input names to artifact references"
    )
    worker = commands.add_parser("worker")
    worker_actions = worker.add_subparsers(dest="worker_action", required=True)
    local = worker_actions.add_parser("run", help="Drain admitted campaign work on the local CPU")
    local.add_argument("campaign_id")
    local.add_argument("--cpu-slots", type=int, default=1)
    local.add_argument("--machine-registry", type=Path)
    local.add_argument("--registration", type=Path)
    describe = worker_actions.add_parser("describe", help="Describe a persistent remote CPU worker")
    describe.add_argument("--worker-dir", type=Path, required=True)
    describe.add_argument("--cpu-slots", type=int, default=1)
    describe.add_argument("--machine-registry", type=Path)
    describe.add_argument("--registration", type=Path)
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
    start = worker_actions.add_parser("start", help="Run the scoped HTTP worker in the foreground")
    start.add_argument("--worker-dir", type=Path, required=True)
    start.add_argument("--bundle", type=Path)
    start.add_argument("--once", action="store_true", help="Drain available work and exit")
    start.add_argument("--poll-ms", type=int, default=500)
    start.add_argument("--maximum-backoff-ms", type=int, default=15000)
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
        item.add_argument("--capture-timeout-seconds", type=float, default=30)
        item.add_argument("--key", required=True)
    evaluation = commands.add_parser("evaluation", help="Advance independent evaluation control")
    evaluations = evaluation.add_subparsers(dest="evaluation_action", required=True)
    advance = evaluations.add_parser("advance")
    advance.add_argument("campaign_id")
    advance.add_argument("--evaluator", required=True)
    advance.add_argument("--evaluator-token-file", type=Path)
    advance.add_argument("--continuation-file", type=Path)
    advance.add_argument("--max-pages", type=int, default=4)
    advance.add_argument("--page-size", type=int, default=50)
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


def _request(path: Path | None, *, limit: int = 1024 * 1024) -> dict[str, Any]:
    if path is None:
        content = sys.stdin.buffer.read(limit + 1)
    else:
        with path.open("rb") as stream:
            content = stream.read(limit + 1)
    if len(content) > limit:
        raise ValueError("Command body exceeds its limit")
    value = strict_json_loads(content)
    if not isinstance(value, dict):
        raise ValueError("Command request must be an object")
    return dict(value)


def _plot_reports(reports: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
    from research_swarm.control.report_plot import render_hill_climb_svg
    from research_swarm.domain.experiments import MetricDefinition

    snapshots = []
    for report in reports:
        content = report.get("data", report)
        if not isinstance(content, dict):
            raise ValueError("Report content must be an object")
        if "report_digest" in content:
            expected = record_digest(
                "research.report",
                {key: value for key, value in content.items() if key != "report_digest"},
            )
            if content["report_digest"] != expected:
                raise ValueError("Report digest does not match its captured content")
        snapshots.append(content.get("snapshot", content))
    metric = MetricDefinition(name=args.metric, unit=args.unit, direction=args.direction)
    renderer = render_hill_climb_svg
    output_format = "svg"
    if args.report_action == "view":
        from research_swarm.control.report_view import render_report_html

        renderer = render_report_html
        output_format = "html"
    presentation = (
        {"project_repository": args.project_repository} if args.report_action == "view" else {}
    )
    rendered = renderer(
        snapshots,
        comparison_digest=args.comparison_digest,
        metric=metric,
        title=args.title,
        labels=args.label,
        **presentation,
    )
    with args.output.open("x", encoding="utf-8", newline="\n") as output:
        output.write(rendered)
    return {"output": str(args.output), "format": output_format, "reports": len(snapshots)}


def _execute(client: ResearchClient, args: argparse.Namespace) -> Any:
    if args.action == "report":
        return _plot_reports(
            [
                client.get("report", identifier)
                for identifier in [args.report_id, *args.compare_report]
            ],
            args,
        )
    if args.action == "run":
        from research_swarm.engines.registration import read_registration
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
                registration=read_registration(args.registration) if args.registration else None,
            )
        ).model_dump(mode="json")
    if args.action == "evaluation":
        from research_swarm.orchestration.evaluation import EvaluationCursor, EvaluationDriver

        continuation = (
            EvaluationCursor.model_validate(_request(args.continuation_file))
            if args.continuation_file
            else None
        )
        role_args = argparse.Namespace(**vars(args))
        role_args.token_file = args.evaluator_token_file or args.token_file
        with _client(role_args) as evaluator:
            return (
                EvaluationDriver(
                    client,
                    evaluator,
                    args.campaign_id,
                    args.evaluator,
                )
                .advance(
                    continuation=continuation,
                    max_pages=args.max_pages,
                    page_size=args.page_size,
                )
                .model_dump(mode="json")
            )
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
        from research_swarm.engines.registration import read_registration
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
            registration=read_registration(args.registration) if args.registration else None,
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
                capture_timeout_seconds=args.capture_timeout_seconds,
            ).model_dump(mode="json")
        return client.register_project(
            args.path,
            idempotency_key=args.key,
            manifest_path=args.manifest,
            revision=args.source_revision,
            capture_timeout_seconds=args.capture_timeout_seconds,
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
    result: Any
    try:
        if args.action == "campaign" and args.campaign_action == "gc":
            from research_swarm.runtime import default_state_directory
            from research_swarm.swarm.campaign_gc import collect

            if args.controller or args.token_file:
                raise ValueError("campaign gc operates on the local resource inventory")
            result, code = asyncio.run(collect(args, args.state_dir or default_state_directory()))
            _emit_json(result)
            return code
        if args.action == "swarm":
            from research_swarm.swarm.command import run_campaign

            if args.controller or args.token_file or (args.db and args.state_dir):
                raise ValueError("Choose a local swarm database with --db or --state-dir")
            directory = args.state_dir or args.spec.parent
            try:
                result, code = run_campaign(args.spec, args.db or directory / "swarm.sqlite")
            except ValueError as error:
                raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, str(error)) from error
            _emit_json(result)
            return code
        if args.action == "report":
            from research_swarm.control.report_plot import MAX_RUNS

            if args.file is not None:
                if args.compare_report or args.controller or args.state_dir or args.token_file:
                    raise ValueError("Offline reports cannot mix live controller inputs")
                if len(args.compare_file) >= MAX_RUNS:
                    raise ValueError("At most eight reports may be plotted")
                _emit_json(
                    _plot_reports(
                        [
                            _request(path, limit=2 * 1024 * 1024)
                            for path in [args.file, *args.compare_file]
                        ],
                        args,
                    )
                )
                return 0
            if args.compare_file or len(args.compare_report) >= MAX_RUNS:
                raise ValueError("Use at most eight report IDs, without offline files")
        if args.action == "reactive":
            if args.state_dir is not None or args.controller or args.token_file is not None:
                raise ValueError(
                    "Reactive tools grant one local session, not controller credentials"
                )
            from research_swarm.reactive.client import DecisionClient

            decisions = DecisionClient(args.session)
            if args.reactive_action == "mcp":
                from research_swarm.reactive.mcp import make_reactive_mcp

                make_reactive_mcp(decisions).run(transport="stdio")
                return 0
            arguments = {}
            if args.reactive_action == "respond":
                arguments = {"proposal": _request(args.file, limit=17408)}
            elif args.reactive_action == "export":
                arguments = {"after": args.after, "limit": args.limit, "max_bytes": args.max_bytes}
            _emit_json(decisions.dispatch(args.reactive_action, arguments))
            return 0
        if args.action == "engine":
            if args.controller or args.token_file or args.state_dir:
                raise ValueError(
                    "Engine registration is a local operation without controller state"
                )
            from research_swarm.engines import ENGINE_DEFINITIONS, probe_engine
            from research_swarm.engines.registration import (
                ExecutionPolicy,
                read_registration,
                register_environment,
                verify_registration,
            )

            if args.engine_action == "list":
                result = {"profiles": [item.model_dump(mode="json") for item in ENGINE_DEFINITIONS]}
            elif args.engine_action == "probe":
                observation = probe_engine(
                    args.profile,
                    executable=args.python,
                    device_index=args.device_index,
                    timeout_ms=args.timeout_ms,
                    probe_directory=args.probe_dir,
                )
                _emit_json(observation.model_dump(mode="json"))
                return 0 if observation.status == "available" else 3
            else:
                if args.engine_action == "register":
                    registered = register_environment(
                        args.file,
                        profile=args.profile,
                        executable=args.python,
                        device_index=args.device_index,
                        probe_directory=args.probe_dir,
                        cpu_slots=args.cpu_slots,
                        memory_bytes=args.memory_bytes,
                        scratch_bytes=args.scratch_bytes,
                        environment_contracts=tuple(args.environment_contract),
                        policy=ExecutionPolicy(
                            precision=args.precision, compilation=args.compilation
                        ),
                    )
                else:
                    registered = read_registration(args.file)
                    verify_registration(registered)
                result = registered.public_description()
            _emit_json(result)
            return 0
        if args.action == "worker" and args.worker_action in {
            "describe",
            "connect",
            "start",
        }:
            from research_swarm.workers.remote_enrollment import connect_worker, describe_worker

            if args.controller or args.token_file or args.state_dir:
                raise ValueError("Worker installation uses --worker-dir and its provisioned bundle")
            if args.worker_action == "describe":
                from research_swarm.engines.registration import read_registration

                description = describe_worker(
                    args.worker_dir,
                    cpu_slots=args.cpu_slots,
                    machine_registry=args.machine_registry,
                    registration=read_registration(args.registration)
                    if args.registration
                    else None,
                )
                _emit_json(description.model_dump(mode="json"))
            else:
                with connect_worker(args.worker_dir, bundle_path=args.bundle) as enrollment:
                    if args.worker_action == "start":
                        from research_swarm.orchestration.scoped import ScopedWorkerDriver

                        outcome = asyncio.run(
                            ScopedWorkerDriver(
                                enrollment,
                                poll_ms=args.poll_ms,
                                maximum_backoff_ms=args.maximum_backoff_ms,
                            ).run(once=args.once)
                        )
                        _emit_json(outcome.model_dump(mode="json"))
                        return 3 if outcome.state in {"blocked", "disconnected"} else 0
                    _emit_json(
                        {
                            "state": "connected",
                            "worker": enrollment.worker.worker.model_dump(mode="json"),
                        }
                    )
            return 0
        if args.action in {"init", "serve"}:
            if args.controller:
                raise ValueError("Local service commands cannot use a remote controller")
            from research_swarm.storage.config import storage_configuration

            storage = (
                storage_configuration.validate_python(_request(args.storage_config))
                if args.action == "init" and args.storage_config is not None
                else None
            )
            runtime = LocalRuntime.open(args.state_dir, storage_config=storage)
            runtime.principal(args.token_file).require("service.manage")
            if args.action == "init":
                _emit_json(runtime.identity.model_dump(mode="json"))
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
            _emit_json(result)
            if args.action in {"worker", "run"} and result["state"] in {"blocked", "needs_review"}:
                return 3
            if args.action == "evaluation" and result["state"] in {
                "needs_review",
                "delivery_uncertain",
                "transport_unavailable",
            }:
                return 3
            if (
                args.action == "search"
                and args.search_action == "advance"
                and result["state"]
                in {
                    "needs_review",
                    "contended",
                    "delivery_uncertain",
                    "transport_unavailable",
                }
            ):
                return 3
        return 0
    except ResearchError as error:
        _emit_json(error.as_dict(), stream=sys.stderr)
        return 2
    except (ValueError, OSError, httpx.RequestError) as error:
        failure = ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Operation failed: " + type(error).__name__
        )
        _emit_json(failure.as_dict(), stream=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
