"""JSON-first local, remote, and agent entry points."""

from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
from pathlib import Path
from typing import Any, TextIO

import httpx

from research_swarm.client import ResearchClient
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.swarm.private_files import read_private
from research_swarm.swarm.report import CampaignReader
from research_swarm.swarm.store import LegacyDatabaseError


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
    from research_swarm.swarm import notify

    notify.add_arguments(swarm_commands.add_parser("notify", help="Watch campaign metrics"))
    swarm_run = swarm_commands.add_parser("run", help="Run or resume a YAML campaign")
    swarm_run.add_argument("spec", type=Path, help="Campaign YAML file")
    swarm_run.add_argument(
        "--retry-exhausted", action="store_true", help="Ask the proposer again after exhaustion"
    )
    swarm_run.add_argument(
        "--db", type=Path, help="SQLite queue (default: swarm.sqlite beside spec)"
    )
    remote_worker = swarm_commands.add_parser(
        "worker", help="Run a locally configured workload for an HTTP campaign"
    )
    remote_worker.add_argument("spec", type=Path)
    remote_worker.add_argument("--worker-id", required=True)
    remote_worker.add_argument("--once", action="store_true", help="Attempt one claim and exit")
    from research_swarm.swarm.agents import add_arguments

    add_arguments(swarm_commands.add_parser("advise", help="Request campaign advice"))
    commands.add_parser("init", help="Initialize a private campaign database")
    serve = commands.add_parser("serve", help="Serve the authenticated control API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--db", type=Path, help="Local swarm campaign database")
    serve.add_argument("--worker-campaign", help="Campaign served to the enrolled worker")
    serve.add_argument("--worker-id", help="Identity recorded on this worker's claims")
    serve.add_argument("--worker-token-file", type=Path, help="Private worker bearer file")
    mcp = commands.add_parser("mcp", help="Expose typed agent tools over stdio")
    mcp.add_argument("--db", type=Path, help="Local swarm campaign database")
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
        source = plot.add_mutually_exclusive_group()
        source.add_argument("--file", type=Path, help="Offline report record or snapshot JSON")
        source.add_argument("--db", type=Path, help="Local swarm campaign database")
        plot.add_argument("--campaign", help="Campaign ID to read from --db")
        plot.add_argument("--compare-file", action="append", type=Path, default=[])
        plot.add_argument("--comparison-digest")
        plot.add_argument("--metric")
        plot.add_argument("--unit")
        plot.add_argument("--direction", choices=("minimize", "maximize"))
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
    project = commands.add_parser("project")
    project_actions = project.add_subparsers(dest="project_action", required=True)
    for action in ("capture", "register"):
        item = project_actions.add_parser(action)
        item.add_argument("path", type=Path)
        item.add_argument("--manifest", default="research.toml")
        item.add_argument("--source-revision")
        item.add_argument(
            "--capture-timeout-seconds", type=float, default=30, help="Time limit for Git reads"
        )
        item.add_argument("--db", type=Path, help="Local swarm campaign database")
    budget = commands.add_parser("budget")
    budget.add_argument("campaign_id")
    budget.add_argument("--db", type=Path, help="Read measured campaign cost from SQLite")
    events = commands.add_parser("events")
    events.add_argument("--campaign")
    events.add_argument("--db", type=Path)
    events.add_argument("--after", type=int, default=0)
    events.add_argument("--limit", type=int, default=100)
    artifact = commands.add_parser("artifact")
    artifact_actions = artifact.add_subparsers(dest="artifact_action", required=True)
    upload = artifact_actions.add_parser("upload")
    upload.add_argument("path", type=Path)
    upload.add_argument("--db", type=Path, help="Local swarm campaign database")
    download = artifact_actions.add_parser("download")
    download.add_argument("--db", type=Path, help="Local swarm campaign database")
    download.add_argument("identifier")
    download.add_argument("destination", type=Path)
    return root


def _read_json_object(path: Path, *, limit: int) -> dict[str, Any]:
    with path.open("rb") as stream:
        content = stream.read(limit + 1)
    if len(content) > limit:
        raise ValueError("Command body exceeds its limit")
    value = strict_json_loads(content)
    if not isinstance(value, dict):
        raise ValueError("Command request must be an object")
    return dict(value)


def _plot_reports(reports: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
    from research_swarm.domain.experiments import MetricDefinition
    from research_swarm.swarm.report.plot import render_hill_climb_svg

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
        from research_swarm.swarm.report.view import render_report_html

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


def _database(args: argparse.Namespace) -> Path:
    from research_swarm.swarm.private_files import campaign_database, default_state_directory

    if args.db is not None:
        if args.state_dir is not None:
            raise ValueError("Choose one database location with --db or --state-dir")
        return Path(args.db).expanduser().absolute()
    return campaign_database(args.state_dir or default_state_directory())


def _read_client(args: argparse.Namespace) -> ResearchClient:
    if args.controller:
        if args.db or args.state_dir or not args.token_file:
            raise ValueError("Remote campaign reads require --controller and --token-file")
        return ResearchClient.connect(args.controller, _token(args.token_file))
    if args.token_file:
        raise ValueError("Local campaign reads use filesystem access")
    return ResearchClient(CampaignReader(_database(args)))


def _token(path: Path) -> str:
    path = path.expanduser().absolute()
    return read_private(path.parent, path.name, limit=256).decode("ascii").strip()


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    result: Any
    try:
        if args.action in {"project", "artifact"}:
            from research_swarm.swarm.files import CampaignFiles

            if args.controller or args.token_file:
                raise ValueError(
                    "Project and artifact operations require a local campaign database"
                )
            database = _database(args)
            if args.action == "artifact" and args.artifact_action == "download":
                if not database.is_file():
                    raise FileNotFoundError(database)
            else:
                database.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            files = CampaignFiles(database)
            if args.action == "project":
                result = files.project(
                    args.path,
                    register=args.project_action == "register",
                    manifest_path=args.manifest,
                    revision=args.source_revision,
                    timeout=args.capture_timeout_seconds,
                )
            elif args.artifact_action == "upload":
                result = files.put(args.path).model_dump(mode="json")
            else:
                result = files.download(args.identifier, args.destination).model_dump(mode="json")
            _emit_json(result)
            return 0
        if args.action in {"serve", "mcp"}:
            from research_swarm.api.campaign import campaign_app, campaign_tools

            if args.controller:
                raise ValueError("This service reads a local campaign database")
            reader = CampaignReader(_database(args))
            if args.action == "mcp":
                if args.token_file:
                    raise ValueError("Local campaign MCP reads the selected database directly")
                campaign_tools(reader).run(transport="stdio")
            else:
                import uvicorn

                if not args.token_file:
                    raise ValueError("Campaign HTTP reads require --token-file")
                token = _token(args.token_file)
                from research_swarm.api.worker import WorkerGrant

                worker = None
                options = (args.worker_campaign, args.worker_id, args.worker_token_file)
                if any(options):
                    if not all(options):
                        raise ValueError(
                            "Supply --worker-campaign, --worker-id and --worker-token-file together"
                        )
                    worker = WorkerGrant(
                        args.worker_campaign, args.worker_id, _token(args.worker_token_file)
                    )
                uvicorn.run(
                    campaign_app(reader, token, worker=worker), host=args.host, port=args.port
                )
            return 0
        if args.action == "campaign" and args.campaign_action == "gc":
            from research_swarm.swarm.campaign_gc import collect
            from research_swarm.swarm.private_files import default_state_directory

            if args.controller or args.token_file:
                raise ValueError("campaign gc operates on the local resource inventory")
            result, code = asyncio.run(collect(args, args.state_dir or default_state_directory()))
            _emit_json(result)
            return code
        if args.action == "swarm" and args.swarm_action == "advise":
            from research_swarm.swarm.agents import advise

            if args.controller or args.token_file:
                raise ValueError("Campaign advice reads a local campaign database")
            result = asyncio.run(advise(_database(args), args))
            _emit_json(result)
            return 0 if result["status"] == "ok" else 3
        if args.action == "swarm" and args.swarm_action == "worker":
            from research_swarm.swarm.http_worker import run_worker

            if args.state_dir or not args.controller or not args.token_file:
                raise ValueError("Remote worker requires --controller and --token-file")
            secret = _token(args.token_file)
            try:
                ran = asyncio.run(
                    run_worker(args.spec, args.controller, secret, args.worker_id, once=args.once)
                )
            except httpx.HTTPStatusError as error:
                raise ValueError(
                    f"Worker request returned HTTP {error.response.status_code}"
                ) from error
            _emit_json({"worker_id": args.worker_id, "ran": ran})
            return 0
        if args.action == "swarm" and args.swarm_action == "notify":
            from research_swarm.swarm.notify import watch

            if args.controller or args.token_file:
                raise ValueError("The notifier reads a local campaign database")
            watch(_database(args), args, _emit_json)
            return 0
        if args.action == "swarm":
            from research_swarm.swarm.command import run_campaign

            if args.controller or args.token_file or (args.db and args.state_dir):
                raise ValueError("Choose a local swarm database with --db or --state-dir")
            from research_swarm.swarm.private_files import campaign_database

            database = args.db or campaign_database(
                args.state_dir or args.spec.parent, project_directory=args.state_dir is None
            )
            try:
                result, code = run_campaign(
                    args.spec, database, retry_exhausted=args.retry_exhausted
                )
            except ValueError as error:
                raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, str(error)) from error
            _emit_json(result)
            return code
        if args.action in {"budget", "events"}:
            with _read_client(args) as reader_client:
                if args.action == "budget":
                    result = reader_client.budget(args.campaign_id)
                else:
                    result = reader_client.events(
                        after=args.after, limit=args.limit, campaign_id=args.campaign
                    )
            _emit_json(result)
            return 0
        if args.action == "report" and args.file is None:
            if not args.campaign or args.compare_file:
                raise ValueError("Select one campaign with --campaign")
            with _read_client(args) as reader_client:
                snapshot = reader_client.snapshot(args.campaign)
            objective = snapshot["research"]["objective"]["primary_metric"]
            selectors = {
                "metric": objective["name"],
                "unit": objective["unit"],
                "direction": objective["direction"],
            }
            for name, recorded in selectors.items():
                requested = getattr(args, name)
                if requested is not None and requested != recorded:
                    raise ValueError(f"{name} does not match the recorded campaign")
                setattr(args, name, recorded)
            if args.comparison_digest is None:
                args.comparison_digest = snapshot["comparison_digest"]
            _emit_json(_plot_reports([snapshot], args))
            return 0
        if args.action == "report":
            from research_swarm.swarm.report.plot import MAX_RUNS

            if args.campaign or not all(
                (args.metric, args.unit, args.direction, args.comparison_digest)
            ):
                raise ValueError(
                    "Archived reports require a metric, unit, direction and comparison digest"
                )

            if args.controller or args.state_dir or args.token_file:
                raise ValueError("Offline reports cannot mix live controller inputs")
            if len(args.compare_file) >= MAX_RUNS:
                raise ValueError("At most eight reports may be plotted")
            _emit_json(
                _plot_reports(
                    [
                        _read_json_object(path, limit=2 * 1024 * 1024)
                        for path in [args.file, *args.compare_file]
                    ],
                    args,
                )
            )
            return 0
        if args.action == "reactive":
            if args.state_dir is not None or args.controller or args.token_file is not None:
                raise ValueError(
                    "Reactive tools grant one local session, not controller credentials"
                )
            from research_swarm.swarm.reactive.client import DecisionClient

            decisions = DecisionClient(args.session)
            if args.reactive_action == "mcp":
                from research_swarm.swarm.reactive.mcp import make_reactive_mcp

                make_reactive_mcp(decisions).run(transport="stdio")
                return 0
            arguments = {}
            if args.reactive_action == "respond":
                arguments = {"proposal": _read_json_object(args.file, limit=17408)}
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
        if args.action == "init":
            from research_swarm.swarm.private_files import (
                campaign_database,
                default_state_directory,
                private_state_directory,
            )
            from research_swarm.swarm.store import SQLiteStore

            if args.controller or args.token_file:
                raise ValueError("Campaign initialization requires a local state directory")
            database = campaign_database(args.state_dir or default_state_directory())
            with private_state_directory(database.parent):
                SQLiteStore(database)
            _emit_json({"database": str(database)})
            return 0
        raise ValueError("Unknown campaign operation")
    except ResearchError as error:
        _emit_json(error.as_dict(), stream=sys.stderr)
        return 2
    except LegacyDatabaseError as error:
        _emit_json(
            ResearchError(ErrorCode.SCHEMA_UNSUPPORTED, str(error)).as_dict(), stream=sys.stderr
        )
        return 2
    except (ValueError, OSError, sqlite3.Error, httpx.HTTPError) as error:
        failure = ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Operation failed: " + type(error).__name__
        )
        _emit_json(failure.as_dict(), stream=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
