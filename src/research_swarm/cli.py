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
from research_swarm.domain.event_recovery import RecoveryItem, RecoveryPage
from research_swarm.runtime import LocalRuntime, read_private


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="research", description=__doc__)
    root.add_argument("--state-dir", type=Path)
    root.add_argument("--controller", help="Use an HTTP research controller")
    root.add_argument("--token-file", type=Path, help="Private credential file, never a raw token")
    commands = root.add_subparsers(dest="action", required=True)
    initialize = commands.add_parser("init", help="Initialize private local state")
    initialize.add_argument("--storage-config", type=Path, help="Immutable storage JSON selection")
    serve = commands.add_parser("serve", help="Serve the authenticated control API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    commands.add_parser("mcp", help="Expose typed agent tools over stdio")
    commands.add_parser("capabilities")
    backup = commands.add_parser("backup", help="Export and verify a sealed native checkpoint")
    backups = backup.add_subparsers(dest="backup_action", required=True)
    export = backups.add_parser("export", help="Export an already sealed filesystem workspace")
    export.add_argument("path", type=Path)
    export.add_argument("--backup-id", required=True)
    verify = backups.add_parser(
        "verify", help="Verify a private checkpoint without opening a runtime"
    )
    verify.add_argument("path", type=Path)
    verify.add_argument("--digest", help="Expected manifest digest from trusted custody")
    restore = backups.add_parser(
        "restore", help="Restore a verified checkpoint into an inactive epoch"
    )
    restore.add_argument("path", type=Path)
    restore.add_argument("--destination", type=Path, required=True)
    restore.add_argument(
        "--restore-id", required=True, help="Stable UUID for interruption recovery"
    )
    restore.add_argument("--digest", help="Expected manifest digest from trusted custody")
    claim_request = backups.add_parser(
        "request-claim", help="Prove possession of one completed inactive restore"
    )
    claim_request.add_argument("path", type=Path)
    claim_request.add_argument("--destination", type=Path, required=True)
    claim_request.add_argument("--restore-id", required=True)
    claim_request.add_argument("--digest", help="Expected manifest digest from trusted custody")
    claim = backups.add_parser("claim", help="Irreversibly elect one target at the original source")
    claim.add_argument("path", type=Path)
    claim.add_argument("--request-file", type=Path, required=True)
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
    run.add_argument("--search-policy", choices=("external", "greedy"), default="external")
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
    replacement = worker_actions.add_parser(
        "provision-replacement", help="Provision a fenced worker replacement"
    )
    replacement.add_argument("--description", type=Path, required=True)
    replacement.add_argument("--fence", required=True)
    replacement.add_argument("--revision", type=int, required=True)
    replacement.add_argument("--reason", required=True)
    replacement.add_argument("--url", required=True)
    replacement.add_argument("--bundle", type=Path, required=True)
    install = worker_actions.add_parser(
        "install-replacement", help="Install a replacement beside original custody"
    )
    install.add_argument("--original-worker-dir", type=Path, required=True)
    install.add_argument("--worker-dir", type=Path, required=True)
    install.add_argument("--bundle", type=Path, required=True)
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
    recover = worker_actions.add_parser("recover", help="Quarantine one original native journal")
    recover.add_argument("--worker-dir", type=Path, required=True)
    recover.add_argument("--attempt", required=True)
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
    search = commands.add_parser("search", help="Plan or advance a bounded research loop")
    search_actions = search.add_subparsers(dest="search_action", required=True)
    for action in ("plan", "advance"):
        item = search_actions.add_parser(action)
        item.add_argument("campaign_id")
        item.add_argument("--proposer")
        item.add_argument("--baseline-trial")
        item.add_argument("--first-seed", type=int)
        item.add_argument("--instructions")
        if action == "advance":
            item.add_argument("--max-steps", type=int, default=32)
    evaluation = commands.add_parser("evaluation", help="Advance independent evaluation control")
    evaluations = evaluation.add_subparsers(dest="evaluation_action", required=True)
    advance = evaluations.add_parser("advance")
    advance.add_argument("campaign_id")
    advance.add_argument("--evaluator", required=True)
    advance.add_argument("--evaluator-token-file", type=Path)
    advance.add_argument("--continuation-file", type=Path)
    advance.add_argument("--max-pages", type=int, default=4)
    advance.add_argument("--page-size", type=int, default=50)
    proposal = commands.add_parser("proposal")
    proposal_actions = proposal.add_subparsers(dest="proposal_action", required=True)
    proposal_get = proposal_actions.add_parser("get", help="Read one assigned research request")
    proposal_get.add_argument("request_id")
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
    snapshot = commands.add_parser("snapshot")
    snapshot_actions = snapshot.add_subparsers(dest="snapshot_action", required=True)
    snapshot_page = snapshot_actions.add_parser("page")
    snapshot_page.add_argument("snapshot_id")
    snapshot_page.add_argument("--limit", type=int, default=50)
    snapshot_page.add_argument("--cursor")
    snapshot_item = snapshot_actions.add_parser("item")
    snapshot_item.add_argument("snapshot_id")
    snapshot_item.add_argument("ordinal", type=int)
    snapshot_item.add_argument("--offset", type=int, default=0)
    snapshot_item.add_argument("--limit", type=int, default=64 * 1024)
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
        from research_swarm.engines.registration import read_registration
        from research_swarm.orchestration.bootstrap import RunOptions
        from research_swarm.orchestration.run import run_project

        options = RunOptions(
            manifest_path=args.manifest,
            source_revision=args.source_revision,
            primary_metric=args.primary_metric,
            seed=args.seed,
            max_trials=args.max_trials,
            search_policy=args.search_policy,
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
    if args.action == "search":
        from research_swarm.orchestration.search import SearchDriver
        from research_swarm.search.greedy import GreedyConfiguration
        from research_swarm.search.portfolio import PortfolioDriverConfiguration

        policy = client.get("campaign", args.campaign_id)["data"]["contract"]["search"]["policy"]
        config: GreedyConfiguration | PortfolioDriverConfiguration
        if policy == "portfolio":
            if any(
                value is not None for value in (args.proposer, args.first_seed, args.instructions)
            ):
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID,
                    "Portfolio proposer, seed, and instructions belong to its sealed themes",
                )
            config = PortfolioDriverConfiguration(baseline_trial_id=args.baseline_trial)
        else:
            if args.proposer is None:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Greedy search requires --proposer"
                )
            config = GreedyConfiguration(
                proposer_id=args.proposer,
                baseline_trial_id=args.baseline_trial,
                first_seed=args.first_seed if args.first_seed is not None else 0,
                **({"instructions": args.instructions} if args.instructions is not None else {}),
            )
        driver = SearchDriver(client, args.campaign_id, config)
        result = (
            driver.plan()
            if args.search_action == "plan"
            else driver.advance(max_steps=args.max_steps)
        )
        return result.model_dump(mode="json")
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
    if args.action == "proposal":
        from research_swarm.control.search_models import ProposalRead

        return client.proposal_get(ProposalRead(request_id=args.request_id))
    if args.action == "worker":
        if args.worker_action == "provision-replacement":
            from research_swarm.workers.remote_enrollment import WorkerDescription
            from research_swarm.workers.remote_replacement import provision_replacement

            bundle = provision_replacement(
                client,
                WorkerDescription.model_validate(_request(args.description)),
                fence_id=args.fence,
                expected_revision=args.revision,
                reason=args.reason,
                url=args.url,
                destination=args.bundle,
            )
            return {"state": "provisioned", "worker": bundle.worker.model_dump(mode="json")}
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
    if args.action == "snapshot":
        if args.snapshot_action == "page":
            return client.snapshot_page(
                RecoveryPage(
                    snapshot_id=args.snapshot_id,
                    limit=args.limit,
                    cursor=args.cursor,
                )
            )
        return client.snapshot_item(
            RecoveryItem(
                snapshot_id=args.snapshot_id,
                ordinal=args.ordinal,
                offset=args.offset,
                limit=args.limit,
            )
        )
    if args.action == "artifact":
        if args.artifact_action == "upload":
            return client.upload_file(args.path, idempotency_key=args.key).model_dump(mode="json")
        return client.download(args.identifier, args.destination).model_dump(mode="json")
    raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Unknown CLI operation")


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    result: Any
    try:
        if args.action == "backup":
            if args.controller:
                raise ValueError("Native backup operations cannot use a remote controller")
            from research_swarm.checkpoints.backup import export_backup, verify_backup
            from research_swarm.domain.identity import CommandContext

            if args.backup_action == "request-claim":
                if args.state_dir or args.token_file:
                    raise ValueError("Restore claim requests use their explicit destination")
                from research_swarm.checkpoints.claims import request_restore_claim

                proof = request_restore_claim(
                    args.path,
                    args.destination,
                    restore_id=args.restore_id,
                    expected_digest=args.digest,
                )
                print(canonical_bytes(proof.model_dump(mode="json")).decode("utf-8"))
                return 0
            if args.backup_action == "claim":
                if args.state_dir is None or not (args.state_dir / "identity.json").is_file():
                    raise ResearchError(
                        ErrorCode.PROJECT_CONTRACT_INVALID,
                        "Restore claims require the explicit existing source state",
                    )
                from research_swarm.checkpoints.claims import claim_restore
                from research_swarm.domain.backup import SignedRestoreRequest

                proof = SignedRestoreRequest.model_validate(_request(args.request_file))
                runtime = LocalRuntime.open(args.state_dir)
                claimed = claim_restore(
                    runtime,
                    CommandContext(
                        principal=runtime.principal(args.token_file),
                        idempotency_key="restore-claim:" + proof.request.restore_id,
                    ),
                    args.path,
                    proof,
                )
                print(canonical_bytes(claimed.model_dump(mode="json")).decode("utf-8"))
                return 0
            if args.backup_action == "restore":
                if args.state_dir or args.token_file:
                    raise ValueError("Inactive restore uses its explicit destination")
                from research_swarm.checkpoints.restore import restore_inactive

                receipt = restore_inactive(
                    args.path,
                    args.destination,
                    restore_id=args.restore_id,
                    expected_digest=args.digest,
                )
                print(canonical_bytes(receipt.model_dump(mode="json")).decode("utf-8"))
                return 0
            if args.backup_action == "export":
                runtime = LocalRuntime.open(args.state_dir)
                manifest = export_backup(
                    runtime,
                    CommandContext(
                        principal=runtime.principal(args.token_file),
                        idempotency_key="backup-export:" + args.backup_id,
                    ),
                    args.path,
                    backup_id=args.backup_id,
                )
            else:
                if args.state_dir or args.token_file:
                    raise ValueError("Backup verification does not open controller state")
                manifest = verify_backup(args.path, expected_digest=args.digest).manifest
            print(
                canonical_bytes(
                    {
                        "manifest_digest": manifest.digest,
                        "manifest": manifest.model_dump(mode="json"),
                    }
                ).decode("utf-8")
            )
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
                print(canonical_bytes(observation.model_dump(mode="json")).decode("utf-8"))
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
            print(canonical_bytes(result).decode("utf-8"))
            return 0
        if args.action == "worker" and args.worker_action in {
            "describe",
            "connect",
            "start",
            "recover",
            "install-replacement",
        }:
            from research_swarm.workers.remote_enrollment import connect_worker, describe_worker

            if args.controller or args.token_file or args.state_dir:
                raise ValueError("Worker installation uses --worker-dir and its provisioned bundle")
            if args.worker_action == "install-replacement":
                from research_swarm.workers.remote_replacement import install_replacement

                worker = install_replacement(args.original_worker_dir, args.worker_dir, args.bundle)
                print(
                    canonical_bytes(
                        {"state": "installed", "worker": worker.model_dump(mode="json")}
                    ).decode("utf-8")
                )
                return 0
            if args.worker_action == "recover":
                from research_swarm.workers.recovery import recover_worker_attempt

                report = recover_worker_attempt(args.worker_dir, args.attempt)
                print(canonical_bytes(report.model_dump(mode="json")).decode("utf-8"))
                return 0 if report.delivery_state in {"quarantined", "controller_terminal"} else 3
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
                print(canonical_bytes(description.model_dump(mode="json")).decode("utf-8"))
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
                        print(canonical_bytes(outcome.model_dump(mode="json")).decode("utf-8"))
                        return 3 if outcome.state in {"blocked", "disconnected"} else 0
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
            from research_swarm.storage.config import storage_configuration

            storage = (
                storage_configuration.validate_python(_request(args.storage_config))
                if args.action == "init" and args.storage_config is not None
                else None
            )
            runtime = LocalRuntime.open(args.state_dir, storage_config=storage)
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
