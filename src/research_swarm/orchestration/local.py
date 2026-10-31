"""Bounded asyncio scheduling over the same commands used by external agents."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Literal

from filelock import FileLock, Timeout
from pydantic import Field, TypeAdapter

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.models import JobData
from research_swarm.control.queries import RecordKind, RecordQuery
from research_swarm.control.worker_queries import WorkerQuery
from research_swarm.control.workers import AttemptData, ClaimJob
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, VersionedRecord
from research_swarm.orchestration.cache_reporting import WorkerCacheReporter
from research_swarm.orchestration.intents import CommandIntent, CommandIntents
from research_swarm.runtime import private_state_directory, read_private, write_private
from research_swarm.state.database import JsonObject
from research_swarm.workers.local_enrollment import LocalEnrollment


class DriverResult(VersionedRecord):
    campaign_id: ID
    state: Literal["idle", "paused", "blocked", "terminal", "stopped"]
    executed_attempt_count: int = Field(default=0, ge=0)
    executed_attempts: tuple[ID, ...] = Field(default=(), max_length=50)
    finding_count: int = Field(default=0, ge=0)
    findings: tuple[str, ...] = Field(default=(), max_length=50)


class LocalDriver:
    def __init__(
        self,
        controller: ResearchClient,
        enrollment: LocalEnrollment,
        *,
        concurrency: int | None = None,
    ) -> None:
        if not isinstance(controller.transport, LocalTransport):
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "This driver requires a local controller"
            )
        self.controller, self.enrollment = controller, enrollment
        self.worker = enrollment.worker
        self.concurrency = (
            self.worker.worker.max_concurrency if concurrency is None else concurrency
        )
        if type(self.concurrency) is not int or not (
            1 <= self.concurrency <= self.worker.worker.max_concurrency
        ):
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Driver concurrency exceeds worker admission"
            )
        self.root = self.worker.root / "driver"
        with private_state_directory(self.root):
            pass
        self.stop = asyncio.Event()
        self._cache_reporter = WorkerCacheReporter(self.worker)

    def _offers(self, campaign_id: str) -> Iterator[JsonObject]:
        cursor = None
        binding = self.worker.worker.binding
        while True:
            page = self.worker.client.worker_list(
                WorkerQuery(
                    worker_id=binding.worker_id,
                    session_id=binding.worker_session_id,
                    kind="offers",
                    campaign_id=campaign_id,
                    limit=100,
                    cursor=cursor,
                )
            )
            yield from page["items"]
            cursor = page["next_cursor"]
            if cursor is None:
                return

    def _records(self, kind: RecordKind, campaign_id: str | None = None) -> Iterator[JsonObject]:
        cursor = None
        while True:
            page = self.controller.list(
                RecordQuery(
                    kind=kind,
                    campaign_id=campaign_id,
                    limit=100,
                    cursor=cursor,
                )
            )
            yield from page["items"]
            cursor = page["next_cursor"]
            if cursor is None:
                return

    def _identity(self) -> None:
        transport = self.controller.transport
        assert isinstance(transport, LocalTransport)
        identity = {
            "binding": self.worker.worker.binding.model_dump(mode="json"),
            "controller_actor": transport.resolve_principal().id,
            "evaluator_actor": self.enrollment.evaluator_id,
        }
        path = self.root / "identity.json"
        if path.exists():
            prior = strict_json_loads(read_private(self.root, path.name, limit=65536))
            if identity != prior:
                raise ResearchError(ErrorCode.STALE_FENCE, "Driver delivery identity changed")
        else:
            if (self.root / "commands").exists():
                raise ResearchError(ErrorCode.STALE_FENCE, "Driver identity is missing")
            write_private(path, canonical_bytes(identity))

    def _closed_trial(self, trial_id: str) -> bool:
        return bool(
            self.controller.get("trial", trial_id)["data"]["execution_outcome"]
            == "closed_unaccepted"
        )

    def _closed_receipt_rejection(self, intent: CommandIntent, error: ResearchError) -> bool:
        if (
            intent.route != "evaluator"
            or intent.operation != "evaluation.complete"
            or error.code != ErrorCode.COMPARISON_INELIGIBLE
        ):
            return False
        request = self.controller.get("evaluation_request", intent.payload["request_id"])
        return self._closed_trial(request["data"]["request"]["trial_id"])

    def _receipts(self, campaign_id: str, intents: CommandIntents) -> list[str]:
        findings = []
        for summary in self._records("job", campaign_id):
            if summary["state"] != "succeeded" or summary["data"]["stage_kind"] != "evaluate":
                continue
            row = self.controller.get("job", summary["id"])
            job = JobData.model_validate(row["data"])
            if self._closed_trial(job.trial_id):
                continue
            if job.evaluation_request_id is None or job.current_attempt_id is None:
                findings.append("EVALUATION_INCOMPLETE:" + row["id"])
                continue
            request = self.controller.get("evaluation_request", job.evaluation_request_id)["data"]
            if request["receipt_id"] is not None:
                continue
            if request["request"]["evaluator_id"] != self.enrollment.evaluator_id:
                findings.append("AUTHORIZATION_DENIED:" + job.evaluation_request_id)
                continue
            try:
                intents.command(
                    "complete:" + job.evaluation_request_id,
                    "evaluator",
                    "evaluation.complete",
                    CompleteEvaluation(
                        request_id=job.evaluation_request_id, attempt_id=job.current_attempt_id
                    ),
                )
            except ResearchError as error:
                if error.code != ErrorCode.COMPARISON_INELIGIBLE or not self._closed_trial(
                    job.trial_id
                ):
                    raise
        return findings

    def _authorize_evaluation(
        self, summary: JsonObject, intents: CommandIntents
    ) -> JsonObject | None:
        row = self.controller.get("job", summary["id"])
        if row["state"] != "ready":
            return None
        job = JobData.model_validate(row["data"])
        assert self.worker.profile is not None
        if job.stage.resources.engine_profile != self.worker.profile.name:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Job requires another engine")
        if job.stage.kind == "evaluate":
            if job.evaluation_request_id is None:
                intents.command(
                    "authorize:" + row["id"] + ":" + str(row["revision"]),
                    "controller",
                    "evaluation.authorize",
                    AuthorizeEvaluation(
                        job_id=row["id"], evaluator_id=self.enrollment.evaluator_id
                    ),
                    revision=row["revision"],
                    retry_rejected=True,
                )
                row = self.controller.get("job", row["id"])
                job = JobData.model_validate(row["data"])
            assert job.evaluation_request_id is not None
            authorization = self.controller.get("evaluation_request", job.evaluation_request_id)
            if authorization["data"]["request"]["evaluator_id"] != self.enrollment.evaluator_id:
                raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Job has another evaluator")
        return row

    def _claim(self, summary: JsonObject, intents: CommandIntents) -> JsonObject | None:
        row = self._authorize_evaluation(summary, intents)
        if row is None:
            return None
        binding = self.worker.worker.binding
        return dict(
            intents.command(
                "claim:" + row["id"] + ":" + str(row["revision"]),
                "worker",
                "worker.claim_job",
                ClaimJob(
                    worker_id=binding.worker_id,
                    session_id=binding.worker_session_id,
                    job_id=row["id"],
                ),
                revision=row["revision"],
                retry_rejected=True,
            )["result"]
        )

    def _recover_attempts(
        self,
        campaign_id: str,
        active: dict[str, asyncio.Task[JsonObject]],
        deferred: dict[str, ErrorCode],
        findings: set[str],
    ) -> bool:
        # Recover previously admitted work before considering new jobs.
        unresolved = False
        for summary in self._records("attempt"):
            if summary["data"]["worker_id"] != self.worker.worker.binding.worker_id:
                continue
            if summary["state"] in {
                "succeeded",
                "failed",
                "cancelled",
                "not_started",
                "closed_unaccepted",
            }:
                continue
            row = self.controller.get("attempt", summary["id"])
            attempt = AttemptData.model_validate(row["data"])
            if attempt.dispatch.campaign_id != campaign_id:
                findings.add("RESOURCE_UNAVAILABLE:" + row["id"])
                unresolved = True
                continue
            binding = self.worker.worker.binding
            if any(
                getattr(attempt.dispatch, name) != getattr(binding, name)
                for name in (
                    "workspace_id",
                    "controller_id",
                    "control_epoch",
                    "worker_id",
                    "worker_session_id",
                )
            ):
                findings.add("STALE_FENCE:" + row["id"])
                unresolved = True
                continue
            if row["id"] in deferred:
                findings.add(deferred[row["id"]].value + ":" + row["id"])
                unresolved = True
            elif row["id"] not in active:
                unresolved = True
                if len(active) < self.concurrency and not self.stop.is_set():
                    active[row["id"]] = asyncio.create_task(
                        asyncio.to_thread(self.worker.execute, row["id"])
                    )
        return unresolved

    async def drain(self, campaign_id: str) -> DriverResult:
        campaign_id = TypeAdapter(ID).validate_python(campaign_id)
        lock = FileLock(self.root / "driver.lock", timeout=0)
        try:
            lock.acquire()
        except Timeout as error:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Local worker driver is owned"
            ) from error
        active: dict[str, asyncio.Task[JsonObject]] = {}
        executed: set[str] = set()
        deferred: dict[str, ErrorCode] = {}
        findings: set[str] = set()
        try:
            self._identity()
            intents = CommandIntents(
                self.root / "commands" / campaign_id,
                {
                    "controller": self.controller,
                    "worker": self.worker.client,
                    "evaluator": self.enrollment.evaluator,
                },
            )
            intents.recover(ignore_rejection=self._closed_receipt_rejection)
            while True:
                findings = set(self._receipts(campaign_id, intents))
                campaign = self.controller.get("campaign", campaign_id)
                unresolved = self._recover_attempts(campaign_id, active, deferred, findings)
                if (
                    not unresolved
                    and not self.stop.is_set()
                    and campaign["state"] in {"running", "pausing"}
                ):
                    # Evaluation requests must exist before eligible offer discovery.
                    for summary in self._records("job", campaign_id):
                        if self.stop.is_set():
                            break
                        if (
                            summary["state"] == "ready"
                            and summary["data"]["stage_kind"] == "evaluate"
                        ):
                            try:
                                self._authorize_evaluation(summary, intents)
                            except ResearchError as error:
                                if error.code in {
                                    ErrorCode.AUTHORIZATION_DENIED,
                                    ErrorCode.STALE_FENCE,
                                }:
                                    raise
                                findings.add(error.code.value + ":" + summary["id"])
                    await self._cache_reporter.refresh()
                    offers = () if self.stop.is_set() else self._offers(campaign_id)
                    for summary in offers:
                        if self.stop.is_set() or len(active) >= self.concurrency:
                            break
                        if summary["state"] != "ready":
                            continue
                        if self.controller.get("campaign", campaign_id)["state"] not in {
                            "running",
                            "pausing",
                        }:
                            break
                        if (
                            campaign["state"] == "pausing"
                            and self.controller.get("trial", summary["data"]["trial_id"])["state"]
                            != "running"
                        ):
                            continue
                        try:
                            attempt_row = self._claim(summary, intents)
                        except ResearchError as error:
                            if error.code in {
                                ErrorCode.AUTHORIZATION_DENIED,
                                ErrorCode.STALE_FENCE,
                            }:
                                raise
                            findings.add(error.code.value + ":" + summary["id"])
                            continue
                        if attempt_row is not None:
                            active[attempt_row["id"]] = asyncio.create_task(
                                asyncio.to_thread(self.worker.execute, attempt_row["id"])
                            )
                if not active:
                    state: Literal["idle", "paused", "blocked", "terminal", "stopped"]
                    if self.stop.is_set():
                        state = "stopped"
                    elif findings or unresolved:
                        state = "blocked"
                    elif campaign["state"] in {"paused", "pausing"}:
                        state = "paused"
                    elif campaign["state"] in {"completed", "cancelled", "failed"}:
                        state = "terminal"
                    else:
                        pending = any(
                            row["state"]
                            not in {"succeeded", "failed", "cancelled", "closed_unaccepted"}
                            for row in self._records("trial", campaign_id)
                        )
                        state = "blocked" if pending else "idle"
                        if pending:
                            findings.add("UNFINISHED_WORK:" + campaign_id)
                    return DriverResult(
                        campaign_id=campaign_id,
                        state=state,
                        executed_attempt_count=len(executed),
                        executed_attempts=tuple(sorted(executed)[:50]),
                        finding_count=len(findings),
                        findings=tuple(sorted(findings)[:50]),
                    )
                done, _ = await asyncio.wait(active.values(), return_when=asyncio.FIRST_COMPLETED)
                capacity_released = False
                for identifier, task in list(active.items()):
                    if task not in done:
                        continue
                    del active[identifier]
                    try:
                        observed = task.result()
                    except ResearchError as error:
                        if error.code not in {
                            ErrorCode.RESOURCE_UNAVAILABLE,
                            ErrorCode.ACCEPTANCE_EXPIRED,
                        }:
                            raise
                        deferred[identifier] = error.code
                    else:
                        executed.add(identifier)
                        if observed["state"] == "execution_unknown":
                            deferred[identifier] = ErrorCode.EXECUTION_UNKNOWN
                        else:
                            capacity_released = True
                if capacity_released:
                    # Process the entire batch before retrying capacity refusals.
                    # A sibling's closure does not resolve uncertain execution.
                    deferred = {
                        identifier: reason
                        for identifier, reason in deferred.items()
                        if reason != ErrorCode.RESOURCE_UNAVAILABLE
                    }
        finally:
            # Cancelling an awaiter does not stop native work. Retain this owner lock
            # until all launched threads finish their own closure/publication path.
            if active:
                drain = asyncio.gather(*active.values(), return_exceptions=True)
                while not drain.done():
                    try:
                        await asyncio.shield(drain)
                    except asyncio.CancelledError:
                        self.stop.set()
            lock.release()
