"""Outbound worker scheduling with one scoped API identity and durable recovery."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from functools import partial
from typing import Any, Literal, TypeVar

import httpx
from filelock import FileLock, Timeout
from pydantic import Field

from research_swarm.client import HTTPTransport
from research_swarm.control.worker_queries import WorkerQuery, WorkerRead
from research_swarm.control.workers import ClaimJob, WorkerData
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, VersionedRecord
from research_swarm.orchestration.intents import CommandIntents
from research_swarm.orchestration.worker_controls import WorkerControlSession
from research_swarm.runtime import private_state_directory, read_private, write_private
from research_swarm.state.database import JsonObject
from research_swarm.workers.journal_records import JournalState
from research_swarm.workers.remote_enrollment import RemoteEnrollment

TRANSIENT_DELIVERY = {ErrorCode.DELIVERY_UNCERTAIN, ErrorCode.TRANSPORT_UNAVAILABLE}
ADMISSION_REFUSALS = {
    ErrorCode.RESOURCE_UNAVAILABLE,
    ErrorCode.REVISION_CONFLICT,
    ErrorCode.BUDGET_EXHAUSTED,
    ErrorCode.INVALID_TRANSITION,
    ErrorCode.CAPABILITY_UNAVAILABLE,
    ErrorCode.EXPOSURE_LIMIT_REACHED,
}
CallResult = TypeVar("CallResult")


class ScopedDriverResult(VersionedRecord):
    worker_id: ID
    state: Literal["idle", "blocked", "disconnected", "stopped"]
    observation_count: int = Field(default=0, ge=0)
    recent_attempts: tuple[ID, ...] = Field(default=(), max_length=50)
    finding_count: int = Field(default=0, ge=0)
    findings: tuple[str, ...] = Field(default=(), max_length=50)


class ScopedWorkerDriver:
    def __init__(
        self,
        enrollment: RemoteEnrollment,
        *,
        poll_ms: int = 500,
        maximum_backoff_ms: int = 15_000,
        heartbeat_ms: int = 15_000,
    ) -> None:
        worker, bundle = enrollment.worker, enrollment.bundle
        transport = worker.client.transport
        if (
            not isinstance(transport, HTTPTransport)
            or worker.worker != bundle.worker
            or str(transport.http.base_url).rstrip("/")
            != str(httpx.URL(bundle.controller_url)).rstrip("/")
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Scoped driver connection differs")
        if (
            type(poll_ms) is not int
            or type(maximum_backoff_ms) is not int
            or not 50 <= poll_ms <= maximum_backoff_ms <= 60_000
        ):
            raise ValueError("Polling requires 50 <= poll_ms <= maximum_backoff_ms <= 60000")
        self.enrollment, self.worker = enrollment, worker
        self.poll_ms, self.maximum_backoff_ms = poll_ms, maximum_backoff_ms
        if type(heartbeat_ms) is not int or not 50 <= heartbeat_ms <= 60_000:
            raise ValueError("Heartbeat interval must be between 50 and 60000 ms")
        self.heartbeat_ms = heartbeat_ms
        self.root = worker.root / "driver"
        with private_state_directory(self.root):
            pass
        self.stop = asyncio.Event()
        self._control_tasks: set[asyncio.Future[Any]] = set()
        self._control_executor: ThreadPoolExecutor | None = None
        self._control_session: WorkerControlSession | None = None
        self._sync_lock = asyncio.Lock()
        self._pump_stop = asyncio.Event()
        self._pump_task: asyncio.Task[None] | None = None
        self._control_error: ResearchError | None = None
        self._pump_failure: ResearchError | None = None
        self._admission: Literal["active", "draining"] = "draining"

    async def _blocking(self, call: Callable[..., CallResult], *args: Any) -> CallResult:
        assert self._control_executor is not None
        task = asyncio.get_running_loop().run_in_executor(
            self._control_executor, partial(call, *args)
        )
        self._control_tasks.add(task)
        try:
            return await asyncio.shield(task)
        finally:
            if task.done():
                self._control_tasks.discard(task)

    async def _sync_controls(self) -> None:
        assert self._control_session is not None
        async with self._sync_lock:
            self._check_pump()
            task = asyncio.create_task(self._blocking(self._control_session.synchronize))
            try:
                self._admission = await asyncio.shield(task)
                self._check_pump()
                self._control_error = None
            except asyncio.CancelledError:
                # A cancelled await cannot release the single heartbeat writer
                # while its HTTP call and durable file update are still running.
                drain = asyncio.gather(task, return_exceptions=True)
                while not drain.done():
                    try:
                        await asyncio.shield(drain)
                    except asyncio.CancelledError:
                        continue
                raise
            except ResearchError as error:
                self._control_error = error
                raise

    async def _control_loop(self) -> None:
        delay = self.poll_ms
        while not self._pump_stop.is_set():
            try:
                await self._sync_controls()
            except ResearchError as error:
                if error.code not in TRANSIENT_DELIVERY | {ErrorCode.RESOURCE_UNAVAILABLE}:
                    self._pump_failure = error
                    return
                delay = min(self.maximum_backoff_ms, delay * 2)
            else:
                delay = self.poll_ms
            with suppress(TimeoutError):
                await asyncio.wait_for(self._pump_stop.wait(), timeout=delay / 1000)

    def _fresh_admission(self) -> bool:
        return (
            self._admission == "active"
            and self._control_error is None
            and self._pump_failure is None
        )

    def _check_pump(self) -> None:
        if self._pump_failure is not None:
            raise self._pump_failure
        if self._pump_task is not None and self._pump_task.done():
            self._pump_task.result()

    async def _drain(self, active: dict[str, asyncio.Task[JsonObject]]) -> None:
        # Keep controls flowing until owned execution closes, including shutdown.
        await asyncio.gather(*active.values(), return_exceptions=True)
        self._pump_stop.set()
        if self._pump_task is not None:
            await asyncio.gather(self._pump_task, return_exceptions=True)
        await asyncio.gather(*self._control_tasks, return_exceptions=True)

    def _identity(self) -> None:
        bundle = self.enrollment.bundle
        identity = {
            "controller_url": bundle.controller_url,
            "credential_id": bundle.credential_id,
            "worker": bundle.worker.model_dump(mode="json"),
            "journal_identity": self.worker.journal.identity,
            "registry_identity": self.worker.resources.identity,
        }
        path = self.root / "identity.json"
        if path.exists():
            if strict_json_loads(read_private(self.root, path.name, limit=128 * 1024)) != identity:
                raise ResearchError(ErrorCode.STALE_FENCE, "Scoped driver authority changed")
        else:
            if (self.root / "commands").exists():
                raise ResearchError(ErrorCode.STALE_FENCE, "Scoped driver identity is missing")
            write_private(path, canonical_bytes(identity))

    def _current(self) -> None:
        binding = self.worker.worker.binding
        row = self.worker.client.worker_get(
            WorkerRead(
                worker_id=binding.worker_id,
                session_id=binding.worker_session_id,
                kind="worker",
                id=binding.worker_id,
            )
        )
        if row["state"] != "active" or WorkerData.model_validate(row["data"]) != self.worker.worker:
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Scoped worker authority is no longer current"
            )

    async def _records(self, kind: Literal["pending", "offers"]) -> AsyncIterator[JsonObject]:
        binding = self.worker.worker.binding
        cursor = None
        for _ in range(128):
            page = await self._blocking(
                self.worker.client.worker_list,
                WorkerQuery(
                    worker_id=binding.worker_id,
                    session_id=binding.worker_session_id,
                    kind=kind,
                    limit=100,
                    cursor=cursor,
                ),
            )
            for item in page["items"]:
                yield item
            cursor = page["next_cursor"]
            if cursor is None:
                return
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE, "Worker discovery exceeds its page bound"
        )

    def _claim(self, offer: JsonObject, intents: CommandIntents) -> JsonObject:
        binding = self.worker.worker.binding
        return dict(
            intents.command(
                "claim:" + offer["id"] + ":" + str(offer["revision"]),
                "worker",
                "worker.claim_job",
                ClaimJob(
                    worker_id=binding.worker_id,
                    session_id=binding.worker_session_id,
                    job_id=offer["id"],
                ),
                revision=offer["revision"],
                retry_rejected=True,
            )["result"]
        )

    def _admission_blocked(
        self, active: dict[str, asyncio.Task[JsonObject]], recovery_fences: set[str]
    ) -> bool:
        # An awaited RPC can outlive a native outcome. Let the owner loop process
        # completed tasks before another admission or release into execution.
        if any(task.done() for task in active.values()):
            return True
        for entry in self.worker.journal.unresolved():
            identifier = entry.dispatch.attempt_id
            if entry.state == JournalState.UNKNOWN or (
                identifier not in active and entry.state != JournalState.RECEIVED
            ):
                recovery_fences.add(identifier)
        return bool(recovery_fences)

    async def _schedule(
        self,
        intents: CommandIntents,
        active: dict[str, asyncio.Task[JsonObject]],
        deferred: dict[str, ErrorCode],
        recovery_fences: set[str],
        findings: set[str],
    ) -> None:
        await self._blocking(self._current)
        await self._blocking(intents.recover)
        self._check_pump()
        await self._sync_controls()
        if self._pump_task is None:
            self._pump_task = asyncio.create_task(self._control_loop())
        pending = {row["id"]: row async for row in self._records("pending")}
        blocked = False
        for entry in self.worker.journal.unresolved():
            identifier = entry.dispatch.attempt_id
            if identifier not in active and entry.state != JournalState.RECEIVED:
                recovery_fences.add(identifier)
            if identifier not in pending and identifier not in active:
                findings.add("EXECUTION_UNKNOWN:" + identifier)
                blocked = True
        for identifier in recovery_fences - pending.keys() - active.keys():
            entry = self.worker.journal.get(identifier)
            if entry.state not in {JournalState.CLOSED, JournalState.NOT_STARTED}:
                findings.add("EXECUTION_UNKNOWN:" + identifier)
                blocked = True
                continue
            binding = self.worker.worker.binding
            pending[identifier] = await self._blocking(
                self.worker.client.worker_get,
                WorkerRead(
                    worker_id=binding.worker_id,
                    session_id=binding.worker_session_id,
                    kind="attempt",
                    id=identifier,
                ),
            )
        for identifier in pending:
            if identifier in active:
                continue
            if any(task.done() for task in active.values()):
                return
            self._admission_blocked(active, recovery_fences)
            try:
                local = self.worker.journal.get(identifier)
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
                local = None
            if pending[identifier]["state"] == "execution_unknown":
                recovery_fences.add(identifier)
                try:
                    self.worker.journal.get(identifier)
                except ResearchError as error:
                    if error.code != ErrorCode.NOT_FOUND:
                        raise
                    findings.add("EXECUTION_UNKNOWN:" + identifier)
                    blocked = True
                    continue
            if identifier in deferred:
                findings.add(deferred[identifier].value + ":" + identifier)
                blocked = True
            elif recovery_fences - {identifier} and (
                local is None or local.state == JournalState.RECEIVED
            ):
                # Claims accepted during an in-flight failure stay reserved.
                # Reconcile existing native work before starting them.
                blocked = True
            elif len(active) < self.worker.worker.max_concurrency and not self.stop.is_set():
                await self._sync_controls()
                self._check_pump()
                if self.stop.is_set() or any(task.done() for task in active.values()):
                    return
                self._admission_blocked(active, recovery_fences)
                if recovery_fences - {identifier} and (
                    local is None or local.state == JournalState.RECEIVED
                ):
                    return
                active[identifier] = asyncio.create_task(
                    asyncio.to_thread(self.worker.execute, identifier)
                )
            else:
                blocked = True
        if blocked or recovery_fences or self.stop.is_set() or not self._fresh_admission():
            return
        assert self.worker.clock_sampler is not None
        self.worker.clock_sampler.read()
        async for offer in self._records("offers"):
            if (
                len(active) >= self.worker.worker.max_concurrency
                or self.stop.is_set()
                or not self._fresh_admission()
                or self._admission_blocked(active, recovery_fences)
            ):
                break
            try:
                attempt = await self._blocking(self._claim, offer, intents)
            except ResearchError as error:
                if error.code not in ADMISSION_REFUSALS:
                    raise
                findings.add(error.code.value + ":" + offer["id"])
                continue
            await self._sync_controls()
            self._check_pump()
            if self.stop.is_set() or self._admission_blocked(active, recovery_fences):
                # The accepted command is durable; the next pending pass owns it.
                return
            active[attempt["id"]] = asyncio.create_task(
                asyncio.to_thread(self.worker.execute, attempt["id"])
            )
            await asyncio.sleep(0)

    async def _wait(self, active: dict[str, asyncio.Task[JsonObject]], timeout_ms: int) -> None:
        if self.stop.is_set():
            if active:
                await asyncio.wait(active.values(), return_when=asyncio.FIRST_COMPLETED)
            return
        requested = asyncio.create_task(self.stop.wait())
        try:
            await asyncio.wait(
                (*active.values(), requested),
                timeout=timeout_ms / 1000,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            requested.cancel()
            with suppress(asyncio.CancelledError):
                await requested

    async def run(self, *, once: bool = False) -> ScopedDriverResult:
        lock = FileLock(self.root / "driver.lock", timeout=0)
        try:
            lock.acquire()
        except Timeout as error:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Scoped worker driver is owned"
            ) from error
        active: dict[str, asyncio.Task[JsonObject]] = {}
        deferred: dict[str, ErrorCode] = {}
        retry_at: dict[str, float] = {}
        recovery_fences: set[str] = set()
        recent: deque[str] = deque(maxlen=50)
        observations, backoff_ms = 0, self.poll_ms
        findings: set[str] = set()
        disconnected = False
        try:
            self._identity()
            self._control_executor = ThreadPoolExecutor(
                max_workers=2, thread_name_prefix="research-control"
            )
            self._control_session = WorkerControlSession(
                self.worker.client,
                self.worker.worker.binding,
                self.worker.journal,
                self.root,
                heartbeat_ms=self.heartbeat_ms,
            )
            intents = CommandIntents(
                self.root / "commands",
                {"worker": self.worker.client},
                remote_worker=self.worker.worker,
            )
            while True:
                capacity_released = False
                for identifier, task in tuple(active.items()):
                    if not task.done():
                        continue
                    del active[identifier]
                    try:
                        result = task.result()
                    except ResearchError as error:
                        if error.code in {ErrorCode.AUTHORIZATION_DENIED, ErrorCode.STALE_FENCE}:
                            raise
                        deferred[identifier] = error.code
                        retry_at[identifier] = (
                            asyncio.get_running_loop().time() + self.maximum_backoff_ms / 1000
                        )
                        recovery_fences.add(identifier)
                        if error.code in TRANSIENT_DELIVERY:
                            disconnected = True
                    else:
                        observations += 1
                        recent.append(identifier)
                        entry = self.worker.journal.get(identifier)
                        if (
                            result["state"]
                            not in {
                                "succeeded",
                                "failed",
                                "cancelled",
                                "not_started",
                                "closed_unaccepted",
                            }
                            or result["data"]["publication_status"]
                            not in {"accepted", "closed_unaccepted"}
                            or entry.state not in {JournalState.CLOSED, JournalState.NOT_STARTED}
                        ):
                            deferred[identifier] = ErrorCode.EXECUTION_UNKNOWN
                            retry_at[identifier] = (
                                asyncio.get_running_loop().time() + self.maximum_backoff_ms / 1000
                            )
                            recovery_fences.add(identifier)
                        else:
                            capacity_released = True
                            recovery_fences.discard(identifier)
                            retry_at.pop(identifier, None)
                if capacity_released:
                    deferred = {
                        key: code
                        for key, code in deferred.items()
                        if code != ErrorCode.RESOURCE_UNAVAILABLE
                    }
                    backoff_ms = self.poll_ms
                now = asyncio.get_running_loop().time()
                for identifier in tuple(deferred):
                    if not once and now >= retry_at.get(identifier, now):
                        del deferred[identifier]
                        retry_at.pop(identifier, None)
                findings = {code.value + ":" + identifier for identifier, code in deferred.items()}
                if not self.stop.is_set():
                    try:
                        await self._schedule(intents, active, deferred, recovery_fences, findings)
                    except ResearchError as error:
                        if error.code in TRANSIENT_DELIVERY:
                            disconnected = True
                            findings.add(error.code.value)
                        elif error.code in ADMISSION_REFUSALS:
                            findings.add(error.code.value)
                        else:
                            raise
                    else:
                        disconnected = any(code in TRANSIENT_DELIVERY for code in deferred.values())
                if not active:
                    if once or self.stop.is_set():
                        return ScopedDriverResult(
                            worker_id=self.worker.worker.binding.worker_id,
                            state="stopped"
                            if self.stop.is_set()
                            else "disconnected"
                            if disconnected
                            else "blocked"
                            if findings
                            else "idle",
                            observation_count=observations,
                            recent_attempts=tuple(recent),
                            finding_count=len(findings),
                            findings=tuple(sorted(findings)[:50]),
                        )
                    await self._wait(active, backoff_ms)
                    backoff_ms = min(self.maximum_backoff_ms, backoff_ms * 2)
                else:
                    await self._wait(active, backoff_ms if disconnected else self.poll_ms)
        finally:
            self.stop.set()
            try:
                drain = asyncio.create_task(self._drain(active))
                while not drain.done():
                    try:
                        await asyncio.shield(drain)
                    except asyncio.CancelledError:
                        continue
                await drain
            finally:
                self._control_tasks.clear()
                if self._control_executor is not None:
                    self._control_executor.shutdown(wait=True)
                    self._control_executor = None
                lock.release()
