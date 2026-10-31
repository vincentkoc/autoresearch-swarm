"""Best-effort cache observations stay outside heartbeat and lease control work."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from research_swarm.control.worker_locality import AdvertiseWorkerCache
from research_swarm.control.worker_queries import WorkerRead
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.workers.local import LocalWorker


@dataclass(frozen=True)
class PendingObservation:
    request: AdvertiseWorkerCache
    revision: int
    key: str


class WorkerCacheReporter:
    def __init__(self, worker: LocalWorker) -> None:
        self.worker = worker
        self._next_refresh = 0.0
        self._pending: PendingObservation | None = None
        self._lock = asyncio.Lock()
        self.last_error: ErrorCode | None = None

    def _refresh(self) -> None:
        try:
            if self._pending is None:
                binding = self.worker.worker.binding
                current = self.worker.client.worker_get(
                    WorkerRead(
                        worker_id=binding.worker_id,
                        session_id=binding.worker_session_id,
                        kind="worker_cache",
                        id=binding.worker_session_id,
                    )
                )
                snapshot = self.worker.input_cache.verified_snapshot()
                previous = current["data"]["observation"]
                if not snapshot.references and (
                    previous is None or not previous["advertisement"]["references"]
                ):
                    self.last_error = None
                    return
                # The cache lock is already released before any network write.
                self._pending = PendingObservation(
                    AdvertiseWorkerCache(
                        binding=binding,
                        snapshot_id=new_id(),
                        references=snapshot.references,
                        truncated=snapshot.truncated,
                        examined_bytes=snapshot.examined_bytes,
                    ),
                    current["revision"],
                    new_id(),
                )
            pending = self._pending
            self.worker.client.command(
                "worker.cache_advertise",
                pending.request,
                expected_revision=pending.revision,
                idempotency_key=pending.key,
            )
        except ResearchError as error:
            # A hint never decides admission. Required control reads/claims still
            # enforce authorization, continuity and availability independently.
            self.last_error = error.code
            if error.code != ErrorCode.DELIVERY_UNCERTAIN:
                self._pending = None
        except (OSError, ValueError):
            # A local write may already have committed. Keep its frozen intent
            # for replay; failures before preparation can simply skip this hint.
            self.last_error = (
                ErrorCode.DELIVERY_UNCERTAIN
                if self._pending is not None
                else ErrorCode.CAPABILITY_UNAVAILABLE
            )
        else:
            self._pending = None
            self.last_error = None

    async def refresh(self) -> None:
        async with self._lock:
            now = asyncio.get_running_loop().time()
            if now < self._next_refresh:
                return
            self._next_refresh = now + 15.0
            task = asyncio.create_task(asyncio.to_thread(self._refresh))
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                # Keep the driver's ownership until this single optional write
                # completes; cancellation of an awaiter does not stop its thread.
                drain = asyncio.gather(task, return_exceptions=True)
                while not drain.done():
                    try:
                        await asyncio.shield(drain)
                    except asyncio.CancelledError:
                        continue
                raise
