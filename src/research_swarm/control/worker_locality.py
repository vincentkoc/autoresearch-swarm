"""Expiring worker-reported cache hints never grant input or execution authority."""

from __future__ import annotations

from typing import Final, Literal

from pydantic import Field, model_validator

from research_swarm.control.workers import WorkerControl, WorkerData, worker_authority
from research_swarm.domain.clocks import ClockMilliseconds
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ID, VersionedRecord
from research_swarm.domain.workers import WorkerBinding
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Database, JsonObject, Transaction

CACHE_HINT_TTL_MS: Final = 60_000


class AdvertiseWorkerCache(VersionedRecord):
    binding: WorkerBinding
    snapshot_id: ID
    references: tuple[ArtifactRef, ...] = Field(default=(), max_length=128)
    truncated: bool = Field(default=False, strict=True)
    examined_bytes: int = Field(default=0, strict=True, ge=0, le=64 * 1024**3)

    @model_validator(mode="after")
    def unique(self) -> AdvertiseWorkerCache:
        if len({reference.id for reference in self.references}) != len(self.references):
            raise ValueError("Cache hints must identify distinct artifacts")
        if sum(reference.size_bytes for reference in self.references) > self.examined_bytes:
            raise ValueError("Cache hints exceed the reported inspection bound")
        return self


class CacheObservation(VersionedRecord):
    advertisement: AdvertiseWorkerCache
    trust: Literal["worker_reported"] = "worker_reported"
    clock_epoch: ID
    observed_ms: ClockMilliseconds
    ttl_ms: Literal[60_000] = CACHE_HINT_TTL_MS


class WorkerCacheState(VersionedRecord):
    binding: WorkerBinding
    observation: CacheObservation | None = None


def cache_state(transaction: Transaction, binding: WorkerBinding) -> JsonObject:
    try:
        row = transaction.get("worker_cache", binding.worker_session_id)
    except ResearchError as error:
        if error.code != ErrorCode.NOT_FOUND:
            raise
        return {
            "id": binding.worker_session_id,
            "kind": "worker_cache",
            "revision": 0,
            "state": "unobserved",
            "campaign_id": None,
            "parent_id": binding.worker_id,
            "data": WorkerCacheState(binding=binding).model_dump(mode="json"),
        }
    state = WorkerCacheState.model_validate(row["data"])
    if state.binding != binding or (
        state.observation is not None and state.observation.advertisement.binding != binding
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Cache hint belongs to another enrollment")
    return row


class WorkerLocality:
    def __init__(self, database: Database, workers: WorkerControl | None) -> None:
        self.database, self.workers = database, workers

    def advertise(self, context: CommandContext, request: AdvertiseWorkerCache) -> JsonObject:
        request = AdvertiseWorkerCache.model_validate(request)

        def commit(transaction: Transaction) -> JsonObject:
            worker = worker_authority(
                transaction, request.binding.worker_id, request.binding.worker_session_id
            )
            if request.binding != worker.binding:
                raise ResearchError(ErrorCode.STALE_FENCE, "Cache hint binding differs")
            previous = cache_state(transaction, worker.binding)
            if context.expected_revision != previous["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Cache observation changed")
            observation = WorkerCacheState.model_validate(previous["data"]).observation
            if (
                observation is not None
                and observation.advertisement.snapshot_id == request.snapshot_id
            ):
                if observation.advertisement != request:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT, "Current cache snapshot contents changed"
                    )
                return previous
            if self.workers is None:
                raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Cache hint clock is absent")
            for reference in request.references:
                ArtifactCatalog.available(transaction, reference)
            observed = self.workers.observation_time(worker)
            data = WorkerCacheState(
                binding=worker.binding,
                observation=CacheObservation(
                    advertisement=request,
                    clock_epoch=observed.epoch,
                    observed_ms=observed.milliseconds,
                ),
            ).model_dump(mode="json")
            if previous["revision"] == 0:
                row = transaction.insert(
                    "worker_cache",
                    worker.binding.worker_session_id,
                    data,
                    state="observed",
                    parent_id=worker.binding.worker_id,
                )
            else:
                row = transaction.update(
                    "worker_cache",
                    worker.binding.worker_session_id,
                    expected_revision=previous["revision"],
                    state="observed",
                    data=data,
                )
            transaction.event("worker.cache_observed", row, {"snapshot_id": request.snapshot_id})
            return row

        return self.database.execute(
            context,
            "worker.cache_advertise",
            request.model_dump(mode="json"),
            "worker.publish",
            commit,
        )

    def fresh_references(
        self, transaction: Transaction, worker: WorkerData
    ) -> tuple[ArtifactRef, ...]:
        """Ignore stale/unavailable hints; callers still perform ordinary admission."""
        if self.workers is None:
            return ()
        try:
            state = WorkerCacheState.model_validate(
                cache_state(transaction, worker.binding)["data"]
            )
            observation = state.observation
            if observation is None:
                return ()
            now = self.workers.observation_time(worker)
            if now.epoch != observation.clock_epoch or not (
                0 <= now.milliseconds - observation.observed_ms < observation.ttl_ms
            ):
                return ()
            available = []
            for reference in observation.advertisement.references:
                try:
                    ArtifactCatalog.available(transaction, reference)
                except ResearchError as error:
                    if error.code not in {ErrorCode.NOT_FOUND, ErrorCode.ARTIFACT_UNVERIFIED}:
                        raise
                else:
                    available.append(reference)
            return tuple(available)
        except ResearchError as error:
            if error.code not in {ErrorCode.CAPABILITY_UNAVAILABLE, ErrorCode.STALE_FENCE}:
                raise
            return ()
