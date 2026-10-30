"""Bounded control synchronization with one durable heartbeat envelope."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from research_swarm.client import ResearchClient
from research_swarm.control.worker_lifecycle import WorkerHeartbeat
from research_swarm.control.worker_queries import WorkerQuery, WorkerRead
from research_swarm.control.worker_state import AttemptCancellation, WorkerAdmission, WorkerPresence
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.controls import CancellationDirective
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, VersionedRecord, new_id
from research_swarm.domain.workers import WorkerBinding
from research_swarm.runtime import read_private, write_private
from research_swarm.state.database import JsonObject
from research_swarm.workers.journal import WorkerJournal


class PendingHeartbeat(VersionedRecord):
    request: WorkerHeartbeat
    key: ID = Field(default_factory=new_id)


class HeartbeatState(VersionedRecord):
    binding: WorkerBinding
    sequence: int = Field(default=0, strict=True, ge=0, le=9_007_199_254_740_991)
    pending: PendingHeartbeat | None = None

    @model_validator(mode="after")
    def ordered(self) -> HeartbeatState:
        if self.pending is not None and (
            self.pending.request.binding != self.binding
            or self.pending.request.sequence != self.sequence + 1
        ):
            raise ValueError("Pending heartbeat differs from its durable predecessor")
        return self


class WorkerControlSession:
    """The driver serializes calls and retains its lock until each call finishes."""

    def __init__(
        self,
        client: ResearchClient,
        binding: WorkerBinding,
        journal: WorkerJournal,
        root: Path,
        *,
        heartbeat_ms: int = 15_000,
    ) -> None:
        if type(heartbeat_ms) is not int or not 50 <= heartbeat_ms <= 60_000:
            raise ValueError("Heartbeat interval must be between 50 and 60000 ms")
        self.client, self.binding, self.journal = client, binding, journal
        self.path = root / "heartbeat.json"
        self.heartbeat_ms = heartbeat_ms
        self.driver_instance_id = new_id()
        self._last_heartbeat = float("-inf")
        self._initialized = False
        self._state: HeartbeatState | None = None

    def _save(self, state: HeartbeatState) -> None:
        write_private(self.path, canonical_bytes(state.model_dump(mode="json")))
        self._state = state

    def _get(self, kind: Literal["worker_admission", "worker_presence"]) -> JsonObject:
        row = self.client.worker_get(
            WorkerRead(
                worker_id=self.binding.worker_id,
                session_id=self.binding.worker_session_id,
                kind=kind,
                id=self.binding.worker_id,
            )
        )
        if row["kind"] != kind or row["id"] != self.binding.worker_id:
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Operational response identifies another worker"
            )
        return row

    def _deliver(self) -> None:
        assert self._state is not None and self._state.pending is not None
        pending = self._state.pending
        reply = self.client.command(
            "worker.heartbeat",
            pending.request,
            idempotency_key=pending.key,
        )["result"]
        row = reply["presence"]
        presence = WorkerPresence.model_validate(row["data"])
        if (
            row["kind"] != "worker_presence"
            or row["id"] != self.binding.worker_id
            or row["state"] != "observed"
            or presence.binding != self.binding
            or presence.heartbeat_sequence != pending.request.sequence
            or presence.driver_instance_id != pending.request.driver_instance_id
            or presence.received_at is None
        ):
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Heartbeat acknowledgment differs from request"
            )
        self.journal.acknowledge_controls(self.binding, pending.request.acknowledgments)
        self._save(HeartbeatState(binding=self.binding, sequence=pending.request.sequence))
        self._last_heartbeat = time.monotonic()

    def _initialize(self) -> None:
        if self._initialized:
            return
        if self._state is None and self.path.exists():
            self._state = HeartbeatState.model_validate(
                strict_json_loads(
                    read_private(self.path.parent, self.path.name, limit=128 * 1024),
                )
            )
            if self._state.binding != self.binding:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Heartbeat state belongs to another enrollment"
                )
        if self._state is not None and self._state.pending is not None:
            self._deliver()
        row = self._get("worker_presence")
        presence = WorkerPresence.model_validate(row["data"])
        if presence.binding != self.binding:
            raise ResearchError(ErrorCode.STALE_FENCE, "Presence binding differs from enrollment")
        if self._state is None:
            if presence.heartbeat_sequence != 0:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Heartbeat state is missing from observed worker"
                )
            self._save(HeartbeatState(binding=self.binding))
        elif presence.heartbeat_sequence != self._state.sequence:
            raise ResearchError(
                ErrorCode.STALE_FENCE, "Heartbeat sequence differs from durable state"
            )
        self._initialized = True

    def _heartbeat(self, *, force: bool = False) -> None:
        assert self._state is not None
        if self._state.pending is not None:
            self._deliver()
        for _ in range(128):
            acknowledgments = self.journal.pending_control_acks(self.binding)
            due = (time.monotonic() - self._last_heartbeat) * 1000 >= self.heartbeat_ms
            if not acknowledgments and not (force or due):
                return
            if self._state.sequence == 9_007_199_254_740_991:
                raise ResearchError(ErrorCode.STALE_FENCE, "Heartbeat sequence is exhausted")
            pending = PendingHeartbeat(
                request=WorkerHeartbeat(
                    binding=self.binding,
                    driver_instance_id=self.driver_instance_id,
                    sequence=self._state.sequence + 1,
                    acknowledgments=acknowledgments,
                )
            )
            self._save(self._state.model_copy(update={"pending": pending}))
            self._deliver()
            force = False
            if not acknowledgments:
                return
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE, "Control acknowledgment backlog exceeds its bound"
        )

    def synchronize(self) -> Literal["active", "draining"]:
        try:
            self._initialize()
            self._heartbeat()
            cursor = None
            for _ in range(128):
                page = self.client.worker_list(
                    WorkerQuery(
                        worker_id=self.binding.worker_id,
                        session_id=self.binding.worker_session_id,
                        kind="controls",
                        limit=100,
                        cursor=cursor,
                    )
                )
                if len(page["items"]) > 100:
                    raise ResearchError(
                        ErrorCode.PROJECT_CONTRACT_INVALID, "Control page exceeds its bound"
                    )
                for row in page["items"]:
                    control = AttemptCancellation.model_validate(row["data"])
                    if row["kind"] != "attempt_control" or row["id"] != control.attempt_id:
                        raise ResearchError(
                            ErrorCode.STALE_FENCE, "Control response identifies another attempt"
                        )
                    directive = CancellationDirective.model_validate(
                        {key: getattr(control, key) for key in CancellationDirective.model_fields}
                    )
                    self.journal.apply_control(self.binding, directive)
                self._heartbeat()
                cursor = page["next_cursor"]
                if cursor is None:
                    break
            else:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "Control discovery exceeds its page bound"
                )
            row = self._get("worker_admission")
            admission = WorkerAdmission.model_validate(row["data"])
            if admission.binding != self.binding or row["state"] not in {"active", "draining"}:
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Admission response differs from enrollment"
                )
            return "active" if row["state"] == "active" else "draining"
        except OSError as error:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Worker control persistence is unavailable"
            ) from error
        except (ValueError, KeyError, TypeError) as error:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid worker control state or response"
            ) from error
