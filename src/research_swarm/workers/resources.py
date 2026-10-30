"""Machine-local admission shared by worker processes and controller enrollments."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

from pydantic import Field, model_validator

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Digest, Name, Revision, VersionedRecord, new_id
from research_swarm.state.identity import connect_existing, initialize_identity
from research_swarm.workers.authority import VerifiedDispatch, WorkerBinding
from research_swarm.workers.journal_records import JournalEntry, JournalState

Capacity = Annotated[int, Field(strict=True, ge=1, le=9_007_199_254_740_991)]


class ResourceDemand(VersionedRecord):
    amounts: dict[Name, Capacity]

    @model_validator(mode="after")
    def nonempty(self) -> ResourceDemand:
        if not self.amounts:
            raise ValueError("A native claim must reserve at least one resource")
        return self


class MachineClaim(VersionedRecord):
    id: ID = Field(default_factory=new_id)
    controller_id: ID
    workspace_id: ID
    attempt_id: ID
    worker_id: ID
    worker_session_id: ID
    fencing_generation: Revision
    dispatch_digest: Digest
    demand: ResourceDemand


_SCHEMA = """
CREATE TABLE IF NOT EXISTS pools (
    name TEXT PRIMARY KEY,
    controller_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK(capacity>0)
);
CREATE TABLE IF NOT EXISTS claims (
    attempt_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    state TEXT NOT NULL,
    finding TEXT NOT NULL DEFAULT '',
    closure_digest TEXT
);
CREATE TABLE IF NOT EXISTS allocations (
    attempt_id TEXT NOT NULL REFERENCES claims(attempt_id),
    pool TEXT NOT NULL REFERENCES pools(name),
    amount INTEGER NOT NULL CHECK(amount>0),
    PRIMARY KEY(attempt_id,pool)
);
PRAGMA user_version=1;
"""


class MachineResources:
    """Use one operator-controlled database per native host, not one per controller.

    Reservations survive process death. Neither time nor lock-file disappearance
    releases capacity. This is scheduling admission, not a kernel resource sandbox.
    """

    _existing_only: bool = False

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self._connect() as connection:
            if connection.execute("PRAGMA user_version").fetchone()[0] not in (0, 1):
                raise ResearchError(ErrorCode.SCHEMA_UNSUPPORTED, "Unsupported machine registry")
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(_SCHEMA)
            self.identity = initialize_identity(connection)

    @classmethod
    def open_existing(cls, path: Path, expected_identity: str) -> MachineResources:
        resources = cls.__new__(cls)
        resources.path, resources.identity = path.absolute(), expected_identity
        resources._existing_only = True
        with resources._connect() as connection:
            try:
                connection.execute(
                    "SELECT name,capacity,controller_id,workspace_id FROM pools LIMIT 0"
                )
                connection.execute(
                    "SELECT attempt_id,payload,state,finding,closure_digest FROM claims LIMIT 0"
                )
                connection.execute("SELECT attempt_id,pool,amount FROM allocations LIMIT 0")
            except sqlite3.Error as error:
                raise ResearchError(
                    ErrorCode.SCHEMA_UNSUPPORTED, "Original resource schema is unavailable"
                ) from error
        return resources

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = (
            connect_existing(self.path, self.identity)
            if getattr(self, "_existing_only", False)
            else sqlite3.connect(self.path, timeout=10, isolation_level=None)
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def enroll(self, binding: WorkerBinding, capacities: ResourceDemand) -> None:
        """Explicit native operator enrollment. It does not probe or claim unlisted devices."""
        binding = WorkerBinding.model_validate(binding)
        capacities = ResourceDemand.model_validate(capacities)
        with self._transaction() as connection:
            for name, amount in capacities.amounts.items():
                prior = connection.execute("SELECT * FROM pools WHERE name=?", (name,)).fetchone()
                if prior is not None:
                    if (
                        prior["controller_id"] != binding.controller_id
                        or prior["workspace_id"] != binding.workspace_id
                        or prior["capacity"] != amount
                    ):
                        raise ResearchError(
                            ErrorCode.RESOURCE_UNAVAILABLE,
                            "Native resource already has another enrollment or capacity",
                            details={"resource": name},
                        )
                    continue
                connection.execute(
                    "INSERT INTO pools VALUES (?,?,?,?)",
                    (name, binding.controller_id, binding.workspace_id, amount),
                )

    @staticmethod
    def _occupied(connection: sqlite3.Connection, name: str) -> int:
        return int(
            connection.execute(
                "SELECT COALESCE(SUM(a.amount),0) FROM allocations a "
                "JOIN claims c ON c.attempt_id=a.attempt_id WHERE a.pool=? AND c.state!='released'",
                (name,),
            ).fetchone()[0]
        )

    def reserve(self, verified: VerifiedDispatch, demand: ResourceDemand) -> MachineClaim:
        verified.spec.verify()
        demand = ResourceDemand.model_validate(demand)
        envelope = verified.envelope
        digest = record_digest("research.dispatch", envelope.model_dump(mode="json"))
        with self._transaction() as connection:
            prior = connection.execute(
                "SELECT payload,state FROM claims WHERE attempt_id=?", (envelope.attempt_id,)
            ).fetchone()
            if prior is not None:
                claim = MachineClaim.model_validate_json(prior["payload"])
                if claim.dispatch_digest != digest or claim.demand != demand:
                    raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Native claim changed")
                if prior["state"] == "released":
                    raise ResearchError(
                        ErrorCode.INVALID_TRANSITION, "Native claim is already closed"
                    )
                return claim
            for name, amount in demand.amounts.items():
                pool = connection.execute("SELECT * FROM pools WHERE name=?", (name,)).fetchone()
                if pool is None or (
                    pool["controller_id"] != envelope.controller_id
                    or pool["workspace_id"] != envelope.workspace_id
                ):
                    raise ResearchError(
                        ErrorCode.AUTHORIZATION_DENIED, "Native pool is not enrolled"
                    )
                available = pool["capacity"] - self._occupied(connection, name)
                if amount > available:
                    raise ResearchError(
                        ErrorCode.RESOURCE_UNAVAILABLE,
                        "Native resource is occupied",
                        details={"resource": name, "available": available},
                    )
            claim = MachineClaim(
                controller_id=envelope.controller_id,
                workspace_id=envelope.workspace_id,
                worker_id=envelope.worker_id,
                worker_session_id=envelope.worker_session_id,
                attempt_id=envelope.attempt_id,
                fencing_generation=envelope.fencing_generation,
                dispatch_digest=digest,
                demand=demand,
            )
            connection.execute(
                "INSERT INTO claims(attempt_id,payload,state) VALUES (?,?,'held')",
                (claim.attempt_id, claim.model_dump_json()),
            )
            connection.executemany(
                "INSERT INTO allocations(attempt_id,pool,amount) VALUES (?,?,?)",
                [(claim.attempt_id, name, amount) for name, amount in demand.amounts.items()],
            )
            return claim

    def quarantine(self, claim: MachineClaim, finding: str) -> None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT payload,state FROM claims WHERE attempt_id=?", (claim.attempt_id,)
            ).fetchone()
            if row is None or MachineClaim.model_validate_json(row["payload"]) != claim:
                raise ResearchError(ErrorCode.STALE_FENCE, "Native claim identity differs")
            if row["state"] == "released":
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Native claim is already closed")
            connection.execute(
                "UPDATE claims SET state='unknown',finding=? WHERE attempt_id=?",
                (finding, claim.attempt_id),
            )

    def verify_held(
        self, entry: JournalEntry, claim_id: str, *, expected: ResourceDemand | None = None
    ) -> MachineClaim:
        """Check a local launch binding without accepting network-provided closure claims."""
        claim = self._claim(entry, claim_id, states={"held"})
        if expected is not None and claim.demand != expected:
            raise ResearchError(ErrorCode.STALE_FENCE, "Native claim demand differs from its stage")
        return claim

    def observe_claim(self, entry: JournalEntry, claim_id: str) -> MachineClaim:
        """Recover held or quarantined custody without issuing launch authority."""
        return self._claim(entry, claim_id, states={"held", "unknown"})

    def _claim(self, entry: JournalEntry, claim_id: str, *, states: set[str]) -> MachineClaim:
        dispatch_digest = record_digest("research.dispatch", entry.dispatch.model_dump(mode="json"))
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload,state FROM claims WHERE attempt_id=?", (entry.dispatch.attempt_id,)
            ).fetchone()
        if row is None:
            raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Native launch has no reservation")
        claim = MachineClaim.model_validate_json(row["payload"])
        if claim.id != claim_id or claim.dispatch_digest != dispatch_digest:
            raise ResearchError(ErrorCode.STALE_FENCE, "Native launch reservation differs")
        if row["state"] not in states:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Native launch reservation is not held"
            )
        return claim

    def release(self, entry: JournalEntry) -> None:
        """The trusted local worker supplies its journal receipt, never a network boolean."""
        entry = JournalEntry.model_validate(entry)
        proven_not_started = (
            entry.state == JournalState.NOT_STARTED
            and entry.native is None
            and entry.outcome is None
        )
        proven_closed = (
            entry.state in {JournalState.CLOSED, JournalState.NOT_STARTED}
            and entry.outcome is not None
            and entry.outcome.closure.scope_closed
            and entry.outcome.closure.identity == entry.native
        )
        if not (proven_not_started or proven_closed):
            raise ResearchError(
                ErrorCode.EXECUTION_UNKNOWN, "Native capacity lacks closure evidence"
            )
        dispatch_digest = record_digest("research.dispatch", entry.dispatch.model_dump(mode="json"))
        closure_digest = record_digest(
            "research.native-release",
            {
                "dispatch_digest": dispatch_digest,
                "not_started": proven_not_started,
                "outcome": entry.outcome.model_dump(mode="json")
                if entry.outcome is not None
                else None,
            },
        )
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT payload,state,closure_digest FROM claims WHERE attempt_id=?",
                (entry.dispatch.attempt_id,),
            ).fetchone()
            if row is None:
                raise ResearchError(ErrorCode.NOT_FOUND, "Native claim not found")
            claim = MachineClaim.model_validate_json(row["payload"])
            if claim.dispatch_digest != dispatch_digest:
                raise ResearchError(ErrorCode.STALE_FENCE, "Closure refers to another native grant")
            if row["state"] == "released":
                if row["closure_digest"] != closure_digest:
                    raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Native closure changed")
                return
            connection.execute(
                "UPDATE claims SET state='released',closure_digest=? WHERE attempt_id=?",
                (closure_digest, entry.dispatch.attempt_id),
            )

    def availability(self) -> dict[str, int]:
        with self._connect() as connection:
            connection.execute("BEGIN")
            pools = connection.execute("SELECT name,capacity FROM pools ORDER BY name").fetchall()
            result = {
                row["name"]: row["capacity"] - self._occupied(connection, row["name"])
                for row in pools
            }
            connection.commit()
            return result
