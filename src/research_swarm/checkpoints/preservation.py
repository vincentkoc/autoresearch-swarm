"""Stream-compare the restored ledger with its source, permitting only recovery metadata."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing
from itertools import zip_longest
from typing import Any

from pydantic import TypeAdapter

from research_swarm.checkpoints.backup import VerifiedBackup
from research_swarm.checkpoints.files import private_reader
from research_swarm.control.worker_records import WorkerData
from research_swarm.control.worker_turnover import WorkerFence
from research_swarm.domain.backup import RestoreReceipt
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import Event
from research_swarm.domain.types import Timestamp
from research_swarm.state.database import Database

Row = dict[str, Any]
RecordKey = tuple[str, str, str]


def _require(condition: bool) -> None:
    if not condition:
        raise ResearchError(
            ErrorCode.ARTIFACT_UNVERIFIED, "Restored ledger does not preserve its source"
        )


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _rows(connection: sqlite3.Connection, table: str, order: tuple[str, ...]) -> Iterator[Row]:
    query = "SELECT * FROM " + _quote(table) + " ORDER BY " + ",".join(map(_quote, order))
    for row in connection.execute(query):
        yield dict(row)


def _schema(connection: sqlite3.Connection) -> list[tuple[Any, ...]]:
    return [
        tuple(row)
        for row in connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' "
            "ORDER BY type,name"
        )
    ]


def _new_record(row: Row, *, state: str, parent: str | None, data: Any) -> None:
    _require(row["revision"] == 1 and row["state"] == state and row["parent_id"] == parent)
    _require(row["campaign_id"] is None and row["created_at"] == row["updated_at"])
    TypeAdapter(Timestamp).validate_python(row["created_at"])
    _require(strict_json_loads(row["payload"]) == data)


def _allowances(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    backup: VerifiedBackup,
    receipt: RestoreReceipt,
) -> tuple[dict[RecordKey, Callable[[Row], None]], dict[tuple[str, str, str], tuple[int, Row]]]:
    workspace = receipt.workspace_id
    additions: dict[RecordKey, Callable[[Row], None]] = {}
    events: dict[tuple[str, str, str], tuple[int, Row]] = {
        ("workspace.restored_inactive", "restoration", receipt.restore_id): (
            1,
            {"backup_id": receipt.backup_id},
        )
    }
    additions[(workspace, "restoration", receipt.restore_id)] = lambda row: _new_record(
        row, state="inactive", parent=None, data=receipt.model_dump(mode="json")
    )
    for worker_id in backup.manifest.seal.worker_ids:
        original = dict(
            source.execute(
                "SELECT * FROM records WHERE kind='worker' AND id=?", (worker_id,)
            ).fetchone()
        )
        worker = WorkerData.model_validate_json(original["payload"])
        session = worker.binding.worker_session_id
        archived = source.execute(
            "SELECT 1 FROM records WHERE kind='worker_session' AND id=?", (session,)
        ).fetchone()
        if archived is None:

            def check_session(row: Row, worker: WorkerData = worker) -> None:
                _new_record(
                    row,
                    state="immutable",
                    parent=worker.binding.worker_id,
                    data=worker.model_dump(mode="json"),
                )

            additions[(workspace, "worker_session", session)] = check_session
            events[("worker.session_recorded", "worker_session", session)] = (
                1,
                {"worker_id": worker_id},
            )
        if original["state"] != "active":
            _require(original["state"] == "fenced")
            continue

        def check_fence(
            row: Row, worker: WorkerData = worker, revision: int = original["revision"]
        ) -> None:
            fence = WorkerFence.model_validate_json(row["payload"])
            _require(
                fence.binding == worker.binding
                and fence.worker_revision == revision
                and fence.controller_id == receipt.controller_id
                and fence.control_epoch == receipt.target_control_epoch
                and fence.operator_id == backup.identity.operator_id
                and fence.reason == "Inactive restore " + receipt.restore_id
            )
            _new_record(
                row,
                state="immutable",
                parent=worker.binding.worker_id,
                data=fence.model_dump(mode="json"),
            )

        additions[(workspace, "worker_fence", session)] = check_fence
        events[("worker.fence_recorded", "worker_fence", session)] = (1, {"worker_id": worker_id})
        events[("worker.fenced", "worker", worker_id)] = (
            original["revision"] + 1,
            {"fence_id": session},
        )
    return additions, events


def _records(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    receipt: RestoreReceipt,
    additions: dict[RecordKey, Callable[[Row], None]],
) -> None:
    order = ("workspace_id", "kind", "id")
    actual = iter(_rows(target, "records", order))
    current = next(actual, None)

    def key(row: Row) -> RecordKey:
        return row["workspace_id"], row["kind"], row["id"]

    def extra(row: Row) -> None:
        validator = additions.pop(key(row), None)
        _require(validator is not None)
        assert validator is not None
        validator(row)

    for previous in _rows(source, "records", order):
        while current is not None and key(current) < key(previous):
            extra(current)
            current = next(actual, None)
        _require(current is not None and key(current) == key(previous))
        assert current is not None
        changed = previous["kind"] == "workspace_gate" or (
            previous["kind"] == "worker" and previous["state"] == "active"
        )
        if changed:
            expected = {
                **previous,
                "revision": previous["revision"] + 1,
                "state": "restoring" if previous["kind"] == "workspace_gate" else "fenced",
                "updated_at": current["updated_at"],
            }
            TypeAdapter(Timestamp).validate_python(current["updated_at"])
            _require(current == expected)
        else:
            _require(current == previous)
        current = next(actual, None)
    while current is not None:
        extra(current)
        current = next(actual, None)
    _require(not additions)


def _events(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    backup: VerifiedBackup,
    receipt: RestoreReceipt,
    expected: dict[tuple[str, str, str], tuple[int, Row]],
) -> dict[str, Row]:
    sequence = backup.manifest.snapshot_evidence_through
    count = len(expected)
    actual = iter(_rows(target, "events", ("workspace_id", "sequence")))
    for previous in _rows(source, "events", ("workspace_id", "sequence")):
        _require(next(actual, None) == previous)
    notifications = {}
    command_id = None
    for row in actual:
        sequence += 1
        event = Event.model_validate_json(row["payload"])
        rule = expected.pop((event.type, event.aggregate_type, event.aggregate_id), None)
        _require(rule is not None)
        assert rule is not None
        _require(
            event.sequence == row["sequence"] == sequence
            and event.id == row["id"]
            and event.workspace_id == row["workspace_id"] == receipt.workspace_id
            and event.actor_id == backup.identity.operator_id
            and event.aggregate_revision == rule[0]
            and event.payload == rule[1]
            and event.campaign_id is None
        )
        command_id = command_id or event.command_id
        _require(event.command_id == command_id)
        notifications[event.id] = {
            "event_id": event.id,
            "workspace_id": receipt.workspace_id,
            "sequence": sequence,
            "delivered": 0,
            "delivery_attempts": 0,
        }
    _require(not expected and sequence == backup.manifest.snapshot_evidence_through + count)
    return notifications


def verify_preservation(backup: VerifiedBackup, database: Database, restore_id: str) -> None:
    """Compare consistent native snapshots without loading database-sized collections."""
    with (
        private_reader(backup.root, "research.sqlite"),
        closing(
            sqlite3.connect(
                (backup.root / "research.sqlite").as_uri() + "?mode=ro&immutable=1", uri=True
            )
        ) as source,
        database.connect() as native,
    ):
        source.row_factory = sqlite3.Row
        source.execute("PRAGMA trusted_schema=OFF")
        target = native.connection
        target.execute("BEGIN")
        try:
            schema = _schema(source)
            _require(schema == _schema(target))
            restored = target.execute(
                "SELECT payload FROM records WHERE kind='restoration' AND id=?", (restore_id,)
            ).fetchall()
            _require(len(restored) <= 1)
            receipt = RestoreReceipt.model_validate_json(restored[0][0]) if restored else None
            special = {"records", "events", "outbox", "workspaces"} if receipt else set()
            for kind, table, _, _ in schema:
                if kind != "table" or table in special:
                    continue
                columns = tuple(
                    row[1] for row in source.execute("PRAGMA table_info(" + _quote(table) + ")")
                )
                for before, after in zip_longest(
                    _rows(source, table, columns), _rows(target, table, columns)
                ):
                    _require(before == after)
            if receipt is not None:
                additions, events = _allowances(source, target, backup, receipt)
                count = len(events)
                _records(source, target, receipt, additions)
                notifications = _events(source, target, backup, receipt, events)
                original = list(_rows(source, "workspaces", ("id",)))
                actual = list(_rows(target, "workspaces", ("id",)))
                _require(len(original) == len(actual) == 1)
                _require(
                    actual[0]
                    == {
                        **original[0],
                        "control_epoch": original[0]["control_epoch"] + 1,
                        "next_sequence": original[0]["next_sequence"] + count,
                    }
                )
                for row in _rows(source, "outbox", ("event_id",)):
                    copied = target.execute(
                        "SELECT * FROM outbox WHERE event_id=?", (row["event_id"],)
                    ).fetchone()
                    _require(copied is not None and dict(copied) == row)
                for row in _rows(target, "outbox", ("event_id",)):
                    prior = source.execute(
                        "SELECT 1 FROM outbox WHERE event_id=?", (row["event_id"],)
                    ).fetchone()
                    if prior is None:
                        _require(notifications.pop(row["event_id"], None) == row)
                _require(not notifications)
        finally:
            target.rollback()
