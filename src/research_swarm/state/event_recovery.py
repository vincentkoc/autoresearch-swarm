"""Freeze authorized read models and pin the event interval needed to catch up."""

from __future__ import annotations

import base64
import binascii
from datetime import datetime, timedelta

from pydantic import ValidationError

from research_swarm.domain.canonical import (
    canonical_bytes,
    content_digest,
    record_digest,
    strict_json_loads,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.event_recovery import (
    AdvanceEventFloor,
    CollectExpiredSnapshots,
    CreateRecoverySnapshot,
    RecoveryCursor,
    RecoveryItem,
    RecoveryPage,
    RecoverySnapshot,
    RecoverySnapshotTarget,
)
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.state.database import Database, JsonObject, Transaction, encode
from research_swarm.state.sql import SqlSession


def snapshot_row(transaction: Transaction, identifier: str) -> JsonObject:
    row = transaction.connection.execute(
        "SELECT * FROM recovery_snapshots WHERE workspace_id=? AND id=? AND actor_id=?",
        (transaction.workspace_id, identifier, transaction.context.principal.id),
    ).fetchone()
    if row is None:
        raise ResearchError(ErrorCode.NOT_FOUND, "Recovery snapshot not found for this reader")
    return dict(row)


def active_snapshot(transaction: Transaction, identifier: str) -> RecoverySnapshot:
    row = snapshot_row(transaction, identifier)
    floor = transaction.connection.execute(
        "SELECT event_floor FROM workspaces WHERE id=?", (transaction.workspace_id,)
    ).one()[0]
    if (
        row["released_at"] is not None
        or row["expired_at"] is not None
        or row["expires_at"] <= utc_now()
        or floor > row["watermark"] + 1
    ):
        raise ResearchError(
            ErrorCode.SNAPSHOT_EXPIRED,
            "Recovery snapshot is no longer usable; start a new recovery",
            details={"recovery_command": "stream.create_snapshot"},
        )
    return RecoverySnapshot.model_validate_json(row["payload"])


def collect_expired(
    transaction: Transaction, *, limit: int, actor_id: str | None = None
) -> list[str]:
    now = utc_now()
    query = (
        "SELECT id FROM recovery_snapshots WHERE workspace_id=? AND released_at IS NULL "
        "AND expired_at IS NULL AND expires_at<=?"
    )
    parameters: list[str | int] = [transaction.workspace_id, now]
    if actor_id is not None:
        query += " AND actor_id=?"
        parameters.append(actor_id)
    rows = transaction.connection.execute(
        query + " ORDER BY expires_at,id LIMIT ?", (*parameters, limit)
    ).fetchall()
    for row in rows:
        transaction.connection.execute(
            "UPDATE recovery_snapshots SET expired_at=? WHERE workspace_id=? AND id=?",
            (now, transaction.workspace_id, row["id"]),
        )
        transaction.connection.execute(
            "DELETE FROM recovery_items WHERE workspace_id=? AND snapshot_id=?",
            (transaction.workspace_id, row["id"]),
        )
    return [row["id"] for row in rows]


class EventRecovery:
    def __init__(self, database: Database, public_kinds: tuple[str, ...]) -> None:
        self.database = database
        self.public_kinds = tuple(sorted(public_kinds))

    def create(self, context: CommandContext, intent: CreateRecoverySnapshot) -> JsonObject:
        def create(transaction: Transaction) -> JsonObject:
            request_digest = record_digest(
                "research.recovery-request-v1", intent.model_dump(mode="json")
            )
            previous = transaction.connection.execute(
                "SELECT actor_id,payload FROM recovery_snapshots WHERE workspace_id=? AND id=?",
                (transaction.workspace_id, intent.snapshot_id),
            ).fetchone()
            if previous is not None:
                saved = RecoverySnapshot.model_validate_json(previous["payload"])
                if saved.actor_id != context.principal.id or saved.request_digest != request_digest:
                    raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Snapshot identity differs")
                return saved.model_dump(mode="json")
            now = utc_now()
            collect_expired(transaction, limit=8, actor_id=context.principal.id)
            counts = transaction.connection.execute(
                "SELECT COUNT(*) AS total,COALESCE(SUM(actor_id=?),0) AS owned "
                "FROM recovery_snapshots WHERE workspace_id=? "
                "AND released_at IS NULL AND expired_at IS NULL",
                (context.principal.id, transaction.workspace_id),
            ).one()
            retained = transaction.connection.execute(
                "SELECT COALESCE(SUM(json_extract(payload,'$.size_bytes')),0) "
                "FROM recovery_snapshots WHERE workspace_id=? "
                "AND released_at IS NULL AND expired_at IS NULL",
                (transaction.workspace_id,),
            ).one()[0]
            if counts["total"] >= 64 or counts["owned"] >= 8:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "Release unused recovery snapshots first"
                )
            workspace = transaction.connection.execute(
                "SELECT * FROM workspaces WHERE id=?", (transaction.workspace_id,)
            ).fetchone()
            assert workspace is not None
            expires = (
                (
                    datetime.fromisoformat(now.replace("Z", "+00:00"))
                    + timedelta(seconds=intent.lifetime_seconds)
                )
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
            snapshot = RecoverySnapshot(
                id=intent.snapshot_id,
                workspace_id=transaction.workspace_id,
                actor_id=context.principal.id,
                controller_id=workspace["controller_id"],
                control_epoch=workspace["control_epoch"],
                watermark=workspace["next_sequence"] - 1,
                event_floor=workspace["event_floor"],
                request_digest=request_digest,
                scope_digest=record_digest(
                    "research.recovery-scope-v1",
                    {
                        "workspace_id": transaction.workspace_id,
                        "actor_id": context.principal.id,
                        "projection_version": "public-read-v1",
                        "record_kinds": self.public_kinds,
                        "accounts": True,
                        "reservations": True,
                        "entries": True,
                    },
                ),
                item_count=0,
                size_bytes=0,
                created_at=now,
                expires_at=expires,
            )
            transaction.connection.execute(
                "INSERT INTO recovery_snapshots"
                "(workspace_id,id,actor_id,watermark,expires_at,payload) "
                "VALUES (?,?,?,?,?,?)",
                (
                    transaction.workspace_id,
                    snapshot.id,
                    snapshot.actor_id,
                    snapshot.watermark,
                    expires,
                    encode(snapshot.model_dump(mode="json")),
                ),
            )
            count = size = 0

            def save(item: JsonObject) -> None:
                nonlocal count, size
                content = canonical_bytes(item)
                count += 1
                size += len(content)
                if count > intent.max_items or size > intent.max_bytes or retained + size > 1024**3:
                    raise ResearchError(
                        ErrorCode.RESOURCE_UNAVAILABLE,
                        "Recovery snapshot exceeds its declared bounds",
                    )
                transaction.connection.execute(
                    "INSERT INTO recovery_items VALUES (?,?,?,?,?,?)",
                    (
                        transaction.workspace_id,
                        snapshot.id,
                        count,
                        content_digest(content),
                        len(content),
                        content,
                    ),
                )

            placeholders = ",".join("?" for _ in self.public_kinds)
            rows = transaction.connection.execute(
                "SELECT kind,id FROM records WHERE workspace_id=? AND kind IN ("
                + placeholders
                + ") ORDER BY kind,id",
                (transaction.workspace_id, *self.public_kinds),
            )
            for row in rows:
                save({"type": "record", "value": transaction.get(row["kind"], row["id"])})
            for table, name, order in (
                ("budgets", "budget_account", "campaign_id,dimension"),
                ("budget_reservations", "budget_reservation", "id"),
                ("budget_entries", "budget_entry", "id"),
            ):
                for row in transaction.connection.execute(
                    f"SELECT payload FROM {table} WHERE workspace_id=? ORDER BY {order}",
                    (transaction.workspace_id,),
                ):
                    save({"type": name, "value": strict_json_loads(row["payload"])})
            snapshot = snapshot.model_copy(update={"item_count": count, "size_bytes": size})
            transaction.connection.execute(
                "UPDATE recovery_snapshots SET payload=? WHERE workspace_id=? AND id=?",
                (encode(snapshot.model_dump(mode="json")), transaction.workspace_id, snapshot.id),
            )
            return snapshot.model_dump(mode="json")

        return self.database.execute(
            context, "stream.create_snapshot", intent.model_dump(mode="json"), "read", create
        )

    def release(self, context: CommandContext, intent: RecoverySnapshotTarget) -> JsonObject:
        def release(transaction: Transaction) -> JsonObject:
            row = snapshot_row(transaction, intent.snapshot_id)
            released = row["released_at"] or utc_now()
            transaction.connection.execute(
                "UPDATE recovery_snapshots SET released_at=? WHERE workspace_id=? AND id=?",
                (released, transaction.workspace_id, intent.snapshot_id),
            )
            transaction.connection.execute(
                "DELETE FROM recovery_items WHERE workspace_id=? AND snapshot_id=?",
                (transaction.workspace_id, intent.snapshot_id),
            )
            return {"snapshot_id": intent.snapshot_id, "released_at": released}

        return self.database.execute(
            context, "stream.release_snapshot", intent.model_dump(mode="json"), "read", release
        )

    def advance_floor(self, context: CommandContext, intent: AdvanceEventFloor) -> JsonObject:
        def advance(transaction: Transaction) -> JsonObject:
            workspace = transaction.connection.execute(
                "SELECT event_floor,next_sequence FROM workspaces WHERE id=?",
                (transaction.workspace_id,),
            ).fetchone()
            assert workspace is not None
            if workspace["event_floor"] != intent.previous_floor:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Event retention floor changed")
            if not intent.previous_floor <= intent.event_floor <= workspace["next_sequence"]:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid event retention floor"
                )
            collect_expired(transaction, limit=64)
            pin = transaction.connection.execute(
                "SELECT MIN(watermark) FROM recovery_snapshots WHERE workspace_id=? "
                "AND released_at IS NULL AND expired_at IS NULL AND expires_at>?",
                (transaction.workspace_id, utc_now()),
            ).one()[0]
            if pin is not None and intent.event_floor > pin + 1:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "A recovery snapshot pins the catch-up interval"
                )
            transaction.connection.execute(
                "UPDATE workspaces SET event_floor=? WHERE id=?",
                (intent.event_floor, transaction.workspace_id),
            )
            return {"event_floor": intent.event_floor, "watermark": workspace["next_sequence"] - 1}

        return self.database.execute(
            context,
            "stream.advance_floor",
            intent.model_dump(mode="json"),
            "stream.manage",
            advance,
        )

    def collect(self, context: CommandContext, intent: CollectExpiredSnapshots) -> JsonObject:
        return self.database.execute(
            context,
            "stream.collect_expired",
            intent.model_dump(mode="json"),
            "stream.manage",
            lambda transaction: {
                "expired_snapshot_ids": collect_expired(transaction, limit=intent.limit)
            },
        )

    def page(self, context: CommandContext, intent: RecoveryPage) -> JsonObject:
        context.principal.require("read")

        def read(connection: SqlSession) -> JsonObject:
            snapshot = active_snapshot(Transaction(connection, context), intent.snapshot_id)
            after = 0
            if intent.cursor is not None:
                try:
                    cursor = RecoveryCursor.model_validate(
                        strict_json_loads(
                            base64.b64decode(
                                intent.cursor.encode("ascii"), altchars=b"-_", validate=True
                            )
                        )
                    )
                    if (cursor.snapshot_id, cursor.scope_digest, cursor.watermark) != (
                        snapshot.id,
                        snapshot.scope_digest,
                        snapshot.watermark,
                    ) or cursor.after > snapshot.item_count:
                        raise ValueError("Recovery cursor belongs to another snapshot")
                    after = cursor.after
                except (ValueError, UnicodeError, binascii.Error, ValidationError) as error:
                    raise ResearchError(
                        ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid recovery cursor"
                    ) from error
            items = [
                dict(row)
                for row in connection.execute(
                    "SELECT ordinal,content_digest,size_bytes FROM recovery_items "
                    "WHERE workspace_id=? AND snapshot_id=? AND ordinal>? ORDER BY ordinal LIMIT ?",
                    (snapshot.workspace_id, snapshot.id, after, intent.limit),
                )
            ]
            following = items[-1]["ordinal"] if items else after
            cursor_out = (
                None
                if following == snapshot.item_count
                else base64.urlsafe_b64encode(
                    canonical_bytes(
                        RecoveryCursor(
                            snapshot_id=snapshot.id,
                            scope_digest=snapshot.scope_digest,
                            watermark=snapshot.watermark,
                            after=following,
                        ).model_dump(mode="json")
                    )
                ).decode("ascii")
            )
            return {
                "snapshot": snapshot.model_dump(mode="json"),
                "items": items,
                "next_cursor": cursor_out,
            }

        return self.database.store.read(read)

    def item(self, context: CommandContext, intent: RecoveryItem) -> JsonObject:
        context.principal.require("read")

        def read(connection: SqlSession) -> JsonObject:
            snapshot = active_snapshot(Transaction(connection, context), intent.snapshot_id)
            row = connection.execute(
                "SELECT content_digest,size_bytes,substr(content,?,?) AS part FROM recovery_items "
                "WHERE workspace_id=? AND snapshot_id=? AND ordinal=?",
                (
                    intent.offset + 1,
                    intent.limit,
                    snapshot.workspace_id,
                    snapshot.id,
                    intent.ordinal,
                ),
            ).fetchone()
            if row is None:
                raise ResearchError(ErrorCode.NOT_FOUND, "Recovery item not found")
            if intent.offset > row["size_bytes"]:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Recovery item offset is out of range"
                )
            return {
                "snapshot_id": snapshot.id,
                "ordinal": intent.ordinal,
                "offset": intent.offset,
                "content_digest": row["content_digest"],
                "size_bytes": row["size_bytes"],
                "content_base64": base64.b64encode(row["part"]).decode("ascii"),
                "next_offset": intent.offset + len(row["part"]),
            }

        return self.database.store.read(read)
