"""Exact portable table admission before reading a logical checkpoint image."""

from research_swarm.checkpoints.image_format import LAYOUT, TableLayout
from research_swarm.state.schema_contracts import INDEXES, unsupported
from research_swarm.state.sql import SqlSession

EXTRA_UNIQUE = {
    "events": (("id",),),
    "budget_reservations": (("workspace_id", "owner_id"),),
    "budget_entries": (("workspace_id", "settlement_id", "campaign_id", "dimension"),),
    "credentials": (("token_digest",),),
}
WORKSPACE_FOREIGN_KEYS = frozenset(
    {
        "records",
        "commands",
        "events",
        "budgets",
        "budget_reservations",
        "recovery_snapshots",
        "credentials",
    }
)
DEFAULTS = {
    ("workspaces", "control_epoch"): "1",
    ("workspaces", "next_sequence"): "1",
    ("workspaces", "event_floor"): "1",
    ("outbox", "delivered"): "0",
    ("outbox", "delivery_attempts"): "0",
    ("credentials", "revoked"): "0",
}


def require_columns(sql: SqlSession, table: TableLayout) -> None:
    rows = sql.execute(
        "SELECT * FROM pragma_table_xinfo(?) ORDER BY cid LIMIT ?",
        (table.name, len(table.columns) + 1),
    ).fetchall()
    if len(rows) != len(table.columns):
        raise unsupported()
    for ordinal, (row, column) in enumerate(zip(rows, table.columns, strict=True)):
        key_position = (
            table.primary_key.index(column.name) + 1 if column.name in table.primary_key else 0
        )
        # Existing single TEXT PRIMARY KEY columns omit an explicit NOT NULL.
        # The image still refuses NULL values; preserve the declared schema here.
        not_null = int(not column.nullable and not (key_position and len(table.primary_key) == 1))
        if (
            row["cid"] != ordinal
            or row["name"] != column.name
            or row["type"].upper() != column.kind.upper()
            or row["notnull"] != not_null
            or row["pk"] != key_position
            or row["hidden"] != 0
            or row["dflt_value"] != DEFAULTS.get((table.name, column.name))
        ):
            raise unsupported()


def _unique_keys(sql: SqlSession, table: TableLayout) -> None:
    indexes = sql.execute("SELECT * FROM pragma_index_list(?) LIMIT 65", (table.name,)).fetchall()
    if len(indexes) > 64:
        raise unsupported()
    actual = set()
    for index in indexes:
        if not index["unique"]:
            continue
        if index["partial"]:
            raise unsupported()
        columns = sql.execute(
            'SELECT * FROM pragma_index_xinfo(?) WHERE "key"=1 ORDER BY seqno LIMIT 11',
            (index["name"],),
        ).fetchall()
        if (
            not columns
            or len(columns) > 10
            or any(row["cid"] < 0 or row["desc"] or row["coll"] != "BINARY" for row in columns)
        ):
            raise unsupported()
        actual.add(tuple(row["name"] for row in columns))
    expected = {table.primary_key, *EXTRA_UNIQUE.get(table.name, ())}
    if actual != expected:
        raise unsupported()


def _foreign_keys(sql: SqlSession, table: TableLayout) -> None:
    rows = sql.execute("SELECT * FROM pragma_foreign_key_list(?) LIMIT 4", (table.name,)).fetchall()
    if table.name in WORKSPACE_FOREIGN_KEYS:
        expected = {(0, 0, "workspaces", "workspace_id", "id", "NO ACTION")}
    elif table.name == "outbox":
        expected = {(0, 0, "events", "event_id", "id", "NO ACTION")}
    elif table.name == "recovery_items":
        expected = {
            (0, 0, "recovery_snapshots", "workspace_id", "workspace_id", "CASCADE"),
            (0, 1, "recovery_snapshots", "snapshot_id", "id", "CASCADE"),
        }
    else:
        expected = set()
    actual = {
        (row["id"], row["seq"], row["table"], row["from"], row["to"], row["on_delete"])
        for row in rows
    }
    if actual != expected or any(
        row["on_update"] != "NO ACTION" or row["match"] != "NONE" for row in rows
    ):
        raise unsupported()


def require_portable_image_schema(sql: SqlSession) -> None:
    """Inspect portable tables only; the owner must separately admit all other state."""
    if sql.execute("PRAGMA foreign_keys").one()[0] != 1:
        raise unsupported()
    for table in LAYOUT:
        found = sql.execute("SELECT type FROM sqlite_master WHERE name=?", (table.name,)).fetchone()
        if found is None or found[0] != "table":
            raise unsupported()
        require_columns(sql, table)
        _unique_keys(sql, table)
        _foreign_keys(sql, table)
    for name, table_name in INDEXES.items():
        found = sql.execute(
            "SELECT type,tbl_name FROM sqlite_master WHERE name=?", (name,)
        ).fetchone()
        if found is None or (found[0], found[1]) != ("index", table_name):
            raise unsupported()
    if sql.execute("SELECT 1 FROM pragma_foreign_key_check LIMIT 1").fetchone() is not None:
        raise unsupported()
