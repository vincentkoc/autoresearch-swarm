"""Read-only structural admission for explicitly provisioned state.

This checks the schema used by the service, not arbitrary database tampering or
row integrity. Initialization and migration remain separate operations.
"""

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.sql import SqlSession


def unsupported() -> ResearchError:
    return ResearchError(ErrorCode.SCHEMA_UNSUPPORTED, "Required state schema is unavailable")


def quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def require_table(
    sql: SqlSession,
    name: str,
    columns: str,
    primary_key: tuple[str, ...],
    *,
    unique: tuple[tuple[str, ...], ...] = (),
    foreign_keys: tuple[tuple[str, str, str], ...] = (),
) -> None:
    found = sql.execute("SELECT type FROM sqlite_master WHERE name=?", (name,)).fetchone()
    if found is None or found[0] != "table":
        raise unsupported()
    metadata = sql.execute(f"PRAGMA table_info({quoted(name)})").fetchall()
    actual_columns = {row["name"] for row in metadata}
    actual_key = tuple(
        row["name"] for row in sorted(metadata, key=lambda row: row["pk"]) if row["pk"]
    )
    if not set(columns.split()) <= actual_columns or actual_key != primary_key:
        raise unsupported()
    if unique:
        actual_unique = set()
        for index in sql.execute(f"PRAGMA index_list({quoted(name)})"):
            if index["unique"] and not index["partial"]:
                fields = sql.execute(f"PRAGMA index_info({quoted(index['name'])})").fetchall()
                actual_unique.add(tuple(row["name"] for row in fields))
        if not set(unique) <= actual_unique:
            raise unsupported()
    if foreign_keys:
        actual_foreign = {
            (row["from"], row["table"], row["to"])
            for row in sql.execute(f"PRAGMA foreign_key_list({quoted(name)})")
        }
        if not set(foreign_keys) <= actual_foreign:
            raise unsupported()


# These names and keys are the current service contract, including migrations.
# Extra adapter-owned tables and columns do not make an otherwise valid store foreign.
TABLES = (
    ("workspaces", "id controller_id control_epoch next_sequence event_floor", ("id",)),
    (
        "records",
        "workspace_id kind id revision state campaign_id parent_id payload created_at updated_at",
        ("workspace_id", "kind", "id"),
    ),
    (
        "commands",
        "workspace_id actor_id idempotency_key request_digest command_id response created_at",
        ("workspace_id", "actor_id", "idempotency_key"),
    ),
    ("events", "workspace_id sequence id payload", ("workspace_id", "sequence")),
    ("outbox", "event_id workspace_id sequence delivered delivery_attempts", ("event_id",)),
    (
        "budgets",
        "workspace_id campaign_id dimension payload",
        ("workspace_id", "campaign_id", "dimension"),
    ),
    (
        "budget_reservations",
        "workspace_id id campaign_id owner_id state payload",
        ("workspace_id", "id"),
    ),
    (
        "budget_settlements",
        "workspace_id settlement_id request_digest response",
        ("workspace_id", "settlement_id"),
    ),
    (
        "budget_entries",
        "workspace_id id campaign_id dimension settlement_id payload",
        ("workspace_id", "id"),
    ),
    (
        "recovery_snapshots",
        "workspace_id id actor_id watermark expires_at released_at expired_at payload",
        ("workspace_id", "id"),
    ),
    (
        "recovery_items",
        "workspace_id snapshot_id ordinal content_digest size_bytes content",
        ("workspace_id", "snapshot_id", "ordinal"),
    ),
)

INDEXES = {
    "records_campaign": "records",
    "records_workspace": "records",
    "records_candidate_tree": "records",
    "records_worker_evidence": "records",
    "records_worker_pending": "records",
    "records_worker_trial": "records",
    "records_ready_jobs": "records",
    "records_worker_controls": "records",
    "acceptance_leases": "records",
    "budget_settlements_reservation": "budget_settlements",
    "recovery_pins": "recovery_snapshots",
}


def require_research_schema(sql: SqlSession) -> None:
    if sql.execute("PRAGMA foreign_keys").one()[0] != 1:
        raise unsupported()
    for name, columns, primary_key in TABLES:
        require_table(sql, name, columns, primary_key)
    for name, table in INDEXES.items():
        found = sql.execute(
            "SELECT type,tbl_name FROM sqlite_master WHERE name=?", (name,)
        ).fetchone()
        if found is None or found[0] != "index" or found[1] != table:
            raise unsupported()


def require_credentials_schema(sql: SqlSession) -> None:
    if sql.execute("PRAGMA foreign_keys").one()[0] != 1:
        raise unsupported()
    require_table(
        sql,
        "credentials",
        "id token_digest workspace_id principal created_at expires_at revoked",
        ("id",),
        unique=(("token_digest",),),
        foreign_keys=(("workspace_id", "workspaces", "id"),),
    )
