"""Bounded retained import metadata, independent of inactive or completed lifecycle policy."""

from research_swarm.domain.backup import RestoreReceipt
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.schema_contracts import unsupported

IMPORT_TABLE = "hosted_checkpoint_import"
IMPORT_COLUMNS = ("singleton", "intent", "receipt", "state_digest")


def saved_import(sql, intent):
    """Read immutable import metadata; this alone does not verify current target state."""
    columns = sql.execute(
        f"SELECT * FROM pragma_table_xinfo('{IMPORT_TABLE}') ORDER BY cid LIMIT 5"
    ).fetchall()
    if len(columns) != 4 or any(
        (
            row["name"],
            row["type"].upper(),
            row["notnull"],
            row["pk"],
            row["hidden"],
            row["dflt_value"],
        )
        != (
            name,
            "INTEGER" if ordinal == 0 else "TEXT",
            int(ordinal > 0),
            int(ordinal == 0),
            0,
            None,
        )
        for ordinal, (row, name) in enumerate(zip(columns, IMPORT_COLUMNS, strict=True))
    ):
        raise unsupported()
    if sql.execute(f"SELECT COUNT(*) FROM (SELECT 1 FROM {IMPORT_TABLE} LIMIT 2)").one()[0] != 1:
        raise unsupported()
    row = sql.execute(
        f"SELECT intent,receipt,state_digest FROM {IMPORT_TABLE} WHERE singleton=1 "
        "AND typeof(intent)='text' AND length(CAST(intent AS BLOB))<=4096 "
        "AND typeof(receipt)='text' AND length(CAST(receipt AS BLOB))<=131072 "
        "AND typeof(state_digest)='text' AND length(CAST(state_digest AS BLOB))=71"
    ).fetchone()
    if row is None or strict_json_loads(row["intent"]) != intent.model_dump(mode="json"):
        raise ResearchError(ErrorCode.STALE_FENCE, "Inactive import intent differs")
    receipt = RestoreReceipt.model_validate_json(row["receipt"])
    return receipt, row["state_digest"]
