"""Read-only admission of every application table before a hosted logical export."""

from research_swarm.checkpoints.image_format import TABLES
from research_swarm.checkpoints.image_schema import require_portable_image_schema
from research_swarm.state.schema import SCHEMA_VERSION
from research_swarm.state.schema_contracts import unsupported

if __package__:
    from .provisioning import require_deployment_in
else:
    from provisioning import require_deployment_in

METADATA = {
    "research_schema": (("singleton", "INTEGER", 0, 1), ("version", "INTEGER", 1, 0)),
    "hosted_authority_config": (
        ("singleton", "INTEGER", 0, 1),
        ("digest", "TEXT", 0, 0),
        ("payload", "TEXT", 0, 0),
    ),
    "hosted_provisioning": (
        ("singleton", "INTEGER", 0, 1),
        ("digest", "TEXT", 1, 0),
        ("manifest", "TEXT", 1, 0),
        ("state", "TEXT", 1, 0),
        ("receipt", "TEXT", 0, 0),
    ),
}


def require_checkpoint_schema(sql, manifest):
    # Both Cloudflare prefixes are reserved, including _cf_METADATA alarm state.
    # Never inspect their columns or export provider-owned rows.
    objects = sql.execute(
        "SELECT type,name,tbl_name FROM sqlite_master "
        "WHERE substr(lower(tbl_name),1,7)!='sqlite_' "
        "AND substr(lower(tbl_name),1,5)!='__cf_' "
        "AND substr(lower(tbl_name),1,4)!='_cf_' LIMIT 129"
    ).fetchall()
    allowed = set(TABLES) | set(METADATA)
    if len(objects) > 128 or any(
        row["type"] not in {"table", "index"}
        or row["tbl_name"] not in allowed
        or (row["type"] == "table" and row["name"] not in allowed)
        for row in objects
    ):
        raise unsupported()
    if {row["name"] for row in objects if row["type"] == "table"} != allowed:
        raise unsupported()
    require_portable_image_schema(sql)
    for name, fields in METADATA.items():
        rows = sql.execute(
            "SELECT * FROM pragma_table_xinfo(?) ORDER BY cid LIMIT ?", (name, len(fields) + 1)
        ).fetchall()
        if len(rows) != len(fields) or any(
            (row["name"], row["type"].upper(), row["notnull"], row["pk"]) != expected
            or row["cid"] != ordinal
            or row["hidden"] != 0
            or row["dflt_value"] is not None
            for ordinal, (row, expected) in enumerate(zip(rows, fields, strict=True))
        ):
            raise unsupported()
        if sql.execute(f"SELECT COUNT(*) FROM (SELECT 1 FROM {name} LIMIT 2)").one()[0] != 1:
            raise unsupported()
        predicates = []
        for field, kind, _, _ in fields:
            predicates.append(f"typeof({field})!='{kind.lower()}'")
            if kind == "TEXT":
                bound = 71 if field == "digest" else 8 if field == "state" else 256 * 1024
                predicates.append(f"length(CAST({field} AS BLOB))>{bound}")
        if (
            sql.execute(f"SELECT 1 FROM {name} WHERE {' OR '.join(predicates)} LIMIT 1").fetchone()
            is not None
        ):
            raise unsupported()
    versions = sql.execute("SELECT singleton,version FROM research_schema LIMIT 2").one()
    if (versions[0], versions[1]) != (1, SCHEMA_VERSION):
        raise unsupported()
    require_deployment_in(sql, manifest)
