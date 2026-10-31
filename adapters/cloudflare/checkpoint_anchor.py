"""Original-authority election state is never copied into a logical checkpoint."""

from dataclasses import dataclass

from research_swarm.domain.checkpoint_elections import LogicalElectionAnchor, SignedLogicalElection
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Transaction, encode
from research_swarm.state.schema_contracts import unsupported

TABLE = "hosted_checkpoint_elections"
KIND = "checkpoint_election_anchor"
COLUMNS = ("activation_id", "custody_id", "anchor_digest", "state", "election_payload")
CREATE = f"""CREATE TABLE {TABLE} (
    activation_id TEXT NOT NULL,
    custody_id TEXT NOT NULL,
    anchor_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    election_payload TEXT,
    PRIMARY KEY (activation_id,custody_id)
)"""


@dataclass(frozen=True)
class AnchorState:
    anchor: LogicalElectionAnchor
    state: str
    election: SignedLogicalElection | None


def require_election_schema(sql):
    columns = sql.execute(
        f"SELECT * FROM pragma_table_xinfo('{TABLE}') ORDER BY cid LIMIT 6"
    ).fetchall()
    if len(columns) != len(COLUMNS) or any(
        (
            row["cid"],
            row["name"],
            row["type"].upper(),
            row["notnull"],
            row["pk"],
            row["hidden"],
            row["dflt_value"],
        )
        != (i, name, "TEXT", int(i != 4), i + 1 if i < 2 else 0, 0, None)
        for i, (row, name) in enumerate(zip(columns, COLUMNS, strict=True))
    ):
        raise unsupported()
    if sql.execute(f"SELECT COUNT(*) FROM (SELECT 1 FROM {TABLE} LIMIT 2)").one()[0] != 1:
        raise unsupported()
    invalid = sql.execute(
        f"SELECT 1 FROM {TABLE} WHERE "
        "typeof(activation_id)!='text' OR length(CAST(activation_id AS BLOB))!=36 OR "
        "typeof(custody_id)!='text' OR length(CAST(custody_id AS BLOB))!=36 OR "
        "typeof(anchor_digest)!='text' OR length(CAST(anchor_digest AS BLOB))!=71 OR "
        "typeof(state)!='text' OR state NOT IN ('open','elected') OR "
        "(state='open' AND election_payload IS NOT NULL) OR "
        "(state='elected' AND (election_payload IS NULL OR typeof(election_payload)!='text' "
        "OR length(CAST(election_payload AS BLOB))>16384)) LIMIT 1"
    ).fetchone()
    if invalid is not None:
        raise unsupported()


def expected_anchor(manifest, identity, pin):
    manifest.require_store(identity)
    return LogicalElectionAnchor(
        source_authority=manifest.configuration.authority,
        source_backend=identity.backend,
        source_location=identity.location,
        source_deployment_digest=manifest.digest,
        custody_pin=pin,
    )


def require_anchor_in(sql, manifest, identity, pin, *, required=False):
    expected = expected_anchor(manifest, identity, pin)
    table = sql.execute("SELECT type FROM sqlite_master WHERE name=?", (TABLE,)).fetchone()
    marker = sql.execute(
        "SELECT 1 FROM records WHERE workspace_id=? AND kind=? AND id=?",
        (pin.workspace_id, KIND, pin.custody_id),
    ).fetchone()
    if table is None and marker is None and not required:
        return None
    if table is None or table[0] != "table" or marker is None:
        raise ResearchError(ErrorCode.STALE_FENCE, "Original checkpoint election state is missing")
    require_election_schema(sql)
    record = sql.execute(
        "SELECT payload FROM records WHERE workspace_id=? AND kind=? AND id=? "
        "AND typeof(revision)='integer' AND revision=1 AND state='immutable' "
        "AND campaign_id IS NULL AND parent_id IS NULL AND typeof(payload)='text' "
        "AND length(CAST(payload AS BLOB))<=8192",
        (pin.workspace_id, KIND, pin.custody_id),
    ).fetchone()
    row = sql.execute(f"SELECT * FROM {TABLE}").one()
    if (
        record is None
        or LogicalElectionAnchor.model_validate_json(record["payload"]) != expected
        or (row["activation_id"], row["custody_id"], row["anchor_digest"])
        != (expected.source_authority.activation_id, pin.custody_id, expected.digest)
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Original checkpoint election identity differs")
    election = (
        SignedLogicalElection.model_validate_json(row["election_payload"])
        if row["state"] == "elected"
        else None
    )
    if election is not None:
        request = election.election.signed_request.request
        claim = election.election.signed_claim.claim
        if (
            row["election_payload"] != encode(election.model_dump(mode="json"))
            or request.source_authority != expected.source_authority
            or request.source_deployment_digest != expected.source_deployment_digest
            or request.signed_request.request.source_custody_id != pin.custody_id
            or claim.source_public_key != pin.public_key
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Stored checkpoint election source differs")
    return AnchorState(expected, row["state"], election)


def register_anchor_in(sql, manifest, identity, pin, context, *, allow_create):
    existing = require_anchor_in(sql, manifest, identity, pin)
    if existing is not None:
        return existing
    if not allow_create:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Legacy sealed custody has no election anchor"
        )
    anchor = expected_anchor(manifest, identity, pin)
    sql.execute(CREATE)
    transaction = Transaction(sql, context, observe_waits=False)
    marker = transaction.insert(
        KIND, pin.custody_id, anchor.model_dump(mode="json"), state="immutable"
    )
    transaction.event(
        "workspace.checkpoint_election_anchored", marker, {"anchor_digest": anchor.digest}
    )
    sql.execute(
        f"INSERT INTO {TABLE} VALUES (?,?,?,'open',NULL)",
        (anchor.source_authority.activation_id, pin.custody_id, anchor.digest),
    )
    return require_anchor_in(sql, manifest, identity, pin, required=True)
