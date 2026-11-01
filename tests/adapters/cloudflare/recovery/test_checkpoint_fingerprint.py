"""Every portable table contributes exact typed rows to inactive replay identity."""

import subprocess
import sys

import pytest

from research_swarm.checkpoints.capture import CaptureLimits, preflight_rows
from research_swarm.checkpoints.fingerprint import INACTIVE_LIMITS, fingerprint_state
from research_swarm.checkpoints.image_format import LAYOUT, row_values
from research_swarm.control.auth import CredentialStore
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ResearchError
from research_swarm.state.database import SCHEMA
from research_swarm.state.sqlite import SQLiteStore
from tests.adapters.cloudflare.recovery.test_checkpoint_images import RAW, image, verified
from tests.workers.test_time_witnesses import identifier


@pytest.fixture
def state(tmp_path):
    manifest, bodies, crypto = image()
    source = verified(manifest, bodies, crypto)
    rows = {table.name: [] for table in LAYOUT}
    for chunk in source.chunks:
        rows[chunk.table].extend(row_values(chunk.table, row) for row in chunk.rows)
    workspace = manifest.source_authority.workspace_id
    rows["records"] = [
        (workspace, "marker", identifier(51), 1, "immutable", None, None, RAW, "created", "created")
    ]
    rows["events"] = [(workspace, 1, identifier(52), RAW)]
    rows["outbox"] = [(identifier(52), workspace, 1, 0, 0)]
    rows["budgets"] = [(workspace, identifier(53), "compute_seconds", RAW)]
    rows["budget_reservations"] = [
        (workspace, identifier(54), identifier(53), identifier(55), "held", RAW)
    ]
    rows["budget_settlements"] = [(workspace, identifier(56), content_digest(b"settled"), RAW)]
    rows["budget_entries"] = [
        (workspace, identifier(57), identifier(53), "compute_seconds", identifier(56), RAW)
    ]
    store = SQLiteStore(tmp_path / "fingerprint.sqlite", SCHEMA)
    store.write(CredentialStore.initialize_in)

    def seed(sql):
        for table in LAYOUT:
            statement = (
                f'INSERT INTO "{table.name}" VALUES (' + ",".join("?" for _ in table.columns) + ")"
            )
            for row in rows[table.name]:
                sql.execute(statement, row)

    store.write(seed)
    return store


def test_fingerprint_has_stable_framing_and_is_page_independent(state):
    first = state.read(fingerprint_state)
    assert first == state.read(
        lambda sql: fingerprint_state(
            sql, limits=INACTIVE_LIMITS.model_copy(update={"page_rows": 1})
        )
    )
    assert first == "sha256:7e6e430e9dbc674cdd025c2938458ab244ac4f4656c7e42a7233963f49a1fc17"


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE workspaces SET event_floor=2",
        "UPDATE records SET payload=payload||' '",
        "UPDATE commands SET response=response||' '",
        "UPDATE events SET payload=payload||' '",
        "UPDATE outbox SET delivery_attempts=1",
        "UPDATE budgets SET payload=payload||' '",
        "UPDATE budget_reservations SET payload=payload||' '",
        "UPDATE budget_settlements SET response=response||' '",
        "UPDATE budget_entries SET payload=payload||' '",
        "UPDATE recovery_snapshots SET payload=payload||' '",
        "UPDATE recovery_items SET content=X'0102'",
        "UPDATE credentials SET revoked=0",
    ],
)
def test_changed_evidence_in_each_portable_table_changes_identity(state, statement):
    before = state.read(fingerprint_state)
    state.write(lambda sql: sql.execute(statement))
    assert state.read(fingerprint_state) != before


def test_deletion_and_addition_change_identity_even_when_schema_stays_the_same(state):
    before = state.read(fingerprint_state)
    row = state.read(
        lambda sql: tuple(
            sql.execute("SELECT * FROM commands ORDER BY idempotency_key LIMIT 1").one()
        )
    )
    state.write(lambda sql: sql.execute("DELETE FROM commands WHERE idempotency_key=?", (row[2],)))
    deleted = state.read(fingerprint_state)
    assert deleted != before
    state.write(lambda sql: sql.execute("INSERT INTO commands VALUES (?,?,?,?,?,?,?)", row))
    assert state.read(fingerprint_state) == before
    added = (*row[:2], "extra", *row[3:])
    state.write(lambda sql: sql.execute("INSERT INTO commands VALUES (?,?,?,?,?,?,?)", added))
    assert state.read(fingerprint_state) not in {before, deleted}


def test_inactive_fingerprint_has_bounded_headroom_for_new_recovery_rows(state):
    workspace = state.read(lambda sql: sql.execute("SELECT id FROM workspaces").one()[0])

    def append(sql):
        for ordinal in range(4096):
            sql.execute(
                "INSERT INTO commands VALUES (?,?,?,?,?,?,?)",
                (
                    workspace,
                    identifier(60),
                    f"added-{ordinal:05}",
                    content_digest(b"request"),
                    identifier(61),
                    "{}",
                    "created",
                ),
            )

    state.write(append)
    with pytest.raises(ResearchError):
        state.read(lambda sql: preflight_rows(sql, CaptureLimits()))
    assert state.read(fingerprint_state).startswith("sha256:")
    with pytest.raises(ResearchError):
        state.read(lambda sql: fingerprint_state(sql, limits=CaptureLimits(image_bytes=1)))


def test_fingerprint_import_has_no_native_database_or_crypto_side_effects():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import research_swarm.checkpoints.fingerprint; "
            "assert not {'sqlite3', 'cryptography', 'filelock', 'research_swarm.runtime'}"
            ".intersection(sys.modules)",
        ],
        check=True,
        capture_output=True,
        timeout=10,
    )
