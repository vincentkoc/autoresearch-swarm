"""A sealed logical image preserves the full cut through one bounded read snapshot."""

from dataclasses import replace

import pytest

from adapters.cloudflare import checkpoint_capture
from adapters.cloudflare.checkpoint_capture import capture_checkpoint
from research_swarm.checkpoints.capture import CaptureLimits, capture_rows, preflight_rows
from research_swarm.checkpoints.image_format import LAYOUT, row_values, sign_manifest, verify_image
from research_swarm.control.backup import BackupControl
from research_swarm.control.worker_records import WorkerData
from research_swarm.crypto import public_key
from research_swarm.domain.backup import BackupCustodyPin, RegisterBackupCustody, SealWorkspace
from research_swarm.domain.canonical import content_digest, strict_json_loads
from research_swarm.domain.checkpoint_images import LogicalChunk
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.domain.workers import EngineCapabilities, WorkerBinding
from research_swarm.state.database import Database, Transaction, encode
from research_swarm.state.schema_contracts import quoted
from tests.adapters.cloudflare.recovery.test_checkpoint_schema import source as source
from tests.adapters.cloudflare.test_cloudflare_deployment import deployment as deployment
from tests.adapters.cloudflare.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.workers.test_time_witnesses import SEED

RAW = ' { "x":1, "x":2, "name":"e\u0301", "nul":"\\u0000" }\n'


@pytest.fixture
async def sealed(source, request):
    e = source
    authority = e.manifest.configuration.authority
    principal = e.manifest.credentials[0].principal
    e.context = CommandContext(principal=principal, idempotency_key="seal", expected_revision=0)
    e.backup_id, e.snapshot_id = new_id(), new_id()

    def evidence(sql):
        if getattr(request, "param", None) == "worker":
            e.worker = WorkerData(
                binding=WorkerBinding(
                    workspace_id=authority.workspace_id,
                    controller_id=authority.controller_id,
                    control_epoch=authority.control_epoch,
                    worker_id=new_id(),
                    worker_session_id=new_id(),
                ),
                principal_id=principal.id,
                name="Synthetic retained worker",
                max_concurrency=1,
                profiles=(
                    EngineCapabilities(
                        name="python-cpu",
                        backend="python",
                        cpu_slots=1,
                        execution_fingerprint=content_digest(b"synthetic-worker"),
                    ),
                ),
            )
            Transaction(sql, e.context).insert(
                "worker",
                e.worker.binding.worker_id,
                e.worker.model_dump(mode="json"),
                state="active",
            )
        for key in ["\ue000", "\U00010000", *(f"command-{i:03}" for i in range(250))]:
            sql.execute(
                "INSERT INTO commands VALUES (?,?,?,?,?,?,?)",
                (
                    authority.workspace_id,
                    principal.id,
                    key,
                    content_digest(b"request"),
                    new_id(),
                    RAW,
                    "created",
                ),
            )
        sql.execute(
            "INSERT INTO recovery_snapshots VALUES (?,?,?,?,?,?,?,?)",
            (authority.workspace_id, e.snapshot_id, principal.id, 1, "expires", None, None, RAW),
        )
        sql.execute(
            "INSERT INTO recovery_items VALUES (?,?,?,?,?,?)",
            (authority.workspace_id, e.snapshot_id, 0, content_digest(b"\0\xff"), 2, b"\0\xff"),
        )
        sql.execute("UPDATE credentials SET revoked=1 WHERE id=?", (e.manifest.credentials[-1].id,))

    e.store.write(evidence)
    control = BackupControl(Database.from_store(e.store))
    control.seal(
        e.context,
        SealWorkspace(
            backup_id=e.backup_id,
            expected_controller_id=authority.controller_id,
            expected_control_epoch=authority.control_epoch,
            reason="Synthetic logical capture",
        ),
    )
    e.pin = BackupCustodyPin(
        custody_id=new_id(),
        workspace_id=authority.workspace_id,
        controller_id=authority.controller_id,
        control_epoch=authority.control_epoch,
        public_key=public_key(e.crypto, SEED).hex(),
    )
    control.register_custody(
        e.context.model_copy(update={"idempotency_key": "pin"}),
        RegisterBackupCustody(backup_id=e.backup_id, pin=e.pin),
    )
    e.token = e.secrets.credentials[e.manifest.credentials[0].id]
    return e


def capture(e, **changes):
    options = {"token": e.token, "backup_id": e.backup_id, "expected_custody": e.pin, **changes}
    return capture_checkpoint(e.store, e.manifest, **options)


def portable_rows(e):
    return e.store.read(
        lambda sql: {
            table.name: [
                tuple(row[column.name] for column in table.columns)
                for row in sql.execute(
                    f"SELECT * FROM {quoted(table.name)} ORDER BY "
                    + ",".join(quoted(name) + " COLLATE BINARY" for name in table.primary_key)
                )
            ]
            for table in LAYOUT
        }
    )


def decoded(image):
    rows = {table.name: [] for table in LAYOUT}
    for body in image.bodies:
        chunk = LogicalChunk.model_validate_json(body)
        rows[chunk.table].extend(row_values(chunk.table, row) for row in chunk.rows)
    return rows


async def test_capture_preserves_exact_cut_and_signs_without_witness_or_mutation(sealed):
    before, witness_calls = portable_rows(sealed), len(sealed.witness.requests)
    sealed.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic outage")
    image = capture(sealed)
    assert decoded(image) == before
    assert portable_rows(sealed) == before
    assert len(sealed.witness.requests) == witness_calls
    assert capture(sealed) == image
    assert image.manifest.source_deployment_digest == sealed.manifest.digest
    assert image.manifest.source_configuration_digest == sealed.manifest.configuration.digest
    assert image.manifest.source_storage_digest == sealed.manifest.configuration.storage.digest
    signed = sign_manifest(image.manifest, SEED, crypto=sealed.crypto)
    verified = verify_image(
        signed,
        image.bodies,
        expected_digest=image.manifest.digest,
        expected_source=sealed.manifest.configuration.authority,
        expected_custody=sealed.pin,
        crypto=sealed.crypto,
    )
    assert verified.manifest == image.manifest
    assert "hosted_provisioning" not in decoded(image)
    assert b"rsw_" not in signed and all(b"rsw_" not in body for body in image.bodies)
    assert all(row[5] == RAW for row in before["commands"] if row[2].startswith("command-"))
    assert before["recovery_items"][0][-1] == b"\0\xff"
    assert any(row[-1] == 1 for row in before["credentials"])


async def test_chunk_boundaries_do_not_depend_on_page_size_and_capture_uses_one_read(sealed):
    original, calls, queries = sealed.store, [], []

    class Observe:
        identity = original.identity

        def read(self, callback):
            calls.append("read")

            class Session:
                def __init__(self, sql):
                    self.sql = sql

                def execute(self, statement, parameters=()):
                    if statement.startswith('SELECT "'):
                        assert " LIMIT ?" in statement and "OFFSET" not in statement
                        assert parameters[-1] <= 8
                        queries.append(statement)
                    return self.sql.execute(statement, parameters)

            return original.read(lambda sql: callback(Session(sql)))

    sealed.store = Observe()
    first = capture(sealed, limits=CaptureLimits(page_rows=1, chunk_bytes=32768))
    assert calls == ["read"]
    assert any(">(" in query for query in queries)
    second = capture(sealed, limits=CaptureLimits(page_rows=8, chunk_bytes=32768))
    assert first == second
    assert len([item for item in first.manifest.chunks if item.table == "commands"]) > 1
    assert sum(item.row_count for item in first.manifest.tables) == sum(
        map(len, decoded(first).values())
    )


@pytest.mark.parametrize(
    "change", ["token", "revoked", "expiry", "principal", "pin", "backup", "physical", "provider"]
)
async def test_source_authentication_scope_and_custody_refuse(sealed, change):
    options = {}
    if change == "token":
        options["token"] = "wrong"
    elif change == "pin":
        options["expected_custody"] = sealed.pin.model_copy(update={"custody_id": new_id()})
    elif change == "backup":
        options["backup_id"] = new_id()
    elif change == "physical":
        sealed.store.identity = replace(sealed.store.identity, location="other/authority")
    elif change == "provider":

        def alter(sql):
            row = sql.execute("SELECT payload FROM records WHERE kind='time_provider'").one()
            payload = strict_json_loads(row[0])
            payload["scope"]["activation_id"] = new_id()
            sql.execute(
                "UPDATE records SET payload=? WHERE kind='time_provider'", (encode(payload),)
            )

        sealed.store.write(alter)
    else:
        column, value = {
            "revoked": ("revoked", 1),
            "expiry": ("expires_at", "2099-01-01T00:00:00.000Z"),
            "principal": (
                "principal",
                encode(
                    sealed.context.principal.model_copy(update={"id": new_id()}).model_dump(
                        mode="json"
                    )
                ),
            ),
        }[change]
        sealed.store.write(
            lambda sql: sql.execute(
                f"UPDATE credentials SET {column}=? WHERE id=?",
                (value, sealed.manifest.credentials[0].id),
            )
        )
    before = portable_rows(sealed)
    with pytest.raises(ResearchError):
        capture(sealed, **options)
    assert portable_rows(sealed) == before


@pytest.mark.parametrize("payload", ["x" * 65536, "\0" * 12000, b"x" * 12000])
async def test_oversized_or_wrong_type_payload_refuses_before_cut_body_reads(
    sealed, monkeypatch, payload
):
    sealed.store.write(
        lambda sql: sql.execute(
            "UPDATE commands SET response=? WHERE idempotency_key='command-000'", (payload,)
        )
    )
    monkeypatch.setattr(
        checkpoint_capture,
        "inspect_sealed_cut",
        lambda *a, **kw: pytest.fail("body read before length preflight"),
    )
    with pytest.raises(ResearchError):
        capture(sealed)


@pytest.mark.parametrize(
    "limits", [{"rows": 2}, {"image_bytes": 512}, {"chunks": 1}, {"manifest_bytes": 64}]
)
async def test_independent_source_profile_bounds_refuse_without_mutation(sealed, limits):
    before = portable_rows(sealed)
    with pytest.raises(ResearchError) as refused:
        capture(sealed, limits=CaptureLimits(**limits))
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert portable_rows(sealed) == before


async def test_capture_rechecks_row_counts_from_preflight(sealed):
    def inconsistent(sql):
        limits = CaptureLimits()
        inventory = preflight_rows(sql, limits)
        changed = tuple(
            item.model_copy(update={"row_count": 0}) if item.table == "commands" else item
            for item in inventory
        )
        return capture_rows(sql, backup_id=sealed.backup_id, inventory=changed, limits=limits)

    with pytest.raises(ResearchError) as refused:
        sealed.store.read(inconsistent)
    assert refused.value.code == ErrorCode.STALE_FENCE


async def test_unqualified_clock_history_is_evidence_and_never_requalified(sealed):
    sealed.store.write(
        lambda sql: sql.execute("UPDATE records SET state='unqualified' WHERE kind='time_provider'")
    )
    before, calls = portable_rows(sealed), len(sealed.witness.requests)
    assert decoded(capture(sealed)) == before
    assert portable_rows(sealed) == before and len(sealed.witness.requests) == calls


async def test_concurrent_connection_cannot_split_the_captured_snapshot(sealed):
    original, observed = sealed.store, []
    before = portable_rows(sealed)

    class Concurrent:
        identity = original.identity

        def read(self, callback):
            class Session:
                def __init__(self, sql):
                    self.sql = sql

                def execute(self, statement, parameters=()):
                    if statement.startswith('SELECT "workspace_id","actor_id"') and not observed:
                        # Direct fixture tampering on an independent connection exercises
                        # WAL snapshot isolation; normal sealed mutations already refuse.
                        original.write(
                            lambda other: other.execute(
                                "UPDATE commands SET response='changed' "
                                "WHERE idempotency_key='command-000'"
                            )
                        )
                        observed.append(True)
                    return self.sql.execute(statement, parameters)

            return original.read(lambda sql: callback(Session(sql)))

    sealed.store = Concurrent()
    image = capture(sealed)
    sealed.store = original
    assert observed == [True]
    assert decoded(image) == before
    assert decoded(capture(sealed)) != before


@pytest.mark.parametrize(
    "table,column",
    [
        ("hosted_provisioning", "manifest"),
        ("hosted_authority_config", "payload"),
        ("hosted_provisioning", "digest"),
        ("hosted_provisioning", "state"),
        ("hosted_authority_config", "digest"),
        ("research_schema", "version"),
    ],
)
async def test_oversized_metadata_refuses_before_deployment_payload_read(
    sealed, monkeypatch, table, column
):
    from adapters.cloudflare import checkpoint_schema

    sealed.store.write(
        lambda sql: sql.execute(f"UPDATE {table} SET {column}=?", ("x" * (256 * 1024 + 1),))
    )
    monkeypatch.setattr(
        checkpoint_schema,
        "require_deployment_in",
        lambda *a: pytest.fail("oversized metadata read"),
    )
    with pytest.raises(ResearchError) as refused:
        capture(sealed)
    assert refused.value.code == ErrorCode.SCHEMA_UNSUPPORTED


@pytest.mark.parametrize("column", ["token_digest", "principal", "expires_at", "created_at"])
async def test_operator_admission_does_not_materialize_unbounded_credential_fields(sealed, column):
    sealed.store.write(
        lambda sql: sql.execute(
            f"UPDATE credentials SET {column}=? WHERE id=?",
            ("x" * 70000, sealed.manifest.credentials[0].id),
        )
    )
    original, queries = sealed.store, []

    class Guard:
        identity = original.identity

        def read(self, callback):
            class Session:
                def __init__(self, sql):
                    self.sql = sql

                def execute(self, statement, parameters=()):
                    assert not statement.startswith("SELECT * FROM credentials")
                    queries.append(statement)
                    return self.sql.execute(statement, parameters)

            return original.read(lambda sql: callback(Session(sql)))

    sealed.store = Guard()
    with pytest.raises(ResearchError):
        capture(sealed)
    assert any(query.startswith("SELECT principal FROM credentials") for query in queries)
