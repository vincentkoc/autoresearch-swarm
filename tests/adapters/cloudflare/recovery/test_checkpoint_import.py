"""Private logical import is atomic and exact replay verifies every inactive row."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace

import pytest

from adapters.cloudflare.recovery import checkpoint_import
from adapters.cloudflare.recovery.checkpoint_anchor import (
    KIND,
    TABLE,
    register_anchor_in,
    require_anchor_in,
)
from adapters.cloudflare.recovery.checkpoint_import import import_checkpoint
from adapters.cloudflare.runtime.deployment import DeploymentManifest, authority_id
from research_swarm.checkpoints.image_format import TABLES, describe_chunk, sign_manifest
from research_swarm.crypto import public_key
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.checkpoint_images import (
    LogicalCheckpointManifest,
    LogicalChunk,
    LogicalImportIntent,
)
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database, Transaction, encode
from research_swarm.state.sql import StoreIdentity
from tests.adapters.cloudflare.hosted.test_hosted_router import FixtureStore
from tests.adapters.cloudflare.recovery.test_checkpoint_capture import capture, portable_rows
from tests.adapters.cloudflare.recovery.test_checkpoint_capture import sealed as sealed
from tests.adapters.cloudflare.recovery.test_checkpoint_schema import source as source
from tests.adapters.cloudflare.test_cloudflare_deployment import deployment as deployment
from tests.adapters.cloudflare.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.workers.test_time_witnesses import SEED

TARGET_SEED = b"t" * 32


def test_import_intent_digest_binds_the_fixed_target_contract():
    intent = LogicalImportIntent(
        restore_id="00000000-0000-0000-0000-000000000001",
        manifest_digest="sha256:" + "1" * 64,
        target_location="synthetic-target/" + "b" * 64,
        target_deployment_digest="sha256:" + "2" * 64,
        target_public_key="3" * 64,
        operator_id="00000000-0000-0000-0000-000000000002",
    )
    assert (
        intent.digest == "sha256:bb4e3768a5fb4c093dca5a43543300deb952ac0e1e9b5d9926e75cfed31c303a"
    )
    assert intent.model_copy(update={"target_location": "other/target"}).digest != intent.digest


@pytest.fixture
async def recovery(sealed, tmp_path):
    return make_recovery(sealed, tmp_path)


def target_secrets(e):
    return getattr(e, "target_secrets", e.source.secrets)


def make_recovery(sealed, tmp_path, *, configure_target=None):
    image = capture(sealed)
    raw = sealed.manifest.model_dump(mode="json")
    raw.update(namespace="restored-fixture", object_id="b" * 64)
    raw["configuration"]["store_location"] = raw["namespace"] + "/" + raw["object_id"]
    raw["configuration"]["authority"].update(
        state_authority_id=authority_id(raw["namespace"], raw["object_id"]),
        activation_id=new_id(),
        control_epoch=2,
    )
    target_manifest, secrets = (
        (DeploymentManifest.model_validate(raw), None)
        if configure_target is None
        else configure_target(raw, sealed)
    )
    target = FixtureStore(Database(tmp_path / "target.sqlite").store)
    target.identity = StoreIdentity("cloudflare-do", target_manifest.configuration.store_location)

    def metadata(sql):
        sql.execute(
            "CREATE TABLE research_schema (singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            "version INTEGER NOT NULL)"
        )
        sql.execute("INSERT INTO research_schema VALUES (1,3)")

    target.write(metadata)
    intent = LogicalImportIntent(
        restore_id=new_id(),
        manifest_digest=image.manifest.digest,
        target_location=target.identity.location,
        target_deployment_digest=target_manifest.digest,
        target_public_key=public_key(sealed.crypto, TARGET_SEED).hex(),
        operator_id=target_manifest.provisioning_actor_id,
    )
    result = SimpleNamespace(
        source=sealed,
        target=target,
        manifest=target_manifest,
        intent=intent,
        image=image,
        envelope=sign_manifest(image.manifest, SEED, crypto=sealed.crypto),
    )
    if secrets is not None:
        result.target_secrets = secrets
    return result


def restore(e, **changes):
    values = dict(
        credential=target_secrets(e).bootstrap,
        target_seed=TARGET_SEED,
        envelope=e.envelope,
        bodies=e.image.bodies,
        expected_source=e.source.manifest.configuration.authority,
        expected_custody=e.source.pin,
        crypto=e.source.crypto,
    )
    values.update(changes)
    return import_checkpoint(e.target, e.manifest, target_secrets(e), e.intent, **values)


def snapshot(store):
    def read(sql):
        names = [
            row[0]
            for row in sql.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        return {
            name: [dict(row) for row in sql.execute(f'SELECT * FROM "{name}"')] for name in names
        }

    return store.read(read)


async def test_import_retains_portable_anchor_without_original_election_authority(recovery):
    e = recovery
    source = e.source
    source.store.write(
        lambda sql: register_anchor_in(
            sql,
            source.manifest,
            source.store.identity,
            source.pin,
            source.context,
            allow_create=True,
        )
    )
    e.image = capture(source)
    e.intent = e.intent.model_copy(update={"manifest_digest": e.image.manifest.digest})
    e.envelope = sign_manifest(e.image.manifest, SEED, crypto=source.crypto)
    before = portable_rows(source)
    receipt = restore(e)
    assert restore(e) == receipt
    imported = snapshot(e.target)
    assert TABLE not in imported
    markers = [row for row in imported["records"] if row["kind"] == KIND]
    assert len(markers) == 1 and markers[0]["id"] == source.pin.custody_id
    assert markers[0]["payload"] == next(row[7] for row in before["records"] if row[1] == KIND)
    with pytest.raises(ResearchError):
        e.target.read(
            lambda sql: require_anchor_in(
                sql, source.manifest, e.target.identity, source.pin, required=True
            )
        )
    assert portable_rows(source) == before


async def test_import_preserves_evidence_and_replays_once_without_opening_runtime(
    recovery, monkeypatch
):
    e = recovery
    original = portable_rows(e.source)
    receipt = restore(e)
    target_rows = portable_rows(SimpleNamespace(store=e.target))
    for name in TABLES:
        if name not in {"records", "workspaces", "events", "outbox"}:
            assert target_rows[name] == original[name]
    assert receipt.target_control_epoch == 2
    assert target_rows["workspaces"][0][2] == 2
    assert (
        next(row for row in target_rows["records"] if row[1] == "workspace_gate")[4] == "restoring"
    )
    assert {row[4] for row in target_rows["records"] if row[1] == "restoration"} == {"inactive"}
    before = snapshot(e.target)
    monkeypatch.setattr(
        checkpoint_import, "restore_inactive_in", lambda *a, **kw: pytest.fail("replayed transform")
    )
    assert restore(e) == receipt
    assert snapshot(e.target) == before
    assert portable_rows(e.source) == original
    assert "hosted_provisioning" not in before and "hosted_authority_config" not in before


@pytest.mark.parametrize("stage", ["insert", "transform", "fingerprint"])
async def test_failed_import_leaves_target_exactly_as_it_was(recovery, monkeypatch, stage):
    e, before = recovery, snapshot(recovery.target)
    if stage == "insert":
        original = checkpoint_import.row_values

        def row_values(table, row):
            if table == "commands":
                raise RuntimeError("Synthetic insertion interruption")
            return original(table, row)

        monkeypatch.setattr(checkpoint_import, "row_values", row_values)
    elif stage == "transform":
        original = Transaction.event

        def event(self, name, *args, **kwargs):
            if name == "workspace.restored_inactive":
                raise RuntimeError("Synthetic transform interruption")
            return original(self, name, *args, **kwargs)

        monkeypatch.setattr(Transaction, "event", event)
    else:
        monkeypatch.setattr(
            checkpoint_import,
            "fingerprint_state",
            lambda *a: (_ for _ in ()).throw(RuntimeError("Synthetic fingerprint interruption")),
        )
    with pytest.raises(RuntimeError, match="Synthetic"):
        restore(e)
    assert snapshot(e.target) == before


@pytest.mark.parametrize("table", tuple(TABLES))
async def test_changed_portable_state_refuses_exact_import_replay(recovery, table):
    e = recovery
    restore(e)
    rows = snapshot(e.target)[table]
    if not rows:
        fields = TABLES[table].columns
        values = tuple(
            e.source.pin.workspace_id
            if c.name == "workspace_id"
            else 1
            if c.kind == "integer"
            else None
            if c.nullable
            else "added"
            for c in fields
        )
        e.target.write(
            lambda sql: sql.execute(
                f'INSERT INTO "{table}" VALUES (' + ",".join("?" for _ in fields) + ")", values
            )
        )
    else:
        assignment = {
            "workspaces": "event_floor=event_floor+1",
            "records": "payload=payload||' '",
            "commands": "response=response||' '",
            "events": "payload=payload||' '",
            "outbox": "delivery_attempts=delivery_attempts+1",
            "recovery_snapshots": "payload=payload||' '",
            "recovery_items": "content=X'0102'",
            "credentials": "revoked=1",
        }[table]
        e.target.write(lambda sql: sql.execute(f'UPDATE "{table}" SET {assignment}'))
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        restore(e)
    assert snapshot(e.target) == before


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TABLE hosted_provisioning (singleton INTEGER)",
        "CREATE TABLE unknown_state (value TEXT)",
        "CREATE TRIGGER _cf_hidden AFTER INSERT ON records BEGIN SELECT 1; END",
        "DELETE FROM commands",
        "DELETE FROM hosted_checkpoint_import",
    ],
)
async def test_added_metadata_or_deleted_state_cannot_bypass_replay(recovery, statement):
    restore(recovery)
    recovery.target.write(lambda sql: sql.execute(statement))
    with pytest.raises((ResearchError, RuntimeError)):
        restore(recovery)


@pytest.mark.parametrize(
    "change", ["credential", "seed", "restore", "digest", "physical", "signature"]
)
async def test_wrong_target_or_unverified_source_never_imports_rows(recovery, change):
    e, options = recovery, {}
    if change == "credential":
        options["credential"] = "wrong"
    elif change == "seed":
        options["target_seed"] = b"x" * 32
    elif change == "restore":
        restore(e)
        e.intent = e.intent.model_copy(update={"restore_id": new_id()})
    elif change == "digest":
        e.intent = e.intent.model_copy(update={"target_deployment_digest": "sha256:" + "0" * 64})
    elif change == "physical":
        e.target.identity = replace(e.target.identity, location="another/authority")
    else:
        options["envelope"] = e.envelope.replace(b'"signature":"', b'"signature":"0', 1)
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        restore(e, **options)
    assert snapshot(e.target) == before


@pytest.mark.parametrize("change", ["seal", "watermark", "custody", "provider", "constraint"])
async def test_signed_but_inconsistent_source_rolls_back_import(recovery, change):
    e, chunks = recovery, []
    for body in e.image.bodies:
        raw = strict_json_loads(body)
        for row in raw["rows"]:
            if raw["table"] == "workspaces" and change == "watermark":
                row[3] += 1
            elif raw["table"] == "records":
                if change == "seal" and row[1] == "workspace_gate":
                    row[4] = "active"
                elif change == "custody" and row[1] == "backup_custody":
                    payload = strict_json_loads(row[7])
                    payload["public_key"] = "f" * 64
                    row[7] = encode(payload)
                elif change == "provider" and row[1] == "time_provider":
                    payload = strict_json_loads(row[7])
                    payload["scope"]["activation_id"] = new_id()
                    row[7] = encode(payload)
            elif raw["table"] == "credentials" and change == "constraint":
                row[1] = "f" * 64
        chunks.append(describe_chunk(LogicalChunk.model_validate(raw)))
    manifest = LogicalCheckpointManifest.model_validate(
        {
            **e.image.manifest.model_dump(mode="json"),
            "chunks": [item[0].model_dump(mode="json") for item in chunks],
        }
    )
    e.image = SimpleNamespace(manifest=manifest, bodies=tuple(item[1] for item in chunks))
    e.envelope = sign_manifest(manifest, SEED, crypto=e.source.crypto)
    e.intent = e.intent.model_copy(update={"manifest_digest": manifest.digest})
    before = snapshot(e.target)
    with pytest.raises(ResearchError):
        restore(e)
    assert snapshot(e.target) == before


async def test_competing_connections_return_one_committed_import(recovery):
    e = recovery
    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts = list(pool.map(lambda _: restore(e), range(2)))
    assert receipts[0] == receipts[1]
    rows = snapshot(e.target)
    assert len([row for row in rows["records"] if row["kind"] == "restoration"]) == 1
    assert rows["workspaces"][0]["control_epoch"] == 2


async def test_mismatched_deployment_secret_pair_refuses_before_sql_write(recovery, monkeypatch):
    e = recovery
    secrets = replace(e.source.secrets, bootstrap="X" * 43)
    monkeypatch.setattr(e.target, "write", lambda *a: pytest.fail("wrong bootstrap opened target"))
    with pytest.raises(ResearchError):
        import_checkpoint(
            e.target,
            e.manifest,
            secrets,
            e.intent,
            credential=secrets.bootstrap,
            target_seed=TARGET_SEED,
            envelope=e.envelope,
            bodies=e.image.bodies,
            expected_source=e.source.manifest.configuration.authority,
            expected_custody=e.source.pin,
            crypto=e.source.crypto,
        )


@pytest.mark.parametrize("sealed", ["worker"], indirect=True)
async def test_import_fences_the_copy_and_archives_original_worker_binding(recovery):
    e = recovery
    before = portable_rows(e.source)
    receipt = restore(e)
    binding = e.source.worker.binding
    rows = snapshot(e.target)["records"]
    worker = next(row for row in rows if row["kind"] == "worker")
    archived = next(row for row in rows if row["kind"] == "worker_session")
    fence = next(row for row in rows if row["kind"] == "worker_fence")
    assert receipt.worker_ids == (binding.worker_id,)
    assert worker["state"] == "fenced" and worker["revision"] == 2
    assert strict_json_loads(worker["payload"]) == e.source.worker.model_dump(mode="json")
    assert strict_json_loads(archived["payload"]) == e.source.worker.model_dump(mode="json")
    assert fence["id"] == archived["id"] == binding.worker_session_id
    assert strict_json_loads(fence["payload"])["control_epoch"] == 2
    assert portable_rows(e.source) == before
    assert restore(e) == receipt
