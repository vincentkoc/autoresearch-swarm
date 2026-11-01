"""Logical image wire integrity preserves raw SQL values and rejects ambiguous coverage."""

import pytest
from pydantic import ValidationError

from research_swarm.checkpoints.image_format import (
    LAYOUT,
    LAYOUT_DIGEST,
    TABLES,
    describe_chunk,
    encode_row,
    row_values,
    sign_manifest,
    verify_image,
)
from research_swarm.crypto import public_key, resolve_crypto
from research_swarm.domain.backup import BackupCustodyPin, WorkspaceSeal
from research_swarm.domain.canonical import (
    MAX_SAFE_INTEGER,
    canonical_bytes,
    content_digest,
    record_digest,
    strict_json_loads,
)
from research_swarm.domain.checkpoint_images import (
    MAX_CHUNK_BYTES,
    MAX_IMAGE_BYTES,
    MAX_MANIFEST_BYTES,
    TABLE_ORDER,
    BlobCell,
    LogicalCheckpointManifest,
    LogicalChunk,
    TableInventory,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.schema_contracts import TABLES as SCHEMA_TABLES
from tests.workers.test_time_witnesses import SEED, identifier, scope

RAW = ' { "x":1, "x":2, "name":"e\u0301" }\n'


def image(*, command_keys=("\ue000", "\U00010000")):
    crypto, authority = resolve_crypto(None), scope().authority
    workspace, controller = authority.workspace_id, authority.controller_id
    custody = BackupCustodyPin(
        custody_id=identifier(11),
        workspace_id=workspace,
        controller_id=controller,
        control_epoch=1,
        public_key=public_key(crypto, SEED).hex(),
    )
    seal = WorkspaceSeal(
        backup_id=identifier(12),
        workspace_id=workspace,
        controller_id=controller,
        control_epoch=1,
        operator_id=identifier(13),
        reason="Synthetic logical image",
        sealed_at="2026-01-01T00:00:00.000Z",
        evidence_through=1,
        unresolved_attempt_ids=(),
        worker_ids=(),
    )
    rows = {name: [] for name in TABLE_ORDER}
    rows["workspaces"] = [(workspace, controller, 1, 4, 1)]
    rows["commands"] = [
        (
            workspace,
            identifier(13),
            key,
            content_digest(b"command"),
            identifier(20 + i),
            RAW,
            "created",
        )
        for i, key in enumerate(command_keys)
    ]
    rows["recovery_snapshots"] = [
        (workspace, identifier(30), identifier(13), 1, "expires", None, None, RAW)
    ]
    rows["recovery_items"] = [
        (workspace, identifier(30), 0, content_digest(b"\0\xff"), 2, b"\0\xff")
    ]
    rows["credentials"] = [(identifier(40), "a" * 64, workspace, RAW, "created", None, 1)]
    chunks = []
    for table in TABLE_ORDER:
        for first_row, values in enumerate(rows[table]):
            chunks.append(
                LogicalChunk(
                    backup_id=seal.backup_id,
                    ordinal=len(chunks),
                    table=table,
                    first_row=first_row,
                    rows=(encode_row(table, values),),
                )
            )
    described = [describe_chunk(chunk) for chunk in chunks]
    manifest = LogicalCheckpointManifest(
        table_layout_digest=LAYOUT_DIGEST,
        source_authority=authority,
        source_deployment_digest=content_digest(b"deployment"),
        source_configuration_digest=content_digest(b"configuration"),
        source_storage_digest=content_digest(b"storage"),
        source_custody=custody,
        operator_id=identifier(13),
        seal=seal,
        seal_revision=1,
        seal_digest=record_digest("research.workspace-seal.v1", seal.model_dump(mode="json")),
        snapshot_evidence_through=3,
        tables=tuple(TableInventory(table=name, row_count=len(rows[name])) for name in TABLE_ORDER),
        chunks=tuple(item[0] for item in described),
    )
    return manifest, tuple(item[1] for item in described), crypto


def verified(manifest, bodies, crypto, **changes):
    expected = {
        "expected_digest": manifest.digest,
        "expected_source": manifest.source_authority,
        "expected_custody": manifest.source_custody,
        **changes,
    }
    return verify_image(
        sign_manifest(manifest, SEED, crypto=crypto), bodies, crypto=crypto, **expected
    )


def test_fixed_layout_covers_portable_schema_and_digest_is_semantic():
    assert tuple(table.name for table in LAYOUT) == TABLE_ORDER
    for name, columns, keys in SCHEMA_TABLES:
        assert tuple(column.name for column in TABLES[name].columns) == tuple(columns.split())
        assert TABLES[name].primary_key == keys
    assert (
        LAYOUT_DIGEST == "sha256:110ef0e0b635e8aed9c15e4706a1da9f67a27291190022ac6e64015f98124db0"
    )


def test_authenticated_roundtrip_keeps_raw_json_unicode_blob_null_and_revocation():
    manifest, bodies, crypto = image()
    assert (
        manifest.digest == "sha256:d9ef26ca39338441801784c99b2b6d743971c682b031be193fb0d2677afc94aa"
    )
    assert (
        manifest.chunks[0].digest
        == "sha256:8792114197b2af3450d418e4fdf8b0a3b5e9677355764839058fb9ccb3496c77"
    )
    result = verified(manifest, bodies, crypto)
    assert result.manifest == manifest
    assert len(result.manifest.tables) == 12
    assert result.manifest.seal.evidence_through == 1
    assert result.manifest.snapshot_evidence_through == 3
    commands = [
        row_values(item.table, item.rows[0]) for item in result.chunks if item.table == "commands"
    ]
    assert [row[2] for row in commands] == ["\ue000", "\U00010000"]
    assert all(row[5] == RAW for row in commands)
    recovery = next(item for item in result.chunks if item.table == "recovery_items")
    assert row_values(recovery.table, recovery.rows[0])[-1] == b"\0\xff"
    credential = next(item for item in result.chunks if item.table == "credentials")
    assert row_values(credential.table, credential.rows[0])[-2:] == (None, 1)
    assert sign_manifest(manifest, SEED, crypto=crypto) == sign_manifest(
        manifest, SEED, crypto=crypto
    )


@pytest.mark.parametrize("change", ["missing", "reordered", "duplicate", "corrupt", "extra"])
def test_chunk_bytes_must_match_every_signed_descriptor(change):
    manifest, bodies, crypto = image()
    altered = {
        "missing": bodies[:-1],
        "reordered": (bodies[1], bodies[0], *bodies[2:]),
        "duplicate": (*bodies[:-1], bodies[0]),
        "corrupt": (bodies[0] + b" ", *bodies[1:]),
        "extra": (*bodies, bodies[0]),
    }[change]
    with pytest.raises(ResearchError) as refused:
        verified(manifest, altered, crypto)
    assert refused.value.code == ErrorCode.ARTIFACT_UNVERIFIED


@pytest.mark.parametrize("keys", [("same", "same"), ("z", "a"), ("\U00010000", "\ue000")])
def test_even_authentic_images_require_strict_row_order_across_chunk_boundaries(keys):
    manifest, bodies, crypto = image(command_keys=keys)
    with pytest.raises(ResearchError):
        verified(manifest, bodies, crypto)


@pytest.mark.parametrize("field", ["source", "custody", "digest"])
def test_trusted_expected_identity_is_required_outside_the_signed_envelope(field):
    manifest, bodies, crypto = image()
    overrides = {
        "source": {
            "expected_source": manifest.source_authority.model_copy(
                update={"activation_id": identifier(99)}
            )
        },
        "custody": {
            "expected_custody": manifest.source_custody.model_copy(
                update={"custody_id": identifier(99)}
            )
        },
        "digest": {"expected_digest": content_digest(b"other")},
    }[field]
    with pytest.raises(ResearchError):
        verified(manifest, bodies, crypto, **overrides)


@pytest.mark.parametrize("field", ["table", "gap", "ordinal", "seal", "scope", "layout"])
def test_manifest_inventory_and_cut_cannot_be_reinterpreted(field):
    manifest, bodies, crypto = image()
    raw = manifest.model_dump(mode="json")
    if field == "table":
        raw["tables"].pop()
    elif field == "gap":
        raw["chunks"][1]["first_row"] = 1
    elif field == "ordinal":
        raw["chunks"][1]["ordinal"] = 2
    elif field == "seal":
        raw["seal"]["reason"] = "Changed sealed cut"
    elif field == "scope":
        raw["source_authority"]["workspace_id"] = identifier(99)
    else:
        raw["table_layout_digest"] = content_digest(b"other layout")
    with pytest.raises((ValidationError, ResearchError)):
        verified(LogicalCheckpointManifest.model_validate(raw), bodies, crypto)


@pytest.mark.parametrize(
    "value",
    [True, False, 1.0, float("nan"), MAX_SAFE_INTEGER + 1, -(MAX_SAFE_INTEGER + 1), "1", None],
)
def test_integer_cells_reject_coercion_and_precision_loss(value):
    with pytest.raises(ValueError):
        encode_row("workspaces", ("workspace", "controller", value, 1, 1))


@pytest.mark.parametrize("encoded", ["AA", "AB==", "AA===", "AA==\n", "é"])
def test_blob_encoding_requires_exact_padded_base64(encoded):
    with pytest.raises(ValueError):
        BlobCell(blob_base64=encoded)


def test_text_blob_null_and_invalid_utf8_cannot_alias():
    assert BlobCell(blob_base64="").blob_base64 == ""
    with pytest.raises(ValueError):
        encode_row("workspaces", (b"", "controller", 1, 1, 1))
    with pytest.raises(ValueError):
        encode_row("workspaces", ("\ud800", "controller", 1, 1, 1))
    with pytest.raises(ValueError):
        encode_row("recovery_items", ("workspace", "snapshot", 0, content_digest(b""), 0, ""))


def test_noncanonical_envelope_and_wire_bounds_refuse_without_truncation():
    manifest, bodies, crypto = image()
    expected = {
        "expected_digest": manifest.digest,
        "expected_source": manifest.source_authority,
        "expected_custody": manifest.source_custody,
        "crypto": crypto,
    }
    envelope = sign_manifest(manifest, SEED, crypto=crypto)
    for encoded, chunks in (
        (b" " + envelope, bodies),
        (b"x" * (MAX_MANIFEST_BYTES + 257), bodies),
        (envelope, (b"x" * (MAX_CHUNK_BYTES + 1),)),
        (envelope, (b"x" * MAX_CHUNK_BYTES,) * (MAX_IMAGE_BYTES // MAX_CHUNK_BYTES + 1)),
    ):
        with pytest.raises(ResearchError):
            verify_image(encoded, chunks, **expected)
    raw = {
        "schema_version": "1.0",
        "profile": "research-state-chunk-v1",
        "backup_id": manifest.seal.backup_id,
        "ordinal": 0,
        "first_row": 0,
        "table": "commands",
        "rows": [["w", "a", "key", "digest", "id", "x" * MAX_CHUNK_BYTES, "now"]],
    }
    with pytest.raises(ValueError):
        LogicalChunk.model_validate(raw)
    assert len(canonical_bytes(manifest.model_dump(mode="json"))) < MAX_MANIFEST_BYTES


def test_valid_identity_does_not_admit_a_forged_signature_or_a_different_signing_seed():
    manifest, bodies, crypto = image()
    raw = strict_json_loads(sign_manifest(manifest, SEED, crypto=crypto))
    raw["signature"] = "0" * 128
    with pytest.raises(ResearchError) as refused:
        verify_image(
            canonical_bytes(raw),
            bodies,
            expected_digest=manifest.digest,
            expected_source=manifest.source_authority,
            expected_custody=manifest.source_custody,
            crypto=crypto,
        )
    assert refused.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    with pytest.raises(ValueError, match="signing key differs"):
        sign_manifest(manifest, bytes(32), crypto=crypto)
