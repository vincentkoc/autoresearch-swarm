"""A portable marker cannot recreate original-authority election state."""

import json

import pytest
from pydantic import ValidationError

from adapters.cloudflare import checkpoint_source
from adapters.cloudflare.checkpoint_anchor import KIND, TABLE, require_anchor_in
from adapters.cloudflare.checkpoint_wire import decode_checkpoint
from research_swarm.domain.checkpoint_elections import LogicalElectionAnchor
from research_swarm.domain.checkpoint_images import LogicalChunk
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from tests.test_checkpoint_capture import portable_rows
from tests.test_checkpoint_elections import election as election
from tests.test_checkpoint_schema import source as source
from tests.test_checkpoint_source import call
from tests.test_checkpoint_source import configured as configured
from tests.test_cloudflare_deployment import deployment as deployment
from tests.test_cloudflare_provisioning import bootstrap as bootstrap


def anchor(e, *, required=False):
    return e.store.read(
        lambda sql: require_anchor_in(sql, e.manifest, e.store.identity, e.pin, required=required)
    )


def metadata(e):
    return e.store.read(lambda sql: [dict(row) for row in sql.execute(f"SELECT * FROM {TABLE}")])


def has_table(e):
    return e.store.read(
        lambda sql: (
            sql.execute("SELECT 1 FROM sqlite_master WHERE name=?", (TABLE,)).fetchone() is not None
        )
    )


def test_anchor_identity_binds_physical_source_and_custody_without_an_image_cycle(election):
    manifest = election.manifest
    value = LogicalElectionAnchor(
        source_authority=manifest.source_authority,
        source_location="synthetic-namespace/source-object",
        source_deployment_digest=manifest.source_deployment_digest,
        custody_pin=manifest.source_custody,
    )
    assert value.digest == (
        "sha256:7e8e1249d2fc3c65361a5dd57845dd9d9324858e250f7f4fd8fc2dfca8a26db1"
    )
    assert value.model_copy(update={"source_location": "other/source"}).digest != value.digest
    raw = value.model_dump(mode="json")
    raw["custody_pin"]["control_epoch"] += 1
    with pytest.raises(ValidationError):
        LogicalElectionAnchor.model_validate(raw)


async def test_first_seal_anchors_once_and_capture_excludes_election_authority(configured):
    e = configured
    assert anchor(e) is None and not has_table(e)
    with pytest.raises(ResearchError):
        anchor(e, required=True)
    result = await call(e)
    current = anchor(e, required=True)
    assert current.state == "open" and current.election is None
    assert current.anchor.custody_pin == e.pin
    assert current.anchor.source_location == e.store.identity.location
    assert current.anchor.source_authority == e.manifest.configuration.authority
    before, original = portable_rows(e), metadata(e)
    captured = await call(e, capture=True)
    assert await call(e) == result
    assert await call(e, capture=True) == captured
    assert portable_rows(e) == before and metadata(e) == original
    _, bodies = decode_checkpoint(captured)
    chunks = [LogicalChunk.model_validate_json(body) for body in bodies]
    assert all(chunk.table != TABLE for chunk in chunks)
    markers = [row for row in before["records"] if row[1] == KIND]
    assert len(markers) == 1 and markers[0][2] == e.pin.custody_id
    assert json.loads(markers[0][7]) == current.anchor.model_dump(mode="json")
    events = [json.loads(row[3])["type"] for row in before["events"]]
    assert events.count("workspace.checkpoint_election_anchored") == 1


@pytest.mark.parametrize(
    "change",
    [
        "table",
        "row",
        "marker",
        "revision",
        "location",
        "digest",
        "activation",
        "custody",
        "state",
        "open_payload",
        "elected_missing",
        "elected_large",
        "columns",
        "duplicate",
    ],
)
async def test_missing_changed_or_orphaned_state_refuses_capture_and_cached_seal(
    configured, change
):
    e = configured
    await call(e)

    def corrupt(sql):
        if change == "table":
            sql.execute(f"DROP TABLE {TABLE}")
        elif change == "row":
            sql.execute(f"DELETE FROM {TABLE}")
        elif change == "marker":
            sql.execute("DELETE FROM records WHERE kind=?", (KIND,))
        elif change == "revision":
            sql.execute("UPDATE records SET revision=2 WHERE kind=?", (KIND,))
        elif change == "location":
            sql.execute(
                "UPDATE records SET payload=json_set(payload,'$.source_location',?) WHERE kind=?",
                ("synthetic-namespace/other-source", KIND),
            )
        elif change == "digest":
            sql.execute(f"UPDATE {TABLE} SET anchor_digest=?", ("sha256:" + "a" * 64,))
        elif change in {"activation", "custody"}:
            sql.execute(f"UPDATE {TABLE} SET {change}_id=?", (new_id(),))
        elif change == "state":
            sql.execute(f"UPDATE {TABLE} SET state='unknown'")
        elif change == "open_payload":
            sql.execute(f"UPDATE {TABLE} SET election_payload='{{}}'")
        elif change == "elected_missing":
            sql.execute(f"UPDATE {TABLE} SET state='elected'")
        elif change == "elected_large":
            sql.execute(f"UPDATE {TABLE} SET state='elected',election_payload=?", (" " * 16385,))
        elif change == "columns":
            sql.execute(f"ALTER TABLE {TABLE} ADD COLUMN other TEXT")
        elif change == "duplicate":
            sql.execute(
                f"INSERT INTO {TABLE} SELECT ?,custody_id,anchor_digest,state,election_payload "
                f"FROM {TABLE}",
                (new_id(),),
            )

    e.store.write(corrupt)
    before = portable_rows(e)
    original = metadata(e) if has_table(e) else None
    for capture in (False, True):
        with pytest.raises(ResearchError):
            await call(e, capture=capture)
        assert portable_rows(e) == before
        assert (metadata(e) if has_table(e) else None) == original


async def test_final_capture_failure_rolls_back_marker_table_events_and_cached_replies(
    configured, monkeypatch
):
    e = configured
    before = portable_rows(e)

    def fail(bound, *args, **kwargs):
        assert bound.sql.execute(f"SELECT state FROM {TABLE}").one()[0] == "open"
        assert bound.sql.execute("SELECT 1 FROM records WHERE kind=?", (KIND,)).fetchone()
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Synthetic final capacity refusal")

    with monkeypatch.context() as patch:
        patch.setattr(checkpoint_source, "capture_checkpoint", fail)
        with pytest.raises(ResearchError):
            await call(e)
    assert portable_rows(e) == before and not has_table(e)
    assert await call(e)
    assert anchor(e, required=True).state == "open"


async def test_legacy_source_still_captures_but_cannot_silently_gain_an_election(
    configured, monkeypatch
):
    e = configured
    with monkeypatch.context() as patch:
        patch.setattr(checkpoint_source, "register_anchor_in", lambda *a, **kw: None)
        await call(e)
    before = portable_rows(e)
    assert await call(e, capture=True)
    assert anchor(e) is None and not has_table(e)
    with pytest.raises(ResearchError) as refused:
        await call(e)
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    with pytest.raises(ResearchError):
        anchor(e, required=True)
    assert portable_rows(e) == before and not has_table(e)


async def test_another_sources_election_cannot_fill_the_local_anchor(configured, election):
    e = configured
    await call(e)
    e.store.write(
        lambda sql: sql.execute(
            f"UPDATE {TABLE} SET state='elected',election_payload=?",
            (election.election.model_dump_json(),),
        )
    )
    with pytest.raises(ResearchError) as refused:
        anchor(e)
    assert refused.value.code == ErrorCode.STALE_FENCE
