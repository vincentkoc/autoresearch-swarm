"""Hosted activation rejects oversized cuts before eager shared-kernel reads."""

import pytest

from adapters.cloudflare import activation_cut
from adapters.cloudflare.activation_cut import inspect_activation_cut
from adapters.cloudflare.checkpoint_runtime import RuntimePreparationReceipt
from research_swarm.domain.artifacts import ArtifactRecord, UploadSpec
from research_swarm.domain.backup import RestoreReceipt
from research_swarm.domain.canonical import content_digest, strict_json_loads
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.recovery_runtime import HostedActivationRuntime, RecoveryRuntimePlan
from research_swarm.domain.types import new_id, utc_now
from research_swarm.state.database import Transaction
from tests import test_checkpoint_runtime as fixtures
from tests.test_checkpoint_import import snapshot
from tests.test_checkpoint_runtime import records, run

deployment = fixtures.deployment
bootstrap = fixtures.bootstrap
source = fixtures.source
sealed = fixtures.sealed
recovery = fixtures.recovery
configured = fixtures.configured
imported = fixtures.imported
elected = fixtures.elected
target = fixtures.target


@pytest.fixture
async def prepared(target):
    e = target
    result = strict_json_loads(await run(e))
    receipt = RuntimePreparationReceipt.model_validate(result["receipt"])
    e.runtime = HostedActivationRuntime(
        plan=RecoveryRuntimePlan.model_validate_json(
            records(e, "recovery_runtime_plan")[0]["payload"]
        ),
        preparation_digest=receipt.digest,
        provider_revision=receipt.provider_revision,
        provider=receipt.provider,
    )
    e.restoration = RestoreReceipt.model_validate(e.receipt)
    e.context = CommandContext(
        principal=e.manifest.credentials[0].principal, idempotency_key="activation-cut"
    )
    return e


def inspect(e):
    return e.target.read(
        lambda sql: inspect_activation_cut(
            sql, e.context, e.restoration, e.runtime, e.manifest.configuration.storage
        )
    )


async def test_preflight_retains_shared_digest_and_exact_credential_retirement(prepared):
    e = prepared
    before = snapshot(e.target)
    cut = inspect(e)
    assert cut.view.evidence_through > 0 and not cut.view.artifacts
    old_ids = tuple(
        sorted(
            row["id"]
            for row in before["credentials"]
            if row["id"] not in {member.id for member in e.manifest.credentials}
        )
    )
    assert cut.retired_credential_ids == old_ids and len(old_ids) == len(
        e.source.manifest.credentials
    )
    original = e.target.read(
        lambda sql: activation_cut.inspect_activation(
            Transaction(sql, e.context, observe_waits=False), e.restoration
        )
    )
    assert cut.view == original and snapshot(e.target) == before


@pytest.mark.parametrize("damage", ["payload", "column", "commands", "turns", "bytes"])
async def test_state_drift_is_bounded_before_any_eager_activation_projection(
    prepared, monkeypatch, damage
):
    e = prepared
    inspect(e)  # The first cut was admissible before a later independent write.

    def change(sql):
        if damage == "payload":
            sql.execute(
                "UPDATE records SET payload=payload || ? WHERE kind='recovery_runtime_plan'",
                (" " * 65536,),
            )
        elif damage == "column":
            sql.execute("ALTER TABLE records ADD COLUMN unexpected TEXT")
        elif damage == "commands":
            sql.execute(
                "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<8193) "
                "INSERT INTO commands SELECT ?,?,'extra-' || x,?,'id-' || x,'{}','created' FROM n",
                (
                    e.context.principal.workspace_id,
                    e.context.principal.id,
                    content_digest(b"request"),
                ),
            )
        elif damage == "turns":
            sql.execute(
                "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<8193) "
                "INSERT INTO records SELECT ?,'drive_turn','turn-' || x,1,'reserved',NULL,NULL,"
                "'{}','created','updated' FROM n",
                (e.context.principal.workspace_id,),
            )
        else:
            # Each row is below its byte limit; aggregate bytes exceed the profile.
            sql.execute("UPDATE commands SET response=?", (" " * 40000 + "{}",))

    e.target.write(change)
    before = snapshot(e.target)
    monkeypatch.setattr(
        activation_cut, "inspect_activation", lambda *a: pytest.fail("eager projection")
    )
    with pytest.raises(ResearchError):
        inspect(e)
    assert snapshot(e.target) == before


def artifacts(e, count, size):
    def write(sql):
        tx = Transaction(sql, e.context)
        for number in range(count):
            upload = UploadSpec(
                id=new_id(),
                workspace_id=e.context.principal.workspace_id,
                actor_id=e.context.principal.id,
                digest=content_digest(str(number).encode()),
                size_bytes=size,
            )
            artifact = ArtifactRecord(
                reference=upload.reference(),
                storage_generation=upload.id,
                workspace_id=e.context.principal.workspace_id,
                published_by=e.context.principal.id,
                verified_at=utc_now(),
            )
            tx.insert(
                "artifact",
                artifact.reference.id,
                artifact.model_dump(mode="json"),
                state="verified",
            )

    e.target.write(write)


@pytest.mark.parametrize(("count", "size"), [(1025, 1), (1, 1024**2 + 1), (33, 1024**2)])
async def test_metadata_size_cannot_hide_unbounded_r2_work(prepared, count, size):
    e = prepared
    artifacts(e, count, size)
    before = snapshot(e.target)
    with pytest.raises(ResearchError, match="artifact bound"):
        inspect(e)
    assert snapshot(e.target) == before


async def test_exact_r2_generations_are_selected_without_changing_custody(prepared):
    e = prepared
    artifacts(e, 2, 1024)
    before = snapshot(e.target)
    cut = inspect(e)
    actual = {item.generation: item.reference for item in cut.view.artifacts}
    expected = {
        value.storage_generation: value.reference
        for value in (
            ArtifactRecord.model_validate_json(row["payload"]) for row in records(e, "artifact")
        )
    }
    assert actual == expected and len(actual) == 2 and snapshot(e.target) == before
