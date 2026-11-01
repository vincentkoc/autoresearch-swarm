"""A real SDK campaign and driver retain funding while hosted recovery fences research."""

import pytest

from adapters.cloudflare import checkpoint_admission
from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Database
from research_swarm.state.recovery_guard import require_recovery_authority
from tests.adapters.cloudflare.hosted.test_production_delivery import delivery, prepare, register
from tests.adapters.cloudflare.hosted.test_production_delivery import production as production
from tests.adapters.cloudflare.recovery.test_checkpoint_admission import admit, context
from tests.adapters.cloudflare.recovery.test_checkpoint_admission import elected as elected
from tests.adapters.cloudflare.recovery.test_checkpoint_capture import sealed as sealed
from tests.adapters.cloudflare.recovery.test_checkpoint_import import recovery as recovery
from tests.adapters.cloudflare.recovery.test_checkpoint_import import snapshot
from tests.adapters.cloudflare.recovery.test_checkpoint_request import imported as imported
from tests.adapters.cloudflare.recovery.test_checkpoint_target import configured as configured
from tests.adapters.cloudflare.test_cloudflare_deployment import deployment as deployment
from tests.adapters.cloudflare.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.projects.test_manifest import project as project


@pytest.fixture
async def source(production, request, monkeypatch):
    e = production
    await register(e)
    e.turn = await prepare(e)
    if getattr(request, "param", None) == "acknowledged":
        status, pending = await delivery(e)
        assert status == 200

        def lost(*args):
            raise RuntimeError("Synthetic reply lost before turn settlement")

        with monkeypatch.context() as patch:
            patch.setattr(e.router.orchestration.service, "_finish", lost)
            assert (await delivery(e, "execute", pending["work"]))[0] == 503

    def schema(sql):
        sql.execute(
            "CREATE TABLE research_schema (singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            "version INTEGER NOT NULL)"
        )
        sql.execute("INSERT INTO research_schema VALUES (1,3)")

    e.store.write(schema)
    return e


async def test_nonempty_research_fences_atomically_and_retains_funding_and_pending_turns(
    elected, monkeypatch
):
    e = elected
    before = snapshot(e.target)
    assert before["budgets"]
    kinds = {row["kind"] for row in before["records"]}
    assert {
        "campaign",
        "orchestration_binding",
        "orchestration_owner",
        "drive_turn",
    } <= kinds
    original = checkpoint_admission.admit_in

    def interrupted(tx, admission):
        result = original(tx, admission)
        assert result["state"] == "immutable"
        assert tx.get("campaign", e.source.admitted.campaign["id"])["state"] == "paused"
        assert (
            tx.get("orchestration_binding", e.source.admitted.campaign["id"])["state"] == "fenced"
        )
        raise ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic fenced interruption")

    with monkeypatch.context() as patch:
        patch.setattr(checkpoint_admission, "admit_in", interrupted)
        with pytest.raises(ResearchError, match="fenced interruption"):
            await admit(e)
    assert snapshot(e.target) == before
    await admit(e)
    after = snapshot(e.target)
    for table in (
        "budgets",
        "budget_reservations",
        "budget_settlements",
        "budget_entries",
        "credentials",
    ):
        assert after[table] == before[table]
    retained = {"orchestration_owner", "drive_turn", "attempt", "job", "trial"}
    assert [r for r in before["records"] if r["kind"] in retained] == [
        r for r in after["records"] if r["kind"] in retained
    ]
    database = Database.from_store(e.target, recovery_verifier=e.verifier)
    campaign = e.source.admitted.campaign["id"]
    assert database.read_record(context(e), "campaign", campaign)["state"] == "paused"
    assert database.read_record(context(e), "orchestration_binding", campaign)["state"] == "fenced"


@pytest.mark.parametrize(
    "kind,state", [("campaign", "running"), ("orchestration_binding", "active")]
)
async def test_reopened_research_fence_refuses_admission_replay_and_recovery_authority(
    elected, kind, state
):
    e = elected
    await admit(e)
    database = Database.from_store(e.target, recovery_verifier=e.verifier)
    principal = context(e)
    proof = recovery_authority(database, principal, crypto_provider=e.source.crypto)
    e.target.write(
        lambda sql: sql.execute("UPDATE records SET state=? WHERE kind=?", (state, kind))
    )
    before = snapshot(e.target)
    with pytest.raises(ResearchError, match="research fence"):
        await admit(e)
    with pytest.raises(ResearchError, match="research fence"):
        recovery_authority(database, context(e), crypto_provider=e.source.crypto)
    with pytest.raises(ResearchError, match="research fence"):
        e.target.read(
            lambda sql: require_recovery_authority(sql, principal, e.target.identity, proof)
        )
    assert snapshot(e.target) == before
