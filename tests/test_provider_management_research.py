"""Recovered authority clock management preserves live research and activation history."""

import asyncio
from types import SimpleNamespace

from research_swarm.control.worker_records import WorkerData
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.workers.clock_estimates import ClockSampler
from tests import test_checkpoint_activation_research as fixtures
from tests.test_checkpoint_activation import activate, opened
from tests.test_checkpoint_import import snapshot
from tests.test_hosted_research import admit, enroll_and_claim, enrollment, sdk
from tests.test_manifest import project as project
from tests.test_provider_management import manage, retire

deployment = fixtures.deployment
bootstrap = fixtures.bootstrap
source = fixtures.source
sealed = fixtures.sealed
recovery = fixtures.recovery
configured = fixtures.configured
imported = fixtures.imported
elected = fixtures.elected
target = fixtures.target
prepared = fixtures.prepared
ready = fixtures.ready


async def test_recovered_active_authority_requalifies_without_rewriting_live_attempts(
    ready, project
):
    e = ready
    activation = await activate(e)
    e.router = await opened(e)
    e.store, e.secrets, e.crypto = e.target, e.target_secrets, e.source.crypto
    principal = e.manifest.credentials[0].principal
    e.operator = SimpleNamespace(token=e.secrets.credentials[e.manifest.credentials[0].id])
    loop = asyncio.get_running_loop()

    def admit_original():
        with sdk(e, loop) as client:
            admitted = admit(client, project)
            claimed, _ = enroll_and_claim(client, admitted, principal.id)
            return claimed["result"]

    original = await asyncio.to_thread(admit_original)
    await retire(e)
    before = snapshot(e.store)
    accepted = strict_json_loads(await manage(e))
    assert accepted["disposition"] == "accepted"
    after = snapshot(e.store)
    for table in (
        "hosted_provisioning",
        "hosted_authority_config",
        "credentials",
        "budgets",
        "budget_reservations",
        "budget_settlements",
        "budget_entries",
    ):
        assert after[table] == before[table]
    changed_kinds = {"time_provider", "time_provider_generation", "time_provider_requalification"}
    assert [row for row in after["records"] if row["kind"] not in changed_kinds] == [
        row for row in before["records"] if row["kind"] not in changed_kinds
    ]
    assert await activate(e) == activation
    e.router = await opened(e)

    def calibrate_new_worker():
        with sdk(e, loop) as client:
            assert client.get("attempt", original["id"]) == original
            request = enrollment(principal.id, client.capabilities()["clock"]["policy_digest"])
            enrolled = client.command(
                "worker.enroll", request, idempotency_key="post-requalification-worker"
            )
            worker = WorkerData.model_validate(enrolled["result"]["data"])
            sampler = ClockSampler(
                worker.assignment_authority, clock_admission=worker.clock_admission
            )
            pending = sampler.begin()
            return pending.complete(client.worker_clock(pending.challenge))

    estimate = await asyncio.to_thread(calibrate_new_worker)
    assert estimate is not None
