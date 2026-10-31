"""A real CPU supervisor closes before recovery; state/R2 remain native test adapters."""

import asyncio
import copy

import pytest

from research_swarm.control.native_time_witnesses import NativeWitnessSource
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import new_id
from research_swarm.workers.journal_records import JournalState
from research_swarm.workers.local import LocalWorker
from tests import test_cloudflare_deployment, test_cloudflare_provisioning, test_manifest
from tests.test_checkpoint_admission import elected as elected
from tests.test_checkpoint_capture import sealed as sealed
from tests.test_checkpoint_import import recovery as recovery
from tests.test_checkpoint_import import snapshot
from tests.test_checkpoint_request import imported as imported
from tests.test_checkpoint_target import configured as configured
from tests.test_hosted_recovery import claim_original, close_attempt, closure_request, upload
from tests.test_hosted_recovery import receiver as receiver
from tests.test_hosted_research import sdk
from tests.test_hosted_result_publication import TRAIN
from tests.test_production_delivery import production as production
from tests.test_time_witnesses import SEED

deployment_defaults = test_cloudflare_deployment.deployment
original_bootstrap = test_cloudflare_provisioning.bootstrap
original_project = test_manifest.project


@pytest.fixture
def native_source(deployment_defaults):
    raw, _, _ = deployment_defaults
    return NativeWitnessSource(
        WitnessAuthorityScope.model_validate(raw["configuration"]["authority"]), new_id(), SEED
    )


@pytest.fixture
def deployment(deployment_defaults, native_source):
    raw, secrets, crypto = deployment_defaults
    raw = copy.deepcopy(raw)
    raw["configuration"]["source"] = native_source.source.model_dump(mode="json")
    return raw, secrets, crypto


@pytest.fixture
def bootstrap(original_bootstrap, native_source):
    e = original_bootstrap

    class NativeTransport:
        def __init__(self):
            self.requests, self.failure = [], None

        async def __call__(self, request):
            self.requests.append(request)
            if self.failure is not None:
                raise self.failure
            return canonical_bytes(native_source.observe(request).model_dump(mode="json"))

    e.witness = NativeTransport()
    return e


@pytest.fixture
def project(original_project):
    (original_project / "train.py").write_text(TRAIN)
    manifest = original_project / "research.toml"
    manifest.write_text(manifest.read_text().replace("timeout_ms = 1000", "timeout_ms = 10000"))
    return original_project


@pytest.fixture
async def source(production, tmp_path):
    e, loop = production, asyncio.get_running_loop()
    await claim_original(e, inputs=True)

    def execute():
        with sdk(e, loop, token=e.original.token) as client:
            runner = LocalWorker(
                client,
                e.worker,
                root=tmp_path / "original-worker",
                machine_registry=tmp_path / "machine" / "resources.sqlite",
            )

            def retain(entry):
                assert entry.state == JournalState.CLOSED
                assert entry.native is not None and entry.outcome is not None
                assert entry.outcome.closure.scope_closed and entry.outcome.released_at is not None
                assert entry.timing is not None and entry.outcome.exit_code == 0
                return entry

            # This boundary follows native reconciliation and resource release.
            # No journal, report or result has been uploaded to the source yet.
            runner._publish = retain
            return runner.execute(e.claimed["result"]["id"])

    e.entry = await asyncio.to_thread(execute)
    e.journal_path = tmp_path / "original-closed-journal.json"
    e.journal_path.write_bytes(canonical_bytes(e.entry.model_dump(mode="json")))
    assert not [
        row
        for row in snapshot(e.store)["records"]
        if row["kind"] in {"run_result", "worker_upload", "attempt_submission"}
    ]
    return e


async def test_native_closed_journal_crosses_recovery_and_settles_original_consumption(receiver):
    e = receiver
    calls = len(e.source.witness.requests)
    e.source.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Source unavailable")
    before = snapshot(e.target)
    reference, submitted = await upload(e)
    assert snapshot(e.target)["budget_settlements"] == before["budget_settlements"]
    request = closure_request(e, submitted["result"])
    closed = await close_attempt(e, request)
    result = closed["result"]
    receipt = result["closure"]["data"]
    assert result["attempt"]["state"] == "closed_unaccepted"
    assert receipt["not_started"] is False
    assert receipt["native_outcome"] == e.source.entry.outcome.model_dump(mode="json")
    assert receipt["consumed"] == {
        "attempts": "1",
        "execution_ms": str(e.source.entry.outcome.elapsed_ms),
    }
    assert receipt["journal_artifact"] == reference.model_dump(mode="json")
    after = snapshot(e.target)
    prior = {row["settlement_id"] for row in before["budget_settlements"]}
    settlements = [
        strict_json_loads(row["response"])
        for row in after["budget_settlements"]
        if row["settlement_id"] not in prior
    ]
    reservations = set(e.source.entry.dispatch.reservation_ids)
    assert len(settlements) == len(reservations) > 0
    assert {item["reservation_id"] for item in settlements} == reservations
    assert all(item["actual"] == receipt["consumed"] for item in settlements)
    assert {
        row["id"] for row in after["budget_reservations"] if row["state"] == "settled"
    } == reservations
    assert not [
        row
        for row in after["records"]
        if row["kind"] in {"run_result", "evaluation_receipt", "promotion"}
    ]
    assert await close_attempt(e, request) == closed
    assert snapshot(e.target) == after
    assert len(e.source.witness.requests) == calls
