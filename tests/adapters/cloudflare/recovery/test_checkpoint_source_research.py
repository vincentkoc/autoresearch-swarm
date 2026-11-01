"""Ordinary SDK research and saved-report evidence must fit the source checkpoint profile."""

import json
from types import SimpleNamespace

from adapters.cloudflare.recovery.checkpoint_source import capture_source, seal_source
from adapters.cloudflare.recovery.checkpoint_wire import decode_checkpoint
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from tests.adapters.cloudflare.hosted.test_production_delivery import delivery, prepare, register
from tests.adapters.cloudflare.hosted.test_production_delivery import production as production
from tests.adapters.cloudflare.recovery.test_checkpoint_capture import decoded, portable_rows
from tests.adapters.cloudflare.test_cloudflare_deployment import deployment as deployment
from tests.adapters.cloudflare.test_cloudflare_production import fixture_custody
from tests.adapters.cloudflare.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.projects.test_manifest import project as project


async def test_ordinary_project_and_two_reports_seal_and_capture_without_fresh_witness(production):
    e = production
    e.store.write(
        lambda sql: sql.execute(
            "CREATE TABLE research_schema (singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            "version INTEGER NOT NULL)"
        )
    )
    e.store.write(lambda sql: sql.execute("INSERT INTO research_schema VALUES (1,3)"))
    await e.router.command(
        e.operator.token,
        "report.create",
        {"campaign_id": e.admitted.campaign["id"]},
        idempotency_key="direct-report",
    )
    await register(e)
    await prepare(e)
    status, pending = await delivery(e)
    assert status == 200
    assert (await delivery(e, "execute", pending["work"]))[1]["disposition"] == "accepted"
    reports = [row for row in portable_rows(e)["records"] if row[1] == "report"]
    assert len(reports) == 2
    e.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic source offline")
    calls = len(e.witness.requests)
    backup_id = new_id()
    common = {
        "identity": e.store.identity,
        "credential": e.secrets.bootstrap,
        "source_json": json.dumps(fixture_custody(e.manifest.model_dump(mode="json"), e.crypto)),
        "crypto": e.crypto,
    }
    result = await seal_source(
        e.state,
        lambda: e.store,
        e.manifest,
        e.secrets,
        request_json=json.dumps(
            {
                "request_id": new_id(),
                "expected_revision": 0,
                "seal": {
                    "backup_id": backup_id,
                    "expected_controller_id": e.manifest.configuration.authority.controller_id,
                    "expected_control_epoch": 1,
                    "reason": "Ordinary research checkpoint",
                },
            }
        ),
        **common,
    )
    assert result["seal"]["result"]["state"] == "sealed"
    exported = await capture_source(
        e.state,
        lambda: e.store,
        e.manifest,
        e.secrets,
        request_json=json.dumps({"backup_id": backup_id}),
        **common,
    )
    envelope, bodies = decode_checkpoint(exported)
    assert envelope and bodies and len(e.witness.requests) == calls
    assert decoded(SimpleNamespace(bodies=bodies)) == portable_rows(e)
    assert [row for row in portable_rows(e)["records"] if row[1] == "report"] == reports
