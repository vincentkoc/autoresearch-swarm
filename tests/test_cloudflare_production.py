"""Actual production Python entrypoint, private service RPC, HTTPS witness and R2."""

import asyncio
import copy
import json
import re
from contextlib import ExitStack

import httpx
import pytest

from adapters.cloudflare.checkpoint_wire import decode_checkpoint
from adapters.cloudflare.deployment import DeploymentManifest, authority_id
from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.checkpoints.image_format import verify_image
from research_swarm.client import ResearchClient
from research_swarm.control.queries import RecordQuery
from research_swarm.crypto import public_key
from research_swarm.domain.backup import BackupCustodyPin
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.checkpoint_images import SignedLogicalCheckpoint
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.orchestration import DriveAction, OrchestrationBinding
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import new_id
from research_swarm.orchestration.cloudflare import CloudflareBridge, WorkflowOptions
from research_swarm.orchestration.delivery_contract import binding_reference
from research_swarm.orchestration.gateway_models import GatewayBinding
from research_swarm.orchestration.turns import DurableTurnDriver
from research_swarm.runtime import write_private
from research_swarm.workers.clocks import native_clock
from tests.test_cloudflare_deployment import deployment as deployment
from tests.test_cloudflare_hybrid import CONTROL, until
from tests.test_cloudflare_native import ROOT, build_current_sources, server
from tests.test_cloudflare_native_source import Endpoint
from tests.test_cloudflare_router import pytestmark as pytestmark
from tests.test_hosted_research import admit
from tests.test_http_delivery import serving, tls_certificate
from tests.test_manifest import project as project
from tests.test_time_witnesses import source


def fixture_config(directory):
    """Auxiliary config owns its local secrets, while code/dependencies stay unchanged."""
    directory.mkdir(mode=0o700)
    raw = "\n".join(
        line
        for line in (ROOT / "wrangler.production.jsonc").read_text().splitlines()
        if not line.lstrip().startswith("//")
    )
    config = json.loads(re.sub(r",(\s*[}\]])", r"\1", raw))
    config["main"] = str(ROOT / ".generated-production/entry.py")
    config["base_dir"] = str(ROOT / ".generated-production")
    vendor = ROOT / "python_modules"
    assert vendor.is_dir(), "Sync the pinned Cloudflare Python dependencies before qualification"
    # CI prepared this exact adapter's frozen dependencies. No install uses this link.
    (directory / "python_modules").symlink_to(vendor, target_is_directory=True)
    path = directory / "wrangler.json"
    write_private(path, json.dumps(config).encode())
    return path


def fixture_secrets(directory, manifest, secrets, *, source=None):
    values = {
        "RESEARCH_MANIFEST": json.dumps(manifest, separators=(",", ":")),
        "RESEARCH_SECRETS": json.dumps(secrets, separators=(",", ":")),
    }
    if source is not None:
        values["RESEARCH_CHECKPOINT_SOURCE"] = json.dumps(source, separators=(",", ":"))
    assert all("'" not in value and "\n" not in value for value in values.values())
    write_private(
        directory / ".dev.vars",
        "".join(f"{name}='{value}'\n" for name, value in values.items()).encode(),
    )
    return (
        tuple(values.values())
        + (() if source is None else (source["custody_seed_hex"],))
        + (
            secrets["assignment_seed_hex"],
            secrets["dispatch_key_hex"],
            secrets["bootstrap"],
            secrets["witness_consumer"],
            *secrets["credentials"].values(),
        )
    )


def post(client, credential, *, drop=False, default=False):
    path = "/fixture/default-provision" if default else "/fixture/provision"
    result = client.post(
        path,
        json={"credential": credential},
        headers={"x-fixture-drop-bootstrap": "true"} if drop else {},
    )
    assert result.status_code == 200, result.text[:1000]
    return result


def checkpoint(client, credential, payload, *, capture=False, drop=False, default=False):
    path = "/fixture/" + ("default-" if default else "")
    path += "checkpoint-capture" if capture else "checkpoint-seal"
    result = client.post(
        path,
        json={"credential": credential, "request_json": json.dumps(payload)},
        headers={"x-fixture-drop-bootstrap": "true"} if drop else {},
    )
    assert result.status_code == 200, result.text[:1000]
    return result


def fixture_custody(manifest, crypto):
    fixed = DeploymentManifest.model_validate(manifest)
    authority = fixed.configuration.authority
    seed = bytes([193]) * 32
    pin = BackupCustodyPin(
        custody_id=new_id(),
        workspace_id=authority.workspace_id,
        controller_id=authority.controller_id,
        control_epoch=authority.control_epoch,
        public_key=public_key(crypto, seed).hex(),
    )
    return {
        "schema_version": "1.0",
        "deployment_digest": fixed.digest,
        "authority": authority.model_dump(mode="json"),
        "pin": pin.model_dump(mode="json"),
        "custody_seed_hex": seed.hex(),
    }


def drive_new_campaign(operator, http, url, manifest, secrets, admitted, directory):
    """A separate real Workflow must produce the receipt before any client replay."""
    _, gateway, driver, evaluator = manifest["credentials"]
    config = content_digest(b"production-workflow-driver")
    campaign = operator.get("campaign", admitted.campaign["id"])
    bound = operator.command(
        "orchestration.bind",
        {
            "campaign_id": campaign["id"],
            "adapter": "cloudflare-workflows",
            "configuration_digest": config,
            "routes": {
                "control": driver["principal"]["id"],
                "evaluator": evaluator["principal"]["id"],
            },
        },
        idempotency_key="production-binding",
        expected_revision=campaign["revision"],
    )["result"]
    intent = {
        "campaign_id": campaign["id"],
        "epoch": 1,
        "configuration_digest": config,
        "gateway_credential_id": gateway["id"],
        "route_credentials": {"control": driver["id"], "evaluator": evaluator["id"]},
    }
    registration = operator.command(
        "orchestration.register_delivery",
        intent,
        idempotency_key="production-registration",
        expected_revision=0,
    )
    assert registration["result"]["id"] == gateway["principal"]["id"]
    reference = binding_reference(OrchestrationBinding.model_validate(bound["data"]))
    with ResearchClient.connect(url, secrets["credentials"][driver["id"]]) as control:
        turn = DurableTurnDriver(
            control, campaign["id"], epoch=1, configuration_digest=config
        ).prepare_turn(
            1,
            lambda: DriveAction(
                operation="report.create",
                payload={"campaign_id": campaign["id"]},
                idempotency_key="production-driven-report",
            ),
        )
    headers = {"authorization": "Bearer " + secrets["credentials"][gateway["id"]]}
    pending = http.post(
        "/v1/delivery/inspect",
        json=GatewayBinding.from_reference(reference).model_dump(mode="json"),
        headers=headers,
    )
    assert pending.status_code == 200 and pending.json()["work"]["record_id"] == turn["id"]
    directory.mkdir()
    variables = {
        "AUTHORITY_PROFILE": "hosted-do-witness-v1",
        "HOSTED_DELIVERY_ORIGIN": url,
        "GATEWAY_TOKEN": secrets["credentials"][gateway["id"]],
        "CONTROL_TOKEN": CONTROL,
        "ALLOW_LOCAL_GATEWAY": "true",
    }
    with server(directory, sequence=1, variables=variables) as workflow_url:

        async def start():
            async with CloudflareBridge(workflow_url, CONTROL) as bridge:
                return await asyncio.gather(
                    *(
                        bridge.start(reference, WorkflowOptions(poll_seconds=1, rotate_after=2))
                        for _ in range(3)
                    )
                )

        identifiers = asyncio.run(start())
        assert identifiers == [identifiers[0]] * 3
        until(lambda: operator.get("drive_turn", turn["id"])["state"] != "prepared")
        receipts = operator.list(RecordQuery(kind="drive_receipt", campaign_id=campaign["id"]))[
            "items"
        ]
        assert len(receipts) == 1 and receipts[0]["data"]["disposition"] == "accepted"
    return intent, registration, pending.json()["work"], headers


def test_production_private_bootstrap_tls_and_reopen_preserve_one_authority(
    tmp_path, deployment, project
):
    build_current_sources(production=True)
    manifest, secrets = (copy.deepcopy(item) for item in deployment[:2])
    manifest["deliveries"] = []
    private = tmp_path / "bindings"
    config = fixture_config(private)
    options = {"config": "wrangler.operator-test.jsonc", "extra_configs": (config,)}

    # Namespace discovery is a separate test operator capability; no DO method runs.
    with (
        server(tmp_path, sequence=1, **options) as url,
        httpx.Client(
            base_url=url,
            trust_env=False,
            timeout=30,
        ) as http,
    ):
        object_id = http.get("/fixture/identity").json()["object_id"]
        assert re.fullmatch(r"[0-9a-f]{64}", object_id)
        assert http.get("/v1/capabilities").status_code != 200
        assert http.get("/").status_code == 404

    config_data = manifest["configuration"]
    manifest["object_id"] = object_id
    config_data["store_location"] = manifest["namespace"] + "/" + object_id
    config_data["authority"]["state_authority_id"] = authority_id(manifest["namespace"], object_id)
    descriptor = source().model_copy(update={"native_profile": native_clock().profile})
    config_data["source"] = descriptor.model_dump(mode="json")
    endpoint = Endpoint(WitnessAuthorityScope.model_validate(config_data["authority"]), descriptor)
    certificate, key = tls_certificate(tmp_path)
    app = create_time_witness_app(endpoint, witness_consumer_digest(secrets["witness_consumer"]))
    operator_token = secrets["credentials"][manifest["credentials"][0]["id"]]

    with serving(app, ssl_certfile=str(certificate), ssl_keyfile=str(key)) as origin:
        manifest["witness_origin"] = origin
        custody = fixture_custody(manifest, deployment[2])
        redactions = fixture_secrets(private, manifest, secrets, source=custody)
        options.update(
            {
                "child_environment": {"NODE_EXTRA_CA_CERTS": str(certificate)},
                "diagnostic_secrets": redactions,
            }
        )
        with server(tmp_path, sequence=2, **options) as url, ExitStack() as clients:
            http = clients.enter_context(httpx.Client(base_url=url, trust_env=False, timeout=30))
            assert post(http, secrets["bootstrap"], default=True).json() == {"denied": True}
            assert post(http, "wrong").json()["error"]["code"] == "AUTHORIZATION_DENIED"
            assert endpoint.requests == []
            operator = clients.enter_context(ResearchClient.connect(url, operator_token))
            with pytest.raises(ResearchError):
                operator.capabilities()
            assert endpoint.requests == []

            endpoint.mode = "unavailable"
            assert (
                post(http, secrets["bootstrap"]).json()["error"]["code"] == "TRANSPORT_UNAVAILABLE"
            )
            with pytest.raises(ResearchError) as incomplete:
                operator.capabilities()
            assert incomplete.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
            endpoint.mode = "normal"
            assert post(http, secrets["bootstrap"], drop=True).text == "{"
            requests = len(endpoint.requests)
            receipt = post(http, secrets["bootstrap"]).json()["receipt"]
            assert len(endpoint.requests) == requests
            assert receipt["workspace_id"] == config_data["authority"]["workspace_id"]
            assert operator.capabilities()["schema_version"] == "1.0"

            admitted = admit(operator, project)
            report_request = {"campaign_id": admitted.campaign["id"]}
            report = operator.command(
                "report.create", report_request, idempotency_key="production-report"
            )
            delivery_intent, registration, work, gateway_headers = drive_new_campaign(
                operator, http, url, manifest, secrets, admitted, tmp_path / "workflow"
            )
            endpoint.mode = "unavailable"
            requests = len(endpoint.requests)
            delivered = http.post("/v1/delivery/execute", json=work, headers=gateway_headers)
            assert delivered.status_code == 200 and delivered.json()["disposition"] == "accepted"
            assert (
                operator.command(
                    "report.create", report_request, idempotency_key="production-report"
                )
                == report
            )
            assert len(endpoint.requests) == requests
            with pytest.raises(ResearchError) as offline:
                operator.command("report.create", report_request, idempotency_key="fresh-report")
            assert offline.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
            for path in (
                "/provision",
                "/v1/provision",
                "/native-research",
                "/research",
                "/witness/sample",
                "/checkpoint_seal",
                "/checkpoint_capture",
                "/v1/checkpoints",
            ):
                assert http.post(path).status_code == 404

        # A new workerd process reopens existing SQL/R2 and replays during source outage.
        requests = len(endpoint.requests)
        with (
            server(tmp_path, sequence=3, **options) as url,
            httpx.Client(
                base_url=url,
                trust_env=False,
                timeout=30,
            ) as http,
            ResearchClient.connect(url, operator_token) as operator,
        ):
            assert post(http, secrets["bootstrap"]).json()["receipt"] == receipt
            assert (
                operator.command(
                    "orchestration.register_delivery",
                    delivery_intent,
                    idempotency_key="production-registration",
                    expected_revision=0,
                )
                == registration
            )
            assert (
                http.post("/v1/delivery/execute", json=work, headers=gateway_headers).json()
                == delivered.json()
            )
            assert (
                operator.command(
                    "report.create", report_request, idempotency_key="production-report"
                )
                == report
            )
            assert operator.get("job", admitted.job["id"])["state"] == "ready"
            reports = operator.list(
                RecordQuery(kind="report", campaign_id=admitted.campaign["id"])
            )["items"]
            assert len(reports) == 2 and report["result"]["id"] in {row["id"] for row in reports}
            assert len(endpoint.requests) == requests

            seal_request = {
                "schema_version": "1.0",
                "request_id": new_id(),
                "expected_revision": 0,
                "seal": {
                    "backup_id": new_id(),
                    "expected_controller_id": config_data["authority"]["controller_id"],
                    "expected_control_epoch": 1,
                    "reason": "Production private checkpoint qualification",
                },
            }
            capture_request = {"backup_id": seal_request["seal"]["backup_id"]}
            for capture in (False, True):
                payload = capture_request if capture else seal_request
                assert checkpoint(
                    http, secrets["bootstrap"], payload, capture=capture, default=True
                ).json() == {"denied": True}
                assert (
                    checkpoint(http, "wrong", payload, capture=capture).json()["error"]["code"]
                    == "AUTHORIZATION_DENIED"
                )
            assert checkpoint(http, secrets["bootstrap"], seal_request, drop=True).text == "{"
            sealed = checkpoint(http, secrets["bootstrap"], seal_request).json()
            assert "error" not in sealed, sealed
            assert sealed["seal"]["result"]["state"] == "sealed"
            captured = checkpoint(http, secrets["bootstrap"], capture_request, capture=True).text
            envelope, bodies = decode_checkpoint(captured)
            image = SignedLogicalCheckpoint.model_validate_json(envelope).manifest
            verified = verify_image(
                envelope,
                bodies,
                expected_digest=image.digest,
                expected_source=WitnessAuthorityScope.model_validate(custody["authority"]),
                expected_custody=BackupCustodyPin.model_validate(custody["pin"]),
                crypto=deployment[2],
            )
            assert image.artifacts
            assert {
                row[2]
                for chunk in verified.chunks
                if chunk.table == "records"
                for row in chunk.rows
                if row[1] == "report"
            } == {row["id"] for row in reports}
            assert all(value not in captured for value in redactions)
            evidence = envelope + b"".join(bodies)
            assert all(value.encode() not in evidence for value in redactions)
            assert len(endpoint.requests) == requests

        # The original custody and exact sealed cut survive another real runtime restart.
        with (
            server(tmp_path, sequence=4, **options) as url,
            httpx.Client(base_url=url, trust_env=False, timeout=30) as http,
        ):
            assert checkpoint(http, secrets["bootstrap"], seal_request).json() == sealed
            assert (
                checkpoint(
                    http, secrets["bootstrap"], capture_request, capture=True, drop=True
                ).text
                == "{"
            )
            assert (
                checkpoint(http, secrets["bootstrap"], capture_request, capture=True).text
                == captured
            )
            changed_seal = copy.deepcopy(seal_request)
            changed_seal["seal"]["reason"] = "Changed request cannot replace a saved seal"
            assert (
                checkpoint(http, secrets["bootstrap"], changed_seal).json()["error"]["code"]
                == "IDEMPOTENCY_CONFLICT"
            )
            assert len(endpoint.requests) == requests

        changed = copy.deepcopy(manifest)
        changed["object_id"] = "b" * 64 if object_id != "b" * 64 else "c" * 64
        changed["configuration"]["store_location"] = (
            changed["namespace"] + "/" + changed["object_id"]
        )
        changed["configuration"]["authority"]["state_authority_id"] = authority_id(
            changed["namespace"], changed["object_id"]
        )
        options["diagnostic_secrets"] = fixture_secrets(private, changed, secrets, source=custody)
        with (
            server(tmp_path, sequence=5, **options) as url,
            ResearchClient.connect(url, operator_token) as operator,
        ):
            with pytest.raises(ResearchError) as mismatch:
                operator.capabilities()
            assert mismatch.value.code == ErrorCode.STALE_FENCE
            assert len(endpoint.requests) == requests
