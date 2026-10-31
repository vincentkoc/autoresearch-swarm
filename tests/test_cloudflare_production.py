"""Actual production Python entrypoint, private service RPC, HTTPS witness and R2."""

import copy
import json
import re
from contextlib import ExitStack

import httpx
import pytest

from adapters.cloudflare.deployment import authority_id
from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.client import ResearchClient
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.runtime import write_private
from research_swarm.workers.clocks import native_clock
from tests.test_cloudflare_deployment import deployment as deployment
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


def fixture_secrets(directory, manifest, secrets):
    values = {
        "RESEARCH_MANIFEST": json.dumps(manifest, separators=(",", ":")),
        "RESEARCH_SECRETS": json.dumps(secrets, separators=(",", ":")),
    }
    assert all("'" not in value and "\n" not in value for value in values.values())
    write_private(
        directory / ".dev.vars",
        "".join(f"{name}='{value}'\n" for name, value in values.items()).encode(),
    )
    return tuple(values.values()) + (
        secrets["assignment_seed_hex"],
        secrets["dispatch_key_hex"],
        secrets["bootstrap"],
        secrets["witness_consumer"],
        *secrets["credentials"].values(),
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
        redactions = fixture_secrets(private, manifest, secrets)
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
            endpoint.mode = "unavailable"
            requests = len(endpoint.requests)
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
                    "report.create", report_request, idempotency_key="production-report"
                )
                == report
            )
            assert operator.get("job", admitted.job["id"])["state"] == "ready"
            reports = operator.list(
                RecordQuery(kind="report", campaign_id=admitted.campaign["id"])
            )["items"]
            assert len(reports) == 1 and reports[0]["id"] == report["result"]["id"]
            assert len(endpoint.requests) == requests

        changed = copy.deepcopy(manifest)
        changed["object_id"] = "b" * 64 if object_id != "b" * 64 else "c" * 64
        changed["configuration"]["store_location"] = (
            changed["namespace"] + "/" + changed["object_id"]
        )
        changed["configuration"]["authority"]["state_authority_id"] = authority_id(
            changed["namespace"], changed["object_id"]
        )
        options["diagnostic_secrets"] = fixture_secrets(private, changed, secrets)
        with (
            server(tmp_path, sequence=4, **options) as url,
            ResearchClient.connect(url, operator_token) as operator,
        ):
            with pytest.raises(ResearchError) as mismatch:
                operator.capabilities()
            assert mismatch.value.code == ErrorCode.STALE_FENCE
            assert len(endpoint.requests) == requests
