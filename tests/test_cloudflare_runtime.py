"""Production reopen/routing contracts on shared SQLite; no workerd qualification claim."""

import pytest

from adapters.cloudflare.runtime import (
    fixed_target,
    open_router,
    public_route,
    recovery_route,
    runtime_failure,
    serve,
)
from research_swarm.control.auth import CredentialStore
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Database
from research_swarm.storage.async_r2 import BoundedR2Objects
from tests.test_async_r2 import Transport
from tests.test_cloudflare_deployment import deployment as deployment
from tests.test_cloudflare_provisioning import bootstrap as bootstrap
from tests.test_cloudflare_provisioning import rows, run


@pytest.mark.parametrize(
    "method,target",
    [
        ("POST", "/v1/worker/list"),
        ("POST", "/v1/worker/clock"),
        ("POST", "/v1/commands/worker.claim_job"),
        ("POST", "/v1/commands/attempt.reconcile_closure"),
        ("POST", "/v1/delivery/execute"),
        ("GET", "/v1/capabilities?recovery=true"),
    ],
)
def test_recovery_ingress_refuses_control_before_opening_authority(method, target):
    with pytest.raises(ResearchError):
        recovery_route(method, target)


def test_recovery_ingress_preserves_existing_sdk_routes():
    assert recovery_route("GET", "/v1/capabilities") == "query"
    assert recovery_route("POST", "/v1/worker/get") == "query"
    assert recovery_route("POST", "/v1/commands/worker.submit_evidence") == "command"


async def opened(e, **overrides):
    storage = e.manifest.configuration.storage
    return await open_router(
        e.state,
        lambda: e.store,
        overrides.get("manifest", e.manifest),
        e.secrets,
        crypto=e.crypto,
        acquire=e.witness,
        objects=BoundedR2Objects(storage, Transport(storage)),
    )


async def request(e, router, *, token=None, headers=None, consumed=None, path="/v1/capabilities"):
    async def body(limit):
        assert limit == 0
        if consumed is not None:
            consumed.append(limit)
        return b""

    return await serve(
        router,
        e.manifest,
        e.secrets,
        "GET",
        path,
        {
            "authorization": "Bearer " + (token or next(iter(e.secrets.credentials.values()))),
            **(headers or {}),
        },
        body,
    )


async def test_ordinary_open_never_provisions_missing_or_prepared_state(bootstrap):
    e = bootstrap
    before = rows(e, "records"), rows(e, "events"), rows(e, "workspaces")
    with pytest.raises(ResearchError):
        await opened(e)
    assert (rows(e, "records"), rows(e, "events"), rows(e, "workspaces")) == before
    assert e.witness.requests == []
    e.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic source offline")
    with pytest.raises(ResearchError):
        await run(e)
    before = rows(e, "records"), rows(e, "events"), rows(e, "credentials")
    requests = len(e.witness.requests)
    with pytest.raises(ResearchError) as refused:
        await opened(e)
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert (rows(e, "records"), rows(e, "events"), rows(e, "credentials")) == before
    assert len(e.witness.requests) == requests


async def test_reopen_preserves_revocation_and_does_not_contact_or_requalify_source(bootstrap):
    e = bootstrap
    await run(e)
    issuer, target = e.manifest.credentials[0], e.manifest.credentials[-1]
    CredentialStore.open_existing(Database.from_store(e.store)).revoke(issuer.principal, target.id)
    e.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Synthetic source offline")
    before = rows(e, "credentials"), rows(e, "records"), rows(e, "events")
    requests = len(e.witness.requests)
    router = await opened(e)
    accepted = await request(e, router)
    assert accepted.status == 200
    consumed = []
    denied = await request(e, router, token=e.secrets.credentials[target.id], consumed=consumed)
    assert denied.status == 401
    assert consumed == []
    assert strict_json_loads(denied.body)["error"]["code"] == "AUTHORIZATION_DENIED"
    assert (rows(e, "credentials"), rows(e, "records"), rows(e, "events")) == before
    assert len(e.witness.requests) == requests


async def test_entire_manifest_is_sealed_and_fixture_headers_cannot_change_routing(bootstrap):
    e = bootstrap
    await run(e)
    changed = e.manifest.model_copy(update={"witness_origin": "https://different.example.invalid"})
    assert changed.configuration.digest == e.manifest.configuration.digest
    with pytest.raises(ResearchError) as refused:
        await opened(e, manifest=changed)
    assert refused.value.code == ErrorCode.STALE_FENCE
    router = await opened(e)
    baseline = await request(e, router)
    assert baseline.status == 200
    altered = await request(
        e,
        router,
        headers={
            "x-fixture-witness-origin": "http://127.0.0.1:1",
            "x-fixture-drop-command": "deployment.provision",
            "x-fixture-startup-profile": "false",
            "x-fixture-native-source": "false",
        },
    )
    assert altered == baseline


def test_physical_namespace_derivation_and_receiving_identity_are_both_checked(deployment):
    from tests.test_cloudflare_deployment import load

    manifest, _ = load(deployment)

    class Namespace:
        def __init__(self):
            self.value, self.names = manifest.object_id, []

        def idFromName(self, name):
            self.names.append(name)
            return self.value

    namespace = Namespace()
    assert fixed_target(namespace, manifest, actual_id=manifest.object_id) == manifest.object_id
    assert namespace.names == [manifest.authority_name]
    for derived, actual in (("b" * 64, manifest.object_id), (manifest.object_id, "b" * 64)):
        namespace.value = derived
        with pytest.raises(ResearchError) as refused:
            fixed_target(namespace, manifest, actual_id=actual)
        assert refused.value.code == ErrorCode.STALE_FENCE


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/"),
        ("POST", "/provision"),
        ("POST", "/v1/provision"),
        ("POST", "/checkpoint_seal"),
        ("POST", "/checkpoint_capture"),
        ("POST", "/v1/checkpoints"),
        ("POST", "/native-contract"),
        ("GET", "/v1/delivery/bootstrap"),
        ("POST", "/v1/delivery/bootstrap?object=other"),
        ("POST", "/v1/delivery/bootstrap#other"),
        ("POST", "/v1/delivery/provision"),
    ],
)
def test_public_routes_exclude_bootstrap_probes_and_delivery_target_overrides(method, path):
    with pytest.raises(ResearchError):
        public_route(method, path)


def test_response_uncertainty_retains_correct_recovery_without_provider_or_secret_details():
    paths = {
        ("GET", "/v1/capabilities"): ("query", "retry_query"),
        ("POST", "/v1/commands/report.create"): ("command", "replay_exact_command"),
        ("PUT", "/v1/artifact-uploads/00000000-0000-4000-8000-000000000001/parts"): (
            "upload_part",
            "inspect_upload_then_replay_exact_range",
        ),
    }
    for (method, path), (mode, recovery) in paths.items():
        assert public_route(method, path) == mode
        body = strict_json_loads(runtime_failure(mode=mode).body)
        assert body["error"]["details"]["recovery"] == recovery
    sensitive = "synthetic-sensitive-provider-input"
    error = ResearchError(ErrorCode.STALE_FENCE, sensitive, details={"secret": sensitive})
    for mode in ("query", "command", "upload_part", "delivery"):
        result = runtime_failure(error, mode=mode)
        assert sensitive.encode() not in result.body
        assert strict_json_loads(result.body)["error"]["code"] == "STALE_FENCE"
