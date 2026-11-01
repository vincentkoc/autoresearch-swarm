"""Production configuration binds fixed authority without retaining cleartext inputs."""

import json
import traceback

import pytest

from adapters.cloudflare.platform.deployment import (
    DeploymentCredential,
    authority_id,
    credential_manifest_digest,
    load_deployment,
    secret_digest,
)
from adapters.cloudflare.research.research_router import (
    HostedStartupConfiguration,
    dispatch_key_digest,
)
from research_swarm.control.auth import CredentialStore
from research_swarm.crypto import public_key, resolve_crypto
from research_swarm.domain.assignments import signing_key_id
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.state.sql import StoreIdentity
from research_swarm.storage.async_r2 import BoundedR2Configuration
from tests.workers.test_time_witnesses import identifier, source

# Synthetic test-only bytes; production receives separate protected bindings.
SEED, DISPATCH = b"s" * 32, b"d" * 32


@pytest.fixture
def deployment():
    crypto = resolve_crypto(None)
    public = public_key(crypto, SEED).hex()
    namespace, object_id, workspace = "synthetic-deployment", "a" * 64, identifier(101)
    roster = tuple(
        DeploymentCredential(
            id=identifier(201 + index),
            principal=Principal(id=identifier(301 + index), workspace_id=workspace, scopes=scopes),
            token_digest=CredentialStore._digest("rsw_" + letter * 43),
        )
        for index, (letter, scopes) in enumerate(
            (
                ("O", {"*"}),
                ("G", {"orchestration.deliver"}),
                ("D", {"read", "orchestration.drive", "report.create"}),
                ("E", {"read", "orchestration.drive", "evaluation.publish"}),
            )
        )
    )
    config = HostedStartupConfiguration(
        store_location=namespace + "/" + object_id,
        authority=WitnessAuthorityScope(
            state_authority_id=authority_id(namespace, object_id),
            activation_id=identifier(102),
            workspace_id=workspace,
            controller_id=identifier(103),
            control_epoch=1,
            assignment_key_id=signing_key_id(public),
        ),
        source=source(),
        assignment_public_key=public,
        dispatch_key_digest=dispatch_key_digest(DISPATCH),
        credential_manifest_digest=credential_manifest_digest(roster),
        storage=BoundedR2Configuration(
            workspace_id=workspace,
            bucket_identity="synthetic-bucket",
            prefix="research",
            max_object_bytes=1024**2,
        ),
        storage_limit_bytes=32 * 1024**2,
    )
    secrets = {
        "assignment_seed_hex": SEED.hex(),
        "dispatch_key_hex": DISPATCH.hex(),
        "witness_consumer": "W" * 43,
        "bootstrap": "B" * 43,
        "credentials": {
            item.id: "rsw_" + letter * 43 for item, letter in zip(roster, "OGDE", strict=True)
        },
    }
    manifest = {
        "namespace": namespace,
        "authority_name": "primary",
        "object_id": object_id,
        "configuration": config.model_dump(mode="json"),
        "witness_origin": "https://witness.example.invalid",
        "witness_credential_digest": secret_digest("witness", secrets["witness_consumer"]),
        "bootstrap_credential_digest": secret_digest("bootstrap", secrets["bootstrap"]),
        "provisioning_actor_id": roster[0].principal.id,
        "credentials": [item.model_dump(mode="json") for item in roster],
        "deliveries": [
            {
                "gateway_credential_id": roster[1].id,
                "binding": {
                    "workspace_id": workspace,
                    "campaign_id": identifier(104),
                    "epoch": 1,
                    "configuration_digest": content_digest(b"synthetic-driver"),
                    "actors": {
                        "control": roster[2].principal.id,
                        "evaluator": roster[3].principal.id,
                    },
                },
                "route_credentials": {"control": roster[2].id, "evaluator": roster[3].id},
            }
        ],
    }
    return manifest, secrets, crypto


def load(values, **options):
    manifest, secrets, crypto = values
    return load_deployment(json.dumps(manifest), json.dumps(secrets), crypto=crypto, **options)


@pytest.mark.parametrize(
    "profile", ["research-reconciliation-witness-v1", "research-timers-witness-v1"]
)
def test_extended_profiles_preserve_explicit_manifest_identity(deployment, profile):
    from adapters.cloudflare.research.hosted_http import capabilities
    from adapters.cloudflare.research.research_router import HostedReconciliationConfiguration

    previous, _ = load(deployment)
    original = previous.digest
    deployment[0]["configuration"]["profile"] = profile
    manifest, _ = load(deployment)
    assert isinstance(manifest.configuration, HostedReconciliationConfiguration)
    assert manifest.digest != original
    assert previous.digest == original
    advertised = capabilities(manifest.configuration)
    assert advertised["commands"]["attempt.reconcile"]["scope"] == "attempt.reconcile"
    assert advertised["commands"]["attempt.reconcile"]["expected_revision"] is True
    assert advertised["publication"]["submission"] == "quarantine-without-settlement"
    assert advertised["orchestration"]["timer_waits"] is (profile == "research-timers-witness-v1")
    assert "worker.submit_evidence" not in capabilities(previous.configuration)["commands"]


def test_public_manifest_secret_roster_and_physical_authority_agree(deployment):
    raw, raw_secrets, _ = deployment
    manifest, secrets = load(deployment)
    assert (
        manifest.digest == "sha256:5ac5e3a7d605c411d66122ee95ca3d0462dbfc27bb9bb880ed1221f7a30150e6"
    )
    assert manifest.configuration.credential_manifest_digest == (
        "sha256:d92fd7cb785366497408e19a142cfe22ce83842d2316a0efe4f1443f27ebdf5f"
    )
    assert manifest.configuration.authority.state_authority_id == (
        "sha256:84c9ddb194688f19a4f6eab6323047b73df4fb329520166303a015e891c1e6ce"
    )
    manifest.require_store(StoreIdentity("cloudflare-do", manifest.configuration.store_location))
    secrets.require_bootstrap(raw_secrets["bootstrap"])
    registered = secrets.registrations(manifest)
    gateway = manifest.credentials[1].principal.id
    assert registered[gateway].binding.model_dump(mode="json") == manifest.deliveries[
        0
    ].binding.model_dump(mode="json")
    assert set(registered[gateway].route_tokens) == {"control", "evaluator"}
    assert secrets.assignment_seed == SEED and secrets.dispatch_key == DISPATCH
    before = manifest.digest
    raw["deliveries"][0]["binding"]["actors"]["control"] = identifier(999)
    raw_secrets["credentials"].clear()
    assert manifest.digest == before and len(secrets.credentials) == 4
    for mapping, key in (
        (manifest.deliveries[0].binding.actors, "control"),
        (manifest.deliveries[0].route_credentials, "control"),
        (secrets.credentials, manifest.credentials[0].id),
    ):
        with pytest.raises(TypeError):
            mapping[key] = "changed"
    assert repr(secrets) == "DeploymentSecrets(<redacted>)"
    with pytest.raises(TypeError):
        json.dumps(secrets)


@pytest.mark.parametrize(
    "field",
    [
        "namespace",
        "object_id",
        "store",
        "state",
        "key",
        "roster",
        "workspace",
        "issuer",
        "delivery",
        "duplicate",
        "binding",
    ],
)
def test_manifest_refuses_inconsistent_authority_and_registration(deployment, field):
    raw, _, _ = deployment
    config = raw["configuration"]
    if field in {"namespace", "object_id"}:
        raw[field] = "b" * 64 if field == "object_id" else "changed"
    elif field == "store":
        config["store_location"] = "changed"
    elif field == "state":
        config["authority"]["state_authority_id"] = content_digest(b"changed")
    elif field == "key":
        config["authority"]["assignment_key_id"] = content_digest(b"changed")
    elif field == "roster":
        config["credential_manifest_digest"] = content_digest(b"changed")
    elif field == "workspace":
        raw["credentials"][0]["principal"]["workspace_id"] = identifier(999)
    elif field == "issuer":
        raw["provisioning_actor_id"] = raw["credentials"][2]["principal"]["id"]
    elif field == "delivery":
        raw["deliveries"][0]["route_credentials"]["control"] = raw["credentials"][1]["id"]
    elif field == "duplicate":
        raw["deliveries"] *= 2
    else:
        raw["artifact_binding"] = "UNDECLARED_BUCKET"
    with pytest.raises(ResearchError) as invalid:
        load(deployment)
    assert invalid.value.code == ErrorCode.PROJECT_CONTRACT_INVALID


@pytest.mark.parametrize(
    "field",
    [
        "assignment_seed_hex",
        "dispatch_key_hex",
        "witness_consumer",
        "bootstrap",
        "credentials",
        "unknown",
    ],
)
def test_secret_failures_are_opaque_and_do_not_retain_printable_exception_chains(deployment, field):
    _, raw, _ = deployment
    private = "synthetic-sensitive-input-" * 3
    raw[field] = private
    with pytest.raises(ResearchError) as invalid:
        load(deployment)
    error = invalid.value
    assert error.code == ErrorCode.PROJECT_CONTRACT_INVALID
    assert error.__cause__ is None and error.__suppress_context__
    assert private not in "".join(traceback.format_exception(error))
    assert private not in str(error.as_dict())


@pytest.mark.parametrize(
    "origin",
    [
        "http://witness.example.invalid",
        "https://user:pass@example.invalid",
        "https://example.invalid/",
        "https://example.invalid?q=1",
        "https://example.invalid#fragment",
        "https://example.invalid:invalid",
    ],
)
def test_production_origin_is_one_https_origin(deployment, origin):
    deployment[0]["witness_origin"] = origin
    with pytest.raises(ResearchError):
        load(deployment)


def test_loopback_requires_explicit_local_admission_and_bootstrap_never_accepts_api_token(
    deployment,
):
    raw, tokens, _ = deployment
    raw["witness_origin"] = "http://127.0.0.1:8765"
    with pytest.raises(ResearchError):
        load(deployment)
    _, secrets = load(deployment, allow_loopback=True)
    for credential in ("", "wrong", "秘密" * 40, next(iter(tokens["credentials"].values()))):
        with pytest.raises(ResearchError) as denied:
            secrets.require_bootstrap(credential)
        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    tokens["bootstrap"] = next(iter(tokens["credentials"].values()))
    raw["bootstrap_credential_digest"] = secret_digest("bootstrap", tokens["bootstrap"])
    with pytest.raises(ResearchError):
        load(deployment, allow_loopback=True)


def test_unknown_duplicate_oversized_fields_and_wrong_store_are_refused(deployment):
    raw, secrets, crypto = deployment
    public = json.dumps(raw)
    secret = json.dumps(secrets)
    for value in (
        '{"unknown":1,"unknown":2}',
        public[:-1] + ',"extra":true}',
        " " * (256 * 1024 + 1),
    ):
        with pytest.raises(ResearchError):
            load_deployment(value, secret, crypto=crypto)
    for value in (
        '{"bootstrap":1,"bootstrap":2}',
        secret[:-1] + ',"extra":true}',
        " " * (32 * 1024 + 1),
    ):
        with pytest.raises(ResearchError):
            load_deployment(public, value, crypto=crypto)
    manifest, _ = load(deployment)
    with pytest.raises(ResearchError) as wrong_store:
        manifest.require_store(StoreIdentity("sqlite", manifest.configuration.store_location))
    assert wrong_store.value.code == ErrorCode.STALE_FENCE
