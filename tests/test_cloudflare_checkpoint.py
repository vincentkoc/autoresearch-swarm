"""Three actual fixed production Workers qualify private inactive import and RPC bounds."""

import copy
import json
import re
from concurrent.futures import ThreadPoolExecutor

import httpx

from adapters.cloudflare.checkpoint_wire import MAX_RPC_BYTES, decode_checkpoint
from adapters.cloudflare.deployment import DeploymentManifest, authority_id, secret_digest
from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.checkpoints.elections import verify_election, verify_election_request
from research_swarm.checkpoints.image_format import verify_image
from research_swarm.client import ResearchClient
from research_swarm.crypto import public_key
from research_swarm.domain.backup import BackupCustodyPin
from research_swarm.domain.checkpoint_elections import (
    SignedLogicalElection,
    SignedLogicalElectionRequest,
)
from research_swarm.domain.checkpoint_images import LogicalImportIntent, SignedLogicalCheckpoint
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import new_id
from research_swarm.runtime import write_private
from research_swarm.workers.clocks import native_clock
from tests.test_cloudflare_deployment import deployment as deployment
from tests.test_cloudflare_native import ROOT, build_current_sources, server
from tests.test_cloudflare_native_source import Endpoint
from tests.test_cloudflare_production import (
    checkpoint,
    fixture_config,
    fixture_custody,
    fixture_secrets,
    post,
)
from tests.test_cloudflare_router import pytestmark as pytestmark
from tests.test_hosted_research import admit
from tests.test_http_delivery import serving, tls_certificate
from tests.test_manifest import project as project
from tests.test_time_witnesses import source


def operator_config(directory):
    directory.mkdir(mode=0o700)
    raw = (ROOT / "wrangler.operator-test.jsonc").read_text()
    config = json.loads(re.sub(r",(\s*[}\]])", r"\1", raw))
    config["name"] = "research-swarm-checkpoint-operator-fixture"
    config["main"] = str(ROOT / "production_checkpoint_fixture.mjs")
    for letter in ("a", "b"):
        name, prefix = "research-swarm-target-" + letter, "TARGET_" + letter.upper()
        config["services"].extend(
            [
                {"binding": prefix + "_API", "service": name},
                {"binding": prefix + "_BOOTSTRAP", "service": name, "entrypoint": "Bootstrap"},
            ]
        )
        config["durable_objects"]["bindings"].append(
            {"name": prefix + "_AUTHORITY", "class_name": "ResearchAuthority", "script_name": name}
        )
    path = directory / "wrangler.json"
    write_private(path, json.dumps(config).encode())
    return path


def locate(raw, namespace, object_id, *, epoch, activation):
    raw["namespace"], raw["object_id"] = namespace, object_id
    raw["configuration"]["store_location"] = namespace + "/" + object_id
    raw["configuration"]["authority"].update(
        state_authority_id=authority_id(namespace, object_id),
        activation_id=activation,
        control_epoch=epoch,
    )


def target_binding(manifest, source_custody, digest, seed, crypto):
    target = DeploymentManifest.model_validate(manifest)
    intent = LogicalImportIntent(
        restore_id=new_id(),
        manifest_digest=digest,
        target_location=target.configuration.store_location,
        target_deployment_digest=target.digest,
        target_public_key=public_key(crypto, seed).hex(),
        operator_id=target.provisioning_actor_id,
    )
    return {
        "schema_version": "1.0",
        "intent": intent.model_dump(mode="json"),
        "expected_source": source_custody["authority"],
        "expected_custody": source_custody["pin"],
        "custody_seed_hex": seed.hex(),
    }


def target_call(http, letter, action, credential, source_credential, capture, *, drop=False):
    response = http.post(
        f"/fixture/target-{letter}/{action}",
        json={
            "credential": credential,
            "source_credential": source_credential,
            "request_json": json.dumps(capture),
        },
        headers={"x-fixture-drop-bootstrap": "true"} if drop else {},
    )
    assert response.status_code == 200, response.text[:1000]
    return response


def elect(http, credential, request, *, drop=False, default=False):
    response = http.post(
        "/fixture/" + ("default-" if default else "") + "checkpoint-elect",
        json={"credential": credential, "request_json": json.dumps(request)},
        headers={"x-fixture-drop-bootstrap": "true"} if drop else {},
    )
    assert response.status_code == 200, response.text[:1000]
    return response


def test_three_production_authorities_import_pinned_image_and_replay_after_restart(
    tmp_path, deployment, project
):
    build_current_sources(production=True)
    manifest, secrets = (copy.deepcopy(item) for item in deployment[:2])
    manifest["deliveries"] = []
    directories = {name: tmp_path / name for name in ("source", "a", "b")}
    configs = [fixture_config(directories["source"])]
    configs.extend(
        fixture_config(
            directories[letter],
            name="research-swarm-target-" + letter,
            bucket_name="research-swarm-target-" + letter + "-artifacts",
        )
        for letter in ("a", "b")
    )
    options = {
        "config": str(operator_config(tmp_path / "operator")),
        "extra_configs": tuple(configs),
    }
    with (
        server(tmp_path, sequence=1, **options) as url,
        httpx.Client(base_url=url, trust_env=False, timeout=30) as http,
    ):
        identities = http.get("/fixture/targets").json()
        identities["source"] = http.get("/fixture/identity").json()["object_id"]
        assert len(set(identities.values())) == 3
        assert all(re.fullmatch(r"[0-9a-f]{64}", value) for value in identities.values())

    locate(
        manifest, "synthetic-checkpoint-source", identities["source"], epoch=1, activation=new_id()
    )
    descriptor = source().model_copy(update={"native_profile": native_clock().profile})
    manifest["configuration"]["source"] = descriptor.model_dump(mode="json")
    endpoint = Endpoint(
        WitnessAuthorityScope.model_validate(manifest["configuration"]["authority"]), descriptor
    )
    certificate, key = tls_certificate(tmp_path)
    app = create_time_witness_app(endpoint, witness_consumer_digest(secrets["witness_consumer"]))
    operator_token = secrets["credentials"][manifest["credentials"][0]["id"]]
    with serving(app, ssl_certfile=str(certificate), ssl_keyfile=str(key)) as origin:
        manifest["witness_origin"] = origin
        custody = fixture_custody(manifest, deployment[2])
        targets, target_secrets = {}, {}
        for letter, token in (("a", "T" * 43), ("b", "U" * 43)):
            targets[letter], target_secrets[letter] = (
                copy.deepcopy(manifest),
                copy.deepcopy(secrets),
            )
            locate(
                targets[letter],
                "synthetic-checkpoint-" + letter,
                identities[letter],
                epoch=2,
                activation=new_id(),
            )
            targets[letter]["configuration"]["storage"].update(
                bucket_identity="synthetic-target-" + letter, prefix="research-target-" + letter
            )
            targets[letter]["bootstrap_credential_digest"] = secret_digest("bootstrap", token)
            target_secrets[letter]["bootstrap"] = token
        bindings = {}

        def configure():
            redactions = fixture_secrets(directories["source"], manifest, secrets, source=custody)
            for letter in ("a", "b"):
                redactions += fixture_secrets(
                    directories[letter],
                    targets[letter],
                    target_secrets[letter],
                    target=bindings.get(letter),
                )
            options.update(
                child_environment={"NODE_EXTRA_CA_CERTS": str(certificate)},
                diagnostic_secrets=redactions,
            )

        configure()
        with (
            server(tmp_path, sequence=2, **options) as url,
            httpx.Client(base_url=url, trust_env=False, timeout=30) as http,
            ResearchClient.connect(url, operator_token) as operator,
        ):
            assert "receipt" in post(http, secrets["bootstrap"]).json()
            admitted = admit(operator, project)
            assert operator.get("job", admitted.job["id"])["state"] == "ready"
            endpoint.mode = "unavailable"
            witness_calls = len(endpoint.requests)
            seal = {
                "request_id": new_id(),
                "expected_revision": 0,
                "seal": {
                    "backup_id": new_id(),
                    "expected_controller_id": manifest["configuration"]["authority"][
                        "controller_id"
                    ],
                    "expected_control_epoch": 1,
                    "reason": "Multi-target private import qualification",
                },
            }
            assert "seal" in checkpoint(http, secrets["bootstrap"], seal).json()
            capture = {"backup_id": seal["seal"]["backup_id"]}
            captured = checkpoint(http, secrets["bootstrap"], capture, capture=True).text
            envelope, bodies = decode_checkpoint(captured)
            image = SignedLogicalCheckpoint.model_validate_json(envelope).manifest
            verify_image(
                envelope,
                bodies,
                expected_digest=image.digest,
                expected_source=WitnessAuthorityScope.model_validate(custody["authority"]),
                expected_custody=BackupCustodyPin.model_validate(custody["pin"]),
                crypto=deployment[2],
            )
            assert image.artifacts and len(endpoint.requests) == witness_calls

        bindings["a"] = target_binding(
            targets["a"], custody, image.digest, b"t" * 32, deployment[2]
        )
        # Deliberate reuse: the same key/intent must not authorize a different physical target.
        bindings["b"] = copy.deepcopy(bindings["a"])
        configure()
        receipts, requests = {}, {}
        with (
            server(tmp_path, sequence=3, **options) as url,
            httpx.Client(base_url=url, trust_env=False, timeout=60) as http,
        ):

            def call(letter, action, *, credential=None, drop=False):
                return target_call(
                    http,
                    letter,
                    action,
                    credential or target_secrets[letter]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                    drop=drop,
                )

            assert call("a", "default-import").json() == {"denied": True}
            assert call("a", "default-request").json() == {"denied": True}
            assert "error" in call("a", "request").json()
            assert (
                call("a", "import", credential="wrong").json()["result"]["error"]["code"]
                == "AUTHORIZATION_DENIED"
            )
            assert call("b", "import").json()["result"]["error"]["code"] == "STALE_FENCE"
            over = call("a", "import-over-limit").json()
            assert over["bundle_bytes"] == MAX_RPC_BYTES + 1
            assert over["result"]["error"]["code"] == "ARTIFACT_UNVERIFIED"
            assert call("a", "import-limit", drop=True).text == "{"
            accepted = call("a", "import-limit").json()
            assert accepted["bundle_bytes"] == MAX_RPC_BYTES
            assert "receipt" in accepted["result"], accepted["result"]
            receipts["a"] = accepted["result"]["receipt"]
            assert call("a", "import").json()["result"]["receipt"] == receipts["a"]
            assert call("a", "request", credential="wrong").json()["error"]["code"] == (
                "AUTHORIZATION_DENIED"
            )
            assert call("a", "request", drop=True).text == "{"
            requests["a"] = call("a", "request").json()
            assert "request" in requests["a"], requests["a"]
            assert "error" in call("a", "provision").json()
            assert (
                http.get(
                    "/fixture/target-a/capabilities",
                    headers={"authorization": "Bearer " + operator_token},
                ).status_code
                != 200
            )
            assert call("a", "import").json()["result"]["receipt"] == receipts["a"]
            assert checkpoint(http, secrets["bootstrap"], capture, capture=True).text == captured
            assert len(endpoint.requests) == witness_calls

        bindings["b"] = target_binding(
            targets["b"], custody, image.digest, b"u" * 32, deployment[2]
        )
        configure()
        with (
            server(tmp_path, sequence=4, **options) as url,
            httpx.Client(base_url=url, trust_env=False, timeout=60) as http,
        ):
            for letter in ("a", "b"):
                result = target_call(
                    http,
                    letter,
                    "import",
                    target_secrets[letter]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                ).json()["result"]
                assert "receipt" in result, result
                receipt = result["receipt"]
                if letter == "a":
                    assert receipt == receipts[letter]
                receipts[letter] = receipt
                requested = target_call(
                    http,
                    letter,
                    "request",
                    target_secrets[letter]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                ).json()
                assert "request" in requested, requested
                if letter == "a":
                    assert requested == requests[letter]
                requests[letter] = requested
                verify_election_request(
                    SignedLogicalElectionRequest.model_validate(requested),
                    expected_intent=LogicalImportIntent.model_validate(bindings[letter]["intent"]),
                    expected_backup_id=image.seal.backup_id,
                    expected_source=image.source_authority,
                    expected_deployment_digest=image.source_deployment_digest,
                    expected_custody=image.source_custody,
                    crypto=deployment[2],
                )
                assert receipt["restore_id"] == bindings[letter]["intent"]["restore_id"]
                assert receipt["manifest_digest"] == image.digest
                assert receipt["source_public_key"] == custody["pin"]["public_key"]
                assert (
                    receipt["target_public_key"] == bindings[letter]["intent"]["target_public_key"]
                )
                assert receipt["target_control_epoch"] == 2
                assert (
                    http.get(
                        f"/fixture/target-{letter}/capabilities",
                        headers={"authorization": "Bearer " + operator_token},
                    ).status_code
                    != 200
                )
            assert receipts["a"]["target_public_key"] != receipts["b"]["target_public_key"]
            assert elect(http, secrets["bootstrap"], requests["a"], default=True).json() == {
                "denied": True
            }
            assert (
                elect(http, "wrong", requests["a"]).json()["error"]["code"]
                == "AUTHORIZATION_DENIED"
            )
            # Both replies are lost, including the winner's committed reply.
            with ThreadPoolExecutor(max_workers=2) as pool:
                replies = list(
                    pool.map(
                        lambda letter: elect(
                            http, secrets["bootstrap"], requests[letter], drop=True
                        ),
                        ("a", "b"),
                    )
                )
            assert [reply.text for reply in replies] == ["{", "{"]
            assert sorted(reply.headers["x-fixture-election-outcome"] for reply in replies) == [
                "conflict",
                "elected",
            ]
            initial_winner = next(
                letter
                for letter, reply in zip(("a", "b"), replies, strict=True)
                if reply.headers["x-fixture-election-outcome"] == "elected"
            )
            responses = {
                letter: elect(http, secrets["bootstrap"], requests[letter]) for letter in ("a", "b")
            }
            outcomes = {letter: response.json() for letter, response in responses.items()}
            winners = [letter for letter, result in outcomes.items() if "election" in result]
            assert len(winners) == 1, outcomes
            winner = winners[0]
            assert winner == initial_winner
            loser = "b" if winner == "a" else "a"
            assert outcomes[loser]["error"]["code"] == "IDEMPOTENCY_CONFLICT"
            verify_election(
                SignedLogicalElection.model_validate(outcomes[winner]),
                expected_intent=LogicalImportIntent.model_validate(bindings[winner]["intent"]),
                expected_backup_id=image.seal.backup_id,
                expected_source=image.source_authority,
                expected_deployment_digest=image.source_deployment_digest,
                expected_custody=image.source_custody,
                crypto=deployment[2],
            )
            assert checkpoint(http, secrets["bootstrap"], capture, capture=True).text == captured
            assert len(endpoint.requests) == witness_calls

        with (
            server(tmp_path, sequence=5, **options) as url,
            httpx.Client(base_url=url, trust_env=False, timeout=60) as http,
        ):
            for letter in ("a", "b"):
                assert (
                    target_call(
                        http,
                        letter,
                        "request",
                        target_secrets[letter]["bootstrap"],
                        secrets["bootstrap"],
                        capture,
                    ).json()
                    == requests[letter]
                )
                repeated = elect(http, secrets["bootstrap"], requests[letter])
                if letter == winner:
                    assert repeated.text == responses[letter].text
                else:
                    assert repeated.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
                assert (
                    http.get(
                        f"/fixture/target-{letter}/capabilities",
                        headers={"authorization": "Bearer " + operator_token},
                    ).status_code
                    != 200
                )
            assert checkpoint(http, secrets["bootstrap"], capture, capture=True).text == captured
            assert len(endpoint.requests) == witness_calls
