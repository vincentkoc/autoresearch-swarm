"""Three actual fixed production Workers qualify private inactive import and RPC bounds."""

import copy
import json
import re
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack

import httpx

from adapters.cloudflare.checkpoint_wire import MAX_RPC_BYTES, decode_checkpoint
from adapters.cloudflare.deployment import DeploymentManifest, authority_id
from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.checkpoints.elections import verify_election, verify_election_request
from research_swarm.checkpoints.image_format import verify_image
from research_swarm.client import ResearchClient
from research_swarm.control.observations import PublishAttempt
from research_swarm.control.queries import RecordQuery
from research_swarm.control.recovery_proofs import verify_admission
from research_swarm.control.recovery_routes import verify_logical_recovery_route
from research_swarm.control.worker_outputs import WorkerOutputIntent
from research_swarm.control.worker_records import WorkerData
from research_swarm.crypto import public_key
from research_swarm.domain.backup import BackupCustodyPin, RecoveryAdmission, RestoreReceipt
from research_swarm.domain.canonical import canonical_bytes, content_digest
from research_swarm.domain.checkpoint_elections import (
    SignedLogicalElection,
    SignedLogicalElectionRequest,
)
from research_swarm.domain.checkpoint_images import LogicalImportIntent, SignedLogicalCheckpoint
from research_swarm.domain.recovery_routes import SignedWorkerRecoveryRoute
from research_swarm.domain.recovery_runtime import (
    RequalifyRecoveryRuntime,
    RuntimePreparationReceipt,
    RuntimeRequalificationReceipt,
)
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import new_id
from research_swarm.runtime import write_private
from research_swarm.workers.clock_estimates import ClockSampler
from research_swarm.workers.clocks import native_clock
from research_swarm.workers.journal_records import JournalState
from research_swarm.workers.local import LocalWorker
from tests.test_checkpoint_runtime import fresh_target_values
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
from tests.test_hosted_research import admit, enroll_and_claim, enrollment
from tests.test_hosted_result_publication import TRAIN
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
                {"binding": prefix + "_RECOVERY", "service": name, "entrypoint": "Recovery"},
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


def target_call(
    http,
    letter,
    action,
    credential,
    source_credential,
    capture,
    *,
    drop=False,
    election=None,
    turn=None,
    artifact=None,
    route=None,
    closure=None,
    activation=None,
    requalification=None,
):
    response = http.post(
        f"/fixture/target-{letter}/{action}",
        json={
            "credential": credential,
            "source_credential": source_credential,
            "request_json": json.dumps(capture),
            "election_json": json.dumps(election),
            "turn_json": json.dumps(turn),
            "artifact_json": json.dumps(artifact),
            "route_json": json.dumps(route),
            "closure_json": json.dumps(closure),
            "activation_json": json.dumps(activation),
            "requalification_json": json.dumps(requalification),
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
    with (
        serving(app, ssl_certfile=str(certificate), ssl_keyfile=str(key)) as origin,
        ExitStack() as target_lifetime,
    ):
        manifest["witness_origin"] = origin
        custody = fixture_custody(manifest, deployment[2])
        targets, target_secrets, target_endpoints = {}, {}, {}
        for index, (letter, token) in enumerate((("a", "T" * 43), ("b", "U" * 43))):
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
            targets[letter], target_secrets[letter] = fresh_target_values(
                targets[letter],
                deployment[2],
                seed=bytes([181 + index * 2]) * 32,
                dispatch=bytes([182 + index * 2]) * 32,
                bootstrap=token,
                consumer=chr(ord("M") + index) * 43,
                credential_letter=chr(ord("a") + index * 4),
            )
            target_descriptor = descriptor.model_copy(update={"id": new_id()})
            targets[letter]["configuration"]["source"] = target_descriptor.model_dump(mode="json")
            target_endpoints[letter] = Endpoint(
                WitnessAuthorityScope.model_validate(targets[letter]["configuration"]["authority"]),
                target_descriptor,
            )
            target_app = create_time_witness_app(
                target_endpoints[letter],
                witness_consumer_digest(target_secrets[letter]["witness_consumer"]),
            )
            targets[letter]["witness_origin"] = target_lifetime.enter_context(
                serving(target_app, ssl_certfile=str(certificate), ssl_keyfile=str(key))
            )
        token_sets = [
            set(item["credentials"].values()) for item in (secrets, *target_secrets.values())
        ]
        assert len(set.union(*token_sets)) == sum(map(len, token_sets))
        assert (
            len({item["assignment_seed_hex"] for item in (secrets, *target_secrets.values())}) == 3
        )
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
            (project / "train.py").write_text(TRAIN)
            project_manifest = project / "research.toml"
            project_manifest.write_text(
                project_manifest.read_text().replace("timeout_ms = 1000", "timeout_ms = 10000")
            )
            admitted = admit(operator, project)
            assert operator.get("job", admitted.job["id"])["state"] == "ready"
            driver_credential, evaluator_credential = manifest["credentials"][2:]
            configuration_digest = content_digest(b"checkpoint-saved-turn")
            binding = operator.command(
                "orchestration.bind",
                {
                    "campaign_id": admitted.campaign["id"],
                    "configuration_digest": configuration_digest,
                    "adapter": "cloudflare-workflows",
                    "routes": {
                        "control": driver_credential["principal"]["id"],
                        "evaluator": evaluator_credential["principal"]["id"],
                    },
                },
                idempotency_key="checkpoint-bind",
                expected_revision=admitted.campaign["revision"],
            )["result"]
            with ResearchClient.connect(
                url, secrets["credentials"][driver_credential["id"]]
            ) as driver:
                saved_turn = driver.command(
                    "orchestration.prepare_turn",
                    {
                        "campaign_id": admitted.campaign["id"],
                        "epoch": 1,
                        "ordinal": 1,
                        "configuration_digest": configuration_digest,
                        "planning_watermark": 0,
                        "action": {
                            "operation": "report.create",
                            "payload": {"campaign_id": admitted.campaign["id"]},
                            "idempotency_key": "checkpoint-unexecuted-report",
                        },
                    },
                    idempotency_key="checkpoint-prepare",
                    expected_revision=binding["revision"],
                )["result"]
            assert saved_turn["state"] == "prepared"
            claimed, _ = enroll_and_claim(
                operator,
                admitted,
                manifest["provisioning_actor_id"],
                project_ids=[admitted.project["id"]],
            )
            worker = WorkerData.model_validate(
                operator.get("worker", claimed["result"]["data"]["dispatch"]["worker_id"])["data"]
            )
            runner = LocalWorker(
                operator,
                worker,
                root=tmp_path / "original-worker",
                machine_registry=tmp_path / "original-machine" / "resources.sqlite",
            )

            def retain_original(entry):
                assert entry.state == JournalState.CLOSED and entry.native is not None
                assert entry.outcome is not None and entry.outcome.closure.scope_closed
                assert entry.outcome.released_at is not None and entry.outcome.exit_code == 0
                return entry

            runner._publish = retain_original
            original_journal = runner.execute(claimed["result"]["id"])
            journal_path = tmp_path / "original-closed-journal.json"
            journal_path.write_bytes(canonical_bytes(original_journal.model_dump(mode="json")))
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

            def admission_call(letter, *, action="admit", credential=None, drop=False):
                return target_call(
                    http,
                    letter,
                    action,
                    credential or target_secrets[letter]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                    drop=drop,
                    election=outcomes[winner],
                )

            assert admission_call(winner, action="default-admit").json() == {"denied": True}
            assert (
                admission_call(winner, credential="wrong").json()["error"]["code"]
                == "AUTHORIZATION_DENIED"
            )
            assert admission_call(loser).json()["error"]["code"] == "STALE_FENCE"
            assert admission_call(winner, drop=True).text == "{"
            accepted_admission = admission_call(winner).text
            result = json.loads(accepted_admission)
            assert result["disposition"] == "accepted", result
            admitted_recovery = RecoveryAdmission.model_validate(result["result"]["data"])
            assert (
                admitted_recovery.logical_election_digest
                == SignedLogicalElection.model_validate(outcomes[winner]).digest
            )
            assert admitted.campaign["id"] in admitted_recovery.paused_campaign_ids
            verify_admission(
                admitted_recovery,
                RestoreReceipt.model_validate(receipts[winner]),
                crypto_provider=deployment[2],
            )
            route_request = {
                "route_id": new_id(),
                "attempt_id": claimed["result"]["id"],
                "credential_id": manifest["credentials"][0]["id"],
                "source_controller_url": "https://source.example",
                "destination_url": "https://target.example",
            }

            def route_call(letter, *, action="route", credential=None, drop=False):
                return target_call(
                    http,
                    letter,
                    action,
                    credential or target_secrets[letter]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                    route=route_request,
                    drop=drop,
                )

            assert route_call(winner, action="default-route").json() == {"denied": True}
            assert "error" in route_call(loser).json()
            assert (
                route_call(winner, credential="wrong").json()["error"]["code"]
                == "AUTHORIZATION_DENIED"
            )
            assert route_call(winner, drop=True).text == "{"
            accepted_route = route_call(winner).text
            route_result = json.loads(accepted_route)
            assert route_result["disposition"] == "accepted"
            signed_route = SignedWorkerRecoveryRoute.model_validate(route_result["result"]["data"])
            verify_logical_recovery_route(
                signed_route, image.source_custody, crypto_provider=deployment[2]
            )
            assert signed_route.route.attempt_id == claimed["result"]["id"]
            assert (
                signed_route.route.binding.worker_session_id
                == claimed["result"]["data"]["dispatch"]["worker_session_id"]
            )
            assert signed_route.route.restoration == RestoreReceipt.model_validate(receipts[winner])
            assert signed_route.logical_election == SignedLogicalElection.model_validate(
                outcomes[winner]
            )
            recovery_url = f"/fixture/recovery-{winner}/v1"
            headers = {"authorization": "Bearer " + operator_token}
            recovery_caps = http.get(recovery_url + "/capabilities", headers=headers)
            assert recovery_caps.status_code == 200, recovery_caps.text
            assert recovery_caps.json()["profile"] == "original-worker-recovery-v1"
            assert recovery_caps.json()["execution"] == "unavailable"
            assert (
                http.get(f"/fixture/recovery-{loser}/v1/capabilities", headers=headers).status_code
                != 200
            )
            historical_request = {
                "worker_id": signed_route.route.binding.worker_id,
                "session_id": signed_route.route.binding.worker_session_id,
                "kind": "historical_attempt",
                "id": signed_route.route.attempt_id,
            }
            raw_history = json.dumps(historical_request).encode()
            history_response = http.post(
                recovery_url + "/worker/get",
                headers=headers,
                content=iter((raw_history[:10], raw_history[10:])),
            )
            assert history_response.status_code == 200, history_response.text
            historical_attempt = history_response.json()
            assert historical_attempt["id"] == claimed["result"]["id"]
            assert historical_attempt["data"]["dispatch"] == claimed["result"]["data"]["dispatch"]
            # Stream past the JSON bound. Authentication must still determine the
            # unauthorized response; authorized requests reach the bounded reader.
            for token, expected in (("invalid", 401), (operator_token, 422)):
                refused = http.post(
                    recovery_url + "/worker/get",
                    headers={"authorization": "Bearer " + token},
                    content=(b" " * 65536 for _ in range(17)),
                )
                assert refused.status_code == expected, refused.text
            assert (
                http.post(
                    recovery_url + "/commands/worker.claim_job",
                    headers={**headers, "idempotency-key": "not-execution"},
                    json={},
                ).status_code
                == 501
            )
            turn_request = {
                "request_id": new_id(),
                "operation": "orchestration.close_turn",
                "expected_revision": saved_turn["revision"],
                "payload": {
                    "turn_id": saved_turn["id"],
                    "epoch": 1,
                    "finding": "Original source sealed before action execution",
                },
            }

            def turn_call(letter, *, action="turn", payload=None, drop=False):
                return target_call(
                    http,
                    letter,
                    action,
                    target_secrets[letter]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                    drop=drop,
                    turn=turn_request if payload is None else payload,
                )

            assert turn_call(winner, action="default-turn").json() == {"denied": True}
            assert "error" in turn_call(loser).json()
            assert (
                turn_call(
                    winner, payload={**turn_request, "operation": "orchestration.execute_turn"}
                ).json()["error"]["code"]
                == "PROJECT_CONTRACT_INVALID"
            )
            assert turn_call(winner, drop=True).text == "{"
            accepted_turn = turn_call(winner).text
            closed = json.loads(accepted_turn)["result"]
            assert closed["turn"]["id"] == saved_turn["id"]
            assert closed["turn"]["state"] == "closed_unacknowledged"
            assert closed["response"] is closed["receipt"] is None

            blob = image.artifacts[0]
            artifact_request = {
                "backup_id": image.seal.backup_id,
                "manifest_digest": image.digest,
                "artifact_id": blob.reference.id,
                "generation": blob.generation,
            }
            source_request = {
                "credential": secrets["bootstrap"],
                "request_json": json.dumps(artifact_request),
            }
            assert http.post(
                "/fixture/default-checkpoint-read-artifact", json=source_request
            ).json() == {"denied": True}
            assert (
                http.post(
                    "/fixture/checkpoint-read-artifact",
                    json={**source_request, "credential": "wrong"},
                ).json()["error"]["code"]
                == "AUTHORIZATION_DENIED"
            )
            exported = http.post("/fixture/checkpoint-read-artifact", json=source_request).json()
            assert exported["artifact"] == blob.model_dump(mode="json")
            assert exported["manifest_digest"] == image.digest
            content = bytes.fromhex(exported["content_hex"])
            assert len(content) == blob.reference.size_bytes
            assert content_digest(content) == blob.reference.digest

            def artifact_call(letter, *, action="artifact", credential=None, drop=False):
                return target_call(
                    http,
                    letter,
                    action,
                    credential or target_secrets[letter]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                    artifact=artifact_request,
                    drop=drop,
                )

            assert artifact_call(winner, action="default-artifact").json() == {"denied": True}
            assert "error" in artifact_call(loser).json()["result"]
            assert (
                artifact_call(winner, credential="wrong").json()["result"]["error"]["code"]
                == "AUTHORIZATION_DENIED"
            )
            assert artifact_call(winner, drop=True).text == "{"
            copied_artifact = artifact_call(winner).text
            copied = json.loads(copied_artifact)
            assert copied["exported"] == exported
            assert copied["result"]["artifact"] == blob.model_dump(mode="json")
            assert copied["result"]["restore_id"] == bindings[winner]["intent"]["restore_id"]
            assert (
                copied["result"]["storage_digest"]
                == DeploymentManifest.model_validate(targets[winner]).configuration.storage.digest
            )

            output_intent = WorkerOutputIntent(
                worker_id=worker.binding.worker_id,
                session_id=worker.binding.worker_session_id,
                attempt_id=original_journal.dispatch.attempt_id,
                role="journal",
            )
            with ResearchClient.connect(
                url + f"/fixture/recovery-{winner}", operator_token
            ) as recovered_worker:
                journal_reference = recovered_worker.worker_upload_file(
                    output_intent, journal_path, idempotency_key="original-journal"
                )
                evidence = PublishAttempt(
                    attempt_id=original_journal.dispatch.attempt_id,
                    journal_artifact=journal_reference,
                )
                submitted = recovered_worker.command(
                    "worker.submit_evidence", evidence, idempotency_key="original-evidence"
                )
                assert submitted["result"]["state"] == "quarantined"
            closure_request = {
                "request_id": new_id(),
                "expected_revision": historical_attempt["revision"],
                "payload": {
                    "attempt_id": original_journal.dispatch.attempt_id,
                    "submission_id": submitted["result"]["id"],
                    "expected_controller_id": receipts[winner]["controller_id"],
                    "expected_control_epoch": receipts[winner]["target_control_epoch"],
                    "reason": "Original native CPU process closed before the source cut",
                },
            }

            def closure_call(letter, *, action="closure", credential=None, drop=False):
                return target_call(
                    http,
                    letter,
                    action,
                    credential or target_secrets[letter]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                    closure=closure_request,
                    drop=drop,
                )

            assert closure_call(winner, action="default-closure").json() == {"denied": True}
            assert "error" in closure_call(loser).json()
            assert (
                closure_call(winner, credential="wrong").json()["error"]["code"]
                == "AUTHORIZATION_DENIED"
            )
            assert closure_call(winner, drop=True).text == "{"
            accepted_closure = closure_call(winner).text
            settled = json.loads(accepted_closure)["result"]
            assert settled["attempt"]["state"] == "closed_unaccepted"
            assert settled["closure"]["data"][
                "native_outcome"
            ] == original_journal.outcome.model_dump(mode="json")
            assert settled["closure"]["data"]["consumed"] == {
                "attempts": "1",
                "execution_ms": str(original_journal.outcome.elapsed_ms),
            }
            for remaining in image.artifacts[1:]:
                restored = target_call(
                    http,
                    winner,
                    "artifact",
                    target_secrets[winner]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                    artifact={
                        "backup_id": image.seal.backup_id,
                        "manifest_digest": image.digest,
                        "artifact_id": remaining.reference.id,
                        "generation": remaining.generation,
                    },
                ).json()
                assert restored["result"]["artifact"] == remaining.model_dump(mode="json")

            def runtime_call(
                letter,
                action,
                *,
                credential=None,
                drop=False,
                activation=None,
                requalification=None,
            ):
                return target_call(
                    http,
                    letter,
                    action,
                    credential or target_secrets[letter]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                    drop=drop,
                    activation=activation,
                    requalification=requalification,
                )

            assert runtime_call(winner, "default-prepare-runtime").json() == {"denied": True}
            assert "error" in runtime_call(loser, "prepare-runtime").json()
            assert (
                runtime_call(winner, "prepare-runtime", credential="wrong").json()["error"]["code"]
                == "AUTHORIZATION_DENIED"
            )
            assert runtime_call(winner, "prepare-runtime", drop=True).text == "{"
            prepared_runtime = runtime_call(winner, "prepare-runtime").text
            prepared_value = json.loads(prepared_runtime)["receipt"]
            assert prepared_value["provider"]["scope"]["control_epoch"] == 2
            target_witness_calls = len(target_endpoints[winner].requests)
            assert target_witness_calls == 1
            target_endpoints[winner].mode = "unavailable"
            fresh_token = target_secrets[winner]["credentials"][
                targets[winner]["credentials"][0]["id"]
            ]
            assert (
                http.get(
                    f"/fixture/target-{winner}/capabilities",
                    headers={"authorization": "Bearer " + fresh_token},
                ).status_code
                != 200
            )
            assert len(endpoint.requests) == witness_calls

        with (
            server(tmp_path, sequence=6, **options) as url,
            httpx.Client(base_url=url, trust_env=False, timeout=60) as http,
        ):
            repeated = target_call(
                http,
                winner,
                "admit",
                target_secrets[winner]["bootstrap"],
                secrets["bootstrap"],
                capture,
                election=outcomes[winner],
            )
            assert repeated.text == accepted_admission
            restored_history = http.post(
                f"/fixture/recovery-{winner}/v1/worker/get",
                headers={"authorization": "Bearer " + operator_token},
                json=historical_request,
            )
            assert restored_history.status_code == 200, restored_history.text
            assert restored_history.json() == settled["attempt"]
            assert (
                target_call(
                    http,
                    winner,
                    "closure",
                    target_secrets[winner]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                    closure=closure_request,
                ).text
                == accepted_closure
            )
            with ResearchClient.connect(
                url + f"/fixture/recovery-{winner}", operator_token
            ) as recovered_worker:
                assert (
                    recovered_worker.worker_upload_file(
                        output_intent, journal_path, idempotency_key="original-journal"
                    )
                    == journal_reference
                )
                assert (
                    recovered_worker.command(
                        "worker.submit_evidence", evidence, idempotency_key="original-evidence"
                    )
                    == submitted
                )
            assert (
                target_call(
                    http,
                    winner,
                    "route",
                    target_secrets[winner]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                    route=route_request,
                ).text
                == accepted_route
            )
            assert (
                target_call(
                    http,
                    winner,
                    "artifact",
                    target_secrets[winner]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                    artifact=artifact_request,
                ).text
                == copied_artifact
            )
            assert (
                target_call(
                    http,
                    winner,
                    "turn",
                    target_secrets[winner]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                    turn=turn_request,
                ).text
                == accepted_turn
            )
            for action in ("request", "provision"):
                assert (
                    "error"
                    in target_call(
                        http,
                        winner,
                        action,
                        target_secrets[winner]["bootstrap"],
                        secrets["bootstrap"],
                        capture,
                    ).json()
                )
            assert (
                "error"
                in target_call(
                    http,
                    winner,
                    "import",
                    target_secrets[winner]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                ).json()["result"]
            )
            assert (
                target_call(
                    http,
                    loser,
                    "request",
                    target_secrets[loser]["bootstrap"],
                    secrets["bootstrap"],
                    capture,
                ).json()
                == requests[loser]
            )
            for letter in ("a", "b"):
                assert (
                    http.get(
                        f"/fixture/target-{letter}/capabilities",
                        headers={"authorization": "Bearer " + operator_token},
                    ).status_code
                    != 200
                )
            assert checkpoint(http, secrets["bootstrap"], capture, capture=True).text == captured
            assert len(endpoint.requests) == witness_calls
            assert runtime_call(winner, "prepare-runtime").text == prepared_runtime
            assert len(target_endpoints[winner].requests) == target_witness_calls
            activation_request = {
                "request_id": new_id(),
                # Import advances sealed -> restoring; admission advances -> recovering.
                "expected_revision": image.seal_revision + 2,
                "payload": {
                    "restore_id": receipts[winner]["restore_id"],
                    "expected_control_epoch": 2,
                    "reason": "Original CPU and driver obligations closed; target prepared",
                },
            }
            assert runtime_call(
                winner, "default-activate", activation=activation_request
            ).json() == {"denied": True}
            assert (
                runtime_call(
                    winner, "activate", credential="wrong", activation=activation_request
                ).json()["error"]["code"]
                == "AUTHORIZATION_DENIED"
            )
            assert "error" in runtime_call(loser, "activate", activation=activation_request).json()
            target_endpoints[winner].mode = "normal"
            initial_runtime = RuntimePreparationReceipt.model_validate(prepared_value)
            target_endpoints[winner].restart()
            refused = runtime_call(winner, "activate", activation=activation_request).json()
            assert refused["error"]["code"] == "STALE_FENCE", refused
            restarted_epoch = target_endpoints[winner].responses[-1].payload.source_epoch
            assert restarted_epoch != initial_runtime.provider.source_epoch
            retired_witness_calls = len(target_endpoints[winner].requests)
            assert retired_witness_calls == target_witness_calls + 1
            assert "error" in runtime_call(winner, "prepare-runtime").json()
            assert len(target_endpoints[winner].requests) == retired_witness_calls
            requalification_request = RequalifyRecoveryRuntime(
                request_id=new_id(),
                expected_provider_revision=initial_runtime.provider_revision + 1,
                previous_qualification_digest=initial_runtime.digest,
                reason="Pinned native witness restarted before target activation",
            ).model_dump(mode="json")
            assert runtime_call(
                winner, "default-requalify-runtime", requalification=requalification_request
            ).json() == {"denied": True}
            assert (
                runtime_call(
                    winner,
                    "requalify-runtime",
                    credential="wrong",
                    requalification=requalification_request,
                ).json()["error"]["code"]
                == "AUTHORIZATION_DENIED"
            )
            assert (
                "error"
                in runtime_call(
                    loser, "requalify-runtime", requalification=requalification_request
                ).json()
            )
            assert len(target_endpoints[winner].requests) == retired_witness_calls
            assert (
                runtime_call(
                    winner, "requalify-runtime", requalification=requalification_request, drop=True
                ).text
                == "{"
            )
            qualified_witness_calls = len(target_endpoints[winner].requests)
            assert qualified_witness_calls == retired_witness_calls + 1
            target_endpoints[winner].mode = "unavailable"

        with (
            server(tmp_path, sequence=7, **options) as url,
            httpx.Client(base_url=url, trust_env=False, timeout=60) as http,
        ):
            requalified_runtime = runtime_call(
                winner, "requalify-runtime", requalification=requalification_request
            ).text
            requalified = json.loads(requalified_runtime)
            assert requalified["disposition"] == "accepted"
            successor = RuntimeRequalificationReceipt.model_validate(requalified["result"]["data"])
            assert successor.request.model_dump(mode="json") == requalification_request
            assert successor.plan_digest == initial_runtime.plan_digest
            assert successor.restoration_digest == initial_runtime.restoration_digest
            assert successor.admission_digest == initial_runtime.admission_digest
            assert (
                successor.previous_generation == initial_runtime.provider.scope.provider_generation
            )
            assert successor.provider_revision == initial_runtime.provider_revision + 2
            assert successor.provider.scope.provider_generation != successor.previous_generation
            assert successor.provider.source_epoch == restarted_epoch
            assert (
                runtime_call(
                    winner, "requalify-runtime", requalification=requalification_request
                ).text
                == requalified_runtime
            )
            assert len(target_endpoints[winner].requests) == qualified_witness_calls
            assert "error" in runtime_call(winner, "prepare-runtime").json()
            assert (
                http.get(
                    f"/fixture/target-{winner}/capabilities",
                    headers={"authorization": "Bearer " + fresh_token},
                ).status_code
                != 200
            )
            assert len(endpoint.requests) == witness_calls
            target_endpoints[winner].mode = "normal"
            assert (
                runtime_call(winner, "activate", activation=activation_request, drop=True).text
                == "{"
            )
            accepted_activation = runtime_call(
                winner, "activate", activation=activation_request
            ).text
            activated = json.loads(accepted_activation)
            assert activated["disposition"] == "accepted"
            assert activated["result"]["data"]["profile"] == "hosted-recovery-activation-v1"
            final_witness = activated["result"]["data"]["final_witness"]["payload"]
            assert (
                final_witness["request"]["scope"]["provider_generation"]
                == successor.provider.scope.provider_generation
            )
            assert final_witness["source_epoch"] == restarted_epoch
            activated_witness_calls = len(target_endpoints[winner].requests)
            assert activated_witness_calls == qualified_witness_calls + 1
            target_endpoints[winner].mode = "unavailable"

        with (
            server(tmp_path, sequence=8, **options) as url,
            httpx.Client(base_url=url, trust_env=False, timeout=60) as http,
            ResearchClient.connect(url + f"/fixture/authority-{winner}", fresh_token) as current,
        ):
            assert (
                runtime_call(winner, "activate", activation=activation_request).text
                == accepted_activation
            )
            assert len(target_endpoints[winner].requests) == activated_witness_calls
            assert current.capabilities()["execution"] != "unavailable"
            assert (
                current.get("attempt", original_journal.dispatch.attempt_id) == settled["attempt"]
            )
            assert current.list(RecordQuery(kind="run_result"))["items"] == []
            assert current.get("campaign", admitted.campaign["id"])["state"] == "paused"
            for token in secrets["credentials"].values():
                assert (
                    http.get(
                        f"/fixture/target-{winner}/capabilities",
                        headers={"authorization": "Bearer " + token},
                    ).status_code
                    == 401
                )
            assert (
                http.get(
                    f"/fixture/recovery-{winner}/v1/capabilities",
                    headers={"authorization": "Bearer " + operator_token},
                ).status_code
                != 200
            )
            assert (
                http.get(
                    f"/fixture/target-{loser}/capabilities",
                    headers={"authorization": "Bearer " + fresh_token},
                ).status_code
                != 200
            )
            target_endpoints[winner].mode = "normal"
            enroll = enrollment(
                targets[winner]["provisioning_actor_id"],
                current.capabilities()["clock"]["policy_digest"],
            )
            enrolled = current.command("worker.enroll", enroll, idempotency_key="new-target-worker")
            fresh_worker = WorkerData.model_validate(enrolled["result"]["data"])
            assert fresh_worker.binding.control_epoch == 2
            assert fresh_worker.binding.worker_session_id != worker.binding.worker_session_id
            assert (
                fresh_worker.assignment_authority.key_id
                == targets[winner]["configuration"]["authority"]["assignment_key_id"]
            )
            sampler = ClockSampler(
                fresh_worker.assignment_authority, clock_admission=fresh_worker.clock_admission
            )
            pending = sampler.begin()
            pending.complete(current.worker_clock(pending.challenge))
            assert len(target_endpoints[winner].requests) > activated_witness_calls
            assert (
                current.command("worker.enroll", enroll, idempotency_key="new-target-worker")
                == enrolled
            )
            assert (
                runtime_call(winner, "activate", activation=activation_request).text
                == accepted_activation
            )
            assert checkpoint(http, secrets["bootstrap"], capture, capture=True).text == captured
            assert len(endpoint.requests) == witness_calls
