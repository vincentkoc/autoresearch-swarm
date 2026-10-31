"""Actual shared ResearchService routing in owned local workerd processes."""

import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import httpx
import pytest

from research_swarm.domain.assignments import AssignmentAuthority
from research_swarm.domain.clocks import ClockChallenge, SignedClockReply
from research_swarm.domain.types import new_id
from research_swarm.domain.workers import WorkerBinding
from research_swarm.workers.clock_signatures import ClockVerifier
from tests.test_cloudflare_native import build_current_sources, server
from tests.test_cloudflare_witness_ordering import WitnessHTTP

pytestmark = pytest.mark.skipif(
    os.environ.get("RESEARCH_CLOUDFLARE_TESTS") != "1" or os.name != "posix",
    reason="Explicit opt-in and POSIX-owned local workerd process group required",
)


def deliver(url, origin, action, key, **fields):
    with httpx.Client(trust_env=False, timeout=20) as client:
        response = client.post(
            url + "/research",
            json={
                "action": action,
                "key": key,
                "witness_origin": origin,
                **fields,
            },
        )
    assert response.status_code == 200, response.text[-4000:]
    return response.json()


def prove_missing_gate_is_detected(url, origin, witness):
    setup = deliver(url, origin, "provision", "control-setup", bypass_gate=True)
    held = witness.hold("control-enroll")
    with ThreadPoolExecutor(max_workers=2) as pool:
        enrollment = pool.submit(
            deliver,
            url,
            origin,
            "command",
            "control-enroll",
            bypass_gate=True,
            operation="worker.enroll",
            token=setup["operator_token"],
            payload=setup["enrollment"],
        )
        try:
            if not witness.event(witness.arrivals, "control-enroll").wait(10):
                early = enrollment.result(timeout=10)
                pytest.fail(f"Negative control returned before its witness: {early!r}")
            pending = pool.submit(
                deliver,
                url,
                origin,
                "query",
                "control-pending",
                bypass_gate=True,
                operation="worker.list",
                token=setup["worker_token"],
                payload={
                    "worker_id": setup["enrollment"]["worker_id"],
                    "session_id": setup["enrollment"]["session_id"],
                    "kind": "pending",
                },
            )
            assert witness.event(witness.ingress, "control-pending").wait(5)
            assert pending.result(timeout=5)["error"]["code"] == "NOT_FOUND"
            assert not enrollment.done()
        finally:
            held.set()
        assert "error" not in enrollment.result(timeout=15)


def test_shared_service_authentication_ordering_and_persistent_replay_in_workerd(tmp_path):
    build_current_sources()
    witness = WitnessHTTP()
    with witness.serving() as origin:
        with server(tmp_path, sequence=1) as url:
            prove_missing_gate_is_detected(url, origin, witness)
            missing = deliver(url, origin, "inspect", "missing")
            assert missing["error"]["code"] == "SCHEMA_UNSUPPORTED"
            setup = deliver(url, origin, "provision", "setup")
            assert "error" not in setup, setup
            enrollment = {
                "operation": "worker.enroll",
                "payload": setup["enrollment"],
                "token": setup["operator_token"],
            }
            worker_query = {
                "operation": "worker.list",
                "token": setup["worker_token"],
                "payload": {
                    "worker_id": setup["enrollment"]["worker_id"],
                    "session_id": setup["enrollment"]["session_id"],
                    "kind": "pending",
                },
            }
            held = witness.hold("enroll")
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(deliver, url, origin, "command", "enroll", **enrollment)
                try:
                    if not witness.event(witness.arrivals, "enroll").wait(10):
                        early = first.result(timeout=10)
                        pytest.fail(f"Router returned before witness acquisition: {early!r}")
                    second = pool.submit(deliver, url, origin, "query", "pending", **worker_query)
                    assert witness.event(witness.ingress, "pending").wait(5)
                    with pytest.raises(TimeoutError):
                        second.result(timeout=0.2)
                finally:
                    held.set()
                accepted = first.result(timeout=15)
                assert "error" not in accepted, accepted
                assert second.result(timeout=15)["items"] == []
            worker = accepted["result"]["data"]
            binding = WorkerBinding.model_validate(worker["binding"])
            cache = {
                "operation": "worker.cache_advertise",
                "token": setup["worker_token"],
                "payload": {"binding": worker["binding"], "snapshot_id": new_id()},
                "expected_revision": 0,
            }
            cached = deliver(url, origin, "command", "cache", **cache)
            assert cached["result"]["revision"] == 1
            challenge = ClockChallenge(
                worker_id=binding.worker_id, session_id=binding.worker_session_id, nonce=new_id()
            )
            clock = deliver(
                url,
                origin,
                "query",
                "clock",
                token=setup["worker_token"],
                operation="worker.clock",
                payload=challenge.model_dump(mode="json"),
            )
            verified = ClockVerifier(
                AssignmentAuthority.model_validate(worker["assignment_authority"])
            ).reply(SignedClockReply.model_validate(clock), challenge)
            assert verified.reply.payload.clock_profile == "hosted-native-witness-v1"
            offers = deliver(
                url,
                origin,
                "query",
                "offers",
                **{
                    **worker_query,
                    "payload": {**worker_query["payload"], "kind": "offers"},
                },
            )
            assert offers["items"] == []
            admission = {
                "operation": "worker.set_admission",
                "token": setup["operator_token"],
                "payload": {
                    "binding": worker["binding"],
                    "admission": "draining",
                    "reason": "fixture",
                },
                "expected_revision": 9,
            }
            rejected = deliver(url, origin, "command", "rejected", **admission)
            assert rejected["error"]["code"] == "REVISION_CONFLICT" and rejected["command_id"]
            before = deliver(url, origin, "inspect", "before")
            late = witness.hold("timeout")
            retry = {
                **cache,
                "payload": {**cache["payload"], "snapshot_id": new_id()},
                "expected_revision": 1,
            }
            try:
                timed_out = deliver(url, origin, "command", "timeout", timeout_ms=500, **retry)
                assert timed_out["error"]["code"] == "TRANSPORT_UNAVAILABLE"
                assert "command_id" not in timed_out
                assert deliver(url, origin, "inspect", "after-timeout") == before
            finally:
                late.set()
            assert deliver(url, origin, "command", "timeout", **retry)["result"]["revision"] == 2
            assert len(set(witness.nonces["timeout"])) == 2
            denied = deliver(
                url,
                origin,
                "command",
                "unsupported",
                token=setup["operator_token"],
                operation="artifact.begin",
            )
            assert denied["error"]["code"] == "CAPABILITY_UNAVAILABLE"
            assert witness.count("unsupported") == 0
            witness.epochs["fenced"] = "epoch-2"
            fencing = {
                **cache,
                "payload": {**cache["payload"], "snapshot_id": new_id()},
                "expected_revision": 2,
            }
            fenced = deliver(url, origin, "command", "fenced", **fencing)
            assert fenced["error"]["code"] == "STALE_FENCE" and fenced["command_id"]
            final_state = deliver(url, origin, "inspect", "final")
            assert (
                final_state["provider_state"] == "unqualified" and final_state["retirements"] == 1
            )
            assert final_state["artifact_effects"] == 0
            assert final_state["counts"]["commands"] == 5
            calls = {key: witness.count(key) for key in ("enroll", "rejected", "fenced")}

        with server(tmp_path, sequence=2) as url:
            # No provision call: ordinary open must preserve the existing lifecycle.
            assert deliver(url, origin, "command", "enroll", **enrollment) == accepted
            assert deliver(url, origin, "command", "rejected", **admission) == rejected
            assert deliver(url, origin, "command", "fenced", **fencing) == fenced
            assert deliver(url, origin, "query", "recovered", **worker_query)["items"] == []
            assert deliver(url, origin, "inspect", "reopened") == final_state
            assert {key: witness.count(key) for key in calls} == calls
            deliver(url, origin, "revoke", "revoke")
            revoked = deliver(url, origin, "command", "enroll", **enrollment)
            assert revoked["error"]["code"] == "AUTHORIZATION_DENIED"
            deliver(url, origin, "expire", "expire")
            expiring = deliver(url, origin, "query", "expired", **worker_query)
            assert expiring["error"]["code"] == "AUTHORIZATION_DENIED"
            assert witness.count("expired") == 0 and witness.count("enroll") == calls["enroll"]
            assert deliver(url, origin, "inspect", "end") == final_state
