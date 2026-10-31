"""Real native source to workerd; authority calibration requires explicit admission."""

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack

import httpx
import pytest

from research_swarm.api.time_witnesses import create_time_witness_app, witness_consumer_digest
from research_swarm.client import ResearchClient
from research_swarm.control.native_time_witnesses import NativeWitnessSource
from research_swarm.domain.assignments import AssignmentAuthority
from research_swarm.domain.clocks import AuthorityClockAdmission
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import new_id
from research_swarm.workers.clock_estimates import ClockSampler
from research_swarm.workers.clock_signatures import ClockVerifier
from research_swarm.workers.clocks import native_clock
from tests.test_cloudflare_native import build_current_sources, server
from tests.test_cloudflare_router import pytestmark as pytestmark
from tests.test_http_delivery import serving
from tests.test_time_witnesses import SEED, source

TOKEN = "public_fixture_consumer_" + "x" * 32


def control(url, action, **fields):
    with httpx.Client(trust_env=False, timeout=20) as client:
        response = client.post(url + "/native-research", json={"action": action, **fields})
    assert response.status_code == 200, response.text[-2000:]
    return response.json()


def connect(url, token):
    client = ResearchClient.connect(url, token)
    client.transport.http.headers["x-fixture-native-source"] = "true"
    return client


class Endpoint:
    """Fault timing around actual NativeWitnessSource.observe, never synthetic time."""

    def __init__(self, authority, descriptor):
        self.authority, self.descriptor = authority, descriptor
        self.mode = "normal"
        self.requests = []
        self.responses = []
        self.arrived, self.release, self.completed = (threading.Event() for _ in range(3))
        self.restart()

    def restart(self):
        self.native = NativeWitnessSource(
            self.authority, self.descriptor.id, SEED, policy=self.descriptor.policy
        )
        assert self.native.source == self.descriptor

    def observe(self, request):
        self.requests.append(request)
        if self.mode == "unavailable":
            raise RuntimeError("Fixture source unavailable")
        if self.mode == "old":
            return self.responses[0]
        result = self.native.observe(request)
        self.responses.append(result)
        if self.mode == "hold":
            self.arrived.set()
            assert self.release.wait(10), "Owned native response was never released"
            self.completed.set()
        return result


def test_native_source_admission_outage_restart_and_durable_fence_in_workerd(tmp_path):
    build_current_sources()
    descriptor = source().model_copy(update={"native_profile": native_clock().profile})
    with ExitStack() as lifetime:
        with server(tmp_path, sequence=1) as url:
            config = control(url, "describe", source=descriptor.model_dump(mode="json"))
            endpoint = Endpoint(
                WitnessAuthorityScope.model_validate(config["authority"]), descriptor
            )
            origin = lifetime.enter_context(
                serving(create_time_witness_app(endpoint, witness_consumer_digest(TOKEN)))
            )
            setup = control(
                url, "provision", source=descriptor.model_dump(mode="json"), origin=origin
            )
            assert "error" not in setup, setup
            assert len(endpoint.requests) == 1
            assert control(url, "inspect")["provider_state"] == "qualified"
            admission = AuthorityClockAdmission(policy_digest=descriptor.policy.digest)
            enrollment = {
                **setup["enrollment"],
                "clock_admission": admission.model_dump(mode="json"),
            }
            with connect(url, setup["operator_token"]) as operator:
                enrolled = operator.command("worker.enroll", enrollment, idempotency_key="enroll")
                worker = enrolled["result"]["data"]
                authority = AssignmentAuthority.model_validate(worker["assignment_authority"])
                sampler = ClockSampler(authority)
                admitted = ClockSampler(authority, clock_admission=admission)
                with connect(url, setup["worker_token"]) as client:
                    waiting = sampler.begin()
                    reply = client.worker_clock(waiting.challenge)
                    verified = ClockVerifier(authority).reply(reply, waiting.challenge)
                    first_epoch = verified.reply.payload.clock_epoch
                    assert reply.payload.policy == descriptor.policy
                    assert reply.payload.clock_profile == "hosted-native-witness-v1"
                    with pytest.raises(ResearchError) as not_admitted:
                        waiting.complete(reply)
                    assert not_admitted.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
                    allowed = admitted.begin()
                    estimate = allowed.complete(client.worker_clock(allowed.challenge))
                    assert estimate.clock_admission == admission

                    before_auth = len(endpoint.requests)
                    with connect(url, "invalid") as invalid:
                        with pytest.raises(ResearchError) as denied:
                            invalid.worker_clock(sampler.begin().challenge)
                        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
                    assert len(endpoint.requests) == before_auth

                    cache = {"binding": worker["binding"], "snapshot_id": new_id()}
                    before = control(url, "inspect")
                    endpoint.mode = "unavailable"
                    with pytest.raises(ResearchError) as unavailable:
                        client.command(
                            "worker.cache_advertise",
                            cache,
                            idempotency_key="outage",
                            expected_revision=0,
                        )
                    assert unavailable.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
                    assert control(url, "inspect") == before
                    calls = len(endpoint.requests)
                    assert (
                        operator.command("worker.enroll", enrollment, idempotency_key="enroll")
                        == enrolled
                    )
                    assert len(endpoint.requests) == calls
                    endpoint.mode = "normal"
                    assert (
                        client.command(
                            "worker.cache_advertise",
                            cache,
                            idempotency_key="outage",
                            expected_revision=0,
                        )["result"]["revision"]
                        == 1
                    )

                    before = control(url, "inspect")
                    late_cache = {"binding": worker["binding"], "snapshot_id": new_id()}
                    endpoint.mode = "hold"
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        late = pool.submit(
                            client.command,
                            "worker.cache_advertise",
                            late_cache,
                            idempotency_key="late",
                            expected_revision=1,
                        )
                        try:
                            assert endpoint.arrived.wait(5)
                            old_nonce = endpoint.requests[-1].nonce
                            with pytest.raises(ResearchError) as expired:
                                late.result(timeout=5)
                            assert expired.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
                            assert control(url, "inspect") == before
                        finally:
                            endpoint.release.set()
                        assert endpoint.completed.wait(5)
                    endpoint.mode = "normal"
                    assert control(url, "inspect") == before
                    cached = client.command(
                        "worker.cache_advertise",
                        late_cache,
                        idempotency_key="late",
                        expected_revision=1,
                    )
                    assert cached["result"]["revision"] == 2
                    assert endpoint.requests[-1].nonce != old_nonce
                    before_restart = control(url, "inspect")

        with server(tmp_path, sequence=2) as url:
            assert control(url, "inspect") == before_restart
            with connect(url, setup["worker_token"]) as client:
                waiting = sampler.begin()
                assert client.worker_clock(waiting.challenge).payload.clock_epoch == first_epoch
                endpoint.restart()
                with pytest.raises(ResearchError) as changed:
                    client.worker_clock(sampler.begin().challenge)
                assert changed.value.code == ErrorCode.STALE_FENCE
                fenced = control(url, "inspect")
                assert fenced["provider_state"] == "unqualified"
                assert fenced["retirements"] == 1
                assert fenced["commands"] == before_restart["commands"]

        with server(tmp_path, sequence=3) as url:
            assert control(url, "inspect") == fenced
            with connect(url, setup["worker_token"]) as client:
                endpoint.mode = "old"
                with pytest.raises(ResearchError):
                    client.worker_clock(sampler.begin().challenge)
                assert control(url, "inspect") == fenced
                calls = len(endpoint.requests)
                assert (
                    client.command(
                        "worker.cache_advertise",
                        late_cache,
                        idempotency_key="late",
                        expected_revision=1,
                    )
                    == cached
                )
                assert len(endpoint.requests) == calls
                endpoint.mode = "normal"
                with pytest.raises(ResearchError) as unqualified:
                    client.worker_clock(sampler.begin().challenge)
                assert unqualified.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
                qualified = control(url, "requalify")
                assert qualified["provider_state"] == "qualified"
                assert qualified["retirements"] == 1
                waiting = sampler.begin()
                reply = client.worker_clock(waiting.challenge)
                assert reply.payload.clock_epoch != first_epoch
                ClockVerifier(authority).reply(reply, waiting.challenge)
                with pytest.raises(ResearchError) as still_not_admitted:
                    waiting.complete(reply)
                assert still_not_admitted.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
                allowed = admitted.begin()
                estimate = allowed.complete(client.worker_clock(allowed.challenge))
                assert estimate.verified.reply.payload.clock_epoch == reply.payload.clock_epoch
