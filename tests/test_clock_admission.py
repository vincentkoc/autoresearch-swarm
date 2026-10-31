"""Explicit hosted authority admission, separate from native clock implementation."""

import json
import subprocess
import sys
from dataclasses import replace

import pytest

from research_swarm.control.workers import EnrollWorker, WorkerData
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.clocks import AuthorityClockAdmission, ClockPolicy
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.workers.clock_estimates import NS_PER_MS
from research_swarm.workers.clock_signatures import ClockVerifier
from research_swarm.workers.clocks import NATIVE_CLOCK_PROFILES
from research_swarm.workers.remote_enrollment import RemoteWorkerBundle, describe_worker
from research_swarm.workers.timed_launch import capture_timing, verify_timing
from tests.test_authority_clock import timed_worker
from tests.test_clock_estimates import advance, fixture
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_remote_enrollment import provision
from tests.test_worker_queries import claim

PROFILE = "hosted-native-witness-v1"


def hosted(*, roundtrip_ms=100):
    estimate, grant, signer, challenge = fixture(roundtrip_ms=roundtrip_ms)
    reply = signer.reply(
        estimate.verified.reply.payload.model_copy(update={"clock_profile": PROFILE})
    )
    admission = AuthorityClockAdmission(policy_digest=reply.payload.policy.digest)
    verified = ClockVerifier(signer.authority).reply(reply, challenge)
    return replace(estimate, verified=verified, clock_admission=admission), grant, signer


def test_hosted_authority_requires_exact_admission_without_native_fallback():
    estimate, _, _ = hosted()
    assert PROFILE not in NATIVE_CLOCK_PROFILES
    for changed in (
        {"clock_admission": None},
        {"clock_admission": AuthorityClockAdmission(policy_digest="sha256:" + "0" * 64)},
        {"origin_profile": PROFILE},
    ):
        with pytest.raises(ResearchError) as denied:
            replace(estimate, **changed)
        assert denied.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    native, _, _, _ = fixture()
    with pytest.raises(ResearchError):
        replace(native, clock_admission=estimate.clock_admission)
    with pytest.raises(ValueError):
        AuthorityClockAdmission(profile="linux-boottime-v1", policy_digest=ClockPolicy().digest)
    assert canonical_bytes(
        AuthorityClockAdmission(policy_digest="sha256:" + "0" * 64).model_dump(mode="json")
    ) == (
        b'{"policy_digest":"sha256:'
        + b"0" * 64
        + b'","profile":"hosted-native-witness-v1","schema_version":"1.0"}'
    )


def test_entire_roundtrip_is_charged_and_historical_evidence_does_not_admit_itself():
    estimate, grant, signer = hosted()
    slower, _, _ = hosted(roundtrip_ms=900)
    first = estimate.deadlines(grant, estimate.received)
    second = slower.deadlines(grant, slower.received)
    assert (
        second.hard_ns - slower.received.continuous_ns
        < first.hard_ns - estimate.received.continuous_ns
    )
    assert slower.interval(slower.received).upper_ns > estimate.interval(estimate.received).upper_ns
    with pytest.raises(ResearchError):
        replace(estimate, received=advance(estimate.sent, 1001 * NS_PER_MS))
    evidence = capture_timing(grant, estimate, estimate.received).evidence
    assert b"clock_admission" not in canonical_bytes(evidence.model_dump(mode="json"))
    with pytest.raises(ResearchError):
        verify_timing(evidence, signer.authority, grant.grant, grant.assignment.assignment)
    verified = verify_timing(
        evidence,
        signer.authority,
        grant.grant,
        grant.assignment.assignment,
        clock_admission=estimate.clock_admission,
    )
    assert verified.evidence == evidence and verified.estimate == estimate
    with pytest.raises(ResearchError):
        verify_timing(
            evidence,
            signer.authority,
            grant.grant,
            grant.assignment.assignment,
            clock_admission=AuthorityClockAdmission(policy_digest="sha256:" + "f" * 64),
        )
    different_epoch = signer.bind(grant.grant.payload.model_copy(update={"clock_epoch": new_id()}))
    with pytest.raises(ResearchError):
        verify_timing(
            evidence,
            signer.authority,
            different_epoch,
            grant.assignment.assignment,
            clock_admission=estimate.clock_admission,
        )
    enlarged = evidence.model_copy(
        update={
            "caps": evidence.caps.model_copy(
                update={"hard_ns": str(int(evidence.caps.hard_ns) + 1)}
            )
        }
    )
    with pytest.raises(ResearchError):
        verify_timing(
            enlarged,
            signer.authority,
            grant.grant,
            grant.assignment.assignment,
            clock_admission=estimate.clock_admission,
        )


def test_description_admission_is_persisted_before_provisioning_and_cannot_drift(tmp_path):
    admission = AuthorityClockAdmission(policy_digest=ClockPolicy().digest)
    root, registry = tmp_path / "worker", tmp_path / "machine" / "resources.sqlite"
    description = describe_worker(root, machine_registry=registry, clock_admission=admission)
    assert (
        describe_worker(root, machine_registry=registry, clock_admission=admission) == description
    )
    with pytest.raises(ResearchError) as changed:
        describe_worker(root, machine_registry=registry)
    assert changed.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    legacy = describe_worker(tmp_path / "legacy", machine_registry=registry)
    assert "clock_admission" not in legacy.model_dump(mode="json")
    assert description.clock_admission == admission
    config = tmp_path / "clock-admission.json"
    config.write_bytes(canonical_bytes(admission.model_dump(mode="json")))
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "research_swarm.cli",
            "worker",
            "describe",
            "--worker-dir",
            str(root),
            "--machine-registry",
            str(registry),
            "--clock-admission",
            str(config),
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == description.model_dump(mode="json")


def install_hosted_provider(pipeline, monkeypatch):
    """Synthetic profile admission test; actual native source has a workerd probe."""
    control = pipeline.runtime.service.workers
    original = control._authority_time_provider

    class Provider:
        profile = PROFILE
        policy = ClockPolicy()
        authority_identity = original.authority_identity

        def observe(self, scope):
            return replace(original.observe(scope), profile=self.profile, policy=self.policy)

    provider = Provider()
    monkeypatch.setattr(control, "_authority_time_provider", provider)
    return provider


def test_controller_requires_admission_before_claim_and_preserves_original_session(
    pipeline, tmp_path, monkeypatch
):
    provider = install_hosted_provider(pipeline, monkeypatch)
    client, unadmitted, _ = timed_worker(pipeline)
    with pytest.raises(ResearchError) as refused:
        claim(client, unadmitted, pipeline.ready())
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert pipeline.ready()["state"] == "ready"
    assert "clock_admission" not in unadmitted.model_dump(mode="json")
    admission = AuthorityClockAdmission(policy_digest=provider.policy.digest)
    description = describe_worker(
        tmp_path / "remote",
        machine_registry=tmp_path / "machine" / "resources.sqlite",
        clock_admission=admission,
    )
    bundle = provision(pipeline, description, tmp_path, "https://controller.example")
    assert bundle.worker.clock_admission == bundle.description.clock_admission == admission
    archived = pipeline.client.get("worker_session", bundle.worker.binding.worker_session_id)
    assert WorkerData.model_validate(archived["data"]).clock_admission == admission
    with pytest.raises(ValueError):
        RemoteWorkerBundle.model_validate(
            bundle.model_copy(
                update={"description": description.model_copy(update={"clock_admission": None})}
            )
        )
    # Permission also reaches local execution and is never inferred from a reply.
    from research_swarm.client import LocalTransport, ResearchClient
    from research_swarm.workers.local import LocalWorker

    worker_client = ResearchClient(
        LocalTransport(pipeline.runtime, lambda: pipeline.runtime.credentials.resolve(bundle.token))
    )
    local = LocalWorker(
        worker_client,
        bundle.worker,
        root=tmp_path / "execution",
        machine_registry=tmp_path / "machine" / "resources.sqlite",
    )
    assert local.clock_sampler.clock_admission == admission
    waiting = local.clock_sampler.begin()
    estimate = waiting.complete(worker_client.worker_clock(waiting.challenge))
    assert estimate.clock_admission == admission
    row = claim(worker_client, bundle.worker, pipeline.ready())
    assert row["data"]["assignment_time"]["payload"]["policy_digest"] == admission.policy_digest
    completed = local.execute(row["id"])
    entry = local.journal.get(row["id"])
    assert completed["state"] == "succeeded", entry.finding
    assert entry.timing is not None and entry.outcome.closure.scope_closed
    from research_swarm.workers.recovery import _authenticate

    _authenticate(entry, bundle)
    without_admission = bundle.model_copy(
        update={
            "worker": bundle.worker.model_copy(update={"clock_admission": None}),
            "description": bundle.description.model_copy(update={"clock_admission": None}),
        }
    )
    with pytest.raises(ResearchError):
        _authenticate(entry, without_admission)
    provider.policy = provider.policy.model_copy(update={"relative_drift_ppm": 1001})
    with pytest.raises(ResearchError):
        worker_client.worker_clock(local.clock_sampler.begin().challenge)
    provider.policy = ClockPolicy()
    provider.profile = "linux-boottime-v1"
    with pytest.raises(ResearchError):
        worker_client.worker_clock(local.clock_sampler.begin().challenge)


def test_controller_rejects_wrong_policy_enrollment_and_utc_admission(pipeline, monkeypatch):
    install_hosted_provider(pipeline, monkeypatch)
    request = EnrollWorker(
        worker_id=new_id(),
        session_id=new_id(),
        principal_id=pipeline.runtime.principal().id,
        name="Synthetic admission probe",
        profiles=pipeline.worker.worker.profiles,
        clock_mode="bounded-continuous-v1",
        clock_admission=AuthorityClockAdmission(policy_digest="sha256:" + "f" * 64),
    )
    with pytest.raises(ResearchError) as denied:
        pipeline.client.command("worker.enroll", request, idempotency_key=new_id())
    assert denied.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    with pytest.raises(ResearchError) as missing:
        pipeline.client.get("worker", request.worker_id)
    assert missing.value.code == ErrorCode.NOT_FOUND
    with pytest.raises(ValueError):
        EnrollWorker.model_validate(request.model_copy(update={"clock_mode": "local-utc-v1"}))
