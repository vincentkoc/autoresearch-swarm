"""Controller time is injected; calibrated decisions stay inside SQL authority."""

from dataclasses import replace

import pytest

from research_swarm.control.authority_clock import AuthorityClock
from research_swarm.control.time_authority import ClockAuthorityScope
from research_swarm.control.worker_artifacts import WorkerArtifacts
from research_swarm.control.workers import AttemptData, ClaimJob, WorkerControl
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.state.database import Database, Transaction
from research_swarm.state.sql import StoreIdentity
from tests.test_authority_clock import challenge, input_request, timed_worker
from tests.test_authority_clock import simulated as simulated
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_portable_preflights import CallbackGuard


class ObservedProvider:
    def __init__(self, store, clock):
        self.authority_identity = store.identity
        self.store, self.clock = store, clock
        self.scopes = []

    def observe(self, scope):
        assert self.store.active, "Time was sampled outside the protected state decision"
        self.scopes.append(scope)
        return self.clock.observe()


def command(principal, revision=None):
    return CommandContext(principal=principal, idempotency_key=new_id(), expected_revision=revision)


def controls(pipeline, provider, portable):
    original = pipeline.runtime.service.workers
    return WorkerControl(
        portable, original.dispatch_key, original._assignment_key, authority_time_provider=provider
    )


def test_calibrated_probe_claim_inputs_and_acceptance_need_no_database_path(pipeline, simulated):
    _, worker, token = timed_worker(pipeline)
    principal = pipeline.runtime.credentials.resolve(token)
    store = CallbackGuard(pipeline.runtime.database.store)
    portable = Database.from_store(store)
    provider = ObservedProvider(store, AuthorityClock(simulated))
    control = controls(pipeline, provider, portable)
    probe = control.clock(command(principal), challenge(worker))
    job = pipeline.ready()
    request = ClaimJob(
        worker_id=worker.binding.worker_id,
        session_id=worker.binding.worker_session_id,
        job_id=job["id"],
    )
    row = control.claim(command(principal, job["revision"]), request)["result"]
    attempt = AttemptData.model_validate(row["data"])
    assert attempt.assignment_time.payload.clock_epoch == probe.payload.clock_epoch
    artifacts = WorkerArtifacts(portable, pipeline.runtime.artifacts, control)
    reference = artifacts.input_metadata(command(principal), input_request(worker, row))
    assert reference.id == row["data"]["source_artifact"]["id"]
    store.read(
        lambda sql: control.acceptance.require(
            Transaction(sql, command(principal)), attempt, worker
        )
    )
    scope = ClockAuthorityScope(
        worker.binding.workspace_id,
        worker.binding.controller_id,
        worker.binding.control_epoch,
        worker.assignment_authority.key_id,
    )
    assert len(provider.scopes) >= 4 and all(observed == scope for observed in provider.scopes)
    simulated.reading = replace(
        simulated.reading, wall_ns=simulated.reading.wall_ns + 2_000_000_000
    )
    with pytest.raises(ResearchError) as poisoned:
        artifacts.input_metadata(command(principal), input_request(worker, row))
    assert poisoned.value.code == ErrorCode.STALE_FENCE
    with pytest.raises(ResearchError) as unqualified:
        store.read(
            lambda sql: control.acceptance.require(
                Transaction(sql, command(principal)), attempt, worker
            )
        )
    assert unqualified.value.code == ErrorCode.ACCEPTANCE_EXPIRED


def test_provider_identity_signer_and_absence_fail_without_fallback(pipeline, simulated):
    _, worker, token = timed_worker(pipeline)
    principal = pipeline.runtime.credentials.resolve(token)
    store = CallbackGuard(pipeline.runtime.database.store)
    portable = Database.from_store(store)
    provider = ObservedProvider(store, AuthorityClock(simulated))
    provider.authority_identity = StoreIdentity("different-authority", "fixture")
    with pytest.raises(ValueError, match="state authority"):
        controls(pipeline, provider, portable)
    provider.authority_identity = store.identity
    absent = controls(pipeline, None, portable)
    with pytest.raises(ResearchError) as unavailable:
        absent.clock(command(principal), challenge(worker))
    assert unavailable.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    wrong_key = WorkerControl(portable, b"d" * 32, b"s" * 32, authority_time_provider=provider)
    with pytest.raises(ResearchError) as changed:
        wrong_key.clock(command(principal), challenge(worker))
    assert changed.value.code == ErrorCode.STALE_FENCE
    assert provider.scopes == []


def test_local_utc_claim_does_not_require_a_calibrated_provider(pipeline):
    worker = pipeline.worker.worker
    assert worker.clock_mode == "local-utc-v1"
    portable = Database.from_store(CallbackGuard(pipeline.runtime.database.store))
    control = controls(pipeline, None, portable)
    principal = Principal(
        id=worker.principal_id, workspace_id=worker.binding.workspace_id, scopes={"worker.claim"}
    )
    job = pipeline.ready()
    row = control.claim(
        command(principal, job["revision"]),
        ClaimJob(
            worker_id=worker.binding.worker_id,
            session_id=worker.binding.worker_session_id,
            job_id=job["id"],
        ),
    )["result"]
    assert row["data"]["assignment_time"] is None
