from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from research_swarm.control.commands import SubmitTrial
from research_swarm.control.models import SearchConfiguration
from research_swarm.control.queries import ResearchQueries
from research_swarm.control.service import ResearchService
from research_swarm.control.workers import ClaimJob, EngineCapabilities, EnrollWorker, WorkerData
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.experiments import DispatchEnvelope, ExperimentSpec
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.types import new_id
from research_swarm.workers.authority import DispatchVerifier
from tests.support import ResearchFixture

KEY = b"synthetic-worker-control-key-32-bytes"


def enroll(research: ResearchFixture, *, concurrency: int = 1) -> WorkerData:
    research.service = ResearchService(
        research.service.database, research.service.artifacts, dispatch_key=KEY
    )
    request = EnrollWorker(
        worker_id=new_id(),
        session_id=new_id(),
        principal_id=research.context().principal.id,
        name="Synthetic CPU worker",
        max_concurrency=concurrency,
        profiles=(
            EngineCapabilities(
                name="python-cpu",
                backend="python",
                cpu_slots=concurrency,
                execution_fingerprint=content_digest(b"synthetic-profile"),
            ),
        ),
    )
    command = research.context(key="enroll-" + request.worker_id)
    result = research.service.enroll_worker(command, request)
    assert research.service.enroll_worker(command, request) == result
    return WorkerData.model_validate(result["result"]["data"])


def trial(research: ResearchFixture, campaign: dict[str, Any]) -> dict[str, Any]:
    candidate, validated = research.candidate(campaign)
    return research.service.submit_trial(
        research.context(revision=campaign["revision"]),
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=candidate["id"],
            validation_id=validated["id"],
            seed=7,
        ),
    )["result"]


def ready_job(research: ResearchFixture, trial: dict[str, Any]) -> dict[str, Any]:
    jobs = [
        research.service.database.read_record(research.context(), "job", identifier)
        for identifier in trial["data"]["job_ids"]
    ]
    return next(job for job in jobs if job["state"] == "ready")


def request(worker: WorkerData, job: dict[str, Any]) -> ClaimJob:
    return ClaimJob(
        worker_id=worker.binding.worker_id,
        session_id=worker.binding.worker_session_id,
        job_id=job["id"],
    )


def test_claim_replay_has_one_attempt_and_reservation(research: ResearchFixture) -> None:
    worker = enroll(research)
    campaign = research.campaign()
    submitted = trial(research, campaign)
    job = ready_job(research, submitted)
    command = research.context(key="claim-once", revision=job["revision"])
    intent = request(worker, job)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: research.service.claim_job(command, intent), range(4)))
    assert all(result == results[0] for result in results)
    result = results[0]["result"]
    envelope = DispatchEnvelope.model_validate(result["data"]["dispatch"])
    verified = DispatchVerifier(worker.binding, KEY).verify(
        envelope, ExperimentSpec.model_validate(submitted["data"]["spec"])
    )
    assert verified.envelope.attempt_id == result["id"]
    accounts = ResearchQueries(research.service.database).budget(research.context(), campaign["id"])
    reserved = {item["dimension"]: item["reserved"] for item in accounts["accounts"]}
    assert reserved == {"attempts": "1", "execution_ms": "15000"}
    with research.service.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM budget_reservations").fetchone()[0] == 1
        assert (
            connection.execute("SELECT owner_id FROM budget_reservations").fetchone()[0]
            == result["id"]
        )


def test_two_workers_cannot_claim_one_job(research: ResearchFixture) -> None:
    workers = [enroll(research), enroll(research)]
    campaign = research.campaign()
    job = ready_job(research, trial(research, campaign))

    def claim(worker: WorkerData) -> str:
        try:
            return research.service.claim_job(
                research.context(revision=job["revision"]), request(worker, job)
            )["result"]["id"]
        except ResearchError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(claim, workers))
    assert results.count(ErrorCode.REVISION_CONFLICT) == 1
    with research.service.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM budget_reservations").fetchone()[0] == 1


def test_worker_authority_and_dependency_are_checked_before_reservation(
    research: ResearchFixture,
) -> None:
    worker = enroll(research)
    campaign = research.campaign()
    submitted = trial(research, campaign)
    job = ready_job(research, submitted)
    intent = request(worker, job)
    stranger = Principal(
        id=new_id(), workspace_id=worker.binding.workspace_id, scopes=frozenset({"worker.claim"})
    )
    with pytest.raises(ResearchError) as denied:
        research.service.claim_job(
            CommandContext(principal=stranger, idempotency_key="foreign", expected_revision=1),
            intent,
        )
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    with pytest.raises(ResearchError) as stale:
        research.service.claim_job(
            research.context(revision=1), intent.model_copy(update={"session_id": new_id()})
        )
    assert stale.value.code == ErrorCode.STALE_FENCE
    blocked_id = next(
        identifier for identifier in submitted["data"]["job_ids"] if identifier != job["id"]
    )
    with pytest.raises(ResearchError, match="not ready"):
        research.service.claim_job(
            research.context(revision=1), intent.model_copy(update={"job_id": blocked_id})
        )
    with research.service.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM budget_reservations").fetchone()[0] == 0


@pytest.mark.parametrize("parallel,concurrency", [(1, 2), (2, 1)])
def test_campaign_and_worker_concurrency_preserve_budget(
    research: ResearchFixture, parallel: int, concurrency: int
) -> None:
    worker = enroll(research, concurrency=concurrency)
    campaign = research.campaign(search=SearchConfiguration(max_parallel_trials=parallel))
    first, second = [ready_job(research, trial(research, campaign)) for _ in range(2)]
    research.service.claim_job(research.context(revision=1), request(worker, first))
    with pytest.raises(ResearchError) as occupied:
        research.service.claim_job(research.context(revision=1), request(worker, second))
    assert occupied.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    with research.service.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM budget_reservations").fetchone()[0] == 1


def test_incompatible_profile_is_not_silently_downgraded(research: ResearchFixture) -> None:
    worker = enroll(research)
    campaign = research.campaign()
    job = ready_job(research, trial(research, campaign))
    # A declared network denial must be satisfied; trusted native execution cannot claim it.
    with pytest.raises(ResearchError) as unsupported:
        worker.profiles[0].require(
            ExperimentSpec.model_validate(trial(research, campaign)["data"]["spec"])
            .stages[0]
            .resources,
            "denied",
        )
    assert unsupported.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    research.service.claim_job(research.context(revision=job["revision"]), request(worker, job))
