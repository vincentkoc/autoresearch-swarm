from __future__ import annotations

import asyncio
import threading

import pytest

from research_swarm.api.http import create_app
from research_swarm.control.models import SearchConfiguration
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.orchestration.cache_reporting import WorkerCacheReporter
from research_swarm.orchestration.scoped import ScopedWorkerDriver
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_http_delivery import serving
from tests.orchestration.test_local_driver import driver
from tests.workers.test_offer_locality import variant
from tests.workers.test_scoped_driver import enrollment
from tests.workers.test_worker_locality import advertisement
from tests.workers.test_worker_queries import read


def warm(pipeline, worker, reference, destination):
    worker.input_cache.materialize(
        reference,
        destination,
        authorize=lambda: pipeline.client.artifact(reference.id).reference,
        fetch=lambda path: pipeline.client.download(reference.id, path),
    )


def test_reporter_does_not_create_empty_cache_or_observation(pipeline: Pipeline):
    reporter = WorkerCacheReporter(pipeline.worker)
    reporter._refresh()
    assert reporter.last_error is None
    assert not pipeline.worker.input_cache.root.exists()
    worker = pipeline.worker.worker
    current = pipeline.worker.client.worker_get(
        read(worker, "worker_cache", worker.binding.worker_session_id)
    )
    assert current["revision"] == 0


@pytest.mark.parametrize("failure", ["delivery", "io", "validation"])
def test_reporter_replays_uncertain_observation_before_verifying_new_bytes(
    pipeline: Pipeline, tmp_path, monkeypatch, failure
):
    request = advertisement(pipeline)
    warm(pipeline, pipeline.worker, request.references[0], tmp_path / "warm.zip")
    reporter = WorkerCacheReporter(pipeline.worker)
    command = pipeline.worker.client.command
    snapshot = pipeline.worker.input_cache.verified_snapshot
    observed, inspected = [], []

    def lose_reply(operation, payload, **kwargs):
        result = command(operation, payload, **kwargs)
        observed.append((payload, kwargs, result))
        if len(observed) == 1:
            if failure == "io":
                raise OSError("Synthetic local write acknowledgment failure")
            if failure == "validation":
                raise ValueError("Synthetic malformed local acknowledgment")
            raise ResearchError(ErrorCode.DELIVERY_UNCERTAIN, "Synthetic lost acknowledgment")
        return result

    def inspect():
        inspected.append(True)
        return snapshot()

    monkeypatch.setattr(pipeline.worker.client, "command", lose_reply)
    monkeypatch.setattr(pipeline.worker.input_cache, "verified_snapshot", inspect)
    reporter._refresh()
    assert reporter.last_error == ErrorCode.DELIVERY_UNCERTAIN
    reporter._refresh()
    assert reporter.last_error is None and len(inspected) == 1
    assert observed[0] == observed[1]
    reporter._refresh()
    assert len(inspected) == 2
    assert observed[2][2]["result"]["revision"] == 2
    assert observed[2][0].snapshot_id != observed[0][0].snapshot_id


async def test_slow_cache_inspection_does_not_block_event_loop_and_cancellation_drains_it(
    pipeline: Pipeline, monkeypatch
):
    entered, release = threading.Event(), threading.Event()
    snapshot = pipeline.worker.input_cache.verified_snapshot

    def inspect():
        entered.set()
        assert release.wait(5)
        return snapshot()

    monkeypatch.setattr(pipeline.worker.input_cache, "verified_snapshot", inspect)
    reporter = WorkerCacheReporter(pipeline.worker)
    task = asyncio.create_task(reporter.refresh())
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert reporter._pending is None


async def test_optional_cache_query_failure_does_not_block_local_execution(
    pipeline: Pipeline, monkeypatch
):
    get = pipeline.worker.client.worker_get

    def absent(request):
        if request.kind == "worker_cache":
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Synthetic optional endpoint")
        return get(request)

    monkeypatch.setattr(pipeline.worker.client, "worker_get", absent)
    run = driver(pipeline)
    result = await run.drain(pipeline.campaign["id"])
    assert result.state == "idle" and result.executed_attempt_count == 2
    assert not result.findings
    assert run._cache_reporter.last_error == ErrorCode.CAPABILITY_UNAVAILABLE


async def test_optional_cache_io_failure_does_not_block_local_execution(
    pipeline: Pipeline, monkeypatch
):
    def unavailable():
        raise OSError("Synthetic optional cache inspection failure")

    monkeypatch.setattr(pipeline.worker.input_cache, "verified_snapshot", unavailable)
    run = driver(pipeline)
    result = await run.drain(pipeline.campaign["id"])
    assert result.state == "idle" and result.executed_attempt_count == 2
    assert run._cache_reporter.last_error == ErrorCode.CAPABILITY_UNAVAILABLE


async def test_stop_during_cache_inspection_prevents_fresh_claims(pipeline: Pipeline, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    snapshot = pipeline.worker.input_cache.verified_snapshot

    def inspect():
        entered.set()
        assert release.wait(5)
        return snapshot()

    monkeypatch.setattr(pipeline.worker.input_cache, "verified_snapshot", inspect)
    run = driver(pipeline)
    task = asyncio.create_task(run.drain(pipeline.campaign["id"]))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        run.stop.set()
    finally:
        release.set()
    result = await task
    assert result.state == "stopped" and result.executed_attempt_count == 0
    assert pipeline.client.list(RecordQuery(kind="attempt"))["items"] == []


async def test_local_driver_claims_cached_candidate_first_and_authorizes_its_evaluation(
    pipeline: Pipeline, tmp_path, monkeypatch
):
    later, source = variant(pipeline, tmp_path, 1)
    warm(pipeline, pipeline.worker, source, tmp_path / "warm.zip")
    run = driver(pipeline)
    original = run._claim
    claimed = []

    def claim(summary, intents):
        result = original(summary, intents)
        if result is not None:
            claimed.append(result["data"]["dispatch"]["trial_id"])
        return result

    monkeypatch.setattr(run, "_claim", claim)
    result = await run.drain(pipeline.campaign["id"])
    assert result.state == "idle" and result.executed_attempt_count == 4
    assert claimed[0] == later["id"]
    for identifier in (pipeline.trial["id"], later["id"]):
        assert pipeline.client.get("trial", identifier)["data"]["evaluation_status"] == "valid"


@pytest.mark.parametrize(
    "pipeline",
    [{"search": SearchConfiguration(max_parallel_trials=limit)} for limit in (1, 2)],
    indirect=True,
)
async def test_scoped_http_driver_reports_cache_and_claims_cached_candidate_first(
    pipeline: Pipeline, tmp_path, monkeypatch
):
    later, source = variant(pipeline, tmp_path, 1)
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        enrollment(pipeline, tmp_path / "remote", url) as connected,
    ):
        warm(pipeline, connected.worker, source, tmp_path / "warm.zip")
        run = ScopedWorkerDriver(connected)
        original = run._claim
        claimed = []

        def claim(offer, intents):
            result = original(offer, intents)
            claimed.append(result["data"]["dispatch"]["trial_id"])
            return result

        monkeypatch.setattr(run, "_claim", claim)
        result = await run.run(once=True)
        limit = pipeline.campaign["data"]["contract"]["search"]["max_parallel_trials"]
        assert result.observation_count == limit, result.findings
        assert claimed == [later["id"], pipeline.trial["id"]][:limit]
        if limit == 1:
            assert result.state == "blocked"
            assert result.findings and all(
                finding.startswith("RESOURCE_UNAVAILABLE:") for finding in result.findings
            )
        else:
            assert result.state == "idle" and not result.findings
        cache = pipeline.client.get(
            "worker_cache", connected.worker.worker.binding.worker_session_id
        )
        assert cache["data"]["observation"]["advertisement"]["references"] == [
            source.model_dump(mode="json")
        ]
        # The scoped driver still leaves evaluator authorization to its controller.
        assert all(
            pipeline.client.get("trial", identifier)["data"]["evaluation_status"] == "pending"
            for identifier in claimed
        )
