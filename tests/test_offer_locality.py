from __future__ import annotations

import shutil
from contextlib import ExitStack
from dataclasses import replace

import pytest

from research_swarm import ResearchClient
from research_swarm.api.http import create_app
from research_swarm.control.commands import (
    PauseCampaign,
    ProposeCandidate,
    SubmitTrial,
    ValidateCandidate,
)
from research_swarm.control.worker_locality import AdvertiseWorkerCache
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import new_id
from tests.test_evaluation_control import prepare_evaluation
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_worker_locality import publish
from tests.test_worker_queries import claim, query, scoped_worker


def variant(pipeline, tmp_path, ordinal):
    path = tmp_path / f"candidate-{ordinal}"
    shutil.copytree(pipeline.source, path)
    training = path / "train.py"
    training.write_text(training.read_text() + f"\n# Locality fixture {ordinal}\n")
    source = pipeline.client.capture_project(path, idempotency_key=new_id())
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    candidate = pipeline.client.command(
        "candidate.propose",
        ProposeCandidate(
            campaign_id=campaign["id"], source_artifact=source, hypothesis="Cache placement fixture"
        ),
        idempotency_key=new_id(),
        expected_revision=campaign["revision"],
    )["result"]
    validation = pipeline.client.command(
        "candidate.validate",
        ValidateCandidate(candidate_id=candidate["id"]),
        idempotency_key=new_id(),
        expected_revision=candidate["revision"],
    )["result"]
    campaign = pipeline.client.get("campaign", campaign["id"])
    trial = pipeline.client.command(
        "trial.submit",
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=candidate["id"],
            validation_id=validation["id"],
            seed=100 + ordinal,
        ),
        idempotency_key=new_id(),
        expected_revision=campaign["revision"],
    )["result"]
    return trial, source


def report(pipeline, *references, revision=0):
    return publish(
        pipeline.worker.client,
        AdvertiseWorkerCache(
            binding=pipeline.worker.worker.binding,
            snapshot_id=new_id(),
            references=references,
            examined_bytes=sum(reference.size_bytes for reference in references),
        ),
        revision=revision,
    )


def trial_order(page):
    return [row["data"]["trial_id"] for row in page["items"]]


@pytest.mark.parametrize("remote", [False, True])
def test_locality_reorders_only_the_eligible_page_and_preserves_fifo_cursor(
    pipeline: Pipeline, tmp_path, monkeypatch, remote
):
    middle, source = variant(pipeline, tmp_path, 1)
    last, _ = variant(pipeline, tmp_path, 2)
    worker = pipeline.worker.worker
    control = pipeline.runtime.service.workers
    now = control.observation_time(worker)
    monkeypatch.setattr(control, "observation_time", lambda worker: now)
    baseline = [pipeline.trial["id"], middle["id"], last["id"]]
    before = pipeline.client.budget(pipeline.campaign["id"])
    with ExitStack() as stack:
        if remote:
            actor = pipeline.worker.client.transport.resolve_principal()
            token = pipeline.runtime.credentials.issue(pipeline.runtime.principal(), actor).token
            url = stack.enter_context(
                serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials))
            )
            client = stack.enter_context(ResearchClient.connect(url, token))
        else:
            client = pipeline.worker.client
        assert trial_order(client.worker_list(query(worker))) == baseline
        report(pipeline, source)
        ranked = [middle["id"], pipeline.trial["id"], last["id"]]
        assert trial_order(client.worker_list(query(worker))) == ranked
        first = client.worker_list(query(worker, limit=2))
        assert trial_order(first) == ranked[:2]
        assert first["scanned"] == 2
        second = client.worker_list(query(worker, limit=2, cursor=first["next_cursor"]))
        assert trial_order(second) == [last["id"]]
        assert second["next_cursor"] is None
        # A later cached row never crosses the scanned page's original boundary.
        only_oldest = client.worker_list(query(worker, limit=1))
        assert trial_order(only_oldest) == baseline[:1]
        other_client, other, _ = scoped_worker(pipeline)
        assert trial_order(other_client.worker_list(query(other))) == baseline
        now = replace(now, milliseconds=now.milliseconds + 60_000)
        assert trial_order(client.worker_list(query(worker))) == baseline
    assert pipeline.client.budget(pipeline.campaign["id"]) == before


def test_offer_campaign_filter_is_bound_into_cursor(pipeline: Pipeline, tmp_path):
    variant(pipeline, tmp_path, 1)
    client, worker = pipeline.worker.client, pipeline.worker.worker
    campaign_id = pipeline.campaign["id"]
    first = client.worker_list(query(worker, campaign_id=campaign_id, limit=1))
    second = client.worker_list(
        query(worker, campaign_id=campaign_id, limit=1, cursor=first["next_cursor"])
    )
    assert len(first["items"]) == len(second["items"]) == 1
    assert client.worker_list(query(worker, campaign_id=new_id()))["items"] == []
    with pytest.raises(ResearchError) as changed:
        client.worker_list(query(worker, cursor=first["next_cursor"]))
    assert changed.value.code == ErrorCode.AUTHORIZATION_DENIED


def test_cached_predecessor_outputs_use_the_same_resolver_as_real_claims(
    pipeline: Pipeline, tmp_path
):
    authority, _, _ = prepare_evaluation(pipeline)
    evaluation = pipeline.ready()
    job = pipeline.client.get("job", evaluation["id"])
    predecessor = pipeline.client.get("job", job["data"]["predecessor_ids"][0])
    checkpoint = ArtifactRef.model_validate(predecessor["data"]["outputs"]["model"])
    later, source = variant(pipeline, tmp_path, 1)
    client, worker = pipeline.worker.client, pipeline.worker.worker
    report(pipeline, source)
    assert trial_order(client.worker_list(query(worker))) == [later["id"], pipeline.trial["id"]]
    report(pipeline, checkpoint, revision=1)
    page = client.worker_list(query(worker))
    assert trial_order(page) == [pipeline.trial["id"], later["id"]]
    attempt = claim(client, worker, page["items"][0])
    assert attempt["data"]["inputs"]["model"] == checkpoint.model_dump(mode="json")
    assert attempt["data"]["evaluation_request_id"] == authority["id"]
    # Ranking is no reservation and cannot bypass the single execution slot.
    budget = pipeline.client.budget(pipeline.campaign["id"])
    with pytest.raises(ResearchError) as full:
        claim(client, worker, page["items"][1])
    assert full.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert pipeline.client.budget(pipeline.campaign["id"]) == budget


def test_cached_sources_do_not_override_paused_campaigns(pipeline: Pipeline, tmp_path):
    _, source = variant(pipeline, tmp_path, 1)
    report(pipeline, source)
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    pipeline.client.command(
        "campaign.pause",
        PauseCampaign(campaign_id=campaign["id"]),
        idempotency_key=new_id(),
        expected_revision=campaign["revision"],
    )
    assert pipeline.worker.client.worker_list(query(pipeline.worker.worker))["items"] == []
