from __future__ import annotations

from collections import Counter
from contextlib import ExitStack

import pytest

from research_swarm import ResearchClient
from research_swarm.api.http import create_app
from research_swarm.control.commands import SubmitTrial
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.workers.local import LocalWorker
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_worker_inputs import assigned_input, revoke_worker


def test_two_native_trials_reuse_source_across_worker_restart(pipeline: Pipeline, monkeypatch):
    transfers = Counter()
    original = pipeline.worker.client.download

    def download(identifier, destination):
        transfers[identifier] += 1
        return original(identifier, destination)

    monkeypatch.setattr(pipeline.worker.client, "download", download)
    assert not pipeline.worker.input_cache.root.exists()
    sources = set()
    for trial_number in range(2):
        if trial_number:
            campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
            trial = pipeline.client.command(
                "trial.submit",
                SubmitTrial(
                    campaign_id=campaign["id"],
                    candidate_id=pipeline.trial["data"]["candidate_id"],
                    validation_id=pipeline.trial["data"]["validation_id"],
                    seed=8,
                ),
                idempotency_key="cache-second-trial",
                expected_revision=campaign["revision"],
            )["result"]
            current = pipeline.worker
            pipeline.worker = LocalWorker(
                current.client,
                current.worker,
                root=current.root,
                machine_registry=current.resources.path,
            )
        else:
            trial = pipeline.trial
        for _ in range(2):
            attempt = pipeline.worker.claim(pipeline.ready())
            sources.add(attempt["data"]["source_artifact"]["id"])
            assert pipeline.worker.execute(attempt["id"])["state"] == "succeeded"
        assert pipeline.client.get("trial", trial["id"])["state"] == "succeeded"
    assert len(sources) == 1
    assert transfers[next(iter(sources))] == 1
    assert sum(transfers.values()) == 3  # One source and two distinct checkpoints.


@pytest.mark.parametrize("remote", [False, True])
def test_scoped_warm_hit_rechecks_live_authority_locally_and_over_http(
    pipeline: Pipeline, tmp_path, monkeypatch, remote
):
    local, data, credential, request = assigned_input(pipeline)
    with ExitStack() as stack:
        if remote:
            url = stack.enter_context(
                serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials))
            )
            client = stack.enter_context(ResearchClient.connect(url, credential.token))
        else:
            client = local
        worker = LocalWorker(
            client,
            data,
            root=tmp_path / "scoped-cache-worker",
            machine_registry=pipeline.worker.resources.path,
        )
        reference = client.worker_input(request)
        original, transfers = client.worker_download, []

        def download(target, destination):
            transfers.append(target.artifact_id)
            return original(target, destination)

        monkeypatch.setattr(client, "worker_download", download)
        assert (
            worker._download(request.attempt_id, reference, tmp_path / "first").origin == "download"
        )
        assert (
            worker._download(request.attempt_id, reference, tmp_path / "second").origin == "cache"
        )
        assert transfers == [reference.id]
        revoke_worker(pipeline, data)
        with pytest.raises(ResearchError) as denied:
            worker._download(request.attempt_id, reference, tmp_path / "revoked")
        assert denied.value.code == ErrorCode.STALE_FENCE
        assert not (tmp_path / "revoked").exists()
        assert transfers == [reference.id]


def test_existing_unscoped_copy_cannot_bypass_withdrawn_artifact(pipeline: Pipeline, tmp_path):
    attempt = pipeline.worker.claim(pipeline.ready())
    reference = ArtifactRef.model_validate(attempt["data"]["source_artifact"])
    target = tmp_path / "existing"
    pipeline.worker._download(attempt["id"], reference, target)
    with pipeline.runtime.database.connect() as connection:
        connection.execute(
            "UPDATE records SET state='deleting',payload=json_set(payload,'$.state','deleting') "
            "WHERE kind='artifact' AND id=?",
            (reference.id,),
        )
    with pytest.raises(ResearchError) as withdrawn:
        pipeline.worker._download(attempt["id"], reference, target)
    assert withdrawn.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    assert target.exists()
