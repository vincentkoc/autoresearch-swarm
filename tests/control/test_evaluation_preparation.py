"""Storage work cannot substitute evidence between selection and evaluation commit."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier

import pytest

from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import encode
from tests.control.test_evaluation_control import prepare_evaluation
from tests.integration.test_local_pipeline import pipeline as pipeline


@pytest.mark.parametrize("fault", ["manifest", "trial"])
def test_authorization_rechecks_prepared_inputs_after_storage(pipeline, monkeypatch, fault):
    train = pipeline.worker.claim(pipeline.ready())
    assert pipeline.worker.execute(train["id"])["state"] == "succeeded"
    job = pipeline.client.get("job", pipeline.ready()["id"])
    original = pipeline.runtime.artifacts.publish_bytes

    def publish(context, content, **kwargs):
        reference = original(context, b"{}" if fault == "manifest" else content, **kwargs)
        if fault == "trial":
            row = pipeline.client.get("trial", pipeline.trial["id"])
            data = {**row["data"], "execution_outcome": "closed_unaccepted"}
            pipeline.runtime.database.store.write(
                lambda sql: sql.execute(
                    "UPDATE records SET payload=? WHERE kind='trial' AND id=?",
                    (encode(data), row["id"]),
                )
            )
        return reference

    monkeypatch.setattr(pipeline.runtime.artifacts, "publish_bytes", publish)
    with pytest.raises(ResearchError) as refused:
        pipeline.client.command(
            "evaluation.authorize",
            AuthorizeEvaluation(job_id=job["id"], evaluator_id=new_id()),
            idempotency_key="changed-preparation",
            expected_revision=job["revision"],
        )
    assert refused.value.code == (
        ErrorCode.ARTIFACT_UNVERIFIED if fault == "manifest" else ErrorCode.REVISION_CONFLICT
    )
    assert pipeline.client.get("job", job["id"]) == job
    assert pipeline.client.list(RecordQuery(kind="evaluation_request"))["items"] == []


def test_completion_rechecks_catalog_after_pinned_read(pipeline, monkeypatch):
    authorized, evaluator, _ = prepare_evaluation(pipeline)
    attempt = pipeline.worker.claim(pipeline.ready())
    assert pipeline.worker.execute(attempt["id"])["state"] == "succeeded"
    original = pipeline.runtime.artifacts.reader

    class ChangedRead:
        def __init__(self, stream, reference):
            self.stream, self.reference = stream, reference

        def read(self, size):
            content = self.stream.read(size)
            record = pipeline.client.artifact(self.reference.id)
            data = {**record.model_dump(mode="json"), "storage_generation": new_id()}
            pipeline.runtime.database.store.write(
                lambda sql: sql.execute(
                    "UPDATE records SET payload=? WHERE kind='artifact' AND id=?",
                    (encode(data), self.reference.id),
                )
            )
            return content

    @contextmanager
    def changed(context, reference):
        with original(context, reference) as stream:
            yield ChangedRead(stream, reference)

    monkeypatch.setattr(pipeline.runtime.artifacts, "reader", changed)
    with pytest.raises(ResearchError) as refused:
        evaluator.command(
            "evaluation.complete",
            CompleteEvaluation(request_id=authorized["id"], attempt_id=attempt["id"]),
            idempotency_key="changed-evidence",
        )
    assert refused.value.code == ErrorCode.STALE_FENCE
    assert pipeline.client.get("evaluation_request", authorized["id"]) == authorized
    assert pipeline.client.list(RecordQuery(kind="evaluation_receipt"))["items"] == []
    assert (
        pipeline.client.get("trial", pipeline.trial["id"])["data"]["evaluation_status"] == "pending"
    )


def test_concurrent_completion_keys_share_the_existing_receipt(pipeline, monkeypatch):
    authorized, evaluator, _ = prepare_evaluation(pipeline)
    attempt = pipeline.worker.claim(pipeline.ready())
    assert pipeline.worker.execute(attempt["id"])["state"] == "succeeded"
    intent = CompleteEvaluation(request_id=authorized["id"], attempt_id=attempt["id"])
    original = pipeline.runtime.artifacts.reader
    barrier = Barrier(2)

    @contextmanager
    def concurrent(context, reference):
        with original(context, reference) as stream:
            barrier.wait(timeout=10)
            yield stream

    monkeypatch.setattr(pipeline.runtime.artifacts, "reader", concurrent)
    with ThreadPoolExecutor(max_workers=2) as pool:
        deliveries = [
            pool.submit(evaluator.command, "evaluation.complete", intent, idempotency_key=key)
            for key in ("completion-one", "completion-two")
        ]
        results = [delivery.result(timeout=15) for delivery in deliveries]
    assert results[0]["result"] == results[1]["result"]
    assert results[0]["result"]["state"] == "valid"
    assert len(pipeline.client.list(RecordQuery(kind="evaluation_receipt"))["items"]) == 1
    for key, result in zip(("completion-one", "completion-two"), results, strict=True):
        assert evaluator.command("evaluation.complete", intent, idempotency_key=key) == result
