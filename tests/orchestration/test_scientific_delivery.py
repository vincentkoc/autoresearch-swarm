from pathlib import Path

from research_swarm.control.completion_models import (
    CaptureCampaignObligations,
    CloseCampaignResources,
)
from research_swarm.domain.orchestration import DriveAction
from research_swarm.domain.types import new_id
from tests.control.test_scientific_completion import finish, review, unknown
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_async_delivery import kernels
from tests.orchestration.test_orchestration_turns import bind, prepare


async def test_scientific_completion_keeps_delivery_available_until_resource_closure(
    pipeline: Pipeline, tmp_path: Path
) -> None:
    drivers = bind(pipeline)
    asynchronous, native, connection = kernels(pipeline, drivers)
    binding = connection.binding
    attempt = pipeline.worker.claim(pipeline.ready())
    unknown(pipeline, attempt, tmp_path)
    finish(pipeline)
    assert pipeline.client.get("campaign", binding.campaign_id)["state"] == "completed"
    assert (
        pipeline.client.context(binding.campaign_id)["operational"]["status"]
        == "reconciliation_pending"
    )
    assert (await asynchronous.inspect(binding)).state == native.inspect(binding).state == "idle"

    campaign = pipeline.client.get("campaign", binding.campaign_id)
    captured = prepare(
        pipeline,
        drivers.control,
        DriveAction(
            operation="campaign.capture_obligations",
            payload=CaptureCampaignObligations(
                campaign_id=campaign["id"], review_id=new_id()
            ).model_dump(mode="json"),
            expected_revision=campaign["revision"],
            idempotency_key="review-after-completion",
        ),
    )
    pending = await asynchronous.inspect(binding)
    assert pending == native.inspect(binding) and pending.state == "work"
    assert pending.work is not None and pending.work.record_id == captured["id"]
    assert (await asynchronous.execute(pending.work)).disposition == "accepted"
    assert (await asynchronous.inspect(binding)).state == native.inspect(binding).state == "idle"

    pipeline.worker.journal.request_cancel(attempt["id"], 1)
    assert pipeline.worker.execute(attempt["id"])["state"] == "not_started"
    captured = review(pipeline)
    closure = prepare(
        pipeline,
        drivers.control,
        DriveAction(
            operation="campaign.close_resources",
            payload=CloseCampaignResources(
                campaign_id=campaign["id"],
                obligations_id=captured["id"],
                reason="Original custody settled",
            ).model_dump(mode="json"),
            expected_revision=captured["data"]["campaign_revision"],
            idempotency_key="close-after-completion",
        ),
    )
    pending = native.inspect(binding)
    assert pending.work is not None and pending.work.record_id == closure["id"]
    assert native.execute(pending.work).disposition == "accepted"
    assert pipeline.client.context(binding.campaign_id)["operational"]["status"] == "closed"
    assert (
        (await asynchronous.inspect(binding)).state == native.inspect(binding).state == "terminal"
    )
