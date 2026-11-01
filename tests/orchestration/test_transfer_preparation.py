"""Transfer preparation preserves operator authority and rechecks the selected cut."""

import pytest

from research_swarm.control.orchestration_transfer import transfer_request_digest
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.state.database import Transaction
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_orchestration_transfer import pause, request, successor, transfer
from tests.orchestration.test_orchestration_turns import bind


def selection(pipeline):
    original = bind(pipeline)
    pause(pipeline)
    intent, revision = request(pipeline, successor(pipeline))
    context = CommandContext(
        principal=pipeline.runtime.principal(),
        idempotency_key="prepared-transfer",
        expected_revision=revision,
    )
    return original, intent, context


def prepare(pipeline, context, intent):
    service = pipeline.runtime.service.orchestration_transfer
    digest = transfer_request_digest(context, intent)
    return pipeline.runtime.database.store.read(
        lambda sql: service._prepare_in(Transaction(sql, context), intent, digest)
    )


def counts(pipeline):
    return pipeline.runtime.database.store.read(
        lambda sql: tuple(
            sql.execute("SELECT COUNT(*) FROM " + table).one()[0]
            for table in ("records", "commands", "events")
        )
    )


def test_preparation_is_read_only_and_matches_the_native_checkpoint(pipeline):
    _, intent, context = selection(pipeline)
    before = counts(pipeline)
    view = prepare(pipeline, context, intent)
    assert view is not None and view.artifacts
    assert counts(pipeline) == before
    completed = transfer(pipeline, intent, context.expected_revision)
    checkpoint = pipeline.client.get("orchestration_checkpoint", completed["data"]["checkpoint_id"])
    assert checkpoint["data"]["manifest_digest"] == view.digest
    assert checkpoint["data"]["artifact_manifest_digest"] == view.artifact_digest
    # The original binding revision and epoch are now stale. An existing transfer
    # still selects its immutable receipt rather than inspecting a new view.
    assert prepare(pipeline, context, intent) is None


@pytest.mark.parametrize("actor", ["driver", "reader"])
def test_shared_preparation_retains_scope_and_operator_separation(pipeline, actor):
    original, intent, context = selection(pipeline)
    principal = (
        original.actor
        if actor == "driver"
        else context.principal.model_copy(update={"scopes": {"read"}})
    )
    before = counts(pipeline)
    with pytest.raises(ResearchError) as refused:
        prepare(pipeline, context.model_copy(update={"principal": principal}), intent)
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert counts(pipeline) == before


def test_commit_rejects_a_view_changed_after_adapter_preparation(pipeline):
    _, intent, context = selection(pipeline)
    service = pipeline.runtime.service.orchestration_transfer
    view = prepare(pipeline, context, intent)
    before = pipeline.client.get("orchestration_binding", intent.campaign_id)
    pipeline.client.command(
        "report.create", {"campaign_id": intent.campaign_id}, idempotency_key="later-report"
    )
    with pytest.raises(ResearchError) as changed:
        pipeline.runtime.database.execute(
            context,
            "orchestration.transfer",
            intent.model_dump(mode="json"),
            "orchestration.manage",
            lambda tx: service._commit(tx, intent, transfer_request_digest(context, intent), view),
        )
    assert changed.value.code == ErrorCode.REVISION_CONFLICT
    assert pipeline.client.get("orchestration_binding", intent.campaign_id) == before
    assert pipeline.client.list(RecordQuery(kind="orchestration_checkpoint"))["items"] == []
    assert pipeline.client.list(RecordQuery(kind="orchestration_start"))["items"] == []
