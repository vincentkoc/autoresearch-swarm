"""Prepared artifacts retain controller-qualified bytes across publication races."""

import pytest

from research_swarm.control.learning_inputs import require_prepared_inputs
from research_swarm.control.learning_models import QualifiedTrajectoryDataset
from research_swarm.control.learning_training_models import (
    PreparedPolicyInputs,
    PreparePolicyInputs,
)
from research_swarm.control.models import SearchConfiguration
from research_swarm.control.promotions import InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.learning.test_learning_datasets import finish_episode, planned, qualify
from tests.search.test_proposals import PROPOSAL_LIMITS


def prepared_batch(pipeline, tmp_path):
    plan = planned(pipeline, tmp_path)
    _, receipt, _ = finish_episode(pipeline, plan)
    dataset = qualify(pipeline, plan)
    intent = PreparePolicyInputs(binding_id=new_id(), dataset_id=dataset["id"])
    return plan, receipt, dataset, intent


def prepare_inputs(pipeline, intent, key="prepare"):
    return pipeline.client.command("learning.prepare_training_inputs", intent, idempotency_key=key)[
        "result"
    ]


def context(runtime, key="inspect", revision=None):
    return CommandContext(
        principal=runtime.principal(), idempotency_key=key, expected_revision=revision
    )


@pytest.mark.parametrize(
    "pipeline",
    [
        {
            "limits": PROPOSAL_LIMITS,
            "search": SearchConfiguration(
                policy="external", version="external-v1", max_parallel_trials=2
            ),
        }
    ],
    indirect=True,
)
class TestTrainingInputs:
    def test_canonical_artifacts_are_retained_and_replay_without_republication(
        self, pipeline, tmp_path, monkeypatch
    ):
        plan, _, dataset, intent = prepared_batch(pipeline, tmp_path)
        result = prepare_inputs(pipeline, intent)
        data = PreparedPolicyInputs.model_validate(result["data"])
        expected = QualifiedTrajectoryDataset.model_validate(dataset["data"])
        for reference, model in (
            (data.behavior, plan.intent.policy),
            (data.trajectories, expected.dataset),
        ):
            with pipeline.runtime.artifacts.reader(context(pipeline.runtime), reference) as stream:
                assert stream.read() == canonical_bytes(model.model_dump(mode="json"))
            with pytest.raises(ResearchError, match="retained"):
                pipeline.runtime.artifacts.delete(
                    context(pipeline.runtime, new_id(), 1), reference.id, "must remain pinned"
                )
        current = pipeline.runtime.database.store.read(
            lambda sql: require_prepared_inputs(
                Transaction(sql, context(pipeline.runtime)), result["id"]
            )
        )
        assert current == data

        def fail(*args, **kwargs):
            raise OSError("publication unavailable")

        monkeypatch.setattr(pipeline.runtime.artifacts, "publish_bytes", fail)
        assert prepare_inputs(pipeline, intent) == result
        assert prepare_inputs(pipeline, intent, key="new-key") == result
        with pytest.raises(ResearchError) as collision:
            prepare_inputs(
                pipeline, intent.model_copy(update={"dataset_id": new_id()}), key="collision"
            )
        assert collision.value.code == ErrorCode.IDEMPOTENCY_CONFLICT

    def test_correction_between_publication_and_commit_cannot_bind_stale_inputs(
        self, pipeline, tmp_path, monkeypatch
    ):
        _, receipt, _, intent = prepared_batch(pipeline, tmp_path)
        original = pipeline.runtime.artifacts.publish_bytes

        def publish(*args, **kwargs):
            reference = original(*args, **kwargs)
            if kwargs["purpose"] == "learning-trajectories":
                pipeline.client.command(
                    "evidence.invalidate",
                    InvalidateEvidence(
                        target=EvidenceReference(kind="evaluation_receipt", id=receipt["id"]),
                        reason="Correction during input publication",
                    ),
                    idempotency_key="during-publication",
                )
            return reference

        monkeypatch.setattr(pipeline.runtime.artifacts, "publish_bytes", publish)
        with pytest.raises(ResearchError):
            prepare_inputs(pipeline, intent)
        assert pipeline.client.list(RecordQuery(kind="learning_inputs"))["items"] == []

    def test_committed_binding_recovers_reply_and_replays_after_later_correction(
        self, pipeline, tmp_path, monkeypatch
    ):
        _, receipt, _, intent = prepared_batch(pipeline, tmp_path)
        execute = pipeline.runtime.database.execute
        faults = []

        def lose_ack(command_context, operation, *args, **kwargs):
            result = execute(command_context, operation, *args, **kwargs)
            if operation == "learning.prepare_training_inputs":
                faults.append(operation)
                raise OSError("lost committed reply")
            return result

        monkeypatch.setattr(pipeline.runtime.database, "execute", lose_ack)
        first = prepare_inputs(pipeline, intent)
        assert faults == ["learning.prepare_training_inputs"]
        monkeypatch.setattr(pipeline.runtime.database, "execute", execute)
        pipeline.client.command(
            "evidence.invalidate",
            InvalidateEvidence(
                target=EvidenceReference(kind="evaluation_receipt", id=receipt["id"]),
                reason="Later correction",
            ),
            idempotency_key="later-correction",
        )

        def fail(*args, **kwargs):
            raise OSError("byte service unavailable")

        monkeypatch.setattr(pipeline.runtime.artifacts, "publish_bytes", fail)
        saved = prepare_inputs(pipeline, intent)
        assert saved == first and saved["id"] == intent.binding_id
        with pytest.raises(ResearchError):
            pipeline.runtime.database.store.read(
                lambda sql: require_prepared_inputs(
                    Transaction(sql, context(pipeline.runtime)), saved["id"]
                )
            )

    def test_event_rollback_does_not_leave_input_binding_or_its_retentions(
        self, pipeline, tmp_path, monkeypatch
    ):
        _, _, _, intent = prepared_batch(pipeline, tmp_path)
        event = Transaction.event

        def fail(tx, event_type, *args, **kwargs):
            if event_type == "learning.inputs_prepared":
                raise RuntimeError("input event failure")
            return event(tx, event_type, *args, **kwargs)

        monkeypatch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="input event"):
            prepare_inputs(pipeline, intent)
        assert pipeline.client.list(RecordQuery(kind="learning_inputs"))["items"] == []
        retained = pipeline.runtime.database.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='artifact_reference' "
                "AND json_extract(payload,'$.retention.owner_id')=?",
                (intent.binding_id,),
            ).one()[0]
        )
        assert retained == 0
        monkeypatch.setattr(Transaction, "event", event)
        assert prepare_inputs(pipeline, intent)["id"] == intent.binding_id

    def test_exact_advertised_scopes_work_and_missing_scope_refuses_before_upload(
        self, pipeline, tmp_path
    ):
        from research_swarm.client import LocalTransport, ResearchClient
        from research_swarm.control.dispatch import COMMANDS
        from research_swarm.domain.identity import Principal

        _, _, _, intent = prepared_batch(pipeline, tmp_path)
        definition = COMMANDS["learning.prepare_training_inputs"]
        required = {definition.scope, *definition.additional_scopes}
        assert required == {"learning.manage", "artifact.upload", "read"}
        for missing in sorted(required):
            actor = Principal(
                id=new_id(),
                workspace_id=pipeline.runtime.identity.workspace_id,
                scopes=frozenset(required - {missing}),
            )
            client = ResearchClient(LocalTransport(pipeline.runtime, lambda actor=actor: actor))
            with pytest.raises(ResearchError) as denied:
                client.command("learning.prepare_training_inputs", intent, idempotency_key=missing)
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        assert pipeline.client.list(RecordQuery(kind="learning_inputs"))["items"] == []
        actor = Principal(
            id=new_id(),
            workspace_id=pipeline.runtime.identity.workspace_id,
            scopes=frozenset(required),
        )
        client = ResearchClient(LocalTransport(pipeline.runtime, lambda: actor))
        result = client.command(
            "learning.prepare_training_inputs", intent, idempotency_key="exact-scopes"
        )["result"]
        assert result["data"]["actor_id"] == actor.id


def test_inline_default_and_explicit_large_bound_roundtrip(tmp_path):
    runtime = LocalRuntime.open(tmp_path / "runtime")
    content = b"x" * (1024 * 1024 + 1)
    with pytest.raises(ResearchError, match="exceeds"):
        runtime.artifacts.publish_bytes(context(runtime, "default"), content, purpose="metadata")
    reference = runtime.artifacts.publish_bytes(
        context(runtime, "large"), content, purpose="metadata", max_bytes=8 * 1024 * 1024
    )
    with runtime.artifacts.reader(context(runtime), reference) as stream:
        assert stream.read() == content
    assert (
        runtime.artifacts.publish_bytes(
            context(runtime, "large"), content, purpose="metadata", max_bytes=8 * 1024 * 1024
        )
        == reference
    )
    for invalid in (0, True, 8 * 1024 * 1024 + 1):
        with pytest.raises(ResearchError, match="Invalid"):
            runtime.artifacts.publish_bytes(
                context(runtime, new_id()), b"", purpose="invalid", max_bytes=invalid
            )
