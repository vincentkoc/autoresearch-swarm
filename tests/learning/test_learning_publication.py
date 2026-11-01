"""A learned checkpoint remains tied to current native research and evaluation."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier

import pytest

from research_swarm.control.dispatch import COMMANDS
from research_swarm.control.learning_publication import (
    check_output_lineage,
    require_current_training,
)
from research_swarm.control.learning_training_evidence import training_evidence
from research_swarm.control.learning_training_models import (
    PublishedPolicyTraining,
    PublishPolicyTraining,
)
from research_swarm.control.models import SearchConfiguration
from research_swarm.control.promotions import InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.learning import PolicyUpdate
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.learning.test_learning_admission import (
    admit_training,
    finish_training,
    training_admission,
)
from tests.learning.test_learning_inputs import context
from tests.learning.test_learning_profiles import scoped_client
from tests.search.test_proposals import PROPOSAL_LIMITS


def completed_training(pipeline, tmp_path, *, source_change=None):
    campaign, intent, research_receipt = training_admission(
        pipeline, tmp_path, source_change=source_change
    )
    admission = admit_training(pipeline.client, campaign, intent)
    checker_receipt = finish_training(pipeline, admission)
    return campaign, admission, research_receipt, checker_receipt


def publish(client, admission, key="publish-training"):
    return client.command(
        "learning.publish_training",
        PublishPolicyTraining(admission_id=admission["id"]),
        idempotency_key=key,
    )["result"]


def current(pipeline, admission):
    return pipeline.runtime.database.store.read(
        lambda sql: require_current_training(
            Transaction(sql, context(pipeline.runtime)), admission["id"]
        )
    )


def invalidate(client, kind, identifier):
    return client.command(
        "evidence.invalidate",
        InvalidateEvidence(
            target=EvidenceReference(kind=kind, id=identifier),
            reason="Training evidence correction",
        ),
        idempotency_key=new_id(),
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
class TestTrainingPublication:
    def test_native_publication_records_trainer_lineage_and_recovers_lost_reply(
        self, pipeline, tmp_path, monkeypatch
    ):
        campaign, admission, research_receipt, checker_receipt = completed_training(
            pipeline, tmp_path
        )
        definition = COMMANDS["learning.publish_training"]
        scopes = {definition.scope, *definition.additional_scopes}
        assert scopes == {"learning.manage", "read"}
        for missing in scopes:
            with pytest.raises(ResearchError) as denied:
                publish(scoped_client(pipeline.runtime, scopes - {missing}), admission, missing)
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        budget = pipeline.client.budget(campaign["id"])
        execute = pipeline.runtime.database.execute

        def lose_ack(ctx, operation, *args, **kwargs):
            result = execute(ctx, operation, *args, **kwargs)
            if operation == "learning.publish_training":
                raise OSError("lost publication reply")
            return result

        monkeypatch.setattr(pipeline.runtime.database, "execute", lose_ack)
        client = scoped_client(pipeline.runtime, scopes)
        row = publish(client, admission)
        monkeypatch.setattr(pipeline.runtime.database, "execute", execute)
        data = PublishedPolicyTraining.model_validate(row["data"])
        trainer = pipeline.client.get("job", admission["data"]["trainer_job_id"])
        assert data.training.job_id == trainer["id"]
        assert data.training.attempt_id == trainer["data"]["current_attempt_id"]
        assert data.training.evaluator_receipt_id == checker_receipt["id"]
        assert data.policy.parent_digest == admission["data"]["behavior_policy_digest"]
        assert data.update_error <= 1e-12
        assert current(pipeline, admission) == data
        assert pipeline.client.budget(campaign["id"]) == budget
        assert (
            pipeline.client.get("campaign", campaign["id"])["data"]["incumbent_candidate_id"]
            is None
        )
        retained = pipeline.runtime.database.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='artifact_reference' "
                "AND json_extract(payload,'$.retention.owner_kind')='learning_publication' "
                "AND json_extract(payload,'$.retention.owner_id')=?",
                (admission["id"],),
            ).one()[0]
        )
        assert retained == 2
        invalidate(pipeline.client, "evaluation_receipt", research_receipt["id"])
        with pytest.raises(ResearchError):
            current(pipeline, admission)

        def unavailable(*args, **kwargs):
            raise OSError("byte service unavailable")

        monkeypatch.setattr(pipeline.runtime.artifacts, "reader", unavailable)
        assert publish(client, admission) == row
        assert publish(client, admission, "fresh-key") == row

    @pytest.mark.parametrize("target", ["research", "trainer", "checker"])
    def test_correction_during_output_read_blocks_publication(
        self, pipeline, tmp_path, monkeypatch, target
    ):
        _, admission, research, checker = completed_training(pipeline, tmp_path)
        trainer = pipeline.client.get("job", admission["data"]["trainer_job_id"])
        reference = trainer["data"]["outputs"]["update"]["id"]
        kind, identifier = {
            "research": ("evaluation_receipt", research["id"]),
            "trainer": ("run_result", trainer["data"]["current_attempt_id"]),
            "checker": ("evaluation_receipt", checker["id"]),
        }[target]
        reader = pipeline.runtime.artifacts.reader

        @contextmanager
        def correcting(ctx, artifact):
            with reader(ctx, artifact) as stream:
                if artifact.id == reference:
                    invalidate(pipeline.client, kind, identifier)
                yield stream

        monkeypatch.setattr(pipeline.runtime.artifacts, "reader", correcting)
        with pytest.raises(ResearchError):
            publish(pipeline.client, admission)
        assert not pipeline.client.list(RecordQuery(kind="learning_publication"))["items"]

    def test_event_failure_rolls_back_output_retentions(self, pipeline, tmp_path, monkeypatch):
        _, admission, _, _ = completed_training(pipeline, tmp_path)
        event = Transaction.event

        def fail(transaction, event_type, *args, **kwargs):
            if event_type == "learning.training_published":
                raise RuntimeError("publication event failed")
            return event(transaction, event_type, *args, **kwargs)

        monkeypatch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="publication event failed"):
            publish(pipeline.client, admission)
        assert not pipeline.client.list(RecordQuery(kind="learning_publication"))["items"]
        count = pipeline.runtime.database.store.read(
            lambda sql: sql.execute(
                "SELECT COUNT(*) FROM records WHERE kind='artifact_reference' "
                "AND json_extract(payload,'$.retention.owner_kind')='learning_publication'"
            ).one()[0]
        )
        assert count == 0
        monkeypatch.setattr(Transaction, "event", event)
        assert publish(pipeline.client, admission)["id"] == admission["id"]

    def test_output_lineage_and_bound_are_independent_of_valid_checker(
        self, pipeline, tmp_path, monkeypatch
    ):
        import research_swarm.control.learning_publication as publication

        _, admission, _, _ = completed_training(pipeline, tmp_path)
        evidence = pipeline.runtime.database.store.read(
            lambda sql: training_evidence(
                Transaction(sql, context(pipeline.runtime)), admission["id"]
            )
        )
        with pipeline.runtime.artifacts.reader(
            context(pipeline.runtime), evidence.training.update_report
        ) as stream:
            report = PolicyUpdate.model_validate_json(stream.read())
        check_output_lineage(evidence, report.policy, report)
        for changed in (
            {"dataset_digest": content_digest(b"other-data")},
            {"reward_spec_digest": content_digest(b"other-reward")},
            {"learning_rate": 0.7},
            {"sample_count": report.sample_count + 1},
        ):
            with pytest.raises(ResearchError, match="provenance"):
                check_output_lineage(evidence, report.policy, report.model_copy(update=changed))
        oversized = evidence.model_copy(
            update={
                "training": evidence.training.model_copy(
                    update={
                        "policy_artifact": evidence.training.policy_artifact.model_copy(
                            update={"size_bytes": 65537}
                        )
                    }
                )
            }
        )
        monkeypatch.setattr(publication, "training_evidence", lambda *args: oversized)

        def no_reader(*args, **kwargs):
            pytest.fail("oversized output must be refused before opening a byte reader")

        monkeypatch.setattr(pipeline.runtime.artifacts, "reader", no_reader)
        with pytest.raises(ResearchError, match="size bound"):
            publish(pipeline.client, admission)

    def test_unfinished_training_cannot_publish(self, pipeline, tmp_path):
        campaign, intent, _ = training_admission(pipeline, tmp_path)
        admission = admit_training(pipeline.client, campaign, intent)
        with pytest.raises(ResearchError) as refused:
            publish(pipeline.client, admission)
        assert refused.value.code == ErrorCode.EVALUATION_INCOMPLETE

    def test_valid_receipt_above_profile_tolerance_cannot_publish(self, pipeline, tmp_path):
        def change(source):
            path = source / "verify.py"
            content = path.read_text()
            assert '"value": error' in content
            path.write_text(content.replace('"value": error', '"value": 0.01'))

        _, admission, _, receipt = completed_training(pipeline, tmp_path, source_change=change)
        assert receipt["state"] == "valid"
        with pytest.raises(ResearchError, match="numerical tolerance"):
            publish(pipeline.client, admission)
        assert not pipeline.client.list(RecordQuery(kind="learning_publication"))["items"]

    def test_concurrent_http_publication_commits_one_receipt(self, pipeline, tmp_path):
        from research_swarm.api.http import create_app
        from research_swarm.client import ResearchClient
        from research_swarm.runtime import read_private
        from tests.orchestration.test_http_delivery import serving

        campaign, admission, _, _ = completed_training(pipeline, tmp_path)
        budget = pipeline.client.budget(campaign["id"])
        token = read_private(pipeline.runtime.root, "operator.token", limit=256).decode("ascii")
        barrier = Barrier(2)
        with serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url:

            def issue(index):
                with ResearchClient.connect(url, token) as client:
                    barrier.wait(timeout=5)
                    return publish(client, admission, "http-" + str(index))

            with ThreadPoolExecutor(max_workers=2) as pool:
                first, second = pool.map(issue, range(2))
        assert first == second
        assert publish(pipeline.client, admission, "local-replay") == first
        assert pipeline.client.budget(campaign["id"]) == budget
        assert len(pipeline.client.list(RecordQuery(kind="learning_publication"))["items"]) == 1
        assert current(pipeline, admission).training.trial_id == admission["data"]["trial_id"]
