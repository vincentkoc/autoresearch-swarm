"""Qualified training admission shares ordinary jobs, budgets and evidence rules."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from research_swarm.control.commands import (
    CampaignTarget,
    CreateCampaign,
    ProposeCandidate,
    ValidateCandidate,
)
from research_swarm.control.dispatch import COMMANDS
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.learning_training_models import (
    AdmitPolicyTraining,
    PreparedPolicyInputs,
)
from research_swarm.control.models import SearchConfiguration
from research_swarm.control.promotions import InvalidateEvidence
from research_swarm.control.queries import RecordQuery
from research_swarm.control.validity import EvidenceReference
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from tests.support import LIMITS
from tests.test_learning_inputs import prepare_inputs, prepared_batch
from tests.test_learning_profiles import (
    profile_intent,
    register_profile,
    scoped_client,
    training_project,
)
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_proposals import PROPOSAL_LIMITS


def training_admission(
    pipeline, tmp_path, *, extra_input=False, admission_limit=10, source_change=None
):
    _, receipt, _, prepare = prepared_batch(pipeline, tmp_path)
    prepared = PreparedPolicyInputs.model_validate(prepare_inputs(pipeline, prepare)["data"])
    registered = training_project(pipeline.client, tmp_path, change=source_change)
    profile = register_profile(pipeline.client, profile_intent(registered))
    inputs = {"behavior": prepared.behavior, "trajectories": prepared.trajectories}
    if extra_input:
        inputs["heldout"] = prepared.trajectories
    campaign = pipeline.client.command(
        "campaign.create",
        CreateCampaign(
            project_id=registered["project"]["id"],
            name="Qualified policy training",
            primary_metric="update_error",
            input_artifacts=inputs,
            limits=(
                *LIMITS,
                BudgetLimit(dimension="trial_admissions", limit=admission_limit, unit="count"),
            ),
        ),
        idempotency_key="training-campaign",
    )["result"]
    for operation in ("campaign.seal", "campaign.start"):
        campaign = pipeline.client.command(
            operation,
            CampaignTarget(campaign_id=campaign["id"]),
            expected_revision=campaign["revision"],
            idempotency_key="training-" + operation,
        )["result"]
    candidate = pipeline.client.command(
        "candidate.propose",
        ProposeCandidate(
            campaign_id=campaign["id"],
            source_artifact=registered["project_revision"]["data"]["source_artifact"],
            hypothesis="Train one update from qualified development evidence",
        ),
        expected_revision=campaign["revision"],
        idempotency_key="training-candidate",
    )["result"]
    validation = pipeline.client.command(
        "candidate.validate",
        ValidateCandidate(candidate_id=candidate["id"]),
        expected_revision=candidate["revision"],
        idempotency_key="training-validation",
    )["result"]
    intent = AdmitPolicyTraining(
        admission_id=new_id(),
        profile_id=profile["id"],
        inputs_id=prepare.binding_id,
        campaign_id=campaign["id"],
        candidate_id=candidate["id"],
        validation_id=validation["id"],
        seed=17,
    )
    return campaign, intent, receipt


def admit_training(client, campaign, intent, key="admit-training"):
    return client.command(
        "learning.admit_training",
        intent,
        expected_revision=campaign["revision"],
        idempotency_key=key,
    )["result"]


def trials(client, campaign):
    return client.list(RecordQuery(kind="trial", campaign_id=campaign["id"]))["items"]


def finish_training(pipeline, admission):
    trainer = pipeline.client.get("job", admission["data"]["trainer_job_id"])
    attempt = pipeline.worker.claim(trainer)
    assert pipeline.worker.execute(attempt["id"])["state"] == "succeeded"
    checker = pipeline.client.get("job", admission["data"]["checker_job_id"])
    authorization = pipeline.client.command(
        "evaluation.authorize",
        AuthorizeEvaluation(job_id=checker["id"], evaluator_id=pipeline.runtime.principal().id),
        expected_revision=checker["revision"],
        idempotency_key="authorize-policy-checker",
    )["result"]
    checker = pipeline.client.get("job", checker["id"])
    attempt = pipeline.worker.claim(checker)
    assert pipeline.worker.execute(attempt["id"])["state"] == "succeeded"
    return pipeline.client.command(
        "evaluation.complete",
        CompleteEvaluation(request_id=authorization["id"], attempt_id=attempt["id"]),
        idempotency_key="policy-checker-receipt",
    )["result"]


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
class TestTrainingAdmission:
    def test_native_training_uses_qualified_inputs_and_normal_checker(self, pipeline, tmp_path):
        campaign, intent, _ = training_admission(pipeline, tmp_path)
        admission = admit_training(pipeline.client, campaign, intent)
        assert admit_training(pipeline.client, campaign, intent, "new-key") == admission
        data = admission["data"]
        trainer = pipeline.client.get("job", data["trainer_job_id"])
        checker = pipeline.client.get("job", data["checker_job_id"])
        assert checker["data"]["input_sources"] == {
            "policy": trainer["id"],
            "update": trainer["id"],
        }
        assert checker["data"]["inputs"] == trainer["data"]["inputs"]
        receipt = finish_training(pipeline, admission)
        assert receipt["state"] == "valid"
        assert len(trials(pipeline.client, campaign)) == 1
        costs = {
            row["dimension"]: row["consumed"]
            for row in pipeline.client.budget(campaign["id"])["accounts"]
        }
        assert costs["trial_admissions"] == "1" and costs["attempts"] == "2"
        with pytest.raises(ResearchError) as collision:
            admit_training(
                pipeline.client, campaign, intent.model_copy(update={"seed": 18}), "collision"
            )
        assert collision.value.code == ErrorCode.IDEMPOTENCY_CONFLICT

    def test_event_rollback_includes_jobs_and_trial_charge(self, pipeline, tmp_path, monkeypatch):
        campaign, intent, _ = training_admission(pipeline, tmp_path)
        before = pipeline.client.budget(campaign["id"])
        event = Transaction.event

        def fail(transaction, event_type, *args, **kwargs):
            if event_type == "learning.training_admitted":
                raise RuntimeError("admission event failed")
            return event(transaction, event_type, *args, **kwargs)

        monkeypatch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="admission event failed"):
            admit_training(pipeline.client, campaign, intent)
        assert not trials(pipeline.client, campaign)
        assert not pipeline.client.list(RecordQuery(kind="job", campaign_id=campaign["id"]))[
            "items"
        ]
        assert not pipeline.client.list(RecordQuery(kind="learning_admission"))["items"]
        assert pipeline.client.budget(campaign["id"]) == before
        monkeypatch.setattr(Transaction, "event", event)
        assert admit_training(pipeline.client, campaign, intent)["id"] == intent.admission_id

    def test_current_evidence_and_campaign_revision_are_required(self, pipeline, tmp_path):
        campaign, intent, receipt = training_admission(pipeline, tmp_path)
        with pytest.raises(ResearchError) as stale:
            admit_training(pipeline.client, {**campaign, "revision": 0}, intent)
        assert stale.value.code == ErrorCode.REVISION_CONFLICT
        pipeline.client.command(
            "evidence.invalidate",
            InvalidateEvidence(
                target=EvidenceReference(kind="evaluation_receipt", id=receipt["id"]),
                reason="Corrected development result",
            ),
            idempotency_key="correct-before-training",
        )
        with pytest.raises(ResearchError):
            admit_training(pipeline.client, campaign, intent)
        assert not trials(pipeline.client, campaign)

    def test_campaign_cannot_add_other_inputs(self, pipeline, tmp_path):
        campaign, intent, _ = training_admission(pipeline, tmp_path, extra_input=True)
        with pytest.raises(ResearchError) as refused:
            admit_training(pipeline.client, campaign, intent)
        assert refused.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
        assert not trials(pipeline.client, campaign)

    def test_wrong_profile_and_exhausted_allowance_refuse(self, pipeline, tmp_path):
        campaign, intent, _ = training_admission(pipeline, tmp_path, admission_limit=1)
        other = pipeline.client.register_project(
            tmp_path / "training-project", idempotency_key="other-training-project"
        )["result"]
        profile = register_profile(pipeline.client, profile_intent(other), "other-profile")
        with pytest.raises(ResearchError) as wrong:
            admit_training(
                pipeline.client,
                campaign,
                intent.model_copy(update={"profile_id": profile["id"]}),
                "wrong-profile",
            )
        assert wrong.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
        admit_training(pipeline.client, campaign, intent)
        with pytest.raises(ResearchError) as exhausted:
            admit_training(
                pipeline.client,
                campaign,
                intent.model_copy(update={"admission_id": new_id()}),
                "exhausted",
            )
        assert exhausted.value.code == ErrorCode.BUDGET_EXHAUSTED
        assert len(trials(pipeline.client, campaign)) == 1

    def test_exact_scopes_and_independent_connections_admit_once(self, pipeline, tmp_path):
        campaign, intent, _ = training_admission(pipeline, tmp_path)
        definition = COMMANDS["learning.admit_training"]
        scopes = {definition.scope, *definition.additional_scopes}
        assert scopes == {"learning.manage", "trial.submit", "read"}
        for missing in scopes:
            with pytest.raises(ResearchError) as refused:
                admit_training(
                    scoped_client(pipeline.runtime, scopes - {missing}), campaign, intent, missing
                )
            assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
        clients = [
            scoped_client(LocalRuntime.open(pipeline.runtime.root), scopes) for _ in range(2)
        ]
        barrier = Barrier(2)

        def issue(index):
            barrier.wait(timeout=5)
            return admit_training(clients[index], campaign, intent, "race-" + str(index))

        with ThreadPoolExecutor(max_workers=2) as pool:
            first, second = pool.map(issue, range(2))
        assert first == second
        assert len(trials(pipeline.client, campaign)) == 1
