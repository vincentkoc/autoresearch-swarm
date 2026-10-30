"""Held-out panels are frozen before fitting and shared admission cannot expose them."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.benchmark_guard import benchmark_plan_key, benchmark_seed_key
from research_swarm.control.benchmark_models import FreezePolicyBenchmark
from research_swarm.control.commands import SubmitTrial
from research_swarm.control.dispatch import COMMANDS
from research_swarm.control.learning_models import QualifyTrajectoryDataset
from research_swarm.control.learning_training_models import PreparePolicyInputs
from research_swarm.control.models import SearchConfiguration
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.learning import CategoricalPolicy
from research_swarm.domain.types import new_id
from research_swarm.evaluation.statistics import FixedPairedPolicy
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from tests.test_learning_admission import admit_training, finish_training, training_admission
from tests.test_learning_profiles import scoped_client
from tests.test_learning_publication import current, publish
from tests.test_local_pipeline import pipeline as pipeline
from tests.test_proposals import PROPOSAL_LIMITS


def benchmark_intent(training):
    return FreezePolicyBenchmark(
        benchmark_id=new_id(),
        training_admission_id=training.admission_id,
        inputs_id=training.inputs_id,
        profile_id=training.profile_id,
        gate=FixedPairedPolicy(seeds=(101, 102, 103, 104, 105)),
        per_arm_limits=(
            BudgetLimit(dimension="attempts", limit=4, unit="count"),
            BudgetLimit(dimension="execution_ms", limit=300000, unit="ms"),
            BudgetLimit(dimension="proposal_requests", limit=1, unit="count"),
            BudgetLimit(dimension="trial_admissions", limit=2, unit="count"),
        ),
    )


def freeze(client, intent, key="freeze-benchmark"):
    return client.command("learning.freeze_benchmark", intent, idempotency_key=key)["result"]


def ordinary_training(client, campaign, training):
    return client.command(
        "trial.submit",
        SubmitTrial(
            campaign_id=training.campaign_id,
            candidate_id=training.candidate_id,
            validation_id=training.validation_id,
            seed=training.seed,
        ),
        expected_revision=campaign["revision"],
        idempotency_key="ordinary-training",
    )["result"]


def requalify_inputs(pipeline, training):
    prepared = pipeline.client.get("learning_inputs", training.inputs_id)["data"]
    dataset = pipeline.client.get("learning_dataset", prepared["request"]["dataset_id"])["data"]
    qualified = pipeline.client.command(
        "learning.qualify_dataset",
        QualifyTrajectoryDataset(plan_id=dataset["plan_id"], dataset_id=new_id()),
        idempotency_key="another-qualification",
    )["result"]
    return pipeline.client.command(
        "learning.prepare_training_inputs",
        PreparePolicyInputs(binding_id=new_id(), dataset_id=qualified["id"]),
        idempotency_key="another-binding",
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
class TestPolicyBenchmark:
    def test_freeze_precedes_training_and_cannot_be_omitted(self, pipeline, tmp_path):
        campaign, training, _ = training_admission(pipeline, tmp_path)
        assert "benchmark_id" not in training.model_dump(mode="json")
        intent = benchmark_intent(training)
        definition = COMMANDS["learning.freeze_benchmark"]
        scopes = {definition.scope, *definition.additional_scopes}
        assert scopes == {"learning.manage", "read"}
        for missing in scopes:
            with pytest.raises(ResearchError) as denied:
                freeze(scoped_client(pipeline.runtime, scopes - {missing}), intent, missing)
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        frozen = freeze(scoped_client(pipeline.runtime, scopes), intent)
        assert freeze(pipeline.client, intent, "repeat") == frozen
        assert len(pipeline.client.list(RecordQuery(kind="benchmark_seed"))["items"]) == 5
        for altered in (training, training.model_copy(update={"admission_id": new_id()})):
            with pytest.raises(ResearchError, match="reserved benchmark"):
                admit_training(pipeline.client, campaign, altered, new_id())
        admitted = admit_training(
            pipeline.client,
            campaign,
            training.model_copy(update={"benchmark_id": intent.benchmark_id}),
        )
        finish_training(pipeline, admitted)
        published = publish(pipeline.client, admitted)
        baseline = CategoricalPolicy.model_validate(frozen["data"]["baseline_policy"])
        assert current(pipeline, admitted).policy.parent_digest == baseline.digest
        assert published["data"]["training"]["trial_id"] == admitted["data"]["trial_id"]

    def test_prior_fitting_cannot_be_hidden_by_requalification(self, pipeline, tmp_path):
        campaign, training, _ = training_admission(pipeline, tmp_path)
        admit_training(pipeline.client, campaign, training)
        other = requalify_inputs(pipeline, training)
        prepared = pipeline.client.get("learning_inputs", training.inputs_id)["data"]
        assert other["data"]["dataset_digest"] != prepared["dataset_digest"]
        assert other["data"]["plan_digest"] == prepared["plan_digest"]
        intent = benchmark_intent(training).model_copy(
            update={"inputs_id": other["id"], "training_admission_id": new_id()}
        )
        with pytest.raises(ResearchError, match="precede fitting"):
            freeze(pipeline.client, intent)
        assert not pipeline.client.list(RecordQuery(kind="policy_benchmark"))["items"]

    def test_reservation_cannot_be_hidden_by_requalification(self, pipeline, tmp_path):
        campaign, training, _ = training_admission(pipeline, tmp_path)
        intent = benchmark_intent(training)
        freeze(pipeline.client, intent)
        other = requalify_inputs(pipeline, training)
        with pytest.raises(ResearchError, match="reserved benchmark"):
            admit_training(
                pipeline.client,
                campaign,
                training.model_copy(update={"inputs_id": other["id"], "admission_id": new_id()}),
            )
        with pytest.raises(ResearchError, match="already reserved"):
            freeze(
                pipeline.client,
                benchmark_intent(training).model_copy(
                    update={"inputs_id": other["id"], "training_admission_id": new_id()}
                ),
                "second-panel",
            )

    def test_exposed_seed_and_changed_panel_refuse(self, pipeline, tmp_path):
        _, training, _ = training_admission(pipeline, tmp_path)
        intent = benchmark_intent(training)
        with pytest.raises(ResearchError, match="already admitted"):
            freeze(
                pipeline.client,
                intent.model_copy(
                    update={"gate": FixedPairedPolicy(seeds=(7, 102, 103, 104, 105))}
                ),
                "exposed-panel",
            )
        freeze(pipeline.client, intent)
        with pytest.raises(ResearchError) as changed:
            freeze(
                pipeline.client,
                intent.model_copy(
                    update={"gate": FixedPairedPolicy(seeds=(201, 202, 203, 204, 205))}
                ),
                "changed",
            )
        assert changed.value.code == ErrorCode.IDEMPOTENCY_CONFLICT

    def test_freeze_event_rolls_back_all_reservations(self, pipeline, tmp_path, monkeypatch):
        campaign, training, _ = training_admission(pipeline, tmp_path)
        intent = benchmark_intent(training)
        event = Transaction.event

        def fail(transaction, event_type, *args, **kwargs):
            if event_type == "learning.benchmark_frozen":
                raise RuntimeError("benchmark event failed")
            return event(transaction, event_type, *args, **kwargs)

        monkeypatch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="benchmark event failed"):
            freeze(pipeline.client, intent)
        for kind in ("policy_benchmark", "benchmark_plan", "benchmark_training", "benchmark_seed"):
            assert not pipeline.client.list(RecordQuery(kind=kind))["items"]
        monkeypatch.setattr(Transaction, "event", event)
        assert admit_training(pipeline.client, campaign, training)["id"] == training.admission_id

    @pytest.mark.parametrize("when", ["before", "after"])
    @pytest.mark.parametrize("extra_input", [False, True])
    def test_ordinary_training_cannot_bypass_freeze(self, pipeline, tmp_path, when, extra_input):
        campaign, training, _ = training_admission(pipeline, tmp_path, extra_input=extra_input)
        intent = benchmark_intent(training)
        if when == "before":
            ordinary_training(pipeline.client, campaign, training)
            with pytest.raises(ResearchError, match="precede fitting"):
                freeze(pipeline.client, intent)
        else:
            freeze(pipeline.client, intent)
            with pytest.raises(ResearchError, match="qualified training admission"):
                ordinary_training(pipeline.client, campaign, training)

    @pytest.mark.parametrize("target", ["research_trial", "training", "ordinary_training"])
    def test_freeze_and_ordinary_trial_race_cannot_both_admit(self, pipeline, tmp_path, target):
        training_campaign, training, _ = training_admission(pipeline, tmp_path)
        intent = benchmark_intent(training)
        campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
        clients = [
            ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
            for _ in range(2)
        ]
        barrier = Barrier(2)

        def issue(index):
            barrier.wait(timeout=5)
            try:
                if index == 0:
                    return freeze(clients[index], intent)
                if target == "training":
                    return admit_training(clients[index], training_campaign, training)
                if target == "ordinary_training":
                    return ordinary_training(clients[index], training_campaign, training)
                return clients[index].command(
                    "trial.submit",
                    SubmitTrial(
                        campaign_id=campaign["id"],
                        candidate_id=pipeline.trial["data"]["candidate_id"],
                        validation_id=pipeline.trial["data"]["validation_id"],
                        seed=101,
                    ),
                    expected_revision=campaign["revision"],
                    idempotency_key="expose-seed",
                )["result"]
            except ResearchError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(issue, range(2)))
        assert sum(isinstance(result, dict) for result in results) == 1
        assert ErrorCode.COMPARISON_INELIGIBLE in results


def test_seed_and_plan_reservation_identity_is_content_based():
    digest = content_digest(b"same-content")
    assert benchmark_seed_key(digest, 101) == "145bc42c-cb73-5e11-938a-51c6e2f28ee7"
    assert benchmark_plan_key(digest) == "f366c6e6-a370-52dd-87e8-1379c5519e9f"
    assert benchmark_seed_key(digest, 101) != benchmark_seed_key(digest, 102)
    assert benchmark_plan_key(digest) != benchmark_seed_key(digest, 101)
