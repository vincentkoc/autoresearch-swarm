"""Atomic benchmark arms use ordinary jobs and preserve fixed admission slots."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.benchmark_execution_models import (
    BenchmarkExecution,
    StartPolicyBenchmark,
)
from research_swarm.control.benchmark_trials import BenchmarkTrialAuthority, execution_digest
from research_swarm.control.commands import SubmitTrial
from research_swarm.control.dispatch import COMMANDS
from research_swarm.control.models import SearchConfiguration
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.learning.test_learning_admission import (
    admit_training,
    finish_training,
    training_admission,
)
from tests.learning.test_learning_benchmarks import benchmark_intent, freeze
from tests.learning.test_learning_inputs import context
from tests.learning.test_learning_profiles import scoped_client
from tests.learning.test_learning_publication import invalidate, publish
from tests.search.test_proposals import PROPOSAL_LIMITS, baseline, proposal_intent


def ready_benchmark(pipeline, tmp_path):
    campaign, training, research_receipt = training_admission(pipeline, tmp_path)
    benchmark = freeze(pipeline.client, benchmark_intent(training))
    admission = admit_training(
        pipeline.client, campaign, training.model_copy(update={"benchmark_id": benchmark["id"]})
    )
    finish_training(pipeline, admission)
    publish(pipeline.client, admission)
    intent = StartPolicyBenchmark(
        benchmark_id=benchmark["id"], proposer_id=pipeline.runtime.principal().id
    )
    return benchmark, intent, research_receipt


def start(client, intent, key="start-benchmark"):
    return client.command("learning.start_benchmark", intent, idempotency_key=key)["result"]


def count(runtime, kind):
    return runtime.database.store.read(
        lambda sql: sql.execute("SELECT COUNT(*) FROM records WHERE kind=?", (kind,)).one()[0]
    )


def direct_trial(client, campaign, arm, seed):
    return client.command(
        "trial.submit",
        SubmitTrial(
            campaign_id=campaign["id"],
            candidate_id=arm["initial_candidate_id"],
            validation_id=arm["initial_validation_id"],
            seed=seed,
        ),
        expected_revision=campaign["revision"],
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
class TestBenchmarkExecution:
    def test_complete_inventory_and_native_comparator_with_no_unplanned_slots(
        self, pipeline, tmp_path
    ):
        benchmark, intent, _ = ready_benchmark(pipeline, tmp_path)
        before = {kind: count(pipeline.runtime, kind) for kind in ("campaign", "trial", "job")}
        definition = COMMANDS["learning.start_benchmark"]
        scopes = {definition.scope, *definition.additional_scopes}
        assert scopes == {
            "learning.manage",
            "campaign.manage",
            "candidate.propose",
            "candidate.validate",
            "trial.submit",
            "read",
        }
        for missing in scopes:
            with pytest.raises(ResearchError) as denied:
                start(scoped_client(pipeline.runtime, scopes - {missing}), intent, missing)
            assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
        row = start(scoped_client(pipeline.runtime, scopes), intent)
        execution = BenchmarkExecution.model_validate(row["data"])
        assert len(execution.arm_ids) == 10
        assert count(pipeline.runtime, "campaign") == before["campaign"] + 10
        assert count(pipeline.runtime, "trial") == before["trial"] + 10
        assert count(pipeline.runtime, "job") == before["job"] + 20
        arms = [
            pipeline.client.get("benchmark_arm", identifier)["data"]
            for identifier in execution.arm_ids
        ]
        assert {(arm["seed"], arm["side"]) for arm in arms} == {
            (seed, side)
            for seed in benchmark["data"]["request"]["gate"]["seeds"]
            for side in ("baseline", "challenger")
        }
        assert len({arm["proposal_request_id"] for arm in arms}) == 10
        for arm in arms:
            campaign = pipeline.client.get("campaign", arm["campaign_id"])
            assert campaign["state"] == "running"
            assert campaign["data"]["parent_campaign_id"] is None
            assert (
                campaign["data"]["contract"]["limits"]
                == benchmark["data"]["request"]["per_arm_limits"]
            )
            assert (
                pipeline.client.get("benchmark_trial_slot", arm["comparator_trial_id"])["state"]
                == "consumed"
            )
            assert (
                pipeline.client.get("benchmark_trial_slot", arm["candidate_trial_id"])["state"]
                == "reserved"
            )
        arm = arms[0]
        campaign = pipeline.client.get("campaign", arm["campaign_id"])
        trial = pipeline.client.get("trial", arm["comparator_trial_id"])
        view = replace(
            pipeline, campaign=campaign, trial=trial, client=scoped_client(pipeline.runtime, {"*"})
        )
        assert baseline(view)["state"] == "valid"
        campaign = pipeline.client.get("campaign", arm["campaign_id"])
        for seed in (arm["seed"], 999):
            with pytest.raises(ResearchError, match="benchmark arm authority"):
                direct_trial(pipeline.client, campaign, arm, seed)
        with pytest.raises(ResearchError, match="planned held-out proposal"):
            view.client.command(
                "search.request_proposal",
                proposal_intent(view, pipeline.runtime.principal()),
                expected_revision=0,
                idempotency_key="unplanned-request",
            )
        assert count(pipeline.runtime, "trial") == before["trial"] + 10
        assert start(pipeline.client, intent, "replay") == row

    def test_atomic_event_failure_leaves_no_arms_or_budget_accounts(
        self, pipeline, tmp_path, monkeypatch
    ):
        _, intent, _ = ready_benchmark(pipeline, tmp_path)
        before = {
            kind: count(pipeline.runtime, kind)
            for kind in ("campaign", "candidate", "trial", "job")
        }

        def active_retentions():
            return pipeline.runtime.database.store.read(
                lambda sql: sql.execute(
                    "SELECT COUNT(*) FROM records WHERE kind='artifact_reference' "
                    "AND state='retained'"
                ).one()[0]
            )

        retained = active_retentions()
        accounts = pipeline.runtime.database.store.read(
            lambda sql: sql.execute("SELECT COUNT(*) FROM budgets").one()[0]
        )
        event = Transaction.event

        def fail(transaction, event_type, *args, **kwargs):
            if event_type == "learning.benchmark_started":
                raise RuntimeError("benchmark setup event failed")
            return event(transaction, event_type, *args, **kwargs)

        monkeypatch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="setup event failed"):
            start(pipeline.client, intent)
        assert all(count(pipeline.runtime, kind) == value for kind, value in before.items())
        assert active_retentions() == retained
        assert all(
            count(pipeline.runtime, kind) == 0
            for kind in ("benchmark_execution", "benchmark_arm", "benchmark_trial_slot")
        )
        assert (
            pipeline.runtime.database.store.read(
                lambda sql: sql.execute("SELECT COUNT(*) FROM budgets").one()[0]
            )
            == accounts
        )
        monkeypatch.setattr(Transaction, "event", event)
        assert len(start(pipeline.client, intent)["data"]["arm_ids"]) == 10

    def test_source_preflight_correction_cannot_start_arms(self, pipeline, tmp_path, monkeypatch):
        benchmark, intent, receipt = ready_benchmark(pipeline, tmp_path)
        fetch = pipeline.runtime.artifacts.store.fetch

        def corrected(workspace, blob, destination):
            result = fetch(workspace, blob, destination)
            if blob.reference.id == benchmark["data"]["initial_source"]["id"]:
                invalidate(pipeline.client, "evaluation_receipt", receipt["id"])
            return result

        monkeypatch.setattr(pipeline.runtime.artifacts.store, "fetch", corrected)
        with pytest.raises(ResearchError):
            start(pipeline.client, intent)
        assert count(pipeline.runtime, "benchmark_arm") == 0

    def test_concurrent_setup_and_lost_reply_recover_one_inventory(
        self, pipeline, tmp_path, monkeypatch
    ):
        _, intent, _ = ready_benchmark(pipeline, tmp_path)
        runtimes = [LocalRuntime.open(pipeline.runtime.root) for _ in range(2)]
        clients = [ResearchClient(LocalTransport(runtime)) for runtime in runtimes]
        execute = runtimes[0].database.execute

        def lose(ctx, operation, *args, **kwargs):
            result = execute(ctx, operation, *args, **kwargs)
            if operation == "learning.start_benchmark":
                raise OSError("lost setup reply")
            return result

        monkeypatch.setattr(runtimes[0].database, "execute", lose)
        barrier = Barrier(2)

        def issue(index):
            barrier.wait(timeout=5)
            return start(clients[index], intent, "concurrent-" + str(index))

        with ThreadPoolExecutor(max_workers=2) as pool:
            first, second = pool.map(issue, range(2))
        assert first == second and count(pipeline.runtime, "benchmark_arm") == 10

        def unavailable(*args, **kwargs):
            raise OSError("source offline")

        monkeypatch.setattr(pipeline.runtime.artifacts.store, "fetch", unavailable)
        assert start(pipeline.client, intent, "after-restart") == first
        with pytest.raises(ResearchError) as conflict:
            start(
                pipeline.client, intent.model_copy(update={"proposer_id": new_id()}), "change-agent"
            )
        assert conflict.value.code == ErrorCode.IDEMPOTENCY_CONFLICT

    def test_private_comparator_authority_is_bound_and_cannot_be_reused(
        self, pipeline, tmp_path, monkeypatch
    ):
        _, intent, _ = ready_benchmark(pipeline, tmp_path)
        submit = pipeline.runtime.service.benchmark_execution.submit_trial
        checked = []

        def guarded(transaction, request, authority):
            if not checked:
                for changed in (
                    replace(authority, arm_id=new_id()),
                    replace(authority, role="candidate"),
                    replace(authority, execution_digest="sha256:" + "0" * 64),
                ):
                    with pytest.raises(ResearchError):
                        submit(transaction, request, changed)
                with pytest.raises(ResearchError):
                    submit(transaction, request.model_copy(update={"seed": 999}), authority)
                checked.append(True)
            return submit(transaction, request, authority)

        monkeypatch.setattr(pipeline.runtime.service.benchmark_execution, "submit_trial", guarded)
        execution = BenchmarkExecution.model_validate(start(pipeline.client, intent)["data"])
        assert checked == [True]
        arm = pipeline.client.get("benchmark_arm", execution.arm_ids[0])["data"]
        request = SubmitTrial(
            campaign_id=arm["campaign_id"],
            candidate_id=arm["initial_candidate_id"],
            validation_id=arm["initial_validation_id"],
            seed=arm["seed"],
        )
        authority = BenchmarkTrialAuthority(
            arm["campaign_id"], "comparator", execution_digest(execution)
        )
        before = pipeline.client.budget(arm["campaign_id"])
        with pytest.raises(ResearchError):
            pipeline.runtime.database.execute(
                context(pipeline.runtime, new_id()),
                "trial.submit",
                request.model_dump(mode="json"),
                "trial.submit",
                lambda tx: pipeline.runtime.service._submit_trial(
                    tx, request, benchmark_authority=authority
                ),
            )
        assert pipeline.client.budget(arm["campaign_id"]) == before
