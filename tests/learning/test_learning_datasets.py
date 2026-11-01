"""Controller qualification joins real native outcomes to frozen policy decisions."""

import shutil
from decimal import Decimal

import pytest

from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.learning_datasets import require_current_dataset
from research_swarm.control.learning_models import (
    QualifiedTrajectoryDataset,
    QualifyTrajectoryDataset,
)
from research_swarm.control.models import SearchConfiguration
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.learning import CategoricalPolicy, PolicyAction
from research_swarm.domain.types import new_id
from research_swarm.learning.reinforce import update
from research_swarm.state.database import Transaction
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.learning.test_learning_control import prepare
from tests.search.test_proposals import PROPOSAL_LIMITS
from tests.search.test_search_admission import admit


def planned(pipeline, tmp_path, count=1):
    prepared = prepare(pipeline, tmp_path, count=count)
    source = tmp_path / "another-action"
    shutil.copytree(pipeline.source, source)
    train = source / "train.py"
    train.write_text(train.read_text().replace("LEARNING_RATE = 0.35", "LEARNING_RATE = 0.6"))
    alternative = pipeline.client.capture_project(source, idempotency_key="new-fast-action")
    artifacts = {**prepared.intent.action_artifacts, "fast": alternative}
    prepared.intent = prepared.intent.model_copy(
        update={
            "action_artifacts": artifacts,
            "policy": CategoricalPolicy(
                actions=tuple(
                    PolicyAction(id=name, source_digest=ref.digest)
                    for name, ref in artifacts.items()
                ),
                logits=(0.0, 0.0),
            ),
        }
    )
    prepared.freeze(pipeline.client)
    return prepared


def finish_episode(pipeline, prepared, *, index=0, revision=1):
    prepared.decide(index=index, key=f"decision-{index}")
    pipeline.client.get("campaign", pipeline.campaign["id"])
    prepared.agent.command(
        "search.respond", prepared.response(index=index), idempotency_key=f"response-{index}"
    )
    admitted = admit(pipeline, prepared.requests[index]["id"], revision=revision)["result"]
    assert admitted["transition"]["data"]["outcome"] == "admitted"
    attempt = pipeline.worker.claim(pipeline.ready())
    pipeline.worker.execute(attempt["id"])
    job = pipeline.ready()
    authorization = pipeline.client.command(
        "evaluation.authorize",
        AuthorizeEvaluation(job_id=job["id"], evaluator_id=pipeline.runtime.principal().id),
        idempotency_key=f"authorize-{index}",
        expected_revision=job["revision"],
    )["result"]
    evaluation = pipeline.worker.claim(pipeline.ready())
    pipeline.worker.execute(evaluation["id"])
    receipt = pipeline.client.command(
        "evaluation.complete",
        CompleteEvaluation(request_id=authorization["id"], attempt_id=evaluation["id"]),
        idempotency_key=f"evaluate-{index}",
    )["result"]
    return admitted, receipt, (attempt["id"], evaluation["id"])


def qualify(pipeline, prepared, *, identifier=None, key="qualify"):
    return pipeline.client.command(
        "learning.qualify_dataset",
        QualifyTrajectoryDataset(
            plan_id=prepared.intent.plan_id, dataset_id=identifier or new_id()
        ),
        idempotency_key=key,
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
def test_native_qualification_is_complete_costed_and_trainable(pipeline, tmp_path):
    prepared = planned(pipeline, tmp_path)
    admitted, receipt, attempts = finish_episode(pipeline, prepared)
    result = qualify(pipeline, prepared)
    qualified = QualifiedTrajectoryDataset.model_validate(result["data"])
    dataset = qualified.dataset
    assert dataset.planned_episode_ids == (prepared.requests[0]["id"],)
    row = dataset.trajectories[0]
    assert row.trial_id == admitted["trial"]["id"] and row.candidate_evaluation_id == receipt["id"]
    assert (
        row.comparator_evaluation_id
        == prepared.requests[0]["data"]["context"]["incumbent_evaluation_id"]
    )
    actual = sum(
        Decimal(pipeline.client.get("run_result", identifier)["data"]["consumed"]["execution_ms"])
        for identifier in attempts
    )
    assert row.consumed == {
        "proposal_requests": Decimal(1),
        "attempts": Decimal(2),
        "execution_ms": actual,
    }
    assert len([ref for ref in row.evidence if ref.kind == "budget_settlement"]) == 3
    updated = update(prepared.intent.policy, dataset, learning_rate=0.5)
    assert updated.dataset_digest == dataset.digest
    assert updated.policy.parent_digest == prepared.intent.policy.digest
    current = pipeline.runtime.database.store.read(
        lambda connection: require_current_dataset(
            Transaction(
                connection,
                CommandContext(
                    principal=pipeline.runtime.principal(), idempotency_key="fresh-validation"
                ),
            ),
            result["id"],
        )
    )
    assert current == qualified


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
class TestDatasetQualification:
    def test_missing_decision_or_response_never_creates_partial_dataset(self, pipeline, tmp_path):
        from research_swarm.control.queries import RecordQuery
        from research_swarm.domain.errors import ResearchError

        prepared = planned(pipeline, tmp_path)
        with pytest.raises(ResearchError) as missing:
            qualify(pipeline, prepared)
        assert missing.value.details["outcome"] == "decision_missing"
        prepared.decide()
        with pytest.raises(ResearchError) as unanswered:
            qualify(pipeline, prepared, key="unanswered")
        assert unanswered.value.details["outcome"] == "response_missing"
        assert pipeline.client.list(RecordQuery(kind="learning_dataset"))["items"] == []

    def test_only_predeclared_development_subset_is_qualified(self, pipeline, tmp_path):
        prepared = planned(pipeline, tmp_path, count=2)
        finish_episode(pipeline, prepared, revision=2)
        result = QualifiedTrajectoryDataset.model_validate(qualify(pipeline, prepared)["data"])
        assert result.dataset.planned_episode_ids == (prepared.requests[0]["id"],)
        held = pipeline.client.get("learning_episode", prepared.requests[1]["id"])
        assert held["data"]["episode"]["split"] == "held_out"
        assert (
            pipeline.client.get("proposal_request", prepared.requests[1]["id"])["state"]
            == "pending"
        )

    @pytest.mark.parametrize("side", ["candidate", "comparator"])
    def test_corrections_block_fresh_qualification_and_revalidation(self, pipeline, tmp_path, side):
        from research_swarm.control.promotions import InvalidateEvidence
        from research_swarm.control.validity import EvidenceReference
        from research_swarm.domain.errors import ResearchError

        prepared = planned(pipeline, tmp_path)
        _, receipt, _ = finish_episode(pipeline, prepared)
        result = qualify(pipeline, prepared)
        evaluation_id = (
            receipt["id"]
            if side == "candidate"
            else prepared.requests[0]["data"]["context"]["incumbent_evaluation_id"]
        )
        pipeline.client.command(
            "evidence.invalidate",
            InvalidateEvidence(
                target=EvidenceReference(kind="evaluation_receipt", id=evaluation_id),
                reason="Invalid comparison evidence",
            ),
            idempotency_key="invalidate",
        )
        with pytest.raises(ResearchError):
            qualify(pipeline, prepared, key="after-correction")
        with pytest.raises(ResearchError):
            pipeline.runtime.database.store.read(
                lambda connection: require_current_dataset(
                    Transaction(
                        connection,
                        CommandContext(
                            principal=pipeline.runtime.principal(), idempotency_key="fresh"
                        ),
                    ),
                    result["id"],
                )
            )
        # The immutable historical receipt survives; it is not renewed authority.
        assert qualify(pipeline, prepared, identifier=result["id"], key="history") == result

    @pytest.mark.parametrize("change", ["settlement", "old_attempt", "parent", "decision"])
    def test_corrupted_or_hidden_evidence_blocks_complete_dataset(self, pipeline, tmp_path, change):
        from research_swarm.domain.canonical import record_digest, strict_json_loads
        from research_swarm.domain.errors import ResearchError
        from research_swarm.state.database import encode

        prepared = planned(pipeline, tmp_path)
        _, _, attempts = finish_episode(pipeline, prepared)
        context = CommandContext(principal=pipeline.runtime.principal(), idempotency_key="fixture")

        def mutate(connection):
            tx = Transaction(connection, context)
            if change == "settlement":
                row = connection.execute(
                    "SELECT response FROM budget_settlements WHERE settlement_id=?", (attempts[0],)
                ).one()
                response = strict_json_loads(row["response"])
                response["actual"]["execution_ms"] = str(
                    Decimal(response["actual"]["execution_ms"]) + 1
                )
                digest = record_digest(
                    "research.budget-settlement",
                    {key: response[key] for key in ("reservation_id", "actual")},
                )
                connection.execute(
                    "UPDATE budget_settlements SET request_digest=?,response=? "
                    "WHERE settlement_id=?",
                    (digest, encode(response), attempts[0]),
                )
            elif change == "old_attempt":
                original = tx.get("attempt", attempts[0])
                data = original["data"]
                identifier = new_id()
                data["dispatch"]["attempt_id"] = identifier
                data["publication_status"] = "pending"
                tx.insert(
                    "attempt",
                    identifier,
                    data,
                    state="execution_unknown",
                    campaign_id=original["campaign_id"],
                    parent_id=original["parent_id"],
                )
            elif change == "parent":
                connection.execute(
                    "UPDATE records SET parent_id=? WHERE kind='attempt' AND id=?",
                    (new_id(), attempts[0]),
                )
            else:
                row = tx.get("learning_decision", prepared.requests[0]["id"])
                data = row["data"]
                data["decision"]["draw"] = (data["decision"]["draw"] + 0.2) % 1
                connection.execute(
                    "UPDATE records SET payload=? WHERE kind='learning_decision' AND id=?",
                    (encode(data), row["id"]),
                )

        pipeline.runtime.database.store.write(mutate)
        with pytest.raises(ResearchError):
            qualify(pipeline, prepared)

    def test_dataset_event_failure_rolls_back_and_retries_same_intent(
        self, pipeline, tmp_path, monkeypatch
    ):
        from research_swarm.control.queries import RecordQuery

        prepared = planned(pipeline, tmp_path)
        finish_episode(pipeline, prepared)
        identifier = new_id()
        original = Transaction.event

        def fail(transaction, event_type, *args, **kwargs):
            if event_type == "learning.dataset_qualified":
                raise RuntimeError("dataset event failed")
            return original(transaction, event_type, *args, **kwargs)

        monkeypatch.setattr(Transaction, "event", fail)
        with pytest.raises(RuntimeError, match="dataset event"):
            qualify(pipeline, prepared, identifier=identifier)
        assert pipeline.client.list(RecordQuery(kind="learning_dataset"))["items"] == []
        monkeypatch.setattr(Transaction, "event", original)
        result = qualify(pipeline, prepared, identifier=identifier)
        assert result["id"] == identifier

    def test_corrected_earlier_accepted_attempt_invalidates_cost(self, pipeline, tmp_path):
        from research_swarm.control.learning_costs import episode_cost
        from research_swarm.control.models import TrialData
        from research_swarm.control.promotions import InvalidateEvidence
        from research_swarm.control.search_models import ProposalRequest
        from research_swarm.control.validity import EvidenceReference
        from research_swarm.domain.errors import ErrorCode, ResearchError
        from research_swarm.domain.evidence import RunResult
        from research_swarm.state.ledger import BudgetLedger

        prepared = planned(pipeline, tmp_path)
        _, _, attempts = finish_episode(pipeline, prepared)
        prior_id = new_id()
        context = CommandContext(
            principal=pipeline.runtime.principal(), idempotency_key="legacy-attempt"
        )

        # A synthetic legacy accepted failure exercises prior-attempt accounting.
        # It is not a second native-execution qualification.
        def prior(connection):
            tx = Transaction(connection, context)
            original = tx.get("attempt", attempts[0])
            data = original["data"]
            actual = {"attempts": Decimal(1), "execution_ms": Decimal(7)}
            reservation = BudgetLedger.reserve(tx, original["campaign_id"], prior_id, actual)
            BudgetLedger.settle(tx, reservation.id, prior_id, actual)
            data["dispatch"]["attempt_id"] = prior_id
            data["dispatch"]["reservation_ids"] = [reservation.id]
            data["signed_assignment"] = None
            data["assignment_time"] = None
            tx.insert(
                "attempt",
                prior_id,
                data,
                state="failed",
                campaign_id=original["campaign_id"],
                parent_id=original["parent_id"],
            )
            result = (
                RunResult.model_validate(tx.get("run_result", attempts[0])["data"])
                .model_copy(
                    update={
                        "attempt_id": prior_id,
                        "status": "failed",
                        "exit_code": 1,
                        "consumed": actual,
                        "result_digest": None,
                    }
                )
                .sealed()
            )
            tx.insert(
                "run_result",
                prior_id,
                result.model_dump(mode="json"),
                state="immutable",
                campaign_id=original["campaign_id"],
                parent_id=prior_id,
            )

        pipeline.runtime.database.store.write(prior)
        first = qualify(pipeline, prepared)
        data = QualifiedTrajectoryDataset.model_validate(first["data"])
        assert data.dataset.trajectories[0].consumed["attempts"] == 3
        assert any(
            ref.kind == "run_result" and ref.id == prior_id
            for ref in data.dataset.trajectories[0].evidence
        )
        pipeline.client.command(
            "evidence.invalidate",
            InvalidateEvidence(
                target=EvidenceReference(kind="run_result", id=prior_id),
                reason="Earlier execution cost evidence invalid",
            ),
            idempotency_key="invalidate-prior",
        )

        def inspect_cost(connection):
            tx = Transaction(connection, context)
            trial_id = data.dataset.trajectories[0].trial_id
            return episode_cost(
                tx,
                prepared.requests[0]["id"],
                ProposalRequest.model_validate(prepared.requests[0]["data"]),
                trial_id,
                TrialData.model_validate(tx.get("trial", trial_id)["data"]),
                [],
            )

        with pytest.raises(ResearchError) as cost:
            pipeline.runtime.database.store.read(inspect_cost)
        assert cost.value.code == ErrorCode.EVIDENCE_INVALIDATED
        with pytest.raises(ResearchError) as fresh:
            qualify(pipeline, prepared, key="fresh-after-prior-correction")
        assert fresh.value.code in {ErrorCode.EVIDENCE_INVALIDATED, ErrorCode.COMPARISON_INELIGIBLE}
        with pytest.raises(ResearchError) as revalidate:
            pipeline.runtime.database.store.read(
                lambda connection: require_current_dataset(
                    Transaction(connection, context), first["id"]
                )
            )
        assert revalidate.value.code in {
            ErrorCode.EVIDENCE_INVALIDATED,
            ErrorCode.COMPARISON_INELIGIBLE,
        }

    @pytest.mark.parametrize("kind", ["not_started", "closed_unaccepted"])
    def test_prior_nonstarted_and_closed_costs_remain_in_batch(self, pipeline, tmp_path, kind):
        from research_swarm.control.acceptance import dispatch_digest
        from research_swarm.control.closure import AttemptClosure
        from research_swarm.control.observations import PublishAttempt
        from research_swarm.control.reconciliation import AttemptSubmission, publication_digest
        from research_swarm.control.worker_records import AttemptData
        from research_swarm.domain.evidence import ArtifactRef
        from research_swarm.domain.types import utc_now
        from research_swarm.domain.workers import WorkerBinding
        from research_swarm.state.ledger import BudgetLedger

        prepared = planned(pipeline, tmp_path)
        _, _, attempts = finish_episode(pipeline, prepared)
        prior_id = new_id()
        context = CommandContext(
            principal=pipeline.runtime.principal(), idempotency_key="legacy-cost"
        )

        # Synthetic controller history isolates cost semantics; current stages run natively.
        def insert_history(connection):
            tx = Transaction(connection, context)
            original = tx.get("attempt", attempts[0])
            data = original["data"]
            actual = {"attempts": Decimal(1), "execution_ms": Decimal(7)}
            reservation = BudgetLedger.reserve(tx, original["campaign_id"], prior_id, actual)
            BudgetLedger.settle(tx, reservation.id, prior_id, actual)
            data["dispatch"]["attempt_id"] = prior_id
            data["dispatch"]["reservation_ids"] = [reservation.id]
            data["signed_assignment"] = data["assignment_time"] = None
            publication = PublishAttempt(
                attempt_id=prior_id,
                journal_artifact=ArtifactRef.model_validate(data["outcome_artifact"]),
            )
            data["result_digest"] = publication_digest(publication)
            if kind == "closed_unaccepted":
                data["publication_status"] = kind
                data["result_digest"] = None
                data["closure_receipt_id"] = new_id()
                binding = WorkerBinding.model_validate(
                    {name: data["dispatch"][name] for name in WorkerBinding.model_fields}
                )
                submission_id = new_id()
                submission = AttemptSubmission(
                    publication=publication,
                    publication_digest=publication_digest(publication),
                    original_binding=binding,
                    submitted_by=context.principal.id,
                    submitted_at=utc_now(),
                )
                tx.insert(
                    "attempt_submission",
                    submission_id,
                    submission.model_dump(mode="json"),
                    state="immutable",
                    campaign_id=original["campaign_id"],
                    parent_id=prior_id,
                )
                closure = AttemptClosure(
                    attempt_id=prior_id,
                    submission_id=submission_id,
                    publication_digest=submission.publication_digest,
                    original_binding=binding,
                    journal_artifact=publication.journal_artifact,
                    native_outcome=None,
                    not_started=True,
                    consumed=actual,
                    controller_id=data["dispatch"]["controller_id"],
                    control_epoch=data["dispatch"]["control_epoch"],
                    operator_id=context.principal.id,
                    recorded_at=utc_now(),
                    reason="Synthetic prior no-start closure",
                )
                tx.insert(
                    "attempt_closure",
                    data["closure_receipt_id"],
                    closure.model_dump(mode="json"),
                    state="immutable",
                    campaign_id=original["campaign_id"],
                    parent_id=prior_id,
                )
                lease = tx.get("acceptance_lease", attempts[0])["data"]
                lease.update(
                    attempt_id=prior_id,
                    disposition=kind,
                    dispatch_digest=dispatch_digest(AttemptData.model_validate(data)),
                )
                tx.insert(
                    "acceptance_lease",
                    prior_id,
                    lease,
                    state="settled",
                    campaign_id=original["campaign_id"],
                )
            tx.insert(
                "attempt",
                prior_id,
                data,
                state=kind,
                campaign_id=original["campaign_id"],
                parent_id=original["parent_id"],
            )

        pipeline.runtime.database.store.write(insert_history)
        qualified = QualifiedTrajectoryDataset.model_validate(qualify(pipeline, prepared)["data"])
        row = qualified.dataset.trajectories[0]
        assert row.consumed["attempts"] == 3
        assert any(ref.kind == "attempt" and ref.id == prior_id for ref in row.evidence)
        assert not any(ref.kind == "run_result" and ref.id == prior_id for ref in row.evidence)
        if kind == "closed_unaccepted":
            assert any(ref.kind == "attempt_closure" for ref in row.evidence)

    def test_http_qualification_and_local_replay_share_one_receipt(self, pipeline, tmp_path):
        from research_swarm.api.http import create_app
        from research_swarm.client import ResearchClient
        from research_swarm.runtime import read_private
        from tests.orchestration.test_http_delivery import serving

        prepared = planned(pipeline, tmp_path)
        finish_episode(pipeline, prepared)
        intent = QualifyTrajectoryDataset(plan_id=prepared.intent.plan_id, dataset_id=new_id())
        token = read_private(pipeline.runtime.root, "operator.token", limit=256).decode("ascii")
        with (
            serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
            ResearchClient.connect(url, token) as client,
        ):
            remote = client.command("learning.qualify_dataset", intent, idempotency_key="wire")
        local = pipeline.client.command("learning.qualify_dataset", intent, idempotency_key="wire")
        assert remote == local
        assert (
            QualifiedTrajectoryDataset.model_validate(remote["result"]["data"])
            .dataset.trajectories[0]
            .consumed["attempts"]
            == 2
        )

    def test_qualification_and_correction_race_preserves_one_consistent_cut(
        self, pipeline, tmp_path
    ):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier

        from research_swarm.client import LocalTransport, ResearchClient
        from research_swarm.control.promotions import InvalidateEvidence
        from research_swarm.control.queries import RecordQuery
        from research_swarm.control.validity import EvidenceReference
        from research_swarm.domain.errors import ResearchError
        from research_swarm.runtime import LocalRuntime

        prepared = planned(pipeline, tmp_path)
        _, receipt, _ = finish_episode(pipeline, prepared)
        identifier = new_id()
        clients = [
            ResearchClient(LocalTransport(LocalRuntime.open(pipeline.runtime.root)))
            for _ in range(2)
        ]
        barrier = Barrier(2)

        def issue(index):
            barrier.wait(timeout=5)
            try:
                if index == 0:
                    return clients[index].command(
                        "learning.qualify_dataset",
                        QualifyTrajectoryDataset(
                            plan_id=prepared.intent.plan_id, dataset_id=identifier
                        ),
                        idempotency_key="race-qualify",
                    )["result"]
                return clients[index].command(
                    "evidence.invalidate",
                    InvalidateEvidence(
                        target=EvidenceReference(kind="evaluation_receipt", id=receipt["id"]),
                        reason="Concurrent correction",
                    ),
                    idempotency_key="race-correction",
                )["result"]
            except ResearchError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(issue, range(2)))
        assert isinstance(results[1], dict)
        saved = pipeline.client.list(RecordQuery(kind="learning_dataset"))["items"]
        assert len(saved) == int(isinstance(results[0], dict))
        if saved:
            with pytest.raises(ResearchError):
                pipeline.runtime.database.store.read(
                    lambda connection: require_current_dataset(
                        Transaction(
                            connection,
                            CommandContext(
                                principal=pipeline.runtime.principal(), idempotency_key="fresh"
                            ),
                        ),
                        identifier,
                    )
                )
