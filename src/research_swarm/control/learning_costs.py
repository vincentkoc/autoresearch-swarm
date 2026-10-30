"""Account for each original episode reservation once, including earlier attempts."""

from decimal import Decimal

from research_swarm.control.acceptance import dispatch_digest, lease_row
from research_swarm.control.acceptance_models import AcceptanceLease
from research_swarm.control.closure import AttemptClosure
from research_swarm.control.learning_evidence import capture_record
from research_swarm.control.models import CampaignData, JobData, TrialData
from research_swarm.control.reconciliation import AttemptSubmission, publication_digest
from research_swarm.control.search_models import ProposalRequest
from research_swarm.control.settlements import VerifiedSettlement, settlement_for_reservation
from research_swarm.control.validity import EvidenceReference, require_current
from research_swarm.control.worker_records import AttemptData
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import RunResult
from research_swarm.domain.learning import BudgetSettlementEvidence, LearningEvidence
from research_swarm.domain.types import amount_sum
from research_swarm.domain.workers import WorkerBinding
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Transaction

MAX_EPISODE_ATTEMPTS = 16
MAX_EPISODE_JOBS = 16


def refuse(message: str) -> None:
    raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, message)


def owned_reservations(
    transaction: Transaction, owner_id: str, campaign_id: str
) -> tuple[str, ...]:
    rows = transaction.connection.execute(
        "SELECT id,campaign_id FROM budget_reservations WHERE workspace_id=? AND owner_id=? "
        "ORDER BY id LIMIT 3",
        (transaction.workspace_id, owner_id),
    ).fetchall()
    if len(rows) > 1 or any(row["campaign_id"] != campaign_id for row in rows):
        refuse("Episode owner has ambiguous reservations")
    return tuple(row["id"] for row in rows)


def attempt_cost(
    transaction: Transaction,
    identifier: str,
    trial_id: str,
    trial: TrialData,
    evidence: list[LearningEvidence],
) -> VerifiedSettlement:
    row = capture_record(transaction, "attempt", identifier, evidence)
    attempt = AttemptData.model_validate(row["data"])
    dispatch = attempt.dispatch
    if row["state"] not in {
        "succeeded",
        "failed",
        "cancelled",
        "not_started",
        "closed_unaccepted",
    } or attempt.publication_status not in {"accepted", "closed_unaccepted"}:
        raise ResearchError(
            ErrorCode.EXECUTION_UNKNOWN, "Episode contains unresolved execution cost"
        )
    if (
        row["campaign_id"] != trial.spec.campaign_id
        or row["parent_id"] != dispatch.job_id
        or dispatch.attempt_id != identifier
        or dispatch.trial_id != trial_id
        or dispatch.campaign_id != trial.spec.campaign_id
        or dispatch.spec_digest != trial.spec.spec_digest
        or dispatch.job_id not in trial.job_ids
        or attempt.source_artifact.id != trial.spec.source.artifact_id
        or attempt.source_tree_digest != trial.spec.source.tree_digest
        or len(dispatch.reservation_ids) != 1
        or owned_reservations(transaction, identifier, dispatch.campaign_id)
        != dispatch.reservation_ids
    ):
        refuse("Attempt cost identity differs from its admitted trial")
    actual = None
    if attempt.outcome_artifact is None or (
        attempt.publication_status == "accepted" and attempt.result_digest is None
    ):
        refuse("Terminal attempt lacks its original publication evidence")
    assert attempt.outcome_artifact is not None
    ArtifactCatalog.available(transaction, attempt.outcome_artifact)
    binding = WorkerBinding.model_validate(
        {name: getattr(dispatch, name) for name in WorkerBinding.model_fields}
    )
    lease_record = lease_row(transaction, identifier)
    lease = AcceptanceLease.model_validate(lease_record["data"])
    legacy_accepted = (
        attempt.publication_status == "accepted"
        and lease_record["revision"] == 0
        and lease_record["state"] == "unqualified"
        and lease.policy_version == "legacy-unqualified-v1"
    )
    if not legacy_accepted and (
        lease_record["state"] != "settled"
        or lease.attempt_id != identifier
        or lease.binding != binding
        or lease.dispatch_digest != dispatch_digest(attempt)
        or lease.disposition != attempt.publication_status
    ):
        refuse("Attempt cost lacks settled acceptance authority")
    if attempt.publication_status == "closed_unaccepted":
        if row["state"] != "closed_unaccepted" or attempt.closure_receipt_id is None:
            refuse("Closed execution lacks its original closure")
        assert attempt.closure_receipt_id is not None
        closure = AttemptClosure.model_validate(
            capture_record(transaction, "attempt_closure", attempt.closure_receipt_id, evidence)[
                "data"
            ]
        )
        submission = AttemptSubmission.model_validate(
            capture_record(transaction, "attempt_submission", closure.submission_id, evidence)[
                "data"
            ]
        )
        if (
            closure.attempt_id != identifier
            or closure.original_binding != binding
            or submission.original_binding != binding
            or closure.journal_artifact != attempt.outcome_artifact
            or submission.publication.attempt_id != identifier
            or submission.publication.journal_artifact != closure.journal_artifact
            or publication_digest(submission.publication) != closure.publication_digest
            or submission.publication_digest != closure.publication_digest
            or (
                not closure.not_started
                and (
                    closure.native_outcome is None
                    or closure.native_outcome.exit_code is None
                    or not closure.native_outcome.closure.scope_closed
                )
            )
        ):
            refuse("Closed execution cost differs from its original evidence")
        actual = closure.consumed
    elif row["state"] != "not_started":
        result = RunResult.model_validate(
            capture_record(transaction, "run_result", identifier, evidence)["data"]
        )
        result.verify()
        require_current(transaction, (EvidenceReference(kind="run_result", id=identifier),))
        if (
            result.attempt_id != identifier
            or result.job_id != dispatch.job_id
            or result.trial_id != trial_id
            or result.campaign_id != dispatch.campaign_id
            or result.worker_id != dispatch.worker_id
            or result.worker_session_id != dispatch.worker_session_id
            or result.control_epoch != dispatch.control_epoch
            or result.fencing_generation != dispatch.fencing_generation
            or result.spec_digest != trial.spec.spec_digest
            or result.status != row["state"]
        ):
            refuse("Accepted execution cost differs from its admitted attempt")
        actual = result.consumed
    proof = settlement_for_reservation(
        transaction, dispatch.reservation_ids[0], identifier, dispatch.campaign_id, actual
    )
    if proof.settlement_id != identifier:
        refuse("Attempt settlement identity differs")
    return proof


def episode_cost(
    transaction: Transaction,
    request_id: str,
    request: ProposalRequest,
    trial_id: str,
    trial: TrialData,
    evidence: list[LearningEvidence],
) -> tuple[dict[str, Decimal], tuple[BudgetSettlementEvidence, ...]]:
    if len(trial.job_ids) > MAX_EPISODE_JOBS or trial.confirmation_plan_id is not None:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Learning episode exceeds its cost profile"
        )
    if owned_reservations(transaction, request_id, request.campaign_id) != (
        request.budget_reservation_id,
    ):
        refuse("Proposal request charge differs from its reservation")
    proofs = [
        settlement_for_reservation(
            transaction,
            request.budget_reservation_id,
            request_id,
            request.campaign_id,
            {"proposal_requests": Decimal(1)},
        )
    ]
    campaign = CampaignData.model_validate(transaction.get("campaign", request.campaign_id)["data"])
    admission_reservations = owned_reservations(transaction, trial_id, request.campaign_id)
    charged_admission = any(
        item.dimension == "trial_admissions" for item in campaign.contract.limits
    )
    if len(admission_reservations) != int(charged_admission):
        refuse("Episode trial admission charge is incomplete")
    for reservation_id in admission_reservations:
        proofs.append(
            settlement_for_reservation(
                transaction,
                reservation_id,
                trial_id,
                request.campaign_id,
                {"trial_admissions": Decimal(1)},
            )
        )
    # Read both linkage directions so a malformed parent cannot hide an older attempt.
    parents: set[str] = set()
    for job_id in trial.job_ids:
        row = capture_record(transaction, "job", job_id, evidence)
        job = JobData.model_validate(row["data"])
        if row["campaign_id"] != request.campaign_id or job.trial_id != trial_id:
            refuse("Trial job cost ownership differs")
        rows = transaction.connection.execute(
            "SELECT id FROM records WHERE workspace_id=? AND kind='attempt' AND parent_id=? "
            "ORDER BY id LIMIT ?",
            (transaction.workspace_id, job_id, MAX_EPISODE_ATTEMPTS + 1),
        ).fetchall()
        parents.update(row["id"] for row in rows)
    direct = {
        row["id"]
        for row in transaction.connection.execute(
            "SELECT id FROM records WHERE workspace_id=? AND kind='attempt' "
            "AND json_extract(payload,'$.dispatch.trial_id')=? ORDER BY id LIMIT ?",
            (transaction.workspace_id, trial_id, MAX_EPISODE_ATTEMPTS + 1),
        ).fetchall()
    }
    if len(parents) > MAX_EPISODE_ATTEMPTS or len(direct) > MAX_EPISODE_ATTEMPTS:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Learning episode has too many attempts"
        )
    if not parents or parents != direct:
        refuse("Episode attempt inventory is incomplete")
    for identifier in sorted(parents):
        proofs.append(attempt_cost(transaction, identifier, trial_id, trial, evidence))
    if len({proof.settlement_id for proof in proofs}) != len(proofs):
        refuse("Episode would count a settlement more than once")
    consumed: dict[str, Decimal] = {}
    references = []
    for proof in proofs:
        for dimension, value in proof.actual.items():
            consumed[dimension] = amount_sum(consumed.get(dimension, Decimal(0)), value)
        references.append(
            BudgetSettlementEvidence(
                id=proof.settlement_id,
                request_digest=proof.request_digest,
                response_digest=proof.response_digest,
            )
        )
    return consumed, tuple(references)
