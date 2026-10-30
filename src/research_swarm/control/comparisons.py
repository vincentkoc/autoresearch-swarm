"""Eligibility binds scoring evidence to every stage's actual execution."""

from dataclasses import dataclass

from research_swarm.control.evaluations import EvaluationAuthorization, EvaluationEvidence
from research_swarm.control.models import CampaignData, JobData, TrialData
from research_swarm.control.validity import EvidenceReference, require_current
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import RunResult
from research_swarm.domain.experiments import Metric
from research_swarm.domain.identity import Principal
from research_swarm.evaluation.eligibility import validate_receipt
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import Transaction


@dataclass(frozen=True)
class EligibleEvaluation:
    authorization: EvaluationAuthorization
    evidence: EvaluationEvidence
    trial: TrialData
    metric: Metric
    comparison_digest: str
    dependencies: tuple[EvidenceReference, ...]


def eligible_evaluation(
    transaction: Transaction, receipt_id: str, campaign_id: str, campaign: CampaignData
) -> EligibleEvaluation:
    row = transaction.get("evaluation_receipt", receipt_id)
    evidence = EvaluationEvidence.model_validate(row["data"])
    receipt = evidence.receipt
    authorization = EvaluationAuthorization.model_validate(
        transaction.get("evaluation_request", receipt.request_id)["data"]
    )
    request = authorization.request
    trial_row = transaction.get("trial", request.trial_id)
    trial = TrialData.model_validate(trial_row["data"])
    trial.spec.verify()
    if (
        row["campaign_id"] != campaign_id
        or request.campaign_id != campaign_id
        or request.candidate_id != trial.candidate_id
        or authorization.receipt_id != receipt_id
        or trial_row["state"] != "succeeded"
        or trial.execution_outcome != "succeeded"
        or trial.resource_status != "closed"
        or trial.evaluation_status != "valid"
        or row["state"] != "valid"
        or trial.spec.protocol_digest != campaign.contract.protocol_digest
        or evidence.comparison_class != campaign.contract.comparison_class
    ):
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Evaluation is not eligible")
    validate_receipt(
        Principal(
            id=request.evaluator_id,
            workspace_id=transaction.workspace_id,
            scopes=frozenset({"evaluation.publish"}),
        ),
        request,
        receipt,
    )
    dependencies = [EvidenceReference(kind="evaluation_receipt", id=receipt_id)]
    fingerprints = {}
    scoring_stages = []
    for identifier in trial.job_ids:
        job_row = transaction.get("job", identifier)
        job = JobData.model_validate(job_row["data"])
        if job_row["state"] != "succeeded" or job.current_attempt_id is None:
            raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Trial has unfinished stages")
        result = RunResult.model_validate(
            transaction.get("run_result", job.current_attempt_id)["data"]
        )
        result.verify()
        if (
            result.spec_digest != trial.spec.spec_digest
            or result.observed_seed != trial.spec.seed
            or result.status != "succeeded"
            or result.job_id != identifier
        ):
            raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Stage evidence differs")
        for artifact in (result.process_closure_ref, *result.artifacts):
            ArtifactCatalog.available(transaction, artifact)
        dependencies.append(EvidenceReference(kind="run_result", id=job.current_attempt_id))
        fingerprints[job.stage.id] = result.execution_fingerprint
        if job.stage.kind == "evaluate":
            scoring_stages.append(identifier)
    # Multiple scoring stages need a declared aggregation policy, not caller selection.
    if scoring_stages != [evidence.job_id]:
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Promotion requires one declared scoring stage"
        )
    for artifact in (authorization.request.input_artifact, evidence.report_artifact):
        ArtifactCatalog.available(transaction, artifact)
    require_current(transaction, tuple(dependencies))
    metric = next(item for item in receipt.metrics if item.name == campaign.contract.primary_metric)
    comparison = record_digest(
        "research.comparison-contract",
        {
            "protocol": trial.spec.protocol_digest,
            "class": trial.spec.comparison_class,
            "environment": trial.spec.environment_digest,
            "evaluator": trial.spec.evaluator_contract_digest,
            "data": trial.spec.data_manifest_digest,
            "panel": receipt.panel_digest,
            "metrics": [item.model_dump(mode="json") for item in trial.spec.metrics],
            "stages": [item.model_dump(mode="json") for item in trial.spec.stages],
            "timing": trial.spec.extensions.get("timing"),
            "execution_fingerprints": fingerprints,
        },
    )
    return EligibleEvaluation(
        authorization, evidence, trial, metric, comparison, tuple(dependencies)
    )
