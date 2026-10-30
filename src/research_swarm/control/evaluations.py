"""Evaluation authority is issued before execution and consumes immutable native evidence."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from research_swarm.control.confirmation_runtime import bound_confirmation
from research_swarm.control.models import CampaignData, JobData, ProjectRevisionData, TrialData
from research_swarm.control.validity import EvidenceReference, invalidated
from research_swarm.control.workers import AttemptData
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import (
    ArtifactRef,
    EvaluationReceipt,
    EvaluationRequest,
    RunResult,
)
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ID, Digest, Name, Timestamp, VersionedRecord, utc_now
from research_swarm.evaluation.eligibility import validate_receipt
from research_swarm.projects.workload import WorkloadReport
from research_swarm.state.artifacts import ArtifactCatalog, ArtifactService
from research_swarm.state.database import Database, JsonObject, Transaction


class AuthorizeEvaluation(VersionedRecord):
    job_id: ID
    evaluator_id: ID
    exposure_class: Literal["exploratory", "confirmation"] = "exploratory"


class CompleteEvaluation(VersionedRecord):
    request_id: ID
    attempt_id: ID


class EvaluationAuthorization(VersionedRecord):
    request: EvaluationRequest
    job_id: ID
    spec_digest: Digest
    inputs: dict[Name, ArtifactRef]
    authorized_at: Timestamp = Field(default_factory=utc_now)
    receipt_id: ID | None = None


class EvaluationEvidence(VersionedRecord):
    receipt: EvaluationReceipt
    job_id: ID
    attempt_id: ID
    report_artifact: ArtifactRef
    protocol_digest: Digest
    comparison_class: Name
    environment_digest: Digest


def evaluation_inputs(transaction: Transaction, job: JobData) -> dict[str, ArtifactRef]:
    inputs = dict(job.inputs)
    for predecessor in job.predecessor_ids:
        if transaction.get("job", predecessor)["state"] != "succeeded":
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Evaluation inputs are incomplete")
    for name, predecessor in job.input_sources.items():
        parent = JobData.model_validate(transaction.get("job", predecessor)["data"])
        if name not in parent.outputs:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Evaluation input is unavailable")
        inputs[name] = parent.outputs[name]
    for reference in inputs.values():
        ArtifactCatalog.available(transaction, reference)
    return inputs


class EvaluationControl:
    def __init__(self, database: Database, artifacts: ArtifactService) -> None:
        self.database, self.artifacts = database, artifacts

    def authorize(self, context: CommandContext, intent: AuthorizeEvaluation) -> JsonObject:
        context.principal.require("read")
        context.principal.require("artifact.upload")
        payload = intent.model_dump(mode="json")
        previous = self.database.replay(
            context, "evaluation.authorize", payload, "evaluation.request"
        )
        if previous is not None:
            return previous
        with self.database.connect() as connection:
            connection.execute("BEGIN")
            transaction = Transaction(connection, context)
            job_row = transaction.get("job", intent.job_id)
            job = JobData.model_validate(job_row["data"])
            if context.expected_revision != job_row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Evaluation job revision changed")
            if (
                job_row["state"] != "ready"
                or job.stage.kind != "evaluate"
                or job.current_attempt_id is not None
            ):
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Evaluation must be authorized before execution"
                )
            if job.evaluation_request_id is not None:
                raise ResearchError(
                    ErrorCode.REVISION_CONFLICT, "Evaluation authority already exists"
                )
            inputs = evaluation_inputs(transaction, job)
            trial = TrialData.model_validate(transaction.get("trial", job.trial_id)["data"])
            connection.commit()
        manifest_bytes = canonical_bytes(
            {
                "schema_version": "1.0",
                "job_id": intent.job_id,
                "spec_digest": trial.spec.spec_digest,
                "inputs": {
                    name: reference.model_dump(mode="json") for name, reference in inputs.items()
                },
            }
        )
        input_manifest = self.artifacts.publish_bytes(
            context, manifest_bytes, purpose="evaluation-input-manifest"
        )

        def authorize(transaction: Transaction) -> JsonObject:
            row = transaction.get("job", intent.job_id)
            if context.expected_revision != row["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Evaluation job revision changed")
            current = JobData.model_validate(row["data"])
            if (
                row["state"] != "ready"
                or current.stage.kind != "evaluate"
                or current.current_attempt_id is not None
            ):
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Evaluation must be authorized before execution"
                )
            if (
                current.evaluation_request_id is not None
                or evaluation_inputs(transaction, current) != inputs
            ):
                raise ResearchError(
                    ErrorCode.REVISION_CONFLICT, "Evaluation authority or inputs changed"
                )
            campaign = CampaignData.model_validate(
                transaction.get("campaign", row["campaign_id"])["data"]
            )
            project = ProjectRevisionData.model_validate(
                transaction.get("project_revision", campaign.contract.project_revision_id)["data"]
            )
            evaluator = project.resolved.manifest.evaluation
            if evaluator.expected_members is None:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Evaluator must declare its expected panel"
                )
            if evaluator.measurement_trust != "authenticated":
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE,
                    "This evaluator trust profile is not qualified",
                )
            bound = bound_confirmation(transaction, trial)
            if intent.exposure_class == "confirmation" and bound is None:
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE,
                    "Confirmation requires a sealed plan",
                )
            if bound is not None and (
                bound[0].evaluator_id != intent.evaluator_id
                or bound[1].scoring_job_id != row["id"]
                or current.evaluation_exposure_id != bound[1].exposure_id
            ):
                raise ResearchError(
                    ErrorCode.AUTHORIZATION_DENIED, "Evaluation differs from confirmation plan"
                )
            request = EvaluationRequest(
                workspace_id=transaction.workspace_id,
                campaign_id=row["campaign_id"],
                candidate_id=trial.candidate_id,
                trial_id=job.trial_id,
                input_artifact=input_manifest,
                evaluator_id=intent.evaluator_id,
                evaluator_contract_digest=trial.spec.evaluator_contract_digest,
                panel_digest=record_digest(
                    "research.evaluation-panel",
                    {
                        "members": evaluator.model_dump(mode="json")["expected_members"],
                        "data_manifest_digest": trial.spec.data_manifest_digest,
                    },
                ),
                comparison_class=trial.spec.comparison_class,
                metric_schema=trial.spec.metrics,
                expected_seed=trial.spec.seed,
                expected_members=evaluator.expected_members,
                exposure_class="confirmation" if bound else intent.exposure_class,
                exposure_authorization_id=bound[1].exposure_id if bound else None,
            )
            assert trial.spec.spec_digest is not None
            authorization = EvaluationAuthorization(
                request=request,
                job_id=intent.job_id,
                spec_digest=trial.spec.spec_digest,
                inputs=inputs,
            )
            record = transaction.insert(
                "evaluation_request",
                request.id,
                authorization.model_dump(mode="json"),
                state="authorized",
                campaign_id=row["campaign_id"],
                parent_id=intent.job_id,
            )
            for reference in (input_manifest, *inputs.values()):
                ArtifactCatalog.available(transaction, reference)
                ArtifactCatalog.retain(
                    transaction,
                    ArtifactRetention(
                        artifact_id=reference.id,
                        owner_kind="evaluation_request",
                        owner_id=request.id,
                        purpose="evaluation",
                    ),
                )
            transaction.update(
                "job",
                row["id"],
                expected_revision=row["revision"],
                state="ready",
                data=current.model_copy(update={"evaluation_request_id": request.id}).model_dump(
                    mode="json"
                ),
            )
            transaction.event("evaluation.authorized", record, {"job_id": intent.job_id})
            return record

        return self.database.execute(
            context, "evaluation.authorize", payload, "evaluation.request", authorize
        )

    def complete(self, context: CommandContext, intent: CompleteEvaluation) -> JsonObject:
        payload = intent.model_dump(mode="json")
        previous = self.database.replay(
            context, "evaluation.complete", payload, "evaluation.publish"
        )
        if previous is not None:
            return previous
        row = self.database.read_record(context, "evaluation_request", intent.request_id)
        authorization = EvaluationAuthorization.model_validate(row["data"])
        if context.principal.id != authorization.request.evaluator_id:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Principal is not the authorized evaluator"
            )
        attempt = AttemptData.model_validate(
            self.database.read_record(context, "attempt", intent.attempt_id)["data"]
        )
        if attempt.report_artifact is None:
            raise ResearchError(
                ErrorCode.EVALUATION_INCOMPLETE, "Evaluation lacks a workload report"
            )
        with self.artifacts.reader(context, attempt.report_artifact) as stream:
            if attempt.report_artifact.size_bytes > 1024 * 1024:
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Evaluation report exceeds its bound"
                )
            report = WorkloadReport.model_validate(strict_json_loads(stream.read(1024 * 1024 + 1)))
            return self.database.execute(
                context,
                "evaluation.complete",
                payload,
                "evaluation.publish",
                lambda transaction: self._complete(transaction, intent, report),
            )

    @staticmethod
    def _complete(
        transaction: Transaction, intent: CompleteEvaluation, report: WorkloadReport
    ) -> JsonObject:
        row = transaction.get("evaluation_request", intent.request_id)
        authorization = EvaluationAuthorization.model_validate(row["data"])
        request = authorization.request
        if transaction.context.principal.id != request.evaluator_id:
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED, "Principal is not the authorized evaluator"
            )
        if row["state"] != "authorized":
            assert authorization.receipt_id is not None
            prior = transaction.get("evaluation_receipt", authorization.receipt_id)
            if prior["data"]["attempt_id"] != intent.attempt_id:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT, "Evaluation already used another attempt"
                )
            return prior
        job_row = transaction.get("job", authorization.job_id)
        job = JobData.model_validate(job_row["data"])
        attempt_row = transaction.get("attempt", intent.attempt_id)
        attempt = AttemptData.model_validate(attempt_row["data"])
        trial_row = transaction.get("trial", request.trial_id)
        trial = TrialData.model_validate(trial_row["data"])
        if (
            attempt_row["state"] != "succeeded"
            or job_row["state"] != "succeeded"
            or job.current_attempt_id != intent.attempt_id
            or job.stage.kind != "evaluate"
            or attempt.dispatch.job_id != authorization.job_id
            or attempt.evaluation_request_id != intent.request_id
            or job.evaluation_request_id != intent.request_id
            or attempt.dispatch.spec_digest != authorization.spec_digest
            or attempt.inputs != authorization.inputs
        ):
            raise ResearchError(
                ErrorCode.COMPARISON_INELIGIBLE, "Execution does not match evaluation authority"
            )
        result = RunResult.model_validate(transaction.get("run_result", intent.attempt_id)["data"])
        result.verify()
        assert attempt.report_artifact is not None
        receipt = EvaluationReceipt(
            request_id=intent.request_id,
            evaluator_id=request.evaluator_id,
            input_digest=request.input_artifact.digest,
            evaluator_contract_digest=request.evaluator_contract_digest,
            panel_digest=request.panel_digest,
            observed_seed=report.observed_seed,
            observed_members=frozenset(report.observed_members),
            metrics=report.measurements,
            validity="valid" if report.measurements else "incomplete",
            execution_fingerprint=result.execution_fingerprint,
        )
        try:
            validate_receipt(transaction.context.principal, request, receipt)
        except ResearchError as error:
            if error.code not in {ErrorCode.COMPARISON_INELIGIBLE, ErrorCode.EVALUATION_INCOMPLETE}:
                raise
            receipt = receipt.model_copy(
                update={
                    "validity": "incomplete"
                    if error.code == ErrorCode.EVALUATION_INCOMPLETE
                    else "invalid",
                    "findings": (error.code.value, error.message),
                }
            )
        evidence = EvaluationEvidence(
            receipt=receipt,
            job_id=job_row["id"],
            attempt_id=intent.attempt_id,
            report_artifact=attempt.report_artifact,
            protocol_digest=trial.spec.protocol_digest,
            comparison_class=trial.spec.comparison_class,
            environment_digest=trial.spec.environment_digest,
        )
        record = transaction.insert(
            "evaluation_receipt",
            receipt.id,
            evidence.model_dump(mode="json"),
            state=receipt.validity,
            campaign_id=row["campaign_id"],
            parent_id=request.candidate_id,
        )
        transaction.update(
            "evaluation_request",
            row["id"],
            expected_revision=row["revision"],
            data=authorization.model_copy(update={"receipt_id": receipt.id}).model_dump(
                mode="json"
            ),
            state="completed",
        )
        states = []
        for identifier in trial.job_ids:
            item = JobData.model_validate(transaction.get("job", identifier)["data"])
            if item.stage.kind == "evaluate":
                status = "pending"
                if item.evaluation_request_id:
                    authorized = transaction.get("evaluation_request", item.evaluation_request_id)
                    if authorized["state"] == "completed":
                        status = transaction.get(
                            "evaluation_receipt", authorized["data"]["receipt_id"]
                        )["state"]
                states.append(status)
            if item.current_attempt_id and invalidated(
                transaction, EvidenceReference(kind="run_result", id=item.current_attempt_id)
            ):
                states.append("invalid")
            if item.evaluation_request_id:
                authorized = transaction.get("evaluation_request", item.evaluation_request_id)
                prior_receipt = authorized["data"]["receipt_id"]
                if prior_receipt and invalidated(
                    transaction, EvidenceReference(kind="evaluation_receipt", id=prior_receipt)
                ):
                    states.append("invalid")
        status = (
            "invalid"
            if "invalid" in states
            else "incomplete"
            if "incomplete" in states
            else ("pending" if "pending" in states else "valid")
        )
        transaction.update(
            "trial",
            trial_row["id"],
            expected_revision=trial_row["revision"],
            state=trial_row["state"],
            data=trial.model_copy(update={"evaluation_status": status}).model_dump(mode="json"),
        )
        transaction.event("evaluation.recorded", record, {"request_id": request.id})
        return record
