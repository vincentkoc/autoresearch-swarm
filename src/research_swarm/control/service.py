"""Research commands shared by local clients and every network transport."""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal

from pydantic import ValidationError

from research_swarm.control.campaigns import CampaignControl
from research_swarm.control.candidates import propose_candidate, retain_source, validate_candidate
from research_swarm.control.closure import ClosureControl
from research_swarm.control.commands import (
    CampaignTarget,
    CompleteCampaign,
    CreateCampaign,
    PauseCampaign,
    ProposeCandidate,
    RegisterProject,
    SubmitTrial,
    ValidateCandidate,
)
from research_swarm.control.confirmation import (
    AssessConfirmation,
    ConfirmationControl,
    CreateConfirmationPlan,
)
from research_swarm.control.evaluations import (
    AuthorizeEvaluation,
    CompleteEvaluation,
    EvaluationControl,
)
from research_swarm.control.models import (
    CampaignContract,
    CampaignData,
    CandidateData,
    CandidateValidation,
    JobData,
    ProjectData,
    ProjectRevisionData,
    TrialData,
)
from research_swarm.control.observation_models import ObserveTrial, ResolveTrialObservation
from research_swarm.control.preflight import verified_source
from research_swarm.control.promotions import (
    AssessPromotion,
    CommitPromotion,
    InvalidateEvidence,
    PromotionControl,
)
from research_swarm.control.proposals import ProposalControl
from research_swarm.control.reconciliation import ReconcileAttempt, ReconciliationControl
from research_swarm.control.reports import ReportCampaign
from research_swarm.control.results import PublishAttempt, ResultControl
from research_swarm.control.search_admission import SearchAdmission
from research_swarm.control.search_models import (
    AdmitProposalResponse,
    CancelProposal,
    RequestProposal,
    RespondToProposal,
)
from research_swarm.control.search_observations import ObservationControl
from research_swarm.control.worker_lifecycle import (
    RequestAttemptCancellation,
    SetWorkerAdmission,
    WorkerHeartbeat,
    WorkerLifecycle,
)
from research_swarm.control.worker_outputs import (
    BeginWorkerUpload,
    WorkerOutputs,
    WorkerUploadTarget,
)
from research_swarm.control.worker_turnover import FenceWorker, ReplaceWorkerSession, WorkerTurnover
from research_swarm.control.workers import ClaimJob, EnrollWorker, WorkerControl
from research_swarm.domain.budgets import BudgetKind
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.clocks import ClockChallenge, SignedClockReply
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.experiments import ExperimentSpec, SourceRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.lifecycle import CampaignState, campaign_transition
from research_swarm.domain.types import VersionedRecord, new_id, utc_now
from research_swarm.evaluation.statistics import FixedPairedPolicy
from research_swarm.projects.manifest import ResolvedProject, resolve_project
from research_swarm.state.artifacts import ArtifactCatalog, ArtifactService
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.ledger import BudgetLedger


def check_revision(context: CommandContext, row: JsonObject) -> None:
    if context.expected_revision != row["revision"]:
        raise ResearchError(
            ErrorCode.REVISION_CONFLICT,
            "The command requires the current aggregate revision",
            details={"expected": context.expected_revision, "current": row["revision"]},
        )


def input_sources(
    resolved: ResolvedProject, constants: dict[str, ArtifactRef]
) -> dict[str, dict[str, str | None]]:
    """Each required name resolves to one direct predecessor or campaign input."""
    stages = {stage.id: stage for stage in resolved.manifest.stages}
    bindings: dict[str, dict[str, str | None]] = {}
    for stage in stages.values():
        if len(set(stage.output_names)) != len(stage.output_names) or len(
            set(stage.required_artifacts)
        ) != len(stage.required_artifacts):
            raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Duplicate stage artifact name")
        bindings[stage.id] = {}
        for name in stage.required_artifacts:
            providers: list[str | None] = [
                parent for parent in stage.dependencies if name in stages[parent].output_names
            ]
            if name in constants:
                providers.append(None)
            if len(providers) != 1:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID,
                    "A required input must have exactly one declared provider",
                    details={"stage": stage.id, "input": name},
                )
            bindings[stage.id][name] = providers[0]
    return bindings


class ResearchService:
    def __init__(
        self,
        database: Database,
        artifacts: ArtifactService,
        *,
        dispatch_key: bytes | None = None,
        assignment_key: bytes | None = None,
    ) -> None:
        if database.path.resolve() != artifacts.database.path.resolve():
            raise ValueError("Research and artifact catalogs must share a transaction authority")
        self.database = database
        self.artifacts = artifacts
        self.worker_outputs = WorkerOutputs(database, artifacts)
        if assignment_key is not None and dispatch_key is None:
            raise ValueError("Assignment signing requires worker control")
        self.workers = (
            WorkerControl(database, dispatch_key, assignment_key)
            if dispatch_key is not None
            else None
        )
        self.worker_turnover = WorkerTurnover(database, assignment_key)
        self.proposals = ProposalControl(database)
        self.search_observations = ObservationControl(database)
        self.search_admission = SearchAdmission(database, artifacts, self._submit_trial)
        self.results = ResultControl(database, artifacts)
        self.reconciliation = ReconciliationControl(database, self.results)
        self.closures = ClosureControl(database, self.reconciliation)
        self.evaluations = EvaluationControl(database, artifacts)
        self.promotions = PromotionControl(database)
        self.confirmations = ConfirmationControl(database, self._submit_trial)
        self.campaigns = CampaignControl(database)
        self.worker_lifecycle = WorkerLifecycle(database)

    def submit_attempt_evidence(
        self, context: CommandContext, request: PublishAttempt
    ) -> JsonObject:
        return self.reconciliation.submit(context, request)

    def reconcile_attempt(self, context: CommandContext, request: ReconcileAttempt) -> JsonObject:
        return self.reconciliation.reconcile(context, request)

    def reconcile_attempt_closure(
        self, context: CommandContext, request: ReconcileAttempt
    ) -> JsonObject:
        return self.closures.reconcile(context, request)

    def observe_search_trial(self, context: CommandContext, request: ObserveTrial) -> JsonObject:
        return self.search_observations.observe(context, request)

    def resolve_search_trial(
        self, context: CommandContext, request: ResolveTrialObservation
    ) -> JsonObject:
        return self.search_observations.resolve(context, request)

    def admit_proposal_response(
        self, context: CommandContext, request: AdmitProposalResponse
    ) -> JsonObject:
        return self.search_admission.admit(context, request)

    def request_proposal(self, context: CommandContext, request: RequestProposal) -> JsonObject:
        return self.proposals.request(context, request)

    def respond_proposal(self, context: CommandContext, request: RespondToProposal) -> JsonObject:
        return self.proposals.respond(context, request)

    def cancel_proposal(self, context: CommandContext, request: CancelProposal) -> JsonObject:
        return self.proposals.cancel(context, request)

    def fence_worker(self, context: CommandContext, request: FenceWorker) -> JsonObject:
        return self.worker_turnover.fence(context, request)

    def replace_worker_session(
        self, context: CommandContext, request: ReplaceWorkerSession
    ) -> JsonObject:
        return self.worker_turnover.replace(context, request)

    def worker_heartbeat(self, context: CommandContext, request: WorkerHeartbeat) -> JsonObject:
        return self.worker_lifecycle.heartbeat(context, request)

    def set_worker_admission(
        self, context: CommandContext, request: SetWorkerAdmission
    ) -> JsonObject:
        return self.worker_lifecycle.set_admission(context, request)

    def request_attempt_cancel(
        self, context: CommandContext, request: RequestAttemptCancellation
    ) -> JsonObject:
        return self.worker_lifecycle.request_cancel(context, request)

    def pause_campaign(self, context: CommandContext, request: PauseCampaign) -> JsonObject:
        return self.campaigns.pause(context, request)

    def complete_campaign(self, context: CommandContext, request: CompleteCampaign) -> JsonObject:
        return self.campaigns.complete(context, request)

    def report_campaign(self, context: CommandContext, request: ReportCampaign) -> JsonObject:
        return self.campaigns.report(context, request)

    def create_confirmation(
        self, context: CommandContext, request: CreateConfirmationPlan
    ) -> JsonObject:
        return self.confirmations.create(context, request)

    def assess_confirmation(
        self, context: CommandContext, request: AssessConfirmation
    ) -> JsonObject:
        return self.confirmations.assess(context, request)

    def assess_promotion(self, context: CommandContext, request: AssessPromotion) -> JsonObject:
        return self.promotions.assess(context, request)

    def reinitialize_baseline(
        self, context: CommandContext, request: AssessPromotion
    ) -> JsonObject:
        return self.promotions.reinitialize(context, request)

    def commit_promotion(self, context: CommandContext, request: CommitPromotion) -> JsonObject:
        return self.promotions.commit(context, request)

    def invalidate_evidence(
        self, context: CommandContext, request: InvalidateEvidence
    ) -> JsonObject:
        return self.promotions.invalidate(context, request)

    def enroll_worker(self, context: CommandContext, request: EnrollWorker) -> JsonObject:
        if self.workers is None:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Worker authority is not configured"
            )
        return self.workers.enroll(context, request)

    def claim_job(self, context: CommandContext, request: ClaimJob) -> JsonObject:
        if self.workers is None:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Worker authority is not configured"
            )
        return self.workers.claim(context, request)

    def worker_clock(self, context: CommandContext, request: ClockChallenge) -> SignedClockReply:
        if self.workers is None:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Worker authority is not configured"
            )
        return self.workers.clock(context, request)

    def publish_attempt(self, context: CommandContext, request: PublishAttempt) -> JsonObject:
        return self.results.publish(context, request)

    def begin_worker_upload(
        self, context: CommandContext, request: BeginWorkerUpload
    ) -> JsonObject:
        return self.worker_outputs.begin(context, request)

    def finalize_worker_upload(
        self, context: CommandContext, request: WorkerUploadTarget
    ) -> JsonObject:
        return self.worker_outputs.finalize(context, request)

    def cleanup_worker_upload(
        self, context: CommandContext, request: WorkerUploadTarget
    ) -> JsonObject:
        return self.worker_outputs.cleanup(context, request)

    def authorize_evaluation(
        self, context: CommandContext, request: AuthorizeEvaluation
    ) -> JsonObject:
        return self.evaluations.authorize(context, request)

    def complete_evaluation(
        self, context: CommandContext, request: CompleteEvaluation
    ) -> JsonObject:
        return self.evaluations.complete(context, request)

    def _execute(
        self,
        context: CommandContext,
        operation: str,
        request: VersionedRecord,
        scope: str,
        handler: Callable[[Transaction], JsonObject],
    ) -> JsonObject:
        return self.database.execute(
            context, operation, request.model_dump(mode="json"), scope, handler
        )

    def register_project(self, context: CommandContext, request: RegisterProject) -> JsonObject:
        operation, scope = "project.register", "project.manage"
        previous = self.database.replay(context, operation, request.model_dump(mode="json"), scope)
        if previous is not None:
            return previous
        with verified_source(self.artifacts, context, request.source_artifact) as (bundle, _):
            try:
                resolved = resolve_project(bundle, manifest_path=request.manifest_path)
            except (ValueError, KeyError, ValidationError) as error:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Project manifest is invalid"
                ) from error

            def register(transaction: Transaction) -> JsonObject:
                project_id = request.project_id or new_id()
                previous_project = (
                    transaction.get("project", project_id) if request.project_id else None
                )
                if previous_project is not None:
                    check_revision(context, previous_project)
                revision_id = new_id()
                identity = resolved.manifest.project
                project = ProjectData(
                    slug=identity.slug,
                    name=identity.name,
                    description=identity.description,
                    current_revision_id=revision_id,
                )
                revision = transaction.insert(
                    "project_revision",
                    revision_id,
                    ProjectRevisionData(
                        project_id=project_id,
                        source_artifact=request.source_artifact,
                        resolved=resolved,
                    ).model_dump(mode="json"),
                    state="immutable",
                    parent_id=project_id,
                )
                retain_source(transaction, request.source_artifact, "project_revision", revision_id)
                if previous_project is None:
                    row = transaction.insert(
                        "project", project_id, project.model_dump(mode="json"), state="active"
                    )
                else:
                    row = transaction.update(
                        "project",
                        project_id,
                        expected_revision=previous_project["revision"],
                        data=project.model_dump(mode="json"),
                        state="active",
                    )
                transaction.event("project.registered", row, {"project_revision_id": revision_id})
                return {"project": row, "project_revision": revision}

            return self._execute(context, operation, request, scope, register)

    def create_campaign(self, context: CommandContext, request: CreateCampaign) -> JsonObject:
        def create(transaction: Transaction) -> JsonObject:
            project = ProjectData.model_validate(
                transaction.get("project", request.project_id)["data"]
            )
            revision_id = request.project_revision_id or project.current_revision_id
            revision = ProjectRevisionData.model_validate(
                transaction.get("project_revision", revision_id)["data"]
            )
            if revision.project_id != request.project_id:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID,
                    "Project revision belongs to another project",
                )
            resolved = revision.resolved
            if request.primary_metric not in {
                metric.name for metric in resolved.manifest.evaluation.metrics
            }:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID,
                    "Primary metric is not declared by the evaluator",
                )
            contract = CampaignContract(
                project_id=request.project_id,
                project_revision_id=revision_id,
                project_manifest_digest=resolved.manifest_digest,
                protocol_digest=record_digest(
                    "research.campaign-protocol",
                    {
                        "project_protocol": resolved.protocol_digest,
                        "inputs": {
                            name: {"digest": reference.digest, "size_bytes": reference.size_bytes}
                            for name, reference in request.input_artifacts.items()
                        },
                    },
                ),
                comparison_class=resolved.manifest.evaluation.comparison_class,
                primary_metric=request.primary_metric,
                limits=request.limits,
                search=request.search,
                promotion=request.promotion,
                stopping=request.stopping,
                input_artifacts=request.input_artifacts,
            )
            identifier = new_id()
            for reference in contract.input_artifacts.values():
                retain_source(transaction, reference, "campaign", identifier)
            row = transaction.insert(
                "campaign",
                identifier,
                CampaignData(name=request.name, contract=contract).model_dump(mode="json"),
                state=CampaignState.DRAFT,
                campaign_id=identifier,
            )
            BudgetLedger.create(transaction, identifier, contract.limits)
            transaction.event("campaign.created", row, {"project_revision_id": revision_id})
            return row

        return self._execute(context, "campaign.create", request, "campaign.manage", create)

    @staticmethod
    def _validate_contract(transaction: Transaction, campaign: CampaignData) -> None:
        revision = ProjectRevisionData.model_validate(
            transaction.get("project_revision", campaign.contract.project_revision_id)["data"]
        )
        ArtifactCatalog.available(transaction, revision.source_artifact)
        for reference in campaign.contract.input_artifacts.values():
            ArtifactCatalog.available(transaction, reference)
        input_sources(revision.resolved, campaign.contract.input_artifacts)
        limits = {limit.dimension: limit for limit in campaign.contract.limits}
        promotion = campaign.contract.promotion
        if promotion.mode == "paired_confirmation":
            if promotion.version != "paired-sign-v1":
                raise ResearchError(
                    ErrorCode.CAPABILITY_UNAVAILABLE, "Unknown paired promotion policy"
                )
            try:
                FixedPairedPolicy(
                    seeds=promotion.confirmation_seeds,
                    minimum_effect=promotion.minimum_effect,
                    confidence=promotion.confidence,
                )
            except ValidationError as error:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Confirmation panel is infeasible"
                ) from error
            exposure = limits.get("evaluation_exposures")
            if exposure is None or exposure.kind != BudgetKind.EXPOSURE or exposure.unit != "count":
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID,
                    "Confirmation requires an evaluation_exposures/count exposure budget",
                )
        for dimension, unit in {"attempts": "count", "execution_ms": "ms"}.items():
            limit = limits.get(dimension)
            if limit is None or limit.kind != BudgetKind.CUMULATIVE or limit.unit != unit:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID,
                    "Native execution requires cumulative attempts/count "
                    "and execution_ms/ms budgets",
                )

    def validate_campaign(self, context: CommandContext, request: CampaignTarget) -> JsonObject:
        def validate(transaction: Transaction) -> JsonObject:
            row = transaction.get("campaign", request.campaign_id)
            check_revision(context, row)
            self._validate_contract(transaction, CampaignData.model_validate(row["data"]))
            return {"campaign_id": row["id"], "revision": row["revision"], "valid": True}

        return self._execute(context, "campaign.validate", request, "campaign.manage", validate)

    def seal_campaign(self, context: CommandContext, request: CampaignTarget) -> JsonObject:
        def seal(transaction: Transaction) -> JsonObject:
            row = transaction.get("campaign", request.campaign_id)
            check_revision(context, row)
            campaign_transition(CampaignState(row["state"]), CampaignState.READY)
            data = CampaignData.model_validate(row["data"])
            self._validate_contract(transaction, data)
            sealed = data.model_copy(
                update={
                    "contract_digest": record_digest(
                        "research.campaign-contract", data.contract.model_dump(mode="json")
                    ),
                    "sealed_at": utc_now(),
                }
            )
            updated = transaction.update(
                "campaign",
                row["id"],
                expected_revision=row["revision"],
                data=sealed.model_dump(mode="json"),
                state=CampaignState.READY,
            )
            transaction.event(
                "campaign.sealed", updated, {"contract_digest": sealed.contract_digest}
            )
            return updated

        return self._execute(context, "campaign.seal", request, "campaign.manage", seal)

    def start_campaign(self, context: CommandContext, request: CampaignTarget) -> JsonObject:
        def start(transaction: Transaction) -> JsonObject:
            row = transaction.get("campaign", request.campaign_id)
            check_revision(context, row)
            data = CampaignData.model_validate(row["data"])
            if data.contract_digest is None:
                raise ResearchError(ErrorCode.INVALID_TRANSITION, "Campaign contract is not sealed")
            campaign_transition(CampaignState(row["state"]), CampaignState.RUNNING)
            updated = transaction.update(
                "campaign",
                row["id"],
                expected_revision=row["revision"],
                data=row["data"],
                state=CampaignState.RUNNING,
            )
            transaction.event("campaign.started", updated, {})
            return updated

        return self._execute(context, "campaign.start", request, "campaign.manage", start)

    def propose_candidate(self, context: CommandContext, request: ProposeCandidate) -> JsonObject:
        operation, scope = "candidate.propose", "candidate.propose"
        previous = self.database.replay(context, operation, request.model_dump(mode="json"), scope)
        if previous is not None:
            return previous
        with verified_source(self.artifacts, context, request.source_artifact) as (_, source):

            def propose(transaction: Transaction) -> JsonObject:
                check_revision(context, transaction.get("campaign", request.campaign_id))
                return propose_candidate(transaction, request, source)

            return self._execute(context, operation, request, scope, propose)

    def validate_candidate(self, context: CommandContext, request: ValidateCandidate) -> JsonObject:
        def validate(transaction: Transaction) -> JsonObject:
            check_revision(context, transaction.get("candidate", request.candidate_id))
            return validate_candidate(transaction, request.candidate_id)

        return self._execute(context, "candidate.validate", request, "candidate.validate", validate)

    def submit_trial(self, context: CommandContext, request: SubmitTrial) -> JsonObject:
        def submit(transaction: Transaction) -> JsonObject:
            check_revision(context, transaction.get("campaign", request.campaign_id))
            return self._submit_trial(transaction, request)

        return self._execute(
            context,
            "trial.submit",
            request,
            "trial.submit",
            submit,
        )

    @staticmethod
    def _submit_trial(
        transaction: Transaction,
        request: SubmitTrial,
        confirmation_plan_id: str | None = None,
        confirmation_side: Literal["candidate", "comparator"] | None = None,
    ) -> JsonObject:
        row = transaction.get("campaign", request.campaign_id)
        campaign = CampaignData.model_validate(row["data"])
        if (
            campaign.contract.promotion.mode == "paired_confirmation"
            and request.seed in campaign.contract.promotion.confirmation_seeds
            and confirmation_plan_id is None
        ):
            raise ResearchError(
                ErrorCode.COMPARISON_INELIGIBLE, "Confirmation seeds require a sealed plan"
            )
        if row["state"] != CampaignState.RUNNING:
            raise ResearchError(
                ErrorCode.INVALID_TRANSITION, "Trial admission requires a running campaign"
            )
        candidate = CandidateData.model_validate(
            transaction.get("candidate", request.candidate_id)["data"]
        )
        validation = CandidateValidation.model_validate(
            transaction.get("candidate_validation", request.validation_id)["data"]
        )
        if (
            candidate.campaign_id != request.campaign_id
            or candidate.project_id != campaign.contract.project_id
            or validation.candidate_id != request.candidate_id
            or validation.source_tree_digest != candidate.source.tree_digest
            or validation.campaign_contract_digest != campaign.contract_digest
            or not validation.valid
        ):
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID,
                "Trial lacks valid candidate evidence for this campaign",
            )
        count = transaction.connection.execute(
            "SELECT COUNT(*) FROM records WHERE workspace_id=? AND kind='trial' AND campaign_id=?",
            (transaction.workspace_id, request.campaign_id),
        ).fetchone()[0]
        if count >= campaign.contract.stopping.max_trials:
            raise ResearchError(ErrorCode.BUDGET_EXHAUSTED, "Campaign trial limit reached")
        revision = ProjectRevisionData.model_validate(
            transaction.get("project_revision", campaign.contract.project_revision_id)["data"]
        )
        resolved = revision.resolved
        ArtifactCatalog.available(transaction, candidate.source_artifact)
        spec = ExperimentSpec(
            workspace_id=transaction.workspace_id,
            project_id=candidate.project_id,
            campaign_id=request.campaign_id,
            candidate_id=request.candidate_id,
            source=SourceRef(
                artifact_id=candidate.source_artifact.id,
                tree_digest=candidate.source.tree_digest,
                base_revision=candidate.source.base_revision,
            ),
            protocol_digest=campaign.contract.protocol_digest,
            comparison_class=campaign.contract.comparison_class,
            environment_digest=resolved.environment_contract_digest,
            evaluator_contract_digest=resolved.evaluator_contract_digest,
            data_manifest_digest=resolved.data_manifest_digest,
            metrics=resolved.manifest.evaluation.metrics,
            stages=resolved.manifest.stages,
            seed=request.seed,
            policy_version=campaign.contract.search.version,
            mutation_allow=resolved.manifest.mutation.allow,
            mutation_deny=resolved.manifest.mutation.deny + resolved.protected_paths,
            extensions={
                "timing": resolved.manifest.timing.model_dump(mode="json"),
                "campaign_contract_digest": campaign.contract_digest,
                **(
                    {
                        "confirmation_plan_id": confirmation_plan_id,
                        "confirmation_side": confirmation_side,
                    }
                    if confirmation_plan_id
                    else {}
                ),
            },
        ).sealed()
        job_ids = {stage.id: new_id() for stage in spec.stages}
        bindings = input_sources(resolved, campaign.contract.input_artifacts)
        trial = transaction.insert(
            "trial",
            spec.trial_id,
            TrialData(
                candidate_id=request.candidate_id,
                validation_id=request.validation_id,
                spec=spec,
                job_ids=tuple(job_ids.values()),
                confirmation_plan_id=confirmation_plan_id,
                confirmation_side=confirmation_side,
            ).model_dump(mode="json"),
            state="queued",
            campaign_id=request.campaign_id,
            parent_id=request.candidate_id,
        )
        for stage in spec.stages:
            job = transaction.insert(
                "job",
                job_ids[stage.id],
                JobData(
                    trial_id=spec.trial_id,
                    stage=stage,
                    predecessor_ids=tuple(job_ids[name] for name in stage.dependencies),
                    input_sources={
                        name: job_ids[provider]
                        for name, provider in bindings[stage.id].items()
                        if provider is not None
                    },
                    inputs={
                        name: campaign.contract.input_artifacts[name]
                        for name, provider in bindings[stage.id].items()
                        if provider is None
                    },
                ).model_dump(mode="json"),
                state="blocked" if stage.dependencies else "ready",
                campaign_id=request.campaign_id,
                parent_id=spec.trial_id,
            )
            transaction.event("job.created", job, {"trial_id": spec.trial_id, "stage_id": stage.id})
        transaction.event("trial.submitted", trial, {"spec_digest": spec.spec_digest})
        return trial
