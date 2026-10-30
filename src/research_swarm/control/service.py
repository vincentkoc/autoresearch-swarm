"""Research commands shared by local clients and every network transport."""

from __future__ import annotations

from collections.abc import Callable

from pydantic import ValidationError

from research_swarm.control.commands import (
    CampaignTarget,
    CreateCampaign,
    ProposeCandidate,
    RegisterProject,
    SubmitTrial,
    ValidateCandidate,
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
from research_swarm.control.preflight import verified_source
from research_swarm.control.results import PublishAttempt, ResultControl
from research_swarm.control.workers import ClaimJob, EnrollWorker, WorkerControl
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.budgets import BudgetKind
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.experiments import ExperimentSpec, SourceRef
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.lifecycle import CampaignState, campaign_transition
from research_swarm.domain.types import VersionedRecord, new_id, utc_now
from research_swarm.projects.manifest import ResolvedProject, resolve_project
from research_swarm.projects.source import validate_mutation
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


def retain_source(
    transaction: Transaction, reference: ArtifactRef, kind: str, identifier: str
) -> None:
    ArtifactCatalog.available(transaction, reference)
    ArtifactCatalog.retain(
        transaction,
        ArtifactRetention(
            artifact_id=reference.id, owner_kind=kind, owner_id=identifier, purpose="replay"
        ),
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
        self, database: Database, artifacts: ArtifactService, *, dispatch_key: bytes | None = None
    ) -> None:
        if database.path.resolve() != artifacts.database.path.resolve():
            raise ValueError("Research and artifact catalogs must share a transaction authority")
        self.database = database
        self.artifacts = artifacts
        self.workers = WorkerControl(database, dispatch_key) if dispatch_key is not None else None
        self.results = ResultControl(database, artifacts)
        self.evaluations = EvaluationControl(database, artifacts)

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

    def publish_attempt(self, context: CommandContext, request: PublishAttempt) -> JsonObject:
        return self.results.publish(context, request)

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
                row = transaction.get("campaign", request.campaign_id)
                check_revision(context, row)
                campaign = CampaignData.model_validate(row["data"])
                if campaign.contract_digest is None or row["state"] not in {
                    "ready",
                    "running",
                    "paused",
                    "awaiting_input",
                }:
                    raise ResearchError(
                        ErrorCode.INVALID_TRANSITION, "Campaign cannot accept proposals"
                    )
                if len(set(request.parent_ids)) != len(request.parent_ids):
                    raise ResearchError(
                        ErrorCode.PROJECT_CONTRACT_INVALID, "Candidate parents must be distinct"
                    )
                for parent_id in request.parent_ids:
                    parent = CandidateData.model_validate(
                        transaction.get("candidate", parent_id)["data"]
                    )
                    if (
                        parent.campaign_id != request.campaign_id
                        or parent.project_id != campaign.contract.project_id
                    ):
                        raise ResearchError(
                            ErrorCode.PROJECT_CONTRACT_INVALID,
                            "Candidate parent belongs to another campaign",
                        )
                identifier = new_id()
                candidate = CandidateData(
                    project_id=campaign.contract.project_id,
                    campaign_id=request.campaign_id,
                    source_artifact=request.source_artifact,
                    source=source,
                    parent_ids=request.parent_ids,
                    hypothesis=request.hypothesis,
                    proposer_id=context.principal.id,
                )
                proposed = transaction.insert(
                    "candidate",
                    identifier,
                    candidate.model_dump(mode="json"),
                    state="proposed",
                    campaign_id=request.campaign_id,
                )
                retain_source(transaction, request.source_artifact, "candidate", identifier)
                transaction.event(
                    "candidate.proposed", proposed, {"source_tree_digest": source.tree_digest}
                )
                return proposed

            return self._execute(context, operation, request, scope, propose)

    def validate_candidate(self, context: CommandContext, request: ValidateCandidate) -> JsonObject:
        def validate(transaction: Transaction) -> JsonObject:
            row = transaction.get("candidate", request.candidate_id)
            check_revision(context, row)
            candidate = CandidateData.model_validate(row["data"])
            campaign = CampaignData.model_validate(
                transaction.get("campaign", candidate.campaign_id)["data"]
            )
            assert campaign.contract_digest is not None
            revision = ProjectRevisionData.model_validate(
                transaction.get("project_revision", campaign.contract.project_revision_id)["data"]
            )
            ArtifactCatalog.available(transaction, candidate.source_artifact)
            findings: tuple[str, ...] = ()
            try:
                changed = validate_mutation(
                    revision.resolved.source,
                    candidate.source,
                    revision.resolved.manifest.mutation,
                    protected_paths=revision.resolved.protected_paths,
                )
            except ResearchError as error:
                if error.code != ErrorCode.PROJECT_CONTRACT_INVALID:
                    raise
                changed = tuple(error.details["paths"])
                findings = (error.code.value,)
            validation = CandidateValidation(
                candidate_id=request.candidate_id,
                source_tree_digest=candidate.source.tree_digest,
                campaign_contract_digest=campaign.contract_digest,
                valid=not findings,
                changed_paths=changed,
                findings=findings,
            )
            result = transaction.insert(
                "candidate_validation",
                new_id(),
                validation.model_dump(mode="json"),
                state="valid" if validation.valid else "invalid",
                campaign_id=candidate.campaign_id,
                parent_id=request.candidate_id,
            )
            transaction.event(
                "candidate.validated",
                result,
                {"candidate_id": request.candidate_id, "valid": validation.valid},
            )
            return result

        return self._execute(context, "candidate.validate", request, "candidate.validate", validate)

    def submit_trial(self, context: CommandContext, request: SubmitTrial) -> JsonObject:
        def submit(transaction: Transaction) -> JsonObject:
            row = transaction.get("campaign", request.campaign_id)
            check_revision(context, row)
            campaign = CampaignData.model_validate(row["data"])
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
                "SELECT COUNT(*) FROM records WHERE workspace_id=? "
                "AND kind='trial' AND campaign_id=?",
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
                transaction.event(
                    "job.created", job, {"trial_id": spec.trial_id, "stage_id": stage.id}
                )
            transaction.event("trial.submitted", trial, {"spec_digest": spec.spec_digest})
            return trial

        return self._execute(context, "trial.submit", request, "trial.submit", submit)
