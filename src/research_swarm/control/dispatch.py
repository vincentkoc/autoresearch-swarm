"""One typed command catalog for SDK, HTTP, CLI, and agent tools."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from research_swarm.control.child_models import (
    CancelChildCampaign,
    CreateChildCampaign,
    SettleChildCampaign,
)
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
from research_swarm.control.confirmation import AssessConfirmation, CreateConfirmationPlan
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.observation_models import (
    AbandonObservationSelection,
    ObserveTrial,
    ResolveTrialObservation,
    SelectTrialObservation,
)
from research_swarm.control.orchestration_models import (
    BindOrchestration,
    CloseDriveTurn,
    DriveTurnTarget,
    FenceOrchestration,
    PrepareDriveTurn,
)
from research_swarm.control.promotions import AssessPromotion, CommitPromotion, InvalidateEvidence
from research_swarm.control.reconciliation import ReconcileAttempt
from research_swarm.control.reports import ReportCampaign
from research_swarm.control.results import PublishAttempt
from research_swarm.control.search_models import (
    AdmitProposalResponse,
    CancelProposal,
    ProposalRead,
    RequestProposal,
    RespondToProposal,
)
from research_swarm.control.service import ResearchService
from research_swarm.control.wait_models import (
    AcknowledgeWakeup,
    OrchestrationWaitTarget,
    RecordOrchestrationWait,
)
from research_swarm.control.worker_artifacts import WorkerArtifactRead
from research_swarm.control.worker_lifecycle import (
    RequestAttemptCancellation,
    SetWorkerAdmission,
    WorkerHeartbeat,
)
from research_swarm.control.worker_outputs import BeginWorkerUpload, WorkerUploadTarget
from research_swarm.control.worker_queries import WorkerQuery, WorkerRead
from research_swarm.control.worker_turnover import FenceWorker, ReplaceWorkerSession
from research_swarm.control.workers import ClaimJob, EnrollWorker
from research_swarm.domain.clocks import ClockChallenge
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.event_recovery import (
    AdvanceEventFloor,
    CollectExpiredSnapshots,
    CreateRecoverySnapshot,
    RecoverySnapshotTarget,
)
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import VersionedRecord
from research_swarm.state.database import JsonObject


@dataclass(frozen=True)
class CommandDefinition:
    request: type[VersionedRecord]
    method: str
    scope: str
    expected_revision: bool
    description: str
    additional_scopes: tuple[str, ...] = ()


COMMANDS = {
    "orchestration.close_turn": CommandDefinition(
        CloseDriveTurn,
        "close_drive_turn",
        "orchestration.manage",
        True,
        "Close a fenced action with no target acknowledgment while preserving evidence.",
        additional_scopes=("read",),
    ),
    "stream.collect_expired": CommandDefinition(
        CollectExpiredSnapshots,
        "collect_expired_snapshots",
        "stream.manage",
        False,
        "Collect a bounded batch of expired frozen contents while retaining terminal metadata.",
    ),
    "stream.create_snapshot": CommandDefinition(
        CreateRecoverySnapshot,
        "create_recovery_snapshot",
        "read",
        False,
        "Freeze an authorized read model and pin its event catch-up interval.",
    ),
    "stream.release_snapshot": CommandDefinition(
        RecoverySnapshotTarget,
        "release_recovery_snapshot",
        "read",
        False,
        "Release this reader's recovery pin and disposable frozen page contents.",
    ),
    "stream.advance_floor": CommandDefinition(
        AdvanceEventFloor,
        "advance_event_floor",
        "stream.manage",
        False,
        "Advance logical stream retention without deleting evidence or crossing recovery pins.",
    ),
    "orchestration.record_wait": CommandDefinition(
        RecordOrchestrationWait,
        "record_orchestration_wait",
        "orchestration.drive",
        True,
        "Persist event and timer conditions without losing events committed during planning.",
        additional_scopes=("read",),
    ),
    "orchestration.reconcile_wait": CommandDefinition(
        OrchestrationWaitTarget,
        "reconcile_orchestration_wait",
        "orchestration.drive",
        False,
        "Reconcile timer or event readiness without a final acknowledgment for pending conditions.",
        additional_scopes=("read",),
    ),
    "orchestration.acknowledge_wakeup": CommandDefinition(
        AcknowledgeWakeup,
        "acknowledge_orchestration_wakeup",
        "orchestration.drive",
        True,
        "Acknowledge destination delivery without consuming a wait or research evidence.",
    ),
    "orchestration.bind": CommandDefinition(
        BindOrchestration,
        "bind_orchestration",
        "orchestration.manage",
        True,
        "Assign dedicated driver actors without changing native worker authority.",
    ),
    "orchestration.prepare_turn": CommandDefinition(
        PrepareDriveTurn,
        "prepare_drive_turn",
        "orchestration.drive",
        True,
        "Persist one exact action and ordinal before any delivery.",
    ),
    "orchestration.execute_turn": CommandDefinition(
        DriveTurnTarget,
        "execute_drive_turn",
        "orchestration.drive",
        False,
        "Execute or recover the original saved action under its authenticated route.",
    ),
    "orchestration.reconcile_turn": CommandDefinition(
        DriveTurnTarget,
        "reconcile_drive_turn",
        "orchestration.drive",
        False,
        "Close a turn only from its authoritative target command acknowledgment.",
    ),
    "orchestration.fence": CommandDefinition(
        FenceOrchestration,
        "fence_orchestration",
        "orchestration.manage",
        True,
        "Stop new driver effects while preserving native work and prior acknowledgments.",
    ),
    "search.abandon_selection": CommandDefinition(
        AbandonObservationSelection,
        "abandon_search_selection",
        "search.manage",
        True,
        "Resolve a pinned trial using fresh closed non-scientific evidence and an explicit reason.",
    ),
    "search.select_observation": CommandDefinition(
        SelectTrialObservation,
        "select_search_observation",
        "search.manage",
        True,
        "Pin the original observation and comparator before independent selection effects.",
    ),
    "search.observe_trial": CommandDefinition(
        ObserveTrial,
        "observe_search_trial",
        "search.observe",
        True,
        "Record typed current evidence using the trial revision, without advancing search.",
    ),
    "search.resolve_trial": CommandDefinition(
        ResolveTrialObservation,
        "resolve_search_trial",
        "search.manage",
        True,
        "Consume evidence under search-state CAS after selection or closed failure.",
    ),
    "search.admit_response": CommandDefinition(
        AdmitProposalResponse,
        "admit_proposal_response",
        "search.manage",
        True,
        "Atomically consume a preserved response, validate its candidate, and queue eligible work.",
        additional_scopes=("candidate.propose", "candidate.validate", "trial.submit", "read"),
    ),
    "search.request_proposal": CommandDefinition(
        RequestProposal,
        "request_proposal",
        "search.manage",
        True,
        "Assign external research from frozen incumbent context and a request-count budget.",
    ),
    "search.respond": CommandDefinition(
        RespondToProposal,
        "respond_proposal",
        "proposal.respond",
        False,
        "Preserve the assigned agent response before candidate or policy publication.",
    ),
    "search.cancel_proposal": CommandDefinition(
        CancelProposal,
        "cancel_proposal",
        "search.manage",
        True,
        "Close proposal admission while retaining late output and already consumed request budget.",
    ),
    "worker.fence": CommandDefinition(
        FenceWorker,
        "fence_worker",
        "worker.manage",
        True,
        "Fence server authority while preserving native work and original evidence access.",
    ),
    "worker.replace_session": CommandDefinition(
        ReplaceWorkerSession,
        "replace_worker_session",
        "worker.manage",
        True,
        "Replace an explicitly fenced session with fresh identities and draining admission.",
    ),
    "attempt.reconcile_closure": CommandDefinition(
        ReconcileAttempt,
        "reconcile_attempt_closure",
        "attempt.reconcile",
        True,
        "Settle proven native closure without accepting research or admitting successors.",
    ),
    "worker.submit_evidence": CommandDefinition(
        PublishAttempt,
        "submit_attempt_evidence",
        "worker.publish",
        False,
        "Quarantine original attempt evidence without accepting execution or settling costs.",
    ),
    "attempt.reconcile": CommandDefinition(
        ReconcileAttempt,
        "reconcile_attempt",
        "attempt.reconcile",
        True,
        "Reconcile quarantined closure against the original attempt and current authority.",
    ),
    "worker.heartbeat": CommandDefinition(
        WorkerHeartbeat,
        "worker_heartbeat",
        "worker.publish",
        False,
        "Record presence and control acknowledgments without renewing execution authority.",
    ),
    "worker.set_admission": CommandDefinition(
        SetWorkerAdmission,
        "set_worker_admission",
        "worker.manage",
        True,
        "Change fresh admission using its own revision; existing grants remain valid.",
    ),
    "attempt.request_cancel": CommandDefinition(
        RequestAttemptCancellation,
        "request_attempt_cancel",
        "campaign.manage",
        True,
        "Request cancellation for one attempt without claiming closure or refunding resources.",
    ),
    "worker.begin_upload": CommandDefinition(
        BeginWorkerUpload,
        "begin_worker_upload",
        "worker.outputs",
        False,
        "Reserve attempt-bound output storage and preserve its upload provenance.",
    ),
    "worker.finalize_upload": CommandDefinition(
        WorkerUploadTarget,
        "finalize_worker_upload",
        "worker.outputs",
        False,
        "Verify bytes and retain canonical output provenance without accepting execution results.",
    ),
    "worker.cleanup_upload": CommandDefinition(
        WorkerUploadTarget,
        "cleanup_worker_upload",
        "worker.outputs",
        False,
        "Resume duplicate-generation cleanup without changing publication acknowledgment.",
    ),
    "campaign.pause": CommandDefinition(
        PauseCampaign,
        "pause_campaign",
        "campaign.manage",
        True,
        "Stop new trial admission and drain already active trials before pausing.",
    ),
    "campaign.complete": CommandDefinition(
        CompleteCampaign,
        "complete_campaign",
        "campaign.manage",
        True,
        "Complete settled research and atomically record its final human and agent report.",
    ),
    "report.create": CommandDefinition(
        ReportCampaign,
        "report_campaign",
        "report.create",
        False,
        "Capture bounded evidence, selection, failures, and budgets as JSON and Markdown.",
    ),
    "confirmation.assess": CommandDefinition(
        AssessConfirmation,
        "assess_confirmation",
        "promotion.assess",
        False,
        "Assess the complete preselected seed panel without changing its membership.",
    ),
    "confirmation.create": CommandDefinition(
        CreateConfirmationPlan,
        "create_confirmation",
        "confirmation.manage",
        True,
        "Seal paired trials, execution profiles, and exposure reservations before admission.",
    ),
    "promotion.reinitialize": CommandDefinition(
        AssessPromotion,
        "reinitialize_baseline",
        "promotion.reinitialize",
        False,
        "Assess a fresh observed baseline after evidence correction suspended the incumbent.",
    ),
    "promotion.assess": CommandDefinition(
        AssessPromotion,
        "assess_promotion",
        "promotion.assess",
        False,
        "Assess eligible evidence against the incumbent without changing it.",
    ),
    "promotion.commit": CommandDefinition(
        CommitPromotion,
        "commit_promotion",
        "promotion.commit",
        False,
        "Recheck a persisted assessment and atomically select the incumbent.",
    ),
    "evidence.invalidate": CommandDefinition(
        InvalidateEvidence,
        "invalidate_evidence",
        "evidence.invalidate",
        False,
        "Append an evidence correction and suspend affected incumbent eligibility.",
    ),
    "evaluation.authorize": CommandDefinition(
        AuthorizeEvaluation,
        "authorize_evaluation",
        "evaluation.request",
        True,
        "Authorize an evaluator, input set, seed, and panel before evaluation executes.",
        ("read", "artifact.upload"),
    ),
    "evaluation.complete": CommandDefinition(
        CompleteEvaluation,
        "complete_evaluation",
        "evaluation.publish",
        False,
        "Record an evaluator receipt from the authorized native report, without supplied metrics.",
        ("read",),
    ),
    "worker.publish_attempt": CommandDefinition(
        PublishAttempt,
        "publish_attempt",
        "worker.publish",
        False,
        "Publish native observation artifacts against the original admitted attempt.",
    ),
    "worker.enroll": CommandDefinition(
        EnrollWorker,
        "enroll_worker",
        "worker.manage",
        False,
        "Enroll one worker session with explicit profiles and publication authority.",
    ),
    "worker.claim_job": CommandDefinition(
        ClaimJob,
        "claim_job",
        "worker.claim",
        True,
        "Reserve a ready job and its attempt budget using the current job revision.",
    ),
    "project.register": CommandDefinition(
        RegisterProject,
        "register_project",
        "project.manage",
        False,
        "Register immutable project source. Updating a project requires its current revision.",
    ),
    "campaign.create": CommandDefinition(
        CreateCampaign,
        "create_campaign",
        "campaign.manage",
        False,
        "Create a draft campaign and its budget accounts.",
    ),
    "campaign.create_child": CommandDefinition(
        CreateChildCampaign,
        "create_child_campaign",
        "campaign.manage",
        True,
        "Fund a draft child atomically using the current parent campaign revision.",
    ),
    "campaign.settle_child": CommandDefinition(
        SettleChildCampaign,
        "settle_child_campaign",
        "campaign.manage",
        True,
        "Settle terminal child funding using its allocation revision; settled receipts replay.",
    ),
    "campaign.cancel_child": CommandDefinition(
        CancelChildCampaign,
        "cancel_child_campaign",
        "campaign.manage",
        True,
        "Cancel an unstarted funded child using its campaign revision and release unused funding.",
    ),
    "campaign.validate": CommandDefinition(
        CampaignTarget,
        "validate_campaign",
        "campaign.manage",
        True,
        "Check campaign inputs and contracts without starting work.",
    ),
    "campaign.seal": CommandDefinition(
        CampaignTarget,
        "seal_campaign",
        "campaign.manage",
        True,
        "Freeze the campaign contract and move draft to ready.",
    ),
    "campaign.start": CommandDefinition(
        CampaignTarget,
        "start_campaign",
        "campaign.manage",
        True,
        "Open admission for a sealed campaign. This does not complete an experiment.",
    ),
    "candidate.propose": CommandDefinition(
        ProposeCandidate,
        "propose_candidate",
        "candidate.propose",
        True,
        "Preserve candidate source, hypothesis, and ancestry using the campaign revision.",
    ),
    "candidate.validate": CommandDefinition(
        ValidateCandidate,
        "validate_candidate",
        "candidate.validate",
        True,
        "Check mutation boundaries using the candidate revision. Does not run code.",
    ),
    "trial.submit": CommandDefinition(
        SubmitTrial,
        "submit_trial",
        "trial.submit",
        True,
        "Atomically queue an immutable trial and all stages using the campaign revision.",
    ),
}


def dispatch_command(
    service: ResearchService, context: CommandContext, operation: str, payload: dict[str, Any]
) -> JsonObject:
    definition = COMMANDS.get(operation)
    if definition is None:
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Unknown research command")
    context.principal.require(definition.scope)
    for scope in definition.additional_scopes:
        context.principal.require(scope)
    try:
        request = definition.request.model_validate(payload)
    except ValidationError as error:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID,
            "Command input does not match its schema",
            details={"fields": [list(item["loc"]) for item in error.errors(include_input=False)]},
        ) from error
    response: JsonObject = getattr(service, definition.method)(context, request)
    return response


def capabilities() -> JsonObject:
    return {
        "schema_version": "1.0",
        "commands": {
            operation: {
                "description": definition.description,
                "scope": definition.scope,
                "required_scopes": [definition.scope, *definition.additional_scopes],
                "expected_revision": definition.expected_revision,
                "request_schema": definition.request.model_json_schema(),
            }
            for operation, definition in COMMANDS.items()
        },
        "queries": ["get", "list", "context", "budget", "events", "snapshot.page", "snapshot.item"],
        "proposal_queries": {
            "scope": "proposal.respond",
            "operations": {"proposal.get": ProposalRead.model_json_schema()},
        },
        "worker_queries": {
            "scope": "worker.query",
            "operations": {
                "worker.get": WorkerRead.model_json_schema(),
                "worker.list": WorkerQuery.model_json_schema(),
                "worker.clock": ClockChallenge.model_json_schema(),
            },
        },
        "worker_inputs": {
            "scope": "worker.inputs",
            "request_schema": WorkerArtifactRead.model_json_schema(),
        },
        "worker_outputs": {
            "scope": "worker.outputs",
            "request_schema": WorkerUploadTarget.model_json_schema(),
            "publication_scope": "worker.publish",
        },
        "execution": "registered_worker_required",
        "clock": {
            "default_mode": "local-utc-v1",
            "calibrated_mode": "bounded-continuous-v1",
            "time_evidence": "signed_process_epoch",
            "calibrated_execution": "live_origin_required",
        },
        "evaluation": "authenticated_receipts",
        "drivers": {
            "local": "explicit_worker",
            "remote": "scoped_http_worker",
            "temporal": "pending",
            "cloudflare": "pending",
        },
    }
