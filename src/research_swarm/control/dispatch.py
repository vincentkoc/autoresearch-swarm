"""One typed command catalog for SDK, HTTP, CLI, and agent tools."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

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
from research_swarm.control.promotions import AssessPromotion, CommitPromotion, InvalidateEvidence
from research_swarm.control.reports import ReportCampaign
from research_swarm.control.results import PublishAttempt
from research_swarm.control.service import ResearchService
from research_swarm.control.workers import ClaimJob, EnrollWorker
from research_swarm.domain.errors import ErrorCode, ResearchError
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
        "queries": ["get", "list", "context", "budget", "events"],
        "execution": "registered_worker_required",
        "evaluation": "authenticated_receipts",
        "drivers": {"local": "explicit_worker", "temporal": "pending", "cloudflare": "pending"},
    }
