"""One typed command catalog for SDK, HTTP, CLI, and agent tools."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from research_swarm.control.commands import (
    CampaignTarget,
    CreateCampaign,
    ProposeCandidate,
    RegisterProject,
    SubmitTrial,
    ValidateCandidate,
)
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


COMMANDS = {
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
                "expected_revision": definition.expected_revision,
                "request_schema": definition.request.model_json_schema(),
            }
            for operation, definition in COMMANDS.items()
        },
        "queries": ["get", "list", "context", "budget", "events"],
        "execution": "worker_integration_pending",
        "drivers": {"local": "control_only", "temporal": "pending", "cloudflare": "pending"},
    }
