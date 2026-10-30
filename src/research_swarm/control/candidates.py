"""Candidate transaction operations shared by manual and policy-driven admission."""

from __future__ import annotations

from research_swarm.control.commands import ProposeCandidate
from research_swarm.control.models import (
    CampaignData,
    CandidateData,
    CandidateValidation,
    ProjectRevisionData,
)
from research_swarm.domain.artifacts import ArtifactRetention
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import new_id
from research_swarm.projects.source import SourceManifest, validate_mutation
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import JsonObject, Transaction


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


def propose_candidate(
    transaction: Transaction,
    request: ProposeCandidate,
    source: SourceManifest,
    *,
    proposer_id: str | None = None,
) -> JsonObject:
    row = transaction.get("campaign", request.campaign_id)
    campaign = CampaignData.model_validate(row["data"])
    if campaign.contract_digest is None or row["state"] not in {
        "ready",
        "running",
        "paused",
        "awaiting_input",
    }:
        raise ResearchError(ErrorCode.INVALID_TRANSITION, "Campaign cannot accept proposals")
    if len(set(request.parent_ids)) != len(request.parent_ids):
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Candidate parents must be distinct"
        )
    for parent_id in request.parent_ids:
        parent = CandidateData.model_validate(transaction.get("candidate", parent_id)["data"])
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
        proposer_id=proposer_id or transaction.context.principal.id,
    )
    proposed = transaction.insert(
        "candidate",
        identifier,
        candidate.model_dump(mode="json"),
        state="proposed",
        campaign_id=request.campaign_id,
    )
    retain_source(transaction, request.source_artifact, "candidate", identifier)
    transaction.event("candidate.proposed", proposed, {"source_tree_digest": source.tree_digest})
    return proposed


def validate_candidate(transaction: Transaction, candidate_id: str) -> JsonObject:
    row = transaction.get("candidate", candidate_id)
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
        candidate_id=candidate_id,
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
        parent_id=candidate_id,
    )
    transaction.event(
        "candidate.validated",
        result,
        {"candidate_id": candidate_id, "valid": validation.valid},
    )
    return result
