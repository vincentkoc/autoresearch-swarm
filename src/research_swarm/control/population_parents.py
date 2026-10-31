"""Freeze archive ancestry while preserving the campaign's independent comparator."""

from research_swarm.control.models import CampaignData
from research_swarm.control.population import member_evidence
from research_swarm.control.population_models import (
    FrozenPopulationParent,
    PopulationChange,
    PopulationMember,
    PopulationParentRef,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Transaction


def require_frozen_parent(
    transaction: Transaction,
    parent: FrozenPopulationParent,
    campaign_id: str,
    campaign: CampaignData,
) -> None:
    row = transaction.get("population_change", parent.change_id)
    change = PopulationChange.model_validate(row["data"])
    member = parent.member
    if (
        row["state"] != "immutable"
        or row["campaign_id"] != campaign_id
        or row["parent_id"] != parent.reference.member_id
        or change.member_id != parent.reference.member_id
        or change.after_revision != parent.reference.revision
        or change.state != "active"
        or change.member != member
        or member.request.campaign_id != campaign_id
        or member.request.candidate_id != parent.reference.member_id
        or member.campaign_contract_digest != campaign.contract_digest
        or (campaign.contract.search.policy, campaign.contract.search.version)
        != ("external", "external-v1")
    ):
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Frozen archive ancestry differs")
    candidate, evaluation = member_evidence(transaction, member.request, campaign)
    if (
        candidate.source_artifact != member.source_artifact
        or candidate.source.tree_digest != member.source_tree_digest
        or evaluation.comparison_digest != member.comparison_digest
    ):
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Archive source or evidence changed")


def freeze_parent(
    transaction: Transaction,
    reference: PopulationParentRef,
    campaign_id: str,
    campaign: CampaignData,
) -> FrozenPopulationParent:
    if (campaign.contract.search.policy, campaign.contract.search.version) != (
        "external",
        "external-v1",
    ):
        raise ResearchError(
            ErrorCode.CAPABILITY_UNAVAILABLE, "Archive parent choice requires external-v1 search"
        )
    row = transaction.get("population_member", reference.member_id)
    if row["revision"] != reference.revision:
        raise ResearchError(ErrorCode.REVISION_CONFLICT, "Selected archive membership changed")
    if row["campaign_id"] != campaign_id or row["state"] != "active":
        raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Archive parent is not selectable")
    changes = transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND campaign_id=? "
        "AND kind='population_change' AND parent_id=? "
        "AND json_extract(payload,'$.after_revision')=? LIMIT 2",
        (transaction.workspace_id, campaign_id, reference.member_id, reference.revision),
    ).fetchall()
    if len(changes) != 1:
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Archive membership history differs")
    frozen = FrozenPopulationParent(
        reference=reference,
        change_id=changes[0]["id"],
        member=PopulationMember.model_validate(row["data"]),
    )
    require_frozen_parent(transaction, frozen, campaign_id, campaign)
    return frozen
