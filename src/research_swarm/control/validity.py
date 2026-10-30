"""Append-only corrections override eligibility without rewriting evidence."""

from typing import Literal

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, VersionedRecord
from research_swarm.state.database import Transaction


class EvidenceReference(VersionedRecord):
    kind: Literal["run_result", "evaluation_receipt"]
    id: ID


def invalidated(transaction: Transaction, reference: EvidenceReference) -> bool:
    rows = transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND kind='evidence_correction' "
        "AND parent_id=? AND state='effective'",
        (transaction.workspace_id, reference.id),
    ).fetchall()
    return any(
        transaction.get("evidence_correction", row["id"])["data"]["target"]
        == reference.model_dump(mode="json")
        for row in rows
    )


def require_current(transaction: Transaction, references: tuple[EvidenceReference, ...]) -> None:
    for reference in references:
        if invalidated(transaction, reference):
            raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Evidence has an active correction")
