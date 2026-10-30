"""Exact controller record references carried by qualified learning evidence."""

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.learning import LearningEvidence
from research_swarm.state.database import JsonObject, Transaction


def record_evidence(row: JsonObject) -> LearningEvidence:
    return LearningEvidence(
        kind=row["kind"],
        id=row["id"],
        revision=row["revision"],
        digest=record_digest("research.learning-evidence.v1", row),
    )


def capture_record(
    transaction: Transaction, kind: str, identifier: str, evidence: list[LearningEvidence]
) -> JsonObject:
    row = transaction.get(kind, identifier)
    reference = record_evidence(row)
    if reference not in evidence:
        evidence.append(reference)
    return row
