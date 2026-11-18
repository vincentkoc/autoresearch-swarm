"""Read completion records written by earlier local runtimes."""

from typing import Any

from pydantic import Field

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, Digest, Revision, Timestamp, VersionedRecord
from research_swarm.state.database import JsonObject, Transaction


class ObligationReview(VersionedRecord):
    campaign_id: ID
    campaign_revision: Revision
    obligations_digest: Digest
    chunk_ids: tuple[ID, ...] = Field(max_length=128)
    entry_count: Revision
    counts: dict[str, int]
    request_digest: Digest
    captured_by: ID
    captured_at: Timestamp


def operational_context(transaction: Transaction, campaign_id: str) -> JsonObject | None:
    try:
        completion = transaction.get("scientific_completion", campaign_id)
    except ResearchError as error:
        if error.code == ErrorCode.NOT_FOUND:
            return None
        raise
    try:
        closed = transaction.get("campaign_resource_closure", campaign_id)
    except ResearchError as error:
        if error.code != ErrorCode.NOT_FOUND:
            raise
        closed = None
    return {
        "scientific_completion_id": completion["id"],
        "final_report_id": completion["data"]["final_report_id"],
        "obligations_id": completion["data"]["obligations_id"],
        "resource_closure_id": closed["id"] if closed else None,
        "status": "closed" if closed else "reconciliation_pending",
    }


def admitted_before_completion(transaction: Transaction, campaign_id: str, attempt_id: str) -> bool:
    try:
        completion = transaction.get("scientific_completion", campaign_id)
    except ResearchError as error:
        if error.code == ErrorCode.NOT_FOUND:
            return False
        raise
    review = ObligationReview.model_validate(
        transaction.get("campaign_obligations", completion["data"]["obligations_id"])["data"]
    )
    for chunk_id in review.chunk_ids:
        entries: list[dict[str, Any]] = transaction.get("campaign_obligation_chunk", chunk_id)[
            "data"
        ]["entries"]
        if any(entry["category"] == "attempt" and entry["id"] == attempt_id for entry in entries):
            return True
    return False
