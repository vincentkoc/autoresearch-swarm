"""Bounded snapshots of unresolved custody; closed historical trials are not rescanned."""

from dataclasses import dataclass
from typing import Any

from research_swarm.control.completion_models import ObligationReview
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import JsonObject, Transaction

CLOSED = ("succeeded", "failed", "cancelled", "not_started", "closed_unaccepted")
MAX_ENTRIES = 16_384
MAX_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class Obligations:
    entries: tuple[JsonObject, ...]
    counts: dict[str, int]

    @property
    def digest(self) -> str:
        return record_digest("research.campaign-obligations-v1", {"entries": self.entries})


def inspect_obligations(transaction: Transaction, campaign_id: str) -> Obligations:
    entries: list[JsonObject] = []
    counts: dict[str, int] = {}
    size = 0

    def append(category: str, value: JsonObject) -> None:
        nonlocal size
        entry = {"category": category, **value}
        size += len(canonical_bytes(entry)) + 1
        if len(entries) >= MAX_ENTRIES or size > MAX_BYTES:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Obligation review exceeds its local profile bound"
            )
        entries.append(entry)
        counts[category] = counts.get(category, 0) + 1

    rows = transaction.connection.execute(
        "SELECT kind,id FROM records WHERE workspace_id=? AND campaign_id=? AND "
        "((kind IN ('attempt','job') AND state NOT IN (" + ",".join("?" for _ in CLOSED) + ")) OR "
        "(kind='trial' AND (json_extract(payload,'$.resource_status')!='closed' "
        "OR json_extract(payload,'$.execution_outcome')='unknown'))) ORDER BY kind,id LIMIT ?",
        (transaction.workspace_id, campaign_id, *CLOSED, MAX_ENTRIES + 1),
    )
    for row in rows:
        value = transaction.get(row["kind"], row["id"])
        append(
            row["kind"],
            {
                "id": value["id"],
                "revision": value["revision"],
                "state": value["state"],
                "record_digest": record_digest("research.obligation-record-v1", value),
            },
        )
    for row in transaction.connection.execute(
        "SELECT payload FROM budgets WHERE workspace_id=? AND campaign_id=? ORDER BY dimension",
        (transaction.workspace_id, campaign_id),
    ):
        append("budget", {"value": strict_json_loads(row["payload"])})
    for row in transaction.connection.execute(
        "SELECT id,owner_id,state,payload FROM budget_reservations WHERE workspace_id=? "
        "AND campaign_id=? AND state!='settled' ORDER BY id",
        (transaction.workspace_id, campaign_id),
    ):
        append(
            "reservation",
            {
                "id": row["id"],
                "owner_id": row["owner_id"],
                "state": row["state"],
                "value": strict_json_loads(row["payload"]),
            },
        )
    for row in transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND kind='funding_projection' "
        "AND parent_id=? AND state='open' ORDER BY id",
        (transaction.workspace_id, campaign_id),
    ):
        child = transaction.get("campaign", row["id"])
        funding = transaction.get("funding_projection", row["id"])
        append(
            "child",
            {
                "id": child["id"],
                "revision": child["revision"],
                "state": child["state"],
                "final_report_id": child["data"].get("final_report_id"),
                "funding_revision": funding["revision"],
                "funding_digest": record_digest("research.obligation-funding-v1", funding),
            },
        )
    return Obligations(tuple(entries), counts)


def require_review(
    transaction: Transaction, campaign: JsonObject, review_id: str
) -> tuple[ObligationReview, Obligations]:
    review = ObligationReview.model_validate(
        transaction.get("campaign_obligations", review_id)["data"]
    )
    current = inspect_obligations(transaction, campaign["id"])
    if (
        review.campaign_id != campaign["id"]
        or review.campaign_revision != campaign["revision"]
        or review.obligations_digest != current.digest
    ):
        raise ResearchError(
            ErrorCode.REVISION_CONFLICT, "Campaign obligations changed after review"
        )
    return review, current


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
