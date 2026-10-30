"""Verify exact settled consumption against its reservation and execution evidence."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pydantic import TypeAdapter, ValidationError

from research_swarm.domain.budgets import Reservation
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import BalanceAmount, Dimension
from research_swarm.state.database import Transaction


@dataclass(frozen=True)
class VerifiedSettlement:
    reservation: Reservation
    settlement_id: str
    request_digest: str
    response: dict[str, Any]
    actual: dict[str, Decimal]

    @property
    def response_digest(self) -> str:
        return record_digest("research.budget-settlement-response.v1", self.response)


def verify_settlement(
    transaction: Transaction,
    reservation_id: str,
    settlement_id: str,
    owner_id: str,
    campaign_id: str,
    actual: dict[str, Decimal] | None,
) -> VerifiedSettlement:
    row = transaction.connection.execute(
        "SELECT * FROM budget_reservations WHERE workspace_id=? AND id=?",
        (transaction.workspace_id, reservation_id),
    ).fetchone()
    settled = transaction.connection.execute(
        "SELECT request_digest,response FROM budget_settlements "
        "WHERE workspace_id=? AND settlement_id=?",
        (transaction.workspace_id, settlement_id),
    ).fetchone()
    if row is None or settled is None:
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE, "Issued execution lacks its reservation or settlement"
        )
    try:
        reservation = Reservation.model_validate_json(row["payload"])
        response = strict_json_loads(settled["response"])
        values = TypeAdapter(dict[Dimension, BalanceAmount]).validate_python(response["actual"])
        payload = {"reservation_id": reservation_id, "actual": response["actual"]}
        valid = (
            set(response) == {"settlement_id", "reservation_id", "actual"}
            and row["state"] == "settled"
            and (row["owner_id"], row["campaign_id"]) == (owner_id, campaign_id)
            and (reservation.id, reservation.owner_id, reservation.campaign_id)
            == (reservation_id, owner_id, campaign_id)
            and response["settlement_id"] == settlement_id
            and response["reservation_id"] == reservation_id
            and set(values) == set(reservation.amounts)
            and (actual is None or values == actual)
            and settled["request_digest"] == record_digest("research.budget-settlement", payload)
        )
    except (ValueError, TypeError, KeyError, ValidationError):
        valid = False
    if not valid:
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE, "Issued execution settlement differs from its evidence"
        )
    matches = transaction.connection.execute(
        "SELECT settlement_id FROM budget_settlements WHERE workspace_id=? "
        "AND CASE WHEN json_valid(response) THEN json_extract(response,'$.reservation_id') END=? "
        "ORDER BY settlement_id LIMIT 2",
        (transaction.workspace_id, reservation_id),
    ).fetchall()
    if len(matches) != 1 or matches[0]["settlement_id"] != settlement_id:
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE, "Reservation requires one exact settlement"
        )
    return VerifiedSettlement(
        reservation, settlement_id, settled["request_digest"], response, values
    )


def settlement_for_reservation(
    transaction: Transaction,
    reservation_id: str,
    owner_id: str,
    campaign_id: str,
    actual: dict[str, Decimal] | None,
) -> VerifiedSettlement:
    rows = transaction.connection.execute(
        "SELECT settlement_id FROM budget_settlements WHERE workspace_id=? "
        "AND CASE WHEN json_valid(response) THEN json_extract(response,'$.reservation_id') END=? "
        "ORDER BY settlement_id LIMIT 2",
        (transaction.workspace_id, reservation_id),
    ).fetchall()
    if len(rows) != 1:
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE, "Reservation requires one exact settlement"
        )
    return verify_settlement(
        transaction, reservation_id, rows[0]["settlement_id"], owner_id, campaign_id, actual
    )
