"""Transactional budget reservations and exactly-once settlements."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from research_swarm.domain.budgets import BudgetAccount, BudgetLimit, Reservation
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Transaction, encode


class BudgetLedger:
    """Called inside the same transaction as the job or campaign transition."""

    @staticmethod
    def create(transaction: Transaction, campaign_id: str, limits: tuple[BudgetLimit, ...]) -> None:
        if len({limit.dimension for limit in limits}) != len(limits):
            raise ValueError("Duplicate budget dimension")
        for limit in limits:
            account = BudgetAccount(campaign_id=campaign_id, **limit.model_dump())
            transaction.connection.execute(
                "INSERT INTO budgets(workspace_id,campaign_id,dimension,payload) VALUES (?,?,?,?)",
                (
                    transaction.workspace_id,
                    campaign_id,
                    limit.dimension,
                    encode(account.model_dump(mode="json")),
                ),
            )

    @staticmethod
    def account(transaction: Transaction, campaign_id: str, dimension: str) -> BudgetAccount:
        row = transaction.connection.execute(
            "SELECT payload FROM budgets WHERE workspace_id=? AND campaign_id=? AND dimension=?",
            (transaction.workspace_id, campaign_id, dimension),
        ).fetchone()
        if row is None:
            raise ResearchError(
                ErrorCode.BUDGET_EXHAUSTED, f"Budget dimension is not allocated: {dimension}"
            )
        return BudgetAccount.model_validate_json(row["payload"])

    @staticmethod
    def _save(transaction: Transaction, account: BudgetAccount) -> None:
        account = BudgetAccount.model_validate(account.model_dump())
        transaction.connection.execute(
            "UPDATE budgets SET payload=? WHERE workspace_id=? AND campaign_id=? AND dimension=?",
            (
                encode(account.model_dump(mode="json")),
                transaction.workspace_id,
                account.campaign_id,
                account.dimension,
            ),
        )

    @classmethod
    def reserve(
        cls,
        transaction: Transaction,
        campaign_id: str,
        owner_id: str,
        amounts: dict[str, Decimal],
    ) -> Reservation:
        existing = transaction.connection.execute(
            "SELECT payload FROM budget_reservations WHERE workspace_id=? AND owner_id=?",
            (transaction.workspace_id, owner_id),
        ).fetchone()
        if existing is not None:
            previous = Reservation.model_validate_json(existing["payload"])
            if previous.campaign_id != campaign_id or previous.amounts != amounts:
                raise ResearchError(
                    ErrorCode.IDEMPOTENCY_CONFLICT,
                    "Reservation owner already has another allocation",
                )
            return previous
        reservation = Reservation(campaign_id=campaign_id, owner_id=owner_id, amounts=amounts)
        for dimension, amount in amounts.items():
            account = cls.account(transaction, campaign_id, dimension)
            cls._save(transaction, account.reserve(amount))
        transaction.connection.execute(
            """INSERT INTO budget_reservations
            (workspace_id,id,campaign_id,owner_id,state,payload) VALUES (?,?,?,?,?,?)""",
            (
                transaction.workspace_id,
                reservation.id,
                campaign_id,
                owner_id,
                "reserved",
                encode(reservation.model_dump(mode="json")),
            ),
        )
        return reservation

    @classmethod
    def mark_uncertain(cls, transaction: Transaction, reservation_id: str) -> Reservation:
        reservation, state = cls._reservation(transaction, reservation_id)
        if state == "uncertain":
            return reservation
        if state != "reserved":
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Reservation is already settled")
        for dimension, amount in reservation.amounts.items():
            account = cls.account(transaction, reservation.campaign_id, dimension)
            cls._save(transaction, account.mark_uncertain(amount))
        updated = reservation.model_copy(update={"uncertain": True})
        transaction.connection.execute(
            "UPDATE budget_reservations SET state='uncertain',payload=? "
            "WHERE workspace_id=? AND id=?",
            (encode(updated.model_dump(mode="json")), transaction.workspace_id, reservation_id),
        )
        return updated

    @classmethod
    def settle(
        cls,
        transaction: Transaction,
        reservation_id: str,
        settlement_id: str,
        actual: dict[str, Decimal],
    ) -> dict[str, Any]:
        payload = {
            "reservation_id": reservation_id,
            "actual": {key: str(value) for key, value in actual.items()},
        }
        digest = record_digest("research.budget-settlement", payload)
        existing = transaction.connection.execute(
            "SELECT request_digest,response FROM budget_settlements "
            "WHERE workspace_id=? AND settlement_id=?",
            (transaction.workspace_id, settlement_id),
        ).fetchone()
        if existing is not None:
            if existing["request_digest"] != digest:
                raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Settlement payload changed")
            return dict(strict_json_loads(existing["response"]))
        reservation, state = cls._reservation(transaction, reservation_id)
        if state not in {"reserved", "uncertain"}:
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Reservation is already settled")
        if set(actual) != set(reservation.amounts):
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID,
                "Settlement must account for every reserved dimension",
            )
        for dimension, reserved in reservation.amounts.items():
            account = cls.account(transaction, reservation.campaign_id, dimension)
            consumed = actual[dimension]
            updated = account.settle(reserved, consumed, from_uncertain=state == "uncertain")
            cls._save(transaction, updated)
        transaction.connection.execute(
            "UPDATE budget_reservations SET state='settled' WHERE workspace_id=? AND id=?",
            (transaction.workspace_id, reservation_id),
        )
        result = {"settlement_id": settlement_id, **payload}
        transaction.connection.execute(
            "INSERT INTO budget_settlements VALUES (?,?,?,?)",
            (transaction.workspace_id, settlement_id, digest, encode(result)),
        )
        return result

    @staticmethod
    def _reservation(transaction: Transaction, reservation_id: str) -> tuple[Reservation, str]:
        row = transaction.connection.execute(
            "SELECT state,payload FROM budget_reservations WHERE workspace_id=? AND id=?",
            (transaction.workspace_id, reservation_id),
        ).fetchone()
        if row is None:
            raise ResearchError(ErrorCode.NOT_FOUND, "Budget reservation not found")
        return Reservation.model_validate_json(row["payload"]), row["state"]
