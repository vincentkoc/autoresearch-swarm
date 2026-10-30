"""Verified costs preserve exact settlement identity and reject inconsistent evidence."""

from dataclasses import replace
from decimal import Decimal

import pytest

from research_swarm.control.settlements import settlement_for_reservation, verify_settlement
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import Transaction, encode
from research_swarm.state.ledger import BudgetLedger
from tests.test_database import create_campaign


@pytest.fixture
def settled(database, context):
    campaign = create_campaign(database, context(), limit=100)["result"]["id"]
    owner, settlement_id = new_id(), new_id()

    def execute(tx):
        reservation = BudgetLedger.reserve(tx, campaign, owner, {"worker_ms": Decimal("10")})
        response = BudgetLedger.settle(
            tx, reservation.id, settlement_id, {"worker_ms": Decimal("3.000000001")}
        )
        return {"reservation_id": reservation.id, "response": response}

    result = database.execute(context(), "settle", {}, "write", execute)["result"]
    return {"campaign_id": campaign, "owner_id": owner, "settlement_id": settlement_id, **result}


def verified(database, context, settled, *, lookup=False, actual=None):
    def read(connection):
        transaction = Transaction(connection, context())
        expected = actual if actual is not None else {"worker_ms": Decimal("3.000000001")}
        if lookup:
            return settlement_for_reservation(
                transaction,
                settled["reservation_id"],
                settled["owner_id"],
                settled["campaign_id"],
                expected,
            )
        return verify_settlement(
            transaction,
            settled["reservation_id"],
            settled["settlement_id"],
            settled["owner_id"],
            settled["campaign_id"],
            expected,
        )

    return database.store.read(read)


def test_generated_settlement_id_resolves_without_rounding_or_mutation(database, context, settled):
    before = database.snapshot(context())
    direct = verified(database, context, settled)
    found = verified(database, context, settled, lookup=True)
    assert direct == found
    assert direct.actual == {"worker_ms": Decimal("3.000000001")}
    assert direct.settlement_id == settled["settlement_id"]
    assert direct.reservation.owner_id == settled["owner_id"]
    assert direct.response_digest == record_digest(
        "research.budget-settlement-response.v1", settled["response"]
    )
    assert database.snapshot(context()) == before


def test_settlement_response_identity_golden_vector(database, context, settled):
    proof = verified(database, context, settled)
    vector = replace(
        proof,
        response={
            "settlement_id": "00000000-0000-0000-0000-000000000002",
            "reservation_id": "00000000-0000-0000-0000-000000000001",
            "actual": {"execution_ms": "3.000000001"},
        },
    )
    assert (
        vector.response_digest
        == "sha256:b5783f923f47fbf0d68d8a4977604c523a3af0c91ccb86edf1e34f56d6acc166"
    )


@pytest.mark.parametrize(
    "change",
    [
        "request_digest",
        "response_id",
        "response_reservation",
        "extra_field",
        "missing_dimension",
        "wrong_dimension",
        "negative",
        "float",
        "boolean",
        "nan",
        "oversize",
        "response_list",
        "reservation_owner",
        "reservation_campaign",
        "row_owner",
        "row_campaign",
        "uncertain",
    ],
)
def test_inconsistent_cost_evidence_is_not_a_qualified_amount(database, context, settled, change):
    def corrupt(connection):
        if change in {"reservation_owner", "reservation_campaign"}:
            row = connection.execute(
                "SELECT payload FROM budget_reservations WHERE id=?", (settled["reservation_id"],)
            ).one()
            payload = strict_json_loads(row["payload"])
            payload["owner_id" if change == "reservation_owner" else "campaign_id"] = new_id()
            connection.execute(
                "UPDATE budget_reservations SET payload=? WHERE id=?",
                (encode(payload), settled["reservation_id"]),
            )
        elif change in {"row_owner", "row_campaign", "uncertain"}:
            column = {"row_owner": "owner_id", "row_campaign": "campaign_id", "uncertain": "state"}[
                change
            ]
            connection.execute(
                f"UPDATE budget_reservations SET {column}=? WHERE id=?",
                ("uncertain" if change == "uncertain" else new_id(), settled["reservation_id"]),
            )
        elif change == "request_digest":
            connection.execute(
                "UPDATE budget_settlements SET request_digest=? WHERE settlement_id=?",
                (record_digest("wrong", {}), settled["settlement_id"]),
            )
        else:
            response = dict(settled["response"])
            response["actual"] = dict(response["actual"])
            if change == "response_id":
                response["settlement_id"] = new_id()
            elif change == "response_reservation":
                response["reservation_id"] = new_id()
            elif change == "extra_field":
                response["extra"] = "unrecognized authority"
            elif change == "missing_dimension":
                response["actual"] = {}
            elif change == "wrong_dimension":
                response["actual"] = {"other_ms": "3.000000001"}
            elif change == "response_list":
                response = []
            else:
                response["actual"]["worker_ms"] = {
                    "negative": "-1",
                    "float": 3.0,
                    "boolean": True,
                    "nan": "NaN",
                    "oversize": "999999999999999999999999999999999999999",
                }[change]
            connection.execute(
                "UPDATE budget_settlements SET response=? WHERE settlement_id=?",
                (encode(response), settled["settlement_id"]),
            )

    database.store.write(corrupt)
    with pytest.raises(ResearchError) as error:
        verified(database, context, settled)
    assert error.value.code == ErrorCode.RESOURCE_UNAVAILABLE


def test_terminal_execution_must_agree_with_settlement(database, context, settled):
    with pytest.raises(ResearchError) as error:
        verified(database, context, settled, actual={"worker_ms": Decimal("3")})
    assert error.value.code == ErrorCode.RESOURCE_UNAVAILABLE


def test_duplicate_settlement_cannot_double_count_a_reservation(database, context, settled):
    response = {**settled["response"], "settlement_id": new_id()}
    digest = record_digest(
        "research.budget-settlement", {key: response[key] for key in ("reservation_id", "actual")}
    )
    database.store.write(
        lambda connection: connection.execute(
            "INSERT INTO budget_settlements VALUES (?,?,?,?)",
            (context().principal.workspace_id, response["settlement_id"], digest, encode(response)),
        )
    )
    for lookup in (False, True):
        with pytest.raises(ResearchError) as error:
            verified(database, context, settled, lookup=lookup)
        assert error.value.code == ErrorCode.RESOURCE_UNAVAILABLE


def test_reopened_store_preserves_exact_lookup_and_old_ledger(database, context, settled):
    from research_swarm.state.database import Database

    reopened = Database(database.path)
    assert verified(reopened, context, settled, lookup=True) == verified(database, context, settled)


def test_valid_ledger_overshoot_uses_consumption_range(database, context):
    campaign = create_campaign(database, context(), limit=10)["result"]["id"]
    owner, settlement_id = new_id(), new_id()
    actual = {"worker_ms": Decimal("10000000000000000000")}

    def execute(tx):
        reservation = BudgetLedger.reserve(tx, campaign, owner, {"worker_ms": Decimal(1)})
        response = BudgetLedger.settle(tx, reservation.id, settlement_id, actual)
        proof = verify_settlement(tx, reservation.id, settlement_id, owner, campaign, actual)
        assert proof.actual == actual
        return response

    response = database.execute(context(), "large-overshoot", {}, "write", execute)["result"]
    assert response["actual"]["worker_ms"] == "10000000000000000000"
    account = database.store.read(
        lambda connection: BudgetLedger.account(
            Transaction(connection, context()), campaign, "worker_ms"
        )
    )
    assert account.consumed == actual["worker_ms"] and account.available < 0


def test_malformed_historical_json_allows_reopen_but_never_qualifies(database, context, settled):
    from research_swarm.state.database import Database

    database.store.write(
        lambda connection: connection.execute(
            "UPDATE budget_settlements SET response=? WHERE settlement_id=?",
            ("{", settled["settlement_id"]),
        )
    )
    reopened = Database(database.path)
    for lookup in (False, True):
        with pytest.raises(ResearchError) as error:
            verified(reopened, context, settled, lookup=lookup)
        assert error.value.code == ErrorCode.RESOURCE_UNAVAILABLE
