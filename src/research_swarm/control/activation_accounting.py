"""Verify issued execution settlement without releasing queued research funding."""

from decimal import Decimal

from research_swarm.control.acceptance import dispatch_digest, lease_row
from research_swarm.control.acceptance_models import AcceptanceLease
from research_swarm.control.closure import AttemptClosure
from research_swarm.control.confirmation_models import ExposureData
from research_swarm.control.confirmation_runtime import bound_confirmation
from research_swarm.control.models import JobData, TrialData
from research_swarm.control.worker_records import AttemptData
from research_swarm.domain.backup import RestoreReceipt
from research_swarm.domain.budgets import BudgetAccount, BudgetEntry, BudgetKind, Reservation
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import amount_sum
from research_swarm.domain.workers import WorkerBinding
from research_swarm.state.database import Transaction

TERMINAL = frozenset({"succeeded", "failed", "cancelled", "not_started", "closed_unaccepted"})


def _refuse(message: str) -> None:
    raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, message)


def _settlement(
    transaction: Transaction,
    reservation_id: str,
    settlement_id: str,
    owner_id: str,
    campaign_id: str,
    actual: dict[str, Decimal] | None,
) -> None:
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
        _refuse("Issued execution lacks its reservation or settlement")
    assert row is not None and settled is not None
    reservation = Reservation.model_validate_json(row["payload"])
    response = strict_json_loads(settled["response"])
    values = {key: Decimal(value) for key, value in response["actual"].items()}
    payload = {"reservation_id": reservation_id, "actual": response["actual"]}
    if (
        row["state"] != "settled"
        or (row["owner_id"], row["campaign_id"]) != (owner_id, campaign_id)
        or (reservation.id, reservation.owner_id, reservation.campaign_id)
        != (reservation_id, owner_id, campaign_id)
        or response["settlement_id"] != settlement_id
        or response["reservation_id"] != reservation_id
        or set(values) != set(reservation.amounts)
        or any(not value.is_finite() or value < 0 for value in values.values())
        or (actual is not None and values != actual)
        or settled["request_digest"] != record_digest("research.budget-settlement", payload)
    ):
        _refuse("Issued execution settlement differs from its evidence")


def _attempt(transaction: Transaction, identifier: str, *, original: bool) -> bool:
    row = transaction.get("attempt", identifier)
    attempt = AttemptData.model_validate(row["data"])
    dispatch = attempt.dispatch
    if (
        row["state"] not in TERMINAL
        or attempt.publication_status not in {"accepted", "closed_unaccepted"}
        or (original and attempt.publication_status != "closed_unaccepted")
        or dispatch.attempt_id != identifier
        or len(dispatch.reservation_ids) != 1
    ):
        _refuse("Original native execution must close before activation")
    binding = WorkerBinding.model_validate(
        {key: getattr(dispatch, key) for key in WorkerBinding.model_fields}
    )
    lease_record = lease_row(transaction, identifier)
    lease = AcceptanceLease.model_validate(lease_record["data"])
    legacy_accepted = (
        not original
        and attempt.publication_status == "accepted"
        and lease_record["revision"] == 0
        and lease_record["state"] == "unqualified"
        and lease.policy_version == "legacy-unqualified-v1"
    )
    if not legacy_accepted and (
        lease_record["state"] != "settled"
        or lease.attempt_id != identifier
        or lease.binding != binding
        or lease.dispatch_digest != dispatch_digest(attempt)
        or lease.disposition != attempt.publication_status
    ):
        _refuse("Attempt acceptance authority is not settled")
    actual = None
    not_started: bool = row["state"] == "not_started"
    if attempt.publication_status == "closed_unaccepted":
        if attempt.closure_receipt_id is None or row["state"] != "closed_unaccepted":
            _refuse("Operational closure receipt is absent")
        assert attempt.closure_receipt_id is not None
        evidence = transaction.get("attempt_closure", attempt.closure_receipt_id)
        closure = AttemptClosure.model_validate(evidence["data"])
        submission = transaction.get("attempt_submission", closure.submission_id)
        if (
            evidence["state"] != "immutable"
            or closure.attempt_id != identifier
            or closure.original_binding != binding
            or closure.journal_artifact != attempt.outcome_artifact
            or submission["data"]["publication_digest"] != closure.publication_digest
            or submission["data"]["publication"]["attempt_id"] != identifier
            or submission["data"]["publication"]["journal_artifact"]
            != closure.journal_artifact.model_dump(mode="json")
            or (
                not closure.not_started
                and (
                    closure.native_outcome is None
                    or closure.native_outcome.exit_code is None
                    or not closure.native_outcome.closure.scope_closed
                )
            )
        ):
            _refuse("Operational closure evidence differs")
        actual, not_started = closure.consumed, closure.not_started
    elif not not_started:
        result = transaction.get("run_result", identifier)
        if result["state"] != "immutable" or result["data"]["attempt_id"] != identifier:
            _refuse("Accepted attempt result differs")
        actual = {key: Decimal(value) for key, value in result["data"]["consumed"].items()}
    _settlement(
        transaction,
        dispatch.reservation_ids[0],
        identifier,
        identifier,
        dispatch.campaign_id,
        actual,
    )
    return not_started


def require_settled_execution(transaction: Transaction, receipt: RestoreReceipt) -> None:
    attempts = {
        row["id"]
        for row in transaction.connection.execute(
            "SELECT id FROM records WHERE workspace_id=? AND kind='attempt'",
            (transaction.workspace_id,),
        )
    }
    original = set(receipt.unresolved_attempt_ids)
    if not original.issubset(attempts):
        _refuse("An original issued attempt is missing")
    not_started = {
        identifier: _attempt(transaction, identifier, original=identifier in original)
        for identifier in sorted(attempts)
    }
    for item in transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND kind='job'",
        (transaction.workspace_id,),
    ):
        row = transaction.get("job", item["id"])
        job = JobData.model_validate(row["data"])
        if row["state"] in {"running", "execution_unknown"}:
            _refuse("An issued job remains unresolved")
        if job.current_attempt_id is not None:
            attempt = AttemptData.model_validate(
                transaction.get("attempt", job.current_attempt_id)["data"]
            )
            if (
                job.current_attempt_id not in not_started
                or attempt.dispatch.job_id != row["id"]
                or row["state"] not in TERMINAL
            ):
                _refuse("Current job attempt differs from its terminal projection")
    for sql_row in transaction.connection.execute(
        "SELECT payload FROM records WHERE workspace_id=? AND kind='trial'",
        (transaction.workspace_id,),
    ):
        trial = TrialData.model_validate_json(sql_row["payload"])
        if trial.resource_status == "unknown" or trial.execution_outcome == "unknown":
            _refuse("Trial native custody remains unknown")
    for item in transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND kind='evaluation_exposure'",
        (transaction.workspace_id,),
    ):
        row = transaction.get("evaluation_exposure", item["id"])
        exposure = ExposureData.model_validate(row["data"])
        job = JobData.model_validate(transaction.get("job", exposure.job_id)["data"])
        trial = TrialData.model_validate(transaction.get("trial", job.trial_id)["data"])
        bound = bound_confirmation(transaction, trial)
        if (
            job.evaluation_exposure_id != row["id"]
            or bound is None
            or bound[0].id != exposure.plan_id
            or bound[1].exposure_id != row["id"]
            or bound[1].scoring_job_id != exposure.job_id
        ):
            _refuse("Exposure job identity differs")
        if exposure.attempt_id is None and row["state"] == "reserved":
            if job.current_attempt_id is not None:
                _refuse("Queued exposure has an issued attempt")
            reservation_row = transaction.connection.execute(
                "SELECT owner_id,campaign_id,state,payload FROM budget_reservations "
                "WHERE workspace_id=? AND id=?",
                (transaction.workspace_id, exposure.reservation_id),
            ).one()
            reservation = Reservation.model_validate_json(reservation_row["payload"])
            if (
                reservation_row["state"] != "reserved"
                or reservation_row["owner_id"] != row["id"]
                or reservation_row["campaign_id"] != row["campaign_id"]
                or reservation.id != exposure.reservation_id
                or reservation.owner_id != row["id"]
                or reservation.campaign_id != row["campaign_id"]
                or reservation.uncertain
                or reservation.amounts != {"evaluation_exposures": Decimal(1)}
            ):
                _refuse("Queued exposure funding differs")
            continue
        if exposure.attempt_id is not None and exposure.attempt_id not in not_started:
            _refuse("Exposure attempt is absent")
        started = exposure.attempt_id is not None and not not_started[exposure.attempt_id]
        if row["state"] != ("consumed" if started else "released") or (
            exposure.attempt_id is not None and job.current_attempt_id != exposure.attempt_id
        ):
            _refuse("Dispatched exposure remains unresolved")
        _settlement(
            transaction,
            exposure.reservation_id,
            row["id"],
            row["id"],
            row["campaign_id"],
            {"evaluation_exposures": Decimal(int(started))},
        )
    uncertain = transaction.connection.execute(
        "SELECT id FROM budget_reservations WHERE workspace_id=? AND state='uncertain' LIMIT 1",
        (transaction.workspace_id,),
    ).fetchone()
    if uncertain is not None:
        _refuse("A reservation still has uncertain custody")
    totals: dict[tuple[str, str], dict[str, Decimal]] = {}
    fields = {
        "consumed": "consumed_delta",
        "reserved": "reserved_delta",
        "uncertain": "uncertain_delta",
        "released_total": "released_delta",
    }
    for sql_row in transaction.connection.execute(
        "SELECT payload FROM budget_entries WHERE workspace_id=?",
        (transaction.workspace_id,),
    ):
        entry = BudgetEntry.model_validate_json(sql_row["payload"])
        balance = totals.setdefault((entry.campaign_id, entry.dimension), {})
        for name, delta in fields.items():
            balance[name] = amount_sum(balance.get(name, Decimal(0)), getattr(entry, delta))
    for sql_row in transaction.connection.execute(
        "SELECT campaign_id,dimension,payload FROM budgets WHERE workspace_id=?",
        (transaction.workspace_id,),
    ):
        account = BudgetAccount.model_validate_json(sql_row["payload"])
        balance = totals.pop((sql_row["campaign_id"], sql_row["dimension"]), {})
        if (
            (account.campaign_id, account.dimension)
            != (sql_row["campaign_id"], sql_row["dimension"])
            or account.uncertain
            or (account.kind == BudgetKind.CAPACITY and account.consumed)
            or any(getattr(account, name) != balance.get(name, Decimal(0)) for name in fields)
        ):
            _refuse("Budget custody or ledger conservation differs")
    if totals:
        _refuse("Ledger entries lack their budget accounts")
