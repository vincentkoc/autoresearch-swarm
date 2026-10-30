"""Finite result-acceptance windows never confer physical resource closure."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta

from research_swarm.control.acceptance_models import AcceptanceLease, ExpireAcceptance, LeaseRenewal
from research_swarm.control.authority_clock import AuthorityTime
from research_swarm.control.models import TrialData
from research_swarm.control.worker_records import AttemptData, WorkerData
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.domain.workers import WorkerBinding
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.ledger import BudgetLedger

PUBLICATION_GRACE_MS = 120_000
MAXIMUM_PUBLICATION_GRACE_MS = 600_000


def after(value: str, milliseconds: int) -> str:
    return (
        (
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            + timedelta(milliseconds=milliseconds)
        )
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def dispatch_digest(attempt: AttemptData) -> str:
    return record_digest("research.dispatch", attempt.dispatch.model_dump(mode="json"))


def lease_row(transaction: Transaction, attempt_id: str) -> JsonObject:
    try:
        return transaction.get("acceptance_lease", attempt_id)
    except ResearchError as error:
        if error.code != ErrorCode.NOT_FOUND:
            raise
    row = transaction.get("attempt", attempt_id)
    attempt = AttemptData.model_validate(row["data"])
    binding = WorkerBinding.model_validate(
        {name: getattr(attempt.dispatch, name) for name in WorkerBinding.model_fields}
    )
    legacy = AcceptanceLease(
        attempt_id=attempt_id,
        binding=binding,
        dispatch_digest=dispatch_digest(attempt),
        policy_version="legacy-unqualified-v1",
        clock_mode="bounded-continuous-v1"
        if attempt.assignment_time is not None
        else "local-utc-v1",
        disposition="No acceptance lease was issued by the legacy controller",
    )
    return {
        "id": attempt_id,
        "kind": "acceptance_lease",
        "revision": 0,
        "state": "unqualified",
        "campaign_id": row["campaign_id"],
        "parent_id": binding.worker_id,
        "data": legacy.model_dump(mode="json"),
    }


def save_lease(
    transaction: Transaction, previous: JsonObject, data: AcceptanceLease, state: str
) -> JsonObject:
    if previous["revision"] == 0:
        return transaction.insert(
            "acceptance_lease",
            previous["id"],
            data.model_dump(mode="json"),
            state=state,
            campaign_id=previous["campaign_id"],
            parent_id=previous["parent_id"],
        )
    return transaction.update(
        "acceptance_lease",
        previous["id"],
        expected_revision=previous["revision"],
        data=data.model_dump(mode="json"),
        state=state,
    )


class AcceptanceControl:
    def __init__(
        self,
        database: Database,
        calibrated_time: Callable[[WorkerData], AuthorityTime] | None = None,
    ) -> None:
        self.database, self.calibrated_time = database, calibrated_time

    def time(self, worker: WorkerData) -> tuple[str, str | None, str | None]:
        if worker.clock_mode == "local-utc-v1":
            return utc_now(), None, None
        if self.calibrated_time is None:
            raise ResearchError(ErrorCode.STALE_FENCE, "Calibrated acceptance clock is unavailable")
        current = self.calibrated_time(worker)
        return (
            current.datetime.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            current.epoch,
            current.policy.digest,
        )

    def issue(
        self,
        transaction: Transaction,
        row: JsonObject,
        worker: WorkerData,
        now: str,
        clock: AuthorityTime | None,
    ) -> JsonObject:
        attempt = AttemptData.model_validate(row["data"])
        data = AcceptanceLease(
            attempt_id=row["id"],
            binding=worker.binding,
            dispatch_digest=dispatch_digest(attempt),
            policy_version="native-window-v1",
            clock_mode=worker.clock_mode,
            clock_epoch=clock.epoch if clock else None,
            clock_policy_digest=clock.policy.digest if clock else None,
            issued_at=now,
            observed_at=now,
            expires_at=after(attempt.dispatch.hard_deadline_at, PUBLICATION_GRACE_MS),
            renew_until=after(attempt.dispatch.hard_deadline_at, MAXIMUM_PUBLICATION_GRACE_MS),
        )
        lease = transaction.insert(
            "acceptance_lease",
            row["id"],
            data.model_dump(mode="json"),
            state="active",
            campaign_id=row["campaign_id"],
            parent_id=worker.binding.worker_id,
        )
        transaction.event("attempt.acceptance_issued", lease, {})
        return lease

    def eligibility(self, row: JsonObject, worker: WorkerData) -> tuple[str, str | None]:
        lease = AcceptanceLease.model_validate(row["data"])
        if row["state"] != "active":
            return row["state"], None
        try:
            now, epoch, policy = self.time(worker)
        except ResearchError as error:
            if error.code not in {ErrorCode.STALE_FENCE, ErrorCode.CAPABILITY_UNAVAILABLE}:
                raise
            return "unqualified", None
        if (
            lease.clock_mode != worker.clock_mode
            or lease.clock_epoch != epoch
            or lease.clock_policy_digest != policy
            or lease.observed_at is None
            or now < lease.observed_at
        ):
            return "unqualified", now
        if lease.expires_at is None or now >= lease.expires_at:
            return "expired", now
        return "active", now

    def require(self, transaction: Transaction, attempt: AttemptData, worker: WorkerData) -> None:
        if attempt.publication_status != "pending":
            return
        row = lease_row(transaction, attempt.dispatch.attempt_id)
        lease = AcceptanceLease.model_validate(row["data"])
        if lease.binding != worker.binding or lease.dispatch_digest != dispatch_digest(attempt):
            raise ResearchError(ErrorCode.STALE_FENCE, "Acceptance identity differs from dispatch")
        disposition, _ = self.eligibility(row, worker)
        if disposition != "active":
            raise ResearchError(
                ErrorCode.ACCEPTANCE_EXPIRED,
                "Original evidence requires explicit reconciliation",
                details={"attempt_id": row["id"], "lease_disposition": disposition},
            )

    def expire_if_due(
        self,
        transaction: Transaction,
        attempt: AttemptData,
        worker: WorkerData,
        *,
        observed_disposition: str | None = None,
    ) -> JsonObject:
        row = lease_row(transaction, attempt.dispatch.attempt_id)
        disposition = observed_disposition or self.eligibility(row, worker)[0]
        if observed_disposition is not None and observed_disposition not in {
            "expired",
            "unqualified",
        }:
            raise ValueError("Only an observed conservative fence can survive rejection")
        if attempt.publication_status != "pending" or disposition not in {"expired", "unqualified"}:
            return row
        # The publication rejection hook re-applies only the already-observed
        # conservative fence after failed effects roll back, in the same transaction.
        data = AcceptanceLease.model_validate(row["data"])
        if row["state"] == "active" or row["revision"] == 0:
            row = save_lease(
                transaction, row, data.model_copy(update={"disposition": disposition}), disposition
            )
            transaction.event("attempt.acceptance_expired", row, {"disposition": disposition})
        attempt_row = transaction.get("attempt", attempt.dispatch.attempt_id)
        if attempt_row["state"] == "execution_unknown":
            return row
        for reservation in attempt.dispatch.reservation_ids:
            BudgetLedger.mark_uncertain(transaction, reservation)
        updated = transaction.update(
            "attempt",
            attempt_row["id"],
            expected_revision=attempt_row["revision"],
            state="execution_unknown",
            data=attempt.model_dump(mode="json"),
        )
        job = transaction.get("job", attempt.dispatch.job_id)
        transaction.update(
            "job",
            job["id"],
            expected_revision=job["revision"],
            state="execution_unknown",
            data=job["data"],
        )
        trial = transaction.get("trial", attempt.dispatch.trial_id)
        trial_data = TrialData.model_validate(trial["data"])
        transaction.update(
            "trial",
            trial["id"],
            expected_revision=trial["revision"],
            state="execution_unknown",
            data=trial_data.model_copy(
                update={
                    "resource_status": "unknown",
                    "execution_outcome": "closed_unaccepted"
                    if trial_data.execution_outcome == "closed_unaccepted"
                    else "unknown",
                }
            ).model_dump(mode="json"),
        )
        transaction.event("attempt.execution_unknown", updated, {"reason": "acceptance_lease"})
        return row

    def expire(self, context: CommandContext, intent: ExpireAcceptance) -> JsonObject:
        def expire(transaction: Transaction) -> JsonObject:
            row = transaction.get("attempt", intent.attempt_id)
            lease = lease_row(transaction, intent.attempt_id)
            if context.expected_revision != lease["revision"]:
                raise ResearchError(
                    ErrorCode.REVISION_CONFLICT, "Acceptance lease revision changed"
                )
            attempt = AttemptData.model_validate(row["data"])
            try:
                session = transaction.get("worker_session", attempt.dispatch.worker_session_id)
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
                session = transaction.get("worker", attempt.dispatch.worker_id)
            else:
                if session["state"] != "immutable":
                    raise ResearchError(ErrorCode.STALE_FENCE, "Original session is not immutable")
            worker = WorkerData.model_validate(session["data"])
            if any(
                getattr(attempt.dispatch, name) != getattr(worker.binding, name)
                for name in WorkerBinding.model_fields
            ):
                raise ResearchError(ErrorCode.STALE_FENCE, "Original worker binding is unavailable")
            return self.expire_if_due(transaction, attempt, worker)

        return self.database.execute(
            context,
            "attempt.expire_acceptance",
            intent.model_dump(mode="json"),
            "campaign.manage",
            expire,
        )

    def renew(
        self, transaction: Transaction, worker: WorkerData, renewal: LeaseRenewal, sequence: int
    ) -> JsonObject:
        attempt = AttemptData.model_validate(transaction.get("attempt", renewal.attempt_id)["data"])
        row = lease_row(transaction, renewal.attempt_id)
        data = AcceptanceLease.model_validate(row["data"])
        if (
            data.binding != worker.binding
            or data.dispatch_digest != renewal.dispatch_digest
            or dispatch_digest(attempt) != renewal.dispatch_digest
        ):
            raise ResearchError(
                ErrorCode.AUTHORIZATION_DENIED,
                "Acceptance renewal does not identify this worker's dispatch",
            )
        if attempt.publication_status != "pending":
            return {"attempt_id": renewal.attempt_id, "disposition": "settled", "lease": row}
        row = self.expire_if_due(transaction, attempt, worker)
        if row["state"] != "active":
            return {"attempt_id": renewal.attempt_id, "disposition": row["state"], "lease": row}
        if row["revision"] != renewal.expected_lease_revision:
            return {"attempt_id": renewal.attempt_id, "disposition": "conflict", "lease": row}
        disposition, now = self.eligibility(row, worker)
        if disposition != "active" or now is None:
            row = self.expire_if_due(transaction, attempt, worker, observed_disposition=disposition)
            return {"attempt_id": renewal.attempt_id, "disposition": row["state"], "lease": row}
        assert data.expires_at is not None and data.renew_until is not None
        expiry = min(data.renew_until, max(data.expires_at, after(now, PUBLICATION_GRACE_MS)))
        data = data.model_copy(
            update={"observed_at": now, "expires_at": expiry, "last_heartbeat_sequence": sequence}
        )
        updated = save_lease(transaction, row, data, "active")
        transaction.event("attempt.acceptance_renewed", updated, {"heartbeat_sequence": sequence})
        return {"attempt_id": renewal.attempt_id, "disposition": "renewed", "lease": updated}

    @staticmethod
    def settle(transaction: Transaction, attempt_id: str, disposition: str) -> JsonObject:
        row = lease_row(transaction, attempt_id)
        if row["state"] == "settled":
            return row
        data = AcceptanceLease.model_validate(row["data"])
        updated = save_lease(
            transaction, row, data.model_copy(update={"disposition": disposition}), "settled"
        )
        transaction.event("attempt.acceptance_settled", updated, {"disposition": disposition})
        return updated
