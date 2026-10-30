"""Inspect the same authority before byte verification and again at transfer commit."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from research_swarm.control.confirmation_models import ConfirmationData
from research_swarm.control.models import CampaignData, TrialData
from research_swarm.control.orchestration_models import TransferOrchestration
from research_swarm.domain.artifacts import ArtifactRecord
from research_swarm.domain.budgets import BudgetAccount, BudgetKind
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.orchestration import OrchestrationBinding
from research_swarm.domain.orchestration_transfer import MAX_MANIFEST_BYTES, MAX_MANIFEST_ENTRIES
from research_swarm.state.artifacts import ArtifactCatalog
from research_swarm.state.database import JsonObject, Transaction

TERMINAL_EXECUTION = ("succeeded", "failed", "cancelled", "not_started", "closed_unaccepted")
FAILED_EXECUTION = {"failed", "cancelled", "not_started", "closed_unaccepted"}


@dataclass(frozen=True)
class TransferView:
    campaign: JsonObject
    binding: JsonObject
    entries: tuple[JsonObject, ...]
    artifacts: tuple[ArtifactRef, ...]

    @property
    def digest(self) -> str:
        return record_digest("research.transfer-view-v1", {"entries": self.entries})

    @property
    def artifact_digest(self) -> str:
        return record_digest(
            "research.transfer-artifacts-v1",
            {
                "artifacts": [item.model_dump(mode="json") for item in self.artifacts],
            },
        )


def _ref_values(value: Any, path: tuple[str, ...]) -> list[ArtifactRef]:
    if value is None:
        return []
    if not path:
        return [ArtifactRef.model_validate(value)]
    if path[0] == "*":
        values = value.values() if isinstance(value, dict) else value
        return [reference for item in values for reference in _ref_values(item, path[1:])]
    return _ref_values(value.get(path[0]), path[1:])


# These typed references have independent source/evidence retention owners.
REFERENCE_PATHS = {
    "campaign": ("contract.input_artifacts.*",),
    "project_revision": ("source_artifact",),
    "candidate": ("source_artifact",),
    "proposal_request": ("parent_source",),
    "proposal_response": ("request.source_artifact", "request.response_artifact"),
    "attempt": ("source_artifact", "inputs.*", "outcome_artifact", "report_artifact"),
    "run_result": ("process_closure_ref", "artifacts.*"),
    "evaluation_request": ("request.input_artifact", "inputs.*"),
    "evaluation_receipt": ("report_artifact",),
    "attempt_submission": ("publication.journal_artifact", "publication.report_artifact"),
    "attempt_closure": ("journal_artifact",),
    "worker_upload": ("artifact",),
}


def require_quiescent(transaction: Transaction, campaign_id: str, evaluator_id: str | None) -> None:
    attempts = transaction.connection.execute(
        "SELECT id,state FROM records WHERE workspace_id=? AND campaign_id=? AND kind='attempt' "
        "AND state NOT IN (" + ",".join("?" for _ in TERMINAL_EXECUTION) + ") LIMIT 16",
        (transaction.workspace_id, campaign_id, *TERMINAL_EXECUTION),
    ).fetchall()
    if attempts:
        raise ResearchError(
            ErrorCode.EXECUTION_UNKNOWN
            if any(row["state"] == "execution_unknown" for row in attempts)
            else ErrorCode.RESOURCE_UNAVAILABLE,
            "Reconcile original native attempts before transferring their driver",
            details={"attempt_ids": [row["id"] for row in attempts]},
        )
    trials = transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND campaign_id=? AND kind='trial' AND "
        "(json_extract(payload,'$.resource_status') IN ('active','unknown') OR "
        "json_extract(payload,'$.execution_outcome')='unknown') LIMIT 16",
        (transaction.workspace_id, campaign_id),
    ).fetchall()
    if trials:
        raise ResearchError(
            ErrorCode.EXECUTION_UNKNOWN,
            "Trial resource custody remains unresolved",
            details={"trial_ids": [row["id"] for row in trials]},
        )
    reservations = transaction.connection.execute(
        "SELECT b.id FROM budget_reservations b WHERE b.workspace_id=? AND b.campaign_id=? "
        "AND (b.state='uncertain' OR (b.state!='settled' AND EXISTS "
        "(SELECT 1 FROM records r WHERE r.workspace_id=b.workspace_id AND r.kind='attempt' "
        "AND r.id=b.owner_id))) LIMIT 16",
        (transaction.workspace_id, campaign_id),
    ).fetchall()
    if reservations:
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE,
            "Native budget reservations are unsettled",
            details={"reservation_ids": [row["id"] for row in reservations]},
        )
    for row in transaction.connection.execute(
        "SELECT payload FROM budgets WHERE workspace_id=? AND campaign_id=?",
        (transaction.workspace_id, campaign_id),
    ):
        account = BudgetAccount.model_validate_json(row["payload"])
        if account.uncertain or (account.kind == BudgetKind.CAPACITY and account.consumed):
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE,
                "Budget custody remains unresolved",
                details={"dimension": account.dimension},
            )
    if evaluator_id is None:
        return
    for row in transaction.connection.execute(
        "SELECT id,payload FROM records WHERE workspace_id=? AND campaign_id=? "
        "AND kind='evaluation_request' AND state='authorized' "
        "AND json_extract(payload,'$.request.evaluator_id')=?",
        (transaction.workspace_id, campaign_id, evaluator_id),
    ):
        authorization = strict_json_loads(row["payload"])
        job = transaction.get("job", authorization["job_id"])
        if job["state"] not in FAILED_EXECUTION:
            raise ResearchError(
                ErrorCode.EVALUATION_INCOMPLETE,
                "Work still requires the retiring evaluator",
                details={"evaluation_request_id": row["id"], "job_id": job["id"]},
            )
    for row in transaction.connection.execute(
        "SELECT id,state,payload FROM records WHERE workspace_id=? AND campaign_id=? "
        "AND kind='confirmation_plan' AND json_extract(payload,'$.plan.evaluator_id')=?",
        (transaction.workspace_id, campaign_id, evaluator_id),
    ):
        panel = ConfirmationData.model_validate_json(row["payload"])
        if panel.decision_id is not None or row["state"] == "ineligible":
            continue
        for member in panel.plan.bindings:
            trial = TrialData.model_validate(transaction.get("trial", member.trial_id)["data"])
            if trial.execution_outcome in {"pending", "unknown"} or (
                trial.execution_outcome == "succeeded" and trial.evaluation_status == "pending"
            ):
                raise ResearchError(
                    ErrorCode.EVALUATION_INCOMPLETE,
                    "A panel still requires the retiring evaluator",
                    details={"plan_id": row["id"], "trial_id": member.trial_id},
                )


def inspect_transfer(transaction: Transaction, intent: TransferOrchestration) -> TransferView:
    campaign = transaction.get("campaign", intent.campaign_id)
    binding_row = transaction.get("orchestration_binding", intent.campaign_id)
    binding = OrchestrationBinding.model_validate(binding_row["data"])
    data = CampaignData.model_validate(campaign["data"])
    if (
        transaction.context.expected_revision != binding_row["revision"]
        or campaign["revision"] != intent.campaign_revision
    ):
        raise ResearchError(ErrorCode.REVISION_CONFLICT, "Campaign or driver binding changed")
    if binding.epoch != intent.epoch or binding_row["state"] not in {"active", "fenced"}:
        raise ResearchError(ErrorCode.STALE_FENCE, "Original driver ownership changed")
    if campaign["state"] != "paused" or data.contract_digest is None:
        raise ResearchError(
            ErrorCode.INVALID_TRANSITION, "Transfer requires a sealed paused campaign"
        )
    if transaction.context.principal.id in intent.routes.values():
        raise ResearchError(
            ErrorCode.AUTHORIZATION_DENIED, "Successors require dedicated principals"
        )
    for actor in intent.routes.values():
        if (
            transaction.connection.execute(
                "SELECT 1 FROM commands WHERE workspace_id=? AND actor_id=? LIMIT 1",
                (transaction.workspace_id, actor),
            ).fetchone()
            or transaction.connection.execute(
                "SELECT 1 FROM records WHERE workspace_id=? "
                "AND kind='orchestration_owner' AND id=?",
                (transaction.workspace_id, actor),
            ).fetchone()
        ):
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Successor principal is not fresh")
    require_quiescent(transaction, campaign["id"], binding.routes.get("evaluator"))
    entries: list[JsonObject] = []
    manifest_size = len(canonical_bytes({"entries": []}))

    def add_entry(entry: JsonObject) -> None:
        nonlocal manifest_size
        manifest_size += len(canonical_bytes(entry)) + bool(entries)
        if len(entries) >= MAX_MANIFEST_ENTRIES or manifest_size > MAX_MANIFEST_BYTES:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Transfer manifest exceeds the local profile bound"
            )
        entries.append(entry)

    declared: dict[str, ArtifactRef] = {}
    rows = transaction.connection.execute(
        "SELECT kind,id FROM records WHERE workspace_id=? AND (campaign_id=? OR "
        "(kind='project_revision' AND id=?) OR (parent_id=? AND kind IN "
        "('campaign_funding','funding_projection','funding_settlement','funding_change'))) "
        "ORDER BY kind,id LIMIT 100001",
        (
            transaction.workspace_id,
            campaign["id"],
            data.contract.project_revision_id,
            campaign["id"],
        ),
    ).fetchall()
    if len(rows) > 100000:
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE, "Transfer manifest exceeds the local profile bound"
        )
    for item in rows:
        row = transaction.get(item["kind"], item["id"])
        add_entry(
            {
                "category": "record",
                "kind": row["kind"],
                "id": row["id"],
                "revision": row["revision"],
                "state": row["state"],
                "record_digest": record_digest("research.transfer-record-v1", row),
            }
        )
        for path in REFERENCE_PATHS.get(row["kind"], ()):
            for reference in _ref_values(row["data"], tuple(path.split("."))):
                if reference.id in declared and reference != declared[reference.id]:
                    raise ResearchError(
                        ErrorCode.ARTIFACT_UNVERIFIED, "Required artifact identities disagree"
                    )
                declared[reference.id] = reference
    for sql_row in transaction.connection.execute(
        "SELECT dimension,payload FROM budgets WHERE workspace_id=? "
        "AND campaign_id=? ORDER BY dimension",
        (transaction.workspace_id, campaign["id"]),
    ):
        add_entry({"category": "budget_account", "value": strict_json_loads(sql_row["payload"])})
    for sql_row in transaction.connection.execute(
        "SELECT id,owner_id,state,payload FROM budget_reservations WHERE workspace_id=? "
        "AND campaign_id=? ORDER BY id",
        (transaction.workspace_id, campaign["id"]),
    ):
        add_entry(
            {
                "category": "budget_reservation",
                "id": sql_row["id"],
                "owner_id": sql_row["owner_id"],
                "state": sql_row["state"],
                "value": strict_json_loads(sql_row["payload"]),
            }
        )
    retained: dict[str, ArtifactRef] = {}
    owners = (
        "WITH owners(kind,id) AS (SELECT kind,id FROM records "
        "WHERE workspace_id=? AND campaign_id=? "
        "UNION SELECT 'project_revision',?) "
        "SELECT r.id FROM owners o JOIN records r ON r.workspace_id=? "
        "AND r.kind='artifact_reference' "
        "AND r.state='retained' AND json_extract(r.payload,'$.retention.owner_kind')=o.kind "
        "AND json_extract(r.payload,'$.retention.owner_id')=o.id ORDER BY r.id"
    )
    for item in transaction.connection.execute(
        owners,
        (
            transaction.workspace_id,
            campaign["id"],
            data.contract.project_revision_id,
            transaction.workspace_id,
        ),
    ):
        row = transaction.get("artifact_reference", item["id"])
        retention = row["data"]["retention"]
        if not retention["required"]:
            continue
        artifact = ArtifactRecord.model_validate(
            transaction.get("artifact", retention["artifact_id"])["data"]
        )
        ArtifactCatalog.available(transaction, artifact.reference)
        retained[artifact.reference.id] = artifact.reference
        if len(retained) > 4096:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Transfer artifacts exceed the local profile bound"
            )
        add_entry(
            {
                "category": "required_artifact",
                "retention": row,
                "artifact": artifact.model_dump(mode="json"),
            }
        )
    for identifier, reference in declared.items():
        if retained.get(identifier) != reference:
            raise ResearchError(
                ErrorCode.ARTIFACT_UNVERIFIED,
                "A declared artifact lacks required retention",
                details={"artifact_id": identifier},
            )
    return TransferView(
        campaign, binding_row, tuple(entries), tuple(retained[key] for key in sorted(retained))
    )
