"""Bounded evidence views with workspace-scoped, insertion-stable cursors."""

from __future__ import annotations

import base64
import binascii
from typing import Literal

from pydantic import Field, ValidationError

from research_swarm.control.funding_views import funding_context
from research_swarm.control.models import CampaignData
from research_swarm.control.obligations import operational_context
from research_swarm.domain.budgets import BudgetAccount
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ID, Digest, VersionedRecord
from research_swarm.state.database import Database, JsonObject, Transaction
from research_swarm.state.sql import SqlSession

RecordKind = Literal[
    "artifact_read_session",
    "workspace_gate",
    "backup_custody",
    "recovery_admission",
    "recovery_activation",
    "restoration",
    "acceptance_lease",
    "imported_evidence",
    "campaign_obligations",
    "campaign_obligation_chunk",
    "scientific_completion",
    "campaign_resource_closure",
    "orchestration_checkpoint",
    "orchestration_checkpoint_chunk",
    "orchestration_transfer",
    "orchestration_binding_history",
    "orchestration_start",
    "orchestration_start_ack",
    "orchestration_wait",
    "wait_readiness",
    "wakeup_delivery",
    "wakeup_acknowledgment",
    "orchestration_binding",
    "drive_turn",
    "drive_receipt",
    "drive_closure",
    "campaign_funding",
    "funding_projection",
    "funding_change",
    "funding_settlement",
    "search_state",
    "policy_transition",
    "search_observation",
    "observation_consumption",
    "proposal_request",
    "proposal_response",
    "proposal_cancellation",
    "project",
    "project_revision",
    "campaign",
    "candidate",
    "candidate_validation",
    "trial",
    "job",
    "attempt",
    "attempt_submission",
    "attempt_reconciliation",
    "attempt_closure",
    "run_result",
    "worker",
    "worker_session",
    "worker_fence",
    "worker_replacement",
    "worker_admission",
    "worker_presence",
    "attempt_control",
    "worker_upload",
    "evaluation_request",
    "evaluation_receipt",
    "promotion",
    "evidence_correction",
    "confirmation_plan",
    "evaluation_exposure",
    "report",
]


class RecordQuery(VersionedRecord):
    kind: RecordKind
    campaign_id: ID | None = None
    parent_id: ID | None = None
    state: str | None = Field(default=None, max_length=64)
    limit: int = Field(default=25, strict=True, ge=1, le=100)
    cursor: str | None = Field(default=None, max_length=2048)


class RecordCursor(VersionedRecord):
    scope_digest: Digest
    after: int = Field(strict=True, ge=0)
    through: int = Field(strict=True, ge=0)


def summarize(row: JsonObject) -> JsonObject:
    result = {
        key: row[key] for key in ("id", "kind", "revision", "state", "campaign_id", "parent_id")
    }
    data = row["data"]
    if row["kind"] in {
        "acceptance_lease",
        "campaign_obligations",
        "scientific_completion",
        "campaign_resource_closure",
        "orchestration_checkpoint",
        "orchestration_transfer",
        "orchestration_start",
        "orchestration_start_ack",
        "orchestration_binding",
        "orchestration_wait",
        "wait_readiness",
        "wakeup_delivery",
        "wakeup_acknowledgment",
    }:
        result["data"] = data
    elif row["kind"] == "imported_evidence":
        manifest = data["manifest"]
        result["data"] = {
            "import_manifest_digest": data["import_manifest_digest"],
            "imported": True,
            "promotion_eligible": False,
            "source_authenticity": data["source_authenticity"],
            "execution_authority": data["execution_authority"],
            "budget_authority": data["budget_authority"],
            "source_system": manifest["source_system"],
            "source_schema": manifest["source_schema"],
            "source_record_id": manifest["source_record_id"],
            "source_status": manifest["source_status"],
            "raw_artifact": manifest["raw_artifact"],
            "observations": manifest["observations"][:8],
            "observation_count": len(manifest["observations"]),
        }
    elif row["kind"] == "campaign_obligation_chunk":
        result["data"] = {
            "review_id": data["review_id"],
            "ordinal": data["ordinal"],
            "entry_count": len(data["entries"]),
        }
    elif row["kind"] == "orchestration_checkpoint_chunk":
        result["data"] = {
            "checkpoint_id": data["checkpoint_id"],
            "ordinal": data["ordinal"],
            "entry_count": len(data["entries"]),
        }
    elif row["kind"] == "drive_turn":
        result["data"] = {
            key: data[key]
            for key in (
                "campaign_id",
                "epoch",
                "ordinal",
                "route",
                "actor_id",
                "configuration_digest",
                "planning_watermark",
            )
        }
        result["data"]["operation"] = data["action"]["operation"]
    elif row["kind"] in {"drive_receipt", "drive_closure"}:
        result["data"] = data
    elif row["kind"] == "campaign_funding":
        result["data"] = {
            key: data[key]
            for key in (
                "parent_campaign_id",
                "child_campaign_id",
                "root_campaign_id",
                "depth",
                "remaining_depth",
            )
        }
        result["data"]["limits"] = data["limits"][:16]
        result["data"]["dimension_count"] = len(data["limits"])
    elif row["kind"] in {"funding_projection", "funding_settlement"}:
        result["data"] = {
            "funding_id": data["funding_id"],
            "balances": dict(sorted(data["balances"].items())[:16]),
            "dimension_count": len(data["balances"]),
        }
        if row["kind"] == "funding_settlement":
            result["data"]["final_report_id"] = data["final_report_id"]
    elif row["kind"] == "funding_change":
        result["data"] = data
    elif row["kind"] == "search_state":
        result["data"] = {
            key: data[key] for key in ("policy", "policy_version", "generation", "request_count")
        }
        for name in ("pending_request_ids", "active_trial_ids"):
            result["data"][name] = data[name][:32]
            result["data"][name + "_count"] = len(data[name])
        if data.get("selection_trial_id") is not None:
            result["data"]["selection_trial_id"] = data["selection_trial_id"]
    elif row["kind"] == "search_observation":
        result["data"] = {
            key: data[key]
            for key in (
                "facts_digest",
                "incumbent_revision",
                "comparator_id",
                "observed_at",
                "watermark",
            )
        }
        result["data"]["facts"] = {
            **data["facts"],
            "evidence": data["facts"]["evidence"][:32],
            "evidence_count": len(data["facts"]["evidence"]),
            "attempt_outcomes": dict(list(data["facts"]["attempt_outcomes"].items())[:32]),
            "attempt_count": len(data["facts"]["attempt_outcomes"]),
        }
    elif row["kind"] == "observation_consumption":
        result["data"] = data
    elif row["kind"] == "policy_transition":
        result["data"] = {**data, "finding": data.get("finding", "")[:1024]}
    elif row["kind"] == "proposal_request":
        result["data"] = {
            key: data[key]
            for key in (
                "proposer_id",
                "policy_revision",
                "parent_candidate_id",
                "parent_source",
                "seed",
                "context_digest",
                "incumbent_revision",
            )
        }
        if data.get("theme_id") is not None:
            result["data"]["theme_id"] = data["theme_id"]
    elif row["kind"] == "proposal_response":
        result["data"] = {
            key: data[key]
            for key in ("response_digest", "responder_id", "recorded_at", "late_at_recording")
        }
        result["data"]["outcome"] = data["request"]["outcome"]
        result["data"]["hypothesis"] = data["request"]["hypothesis"][:1024]
        result["data"]["usage"] = data["request"]["usage"]
    elif row["kind"] == "proposal_cancellation":
        result["data"] = data
    elif row["kind"] == "attempt_submission":
        result["data"] = {
            key: data[key]
            for key in ("publication_digest", "original_binding", "submitted_by", "submitted_at")
        }
        result["data"]["attempt_id"] = data["publication"]["attempt_id"]
    elif row["kind"] == "attempt_closure":
        result["data"] = {
            key: data[key]
            for key in (
                "attempt_id",
                "submission_id",
                "publication_digest",
                "not_started",
                "consumed",
                "controller_id",
                "control_epoch",
                "operator_id",
                "recorded_at",
                "reason",
            )
        }
    elif row["kind"] in {
        "worker_admission",
        "worker_presence",
        "attempt_control",
        "attempt_reconciliation",
    }:
        result["data"] = data
    elif row["kind"] == "candidate":
        result["data"] = {
            "hypothesis": data["hypothesis"][:1024],
            "parent_ids": data["parent_ids"],
            "source_tree_digest": data["source"]["tree_digest"],
            "source_artifact": data["source_artifact"],
        }
    elif row["kind"] == "trial":
        result["data"] = {
            key: data[key]
            for key in ("candidate_id", "execution_outcome", "evaluation_status", "resource_status")
        }
        result["data"].update(seed=data["spec"]["seed"], spec_digest=data["spec"]["spec_digest"])
    elif row["kind"] == "campaign":
        result["data"] = {
            key: data[key]
            for key in (
                "name",
                "contract_digest",
                "incumbent_candidate_id",
                "incumbent_revision",
                "stopping_reason",
            )
        }
        result["data"]["primary_metric"] = data["contract"]["primary_metric"]
        result["data"]["incumbent_eligibility"] = data.get("incumbent_eligibility", "eligible")
        result["data"]["incumbent_evidence_level"] = data.get("incumbent_evidence_level")
        if data.get("parent_campaign_id") is not None:
            result["data"]["parent_campaign_id"] = data["parent_campaign_id"]
    elif row["kind"] == "job":
        result["data"] = {
            "stage_id": data["stage"]["id"],
            "stage_kind": data["stage"]["kind"],
            "trial_id": data["trial_id"],
            "current_attempt_id": data["current_attempt_id"],
        }
    elif row["kind"] == "project":
        result["data"] = {key: data[key] for key in ("name", "slug", "current_revision_id")}
    elif row["kind"] == "project_revision":
        result["data"] = {
            "project_id": data["project_id"],
            "source_artifact": data["source_artifact"],
            "manifest_digest": data["resolved"]["manifest_digest"],
            "protocol_digest": data["resolved"]["protocol_digest"],
        }
    elif row["kind"] == "candidate_validation":
        result["data"] = {
            "candidate_id": data["candidate_id"],
            "valid": data["valid"],
            "findings": data["findings"][:20],
            "changed_paths": data["changed_paths"][:50],
            "changed_path_count": len(data["changed_paths"]),
        }
    elif row["kind"] in {"worker", "worker_session"}:
        result["data"] = {
            "name": data["name"],
            "binding": data["binding"],
            "profiles": [profile["name"] for profile in data["profiles"]],
            "max_concurrency": data["max_concurrency"],
            "project_ids": data.get("project_ids"),
        }
    elif row["kind"] == "attempt":
        result["data"] = {
            "job_id": data["dispatch"]["job_id"],
            "worker_id": data["dispatch"]["worker_id"],
            "stage_id": data["dispatch"]["stage_id"],
            "publication_status": data["publication_status"],
            "finding": data["finding"],
            "execution_fingerprint": data["profile"]["execution_fingerprint"],
        }
    elif row["kind"] == "evaluation_request":
        request = data["request"]
        result["data"] = {
            "job_id": data["job_id"],
            "receipt_id": data["receipt_id"],
            "candidate_id": request["candidate_id"],
            "evaluator_id": request["evaluator_id"],
            "expected_seed": request["expected_seed"],
            "panel_digest": request["panel_digest"],
            "expected_member_count": len(request["expected_members"]),
            "exposure_class": request["exposure_class"],
            "measurement_trust": request["measurement_trust"],
        }
    elif row["kind"] == "evaluation_receipt":
        receipt = data["receipt"]
        result["data"] = {
            "request_id": receipt["request_id"],
            "attempt_id": data["attempt_id"],
            "metrics": receipt["metrics"][:50],
            "metric_count": len(receipt["metrics"]),
            "validity": receipt["validity"],
            "findings": receipt["findings"][:20],
            "observed_seed": receipt["observed_seed"],
            "panel_digest": receipt["panel_digest"],
            "execution_fingerprint": receipt["execution_fingerprint"],
        }
    elif row["kind"] == "confirmation_plan":
        plan = data["plan"]
        result["data"] = {
            "candidate_id": plan["candidate_id"],
            "comparator_id": plan["comparator_id"],
            "plan_digest": plan["plan_digest"],
            "trial_count": len(plan["bindings"]),
            "decision_id": data["decision_id"],
            "policy": plan["policy"],
        }
    elif row["kind"] == "evaluation_exposure":
        result["data"] = data
    elif row["kind"] == "promotion":
        result["data"] = {
            "decision": data["decision"],
            "comparison_digest": data["comparison_digest"],
            "evidence_level": data["evidence_level"],
        }
    elif row["kind"] == "evidence_correction":
        result["data"] = {"target": data["target"], "reason": data["reason"][:1024]}
    else:
        result["data"] = {"detail_available": True}
    return result


class ResearchQueries:
    def __init__(self, database: Database) -> None:
        self.database = database

    def get(self, context: CommandContext, kind: RecordKind, identifier: str) -> JsonObject:
        # Reuse the allowlist even for in-process clients. Internal storage rows are not public.
        RecordQuery(kind=kind)
        return self.database.read_record(context, kind, identifier)

    def list(self, context: CommandContext, request: RecordQuery) -> JsonObject:
        context.principal.require("read")
        request = RecordQuery.model_validate(request)
        scope = record_digest(
            "research.query-scope",
            {
                "workspace_id": context.principal.workspace_id,
                **request.model_dump(mode="json", exclude={"cursor", "limit"}),
            },
        )
        cursor: RecordCursor | None = None
        if request.cursor is not None:
            try:
                encoded = request.cursor.encode("ascii")
                cursor = RecordCursor.model_validate(
                    strict_json_loads(base64.b64decode(encoded, altchars=b"-_", validate=True))
                )
                if cursor.scope_digest != scope or cursor.after > cursor.through:
                    raise ValueError("Cursor belongs to another query")
            except (ValueError, UnicodeError, binascii.Error, ValidationError) as error:
                raise ResearchError(
                    ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid query cursor"
                ) from error

        def read(connection: SqlSession) -> JsonObject:
            through = (
                cursor.through
                if cursor
                else connection.execute(
                    "SELECT COALESCE(MAX(rowid),0) FROM records WHERE workspace_id=?",
                    (context.principal.workspace_id,),
                ).one()[0]
            )
            clauses = ["workspace_id=?", "kind=?", "rowid>?", "rowid<=?"]
            parameters: list[str | int] = [
                context.principal.workspace_id,
                request.kind,
                cursor.after if cursor else 0,
                through,
            ]
            for field in ("campaign_id", "parent_id", "state"):
                value = getattr(request, field)
                if value is not None:
                    clauses.append(field + "=?")
                    parameters.append(value)
            rows = connection.execute(
                "SELECT rowid AS ordinal,id FROM records WHERE "
                + " AND ".join(clauses)
                + " ORDER BY rowid LIMIT ?",
                (*parameters, request.limit + 1),
            ).fetchall()
            transaction = Transaction(connection, context)
            items = [
                summarize(transaction.get(request.kind, row["id"])) for row in rows[: request.limit]
            ]
            next_cursor = None
            if len(rows) > request.limit:
                next_cursor = base64.urlsafe_b64encode(
                    canonical_bytes(
                        RecordCursor(
                            scope_digest=scope,
                            after=rows[request.limit - 1]["ordinal"],
                            through=through,
                        ).model_dump(mode="json")
                    )
                ).decode("ascii")
            return {"items": items, "next_cursor": next_cursor, "membership_through": through}

        return self.database.store.read(read)

    def budget(self, context: CommandContext, campaign_id: str) -> JsonObject:
        context.principal.require("read")

        def read(connection: SqlSession) -> JsonObject:
            campaign = Transaction(connection, context).get("campaign", campaign_id)
            rows = connection.execute(
                "SELECT payload FROM budgets WHERE workspace_id=? "
                "AND campaign_id=? ORDER BY dimension",
                (context.principal.workspace_id, campaign_id),
            ).fetchall()
            accounts = [BudgetAccount.model_validate_json(row["payload"]) for row in rows]
            return {
                "campaign_id": campaign_id,
                "campaign_revision": campaign["revision"],
                "accounts": [
                    {**account.model_dump(mode="json"), "available": str(account.available)}
                    for account in accounts
                ],
            }

        return self.database.store.read(read)

    def context(self, context: CommandContext, campaign_id: str, *, limit: int = 10) -> JsonObject:
        from research_swarm.control.campaigns import target_met

        context.principal.require("read")
        if type(limit) is not int or not 1 <= limit <= 50:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Context limit must be between 1 and 50"
            )

        def read(connection: SqlSession) -> JsonObject:
            transaction = Transaction(connection, context)
            campaign = transaction.get("campaign", campaign_id)
            data = CampaignData.model_validate(campaign["data"])
            authority = connection.execute(
                "SELECT next_sequence-1 AS watermark,controller_id,control_epoch "
                "FROM workspaces WHERE id=?",
                (context.principal.workspace_id,),
            ).one()
            counts = connection.execute(
                "SELECT kind,state,COUNT(*) AS count FROM records WHERE workspace_id=? "
                "AND campaign_id=? GROUP BY kind,state",
                (context.principal.workspace_id, campaign_id),
            ).fetchall()
            recent = connection.execute(
                "SELECT kind,id FROM records WHERE workspace_id=? AND campaign_id=? "
                "AND kind IN ('candidate','trial','evaluation_receipt','promotion',"
                "'search_observation','policy_transition') "
                "ORDER BY rowid DESC LIMIT ?",
                (context.principal.workspace_id, campaign_id, limit),
            ).fetchall()
            evidence = [summarize(transaction.get(row["kind"], row["id"])) for row in recent]
            try:
                search = summarize(transaction.get("search_state", campaign_id))
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
                search = None
            pending = connection.execute(
                "SELECT id FROM records WHERE workspace_id=? AND campaign_id=? "
                "AND kind='proposal_request' AND state IN ('pending','responded') "
                "ORDER BY rowid LIMIT ?",
                (context.principal.workspace_id, campaign_id, limit),
            ).fetchall()
            requests = [
                summarize(transaction.get("proposal_request", row["id"])) for row in pending
            ]
            budgets = connection.execute(
                "SELECT payload FROM budgets WHERE workspace_id=? "
                "AND campaign_id=? ORDER BY dimension",
                (context.principal.workspace_id, campaign_id),
            ).fetchall()
            accounts = [BudgetAccount.model_validate_json(row["payload"]) for row in budgets]
            stopping_status = {"target_met": target_met(transaction, campaign_id, data)}
            funding = funding_context(transaction, campaign_id)
            operational = operational_context(transaction, campaign_id)
            return {
                "schema_version": "1.0",
                **({"funding": funding} if funding is not None else {}),
                **({"operational": operational} if operational is not None else {}),
                "watermark": authority["watermark"],
                "authority": {
                    "controller_id": authority["controller_id"],
                    "control_epoch": authority["control_epoch"],
                },
                "campaign": summarize(campaign),
                "contract": {
                    "project_id": data.contract.project_id,
                    "project_revision_id": data.contract.project_revision_id,
                    "protocol_digest": data.contract.protocol_digest,
                    "comparison_class": data.contract.comparison_class,
                    "primary_metric": data.contract.primary_metric,
                    "search": data.contract.search.model_dump(mode="json"),
                    "promotion": data.contract.promotion.model_dump(mode="json"),
                    "stopping": data.contract.stopping.model_dump(mode="json"),
                    **(
                        {"delegation": data.contract.delegation.model_dump(mode="json")}
                        if data.contract.delegation is not None
                        else {}
                    ),
                },
                "counts": [dict(row) for row in counts],
                "recent_evidence": evidence,
                "search_state": search,
                "pending_proposals": requests,
                "stopping_status": stopping_status,
                "budget": [
                    {**account.model_dump(mode="json"), "available": str(account.available)}
                    for account in accounts
                ],
                "claim": "No validated improvement is implied by queued or measured work.",
            }

        return self.database.store.read(read)
