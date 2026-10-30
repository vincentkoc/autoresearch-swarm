"""Bounded evidence views with workspace-scoped, insertion-stable cursors."""

from __future__ import annotations

import base64
import binascii
from typing import Literal

from pydantic import Field, ValidationError

from research_swarm.control.models import CampaignData
from research_swarm.domain.budgets import BudgetAccount
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ID, Digest, VersionedRecord
from research_swarm.state.database import Database, JsonObject, Transaction

RecordKind = Literal[
    "project",
    "project_revision",
    "campaign",
    "candidate",
    "candidate_validation",
    "trial",
    "job",
    "attempt",
    "run_result",
    "worker",
    "evaluation_request",
    "evaluation_receipt",
    "promotion",
    "evidence_correction",
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
    if row["kind"] == "candidate":
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
    elif row["kind"] == "worker":
        result["data"] = {
            "name": data["name"],
            "binding": data["binding"],
            "profiles": [profile["name"] for profile in data["profiles"]],
            "max_concurrency": data["max_concurrency"],
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
        with self.database.connect() as connection:
            connection.execute("BEGIN")
            through = (
                cursor.through
                if cursor
                else connection.execute(
                    "SELECT COALESCE(MAX(rowid),0) FROM records WHERE workspace_id=?",
                    (context.principal.workspace_id,),
                ).fetchone()[0]
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
            connection.commit()
        return {"items": items, "next_cursor": next_cursor, "membership_through": through}

    def budget(self, context: CommandContext, campaign_id: str) -> JsonObject:
        context.principal.require("read")
        with self.database.connect() as connection:
            connection.execute("BEGIN")
            campaign = Transaction(connection, context).get("campaign", campaign_id)
            rows = connection.execute(
                "SELECT payload FROM budgets WHERE workspace_id=? "
                "AND campaign_id=? ORDER BY dimension",
                (context.principal.workspace_id, campaign_id),
            ).fetchall()
            accounts = [BudgetAccount.model_validate_json(row["payload"]) for row in rows]
            connection.commit()
        return {
            "campaign_id": campaign_id,
            "campaign_revision": campaign["revision"],
            "accounts": [
                {**account.model_dump(mode="json"), "available": str(account.available)}
                for account in accounts
            ],
        }

    def context(self, context: CommandContext, campaign_id: str, *, limit: int = 10) -> JsonObject:
        context.principal.require("read")
        if type(limit) is not int or not 1 <= limit <= 50:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Context limit must be between 1 and 50"
            )
        with self.database.connect() as connection:
            connection.execute("BEGIN")
            transaction = Transaction(connection, context)
            campaign = transaction.get("campaign", campaign_id)
            data = CampaignData.model_validate(campaign["data"])
            watermark = connection.execute(
                "SELECT next_sequence-1 FROM workspaces WHERE id=?",
                (context.principal.workspace_id,),
            ).fetchone()[0]
            counts = connection.execute(
                "SELECT kind,state,COUNT(*) AS count FROM records WHERE workspace_id=? "
                "AND campaign_id=? GROUP BY kind,state",
                (context.principal.workspace_id, campaign_id),
            ).fetchall()
            recent = connection.execute(
                "SELECT kind,id FROM records WHERE workspace_id=? AND campaign_id=? "
                "AND kind IN ('candidate','trial','evaluation_receipt','promotion') "
                "ORDER BY rowid DESC LIMIT ?",
                (context.principal.workspace_id, campaign_id, limit),
            ).fetchall()
            evidence = [summarize(transaction.get(row["kind"], row["id"])) for row in recent]
            budgets = connection.execute(
                "SELECT payload FROM budgets WHERE workspace_id=? "
                "AND campaign_id=? ORDER BY dimension",
                (context.principal.workspace_id, campaign_id),
            ).fetchall()
            accounts = [BudgetAccount.model_validate_json(row["payload"]) for row in budgets]
            connection.commit()
        return {
            "schema_version": "1.0",
            "watermark": watermark,
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
            },
            "counts": [dict(row) for row in counts],
            "recent_evidence": evidence,
            "budget": [
                {**account.model_dump(mode="json"), "available": str(account.available)}
                for account in accounts
            ],
            "claim": "No validated improvement is implied by queued or measured work.",
        }
