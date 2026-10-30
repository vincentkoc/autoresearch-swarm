"""Human and agent reports share one bounded, immutable evidence snapshot."""

import re

from pydantic import Field

from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.funding_views import funding_context
from research_swarm.control.models import CampaignData
from research_swarm.control.obligations import operational_context
from research_swarm.control.queries import summarize
from research_swarm.domain.budgets import BudgetAccount
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.types import ID, VersionedRecord, new_id, utc_now
from research_swarm.state.database import JsonObject, Transaction


class ReportCampaign(VersionedRecord):
    campaign_id: ID
    recent_limit: int = Field(default=20, strict=True, ge=1, le=50)


def literal(value: str) -> str:
    value = "".join(
        character if ord(character) >= 32 and ord(character) != 127 else " " for character in value
    )
    delimiter = "`" * (
        1 + max((len(match.group()) for match in re.finditer(r"`+", value)), default=0)
    )
    return f"{delimiter} {value} {delimiter}"


def markdown(snapshot: JsonObject) -> str:
    """Render structured fields only; raw hypotheses, logs and HTML are not embedded."""
    lines = [
        "# Research report",
        "",
        f"Campaign: `{snapshot['campaign_id']}`",
        "",
        "Comparison: " + literal(str(snapshot["comparison_class"])),
        "",
        f"Evidence through event: {snapshot['evidence_through']}",
        "",
        f"State: {snapshot['campaign_state']}",
        "",
        "Stopping reason: " + literal(snapshot["stopping_reason"] or "Not completed"),
        "",
    ]
    if operational := snapshot.get("operational"):
        lines.extend(
            [
                "Operational status: " + operational["status"],
                "",
                "Obligation review: `" + operational["obligations_id"] + "`",
                "",
                "Resource closure: "
                + (
                    "`" + operational["resource_closure_id"] + "`"
                    if operational["resource_closure_id"]
                    else "pending"
                ),
                "",
            ]
        )
    lines.extend(["## Selection", ""])
    incumbent = snapshot["incumbent"]
    if incumbent["candidate_id"]:
        lines.extend(
            [
                f"Candidate: `{incumbent['candidate_id']}`",
                "",
                f"Evidence level: {incumbent['evidence_level']}. "
                f"Eligibility: {incumbent['eligibility']}.",
                "",
            ]
        )
    else:
        lines.extend(["No incumbent is selected.", ""])
    lines.extend(["## Recorded outcomes", "", "| Record | State | Count |", "|---|---|---:|"])
    for item in snapshot["counts"]:
        lines.append(f"| {item['kind']} | {item['state']} | {item['count']} |")
    lines.extend(
        [
            "",
            "## Budget",
            "",
            "| Dimension | Consumed | Reserved | Uncertain |",
            "|---|---:|---:|---:|",
        ]
    )
    for account in snapshot["budget"]:
        lines.append(
            f"| {account['dimension']} | {account['consumed']} | "
            f"{account['reserved']} | {account['uncertain']} |"
        )
    lines.extend(
        [
            "",
            "## Evidence",
            "",
            f"The JSON snapshot includes {len(snapshot['recent_evaluations'])} recent evaluations. "
            f"Total recorded evaluations: {snapshot['evaluation_count']}.",
            "",
            "Each evaluation includes its receipt ID, raw measurements, and current eligibility. "
            "These observations do not constitute a global leaderboard.",
            "",
            "Confirmed selection means the declared gate passed. It does not establish held-out "
            "generalization or erase earlier dataset exposure.",
            "",
            "This immutable report records eligibility at capture. Later correction events can "
            "change current eligibility without rewriting this snapshot.",
            "",
        ]
    )
    if funding := snapshot.get("funding"):
        lines.extend(
            [
                "## Campaign funding",
                "",
                (
                    "Budget totals already include descendant consumption; "
                    "do not add child totals again."
                ),
                "",
            ]
        )
        if incoming := funding["incoming"]:
            lines.extend(
                [
                    f"Parent: `{incoming['parent_campaign_id']}`. Allocation: {incoming['state']}.",
                    "",
                ]
            )
        lines.extend(
            [
                f"Direct children: {funding['child_count']}. "
                f"Open allocations: {funding['open_child_count']}.",
                "",
            ]
        )
    return "\n".join(lines)


def create_report(
    transaction: Transaction,
    campaign_row: JsonObject,
    *,
    recent_limit: int = 20,
    report_id: str | None = None,
) -> JsonObject:
    campaign = CampaignData.model_validate(campaign_row["data"])
    counts = transaction.connection.execute(
        "SELECT kind,state,COUNT(*) AS count FROM records WHERE workspace_id=? "
        "AND campaign_id=? GROUP BY kind,state ORDER BY kind,state",
        (transaction.workspace_id, campaign_row["id"]),
    ).fetchall()
    recent = transaction.connection.execute(
        "SELECT id FROM records WHERE workspace_id=? AND campaign_id=? "
        "AND kind='evaluation_receipt' ORDER BY rowid DESC LIMIT ?",
        (transaction.workspace_id, campaign_row["id"], recent_limit),
    ).fetchall()
    evaluations = []
    for item in recent:
        row = transaction.get("evaluation_receipt", item["id"])
        evidence = summarize(row)
        try:
            eligible_evaluation(transaction, row["id"], campaign_row["id"], campaign)
            evidence["eligible"] = True
        except ResearchError as error:
            evidence["eligible"] = False
            evidence["ineligibility"] = error.code.value
        evaluations.append(evidence)
    budget_rows = transaction.connection.execute(
        "SELECT payload FROM budgets WHERE workspace_id=? AND campaign_id=? ORDER BY dimension",
        (transaction.workspace_id, campaign_row["id"]),
    ).fetchall()
    accounts = [BudgetAccount.model_validate_json(item["payload"]) for item in budget_rows]
    watermark = transaction.connection.execute(
        "SELECT next_sequence-1 FROM workspaces WHERE id=?",
        (transaction.workspace_id,),
    ).fetchone()[0]
    selection = None
    if campaign.incumbent_promotion_id:
        selection = summarize(transaction.get("promotion", campaign.incumbent_promotion_id))
    snapshot = {
        "schema_version": "1.0",
        "campaign_id": campaign_row["id"],
        "campaign_state": campaign_row["state"],
        "campaign_revision": campaign_row["revision"],
        "stopping_reason": campaign.stopping_reason,
        "captured_at": utc_now(),
        "evidence_through": watermark,
        "protocol_digest": campaign.contract.protocol_digest,
        "campaign_contract_digest": campaign.contract_digest,
        "comparison_class": campaign.contract.comparison_class,
        "primary_metric": campaign.contract.primary_metric,
        "incumbent": {
            "candidate_id": campaign.incumbent_candidate_id,
            "revision": campaign.incumbent_revision,
            "evidence_level": campaign.incumbent_evidence_level,
            "eligibility": campaign.incumbent_eligibility,
            "selection": selection,
        },
        "counts": [dict(item) for item in counts],
        "budget": [
            {**account.model_dump(mode="json"), "available": str(account.available)}
            for account in accounts
        ],
        "recent_evaluations": evaluations,
        "evaluation_count": sum(
            item["count"] for item in counts if item["kind"] == "evaluation_receipt"
        ),
        "detail_query": {"kind": "evaluation_receipt", "campaign_id": campaign_row["id"]},
    }
    funding = funding_context(transaction, campaign_row["id"])
    operational = operational_context(transaction, campaign_row["id"])
    if operational is not None:
        snapshot["operational"] = operational
    if funding is not None:
        snapshot["funding"] = funding
    content: JsonObject = {"snapshot": snapshot, "markdown": markdown(snapshot)}
    content["report_digest"] = record_digest("research.report", content)
    row = transaction.insert(
        "report",
        report_id or new_id(),
        content,
        state="immutable",
        campaign_id=campaign_row["id"],
        parent_id=campaign_row["id"],
    )
    transaction.event("report.created", row, {"evidence_through": watermark})
    return row
