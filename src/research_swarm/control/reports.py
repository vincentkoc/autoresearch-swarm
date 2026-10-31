"""Human and agent reports share one bounded, immutable evidence snapshot."""

import re

from pydantic import Field

from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.evaluations import EvaluationAuthorization
from research_swarm.control.funding_views import funding_context
from research_swarm.control.models import CampaignData
from research_swarm.control.obligations import operational_context
from research_swarm.control.queries import summarize
from research_swarm.control.report_details import research_details
from research_swarm.control.report_selection import observed_cohorts
from research_swarm.domain.budgets import BudgetAccount
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import ID, VersionedRecord, new_id, utc_now
from research_swarm.state.database import JsonObject, Transaction


class ReportCampaign(VersionedRecord):
    campaign_id: ID
    recent_limit: int = Field(default=20, strict=True, ge=1, le=50)


MAX_REPORT_BYTES = 1024 * 1024


def literal(value: str) -> str:
    value = "".join(
        character if ord(character) >= 32 and ord(character) != 127 else " " for character in value
    )
    delimiter = "`" * (
        1 + max((len(match.group()) for match in re.finditer(r"`+", value)), default=0)
    )
    return f"{delimiter} {value} {delimiter}"


def detail_markdown(details: JsonObject) -> list[str]:
    metric = details["objective"]["primary_metric"]
    timing = details["scope"]["timing"]
    stopping = details["stopping"]
    lines = [
        "## Research contract",
        "",
        "Objective: " + literal(details["objective"]["name"]),
        "",
        f"Metric: {literal(metric['name'])}; direction: {metric['direction']}; "
        f"unit: {literal(metric['unit'])}.",
        "",
        f"Timing: {timing['budget_ms']} ms; scope: {timing['scope']}; "
        f"authority: {timing['timer_authority']}.",
        "",
        f"Trial limit: {stopping['rule']['max_trials']}. "
        f"Target: {stopping['rule']['target_metric']}. Target met: {stopping['target_met']}.",
        "",
        "Original baseline: "
        + (
            f"`{details['baseline']['candidate_id']}`; "
            f"promotion `{details['baseline']['promotion']['id']}`; "
            f"evaluation `{details['baseline']['evaluation_id']}`; "
            f"eligible at capture: {details['baseline']['eligible_at_capture']}."
            if details["baseline"]
            else "not initialized."
        ),
        "",
    ]
    for key in ("data_manifest_digest", "environment_contract_digest", "evaluator_contract_digest"):
        lines.extend([key + ": " + literal(details["scope"][key]), ""])
    candidates = details["candidates"]
    lines.extend(
        [
            "## Candidate changes and lineage",
            "",
            f"Included {candidates['included']} of {candidates['total']} candidates. "
            f"Complete: {candidates['complete']}. Baseline and incumbent are pinned.",
            "",
        ]
    )
    for candidate in candidates["items"]:
        lines.extend(
            [
                f"Candidate `{candidate['id']}`; source artifact "
                f"`{candidate['source_artifact']['id']}`; "
                f"tree `{candidate['source_tree_digest']}`.",
                "",
            ]
        )
        for change in candidate["changes"]:
            lines.append(
                f"Parent {change['parent']['kind']} `{change['parent']['id']}`: "
                f"{change['included']}/{change['total']} changed paths shown."
            )
            lines.append("")
            for path in change["paths"]:
                before = path["before"]["digest"] if path["before"] else "absent"
                after = path["after"]["digest"] if path["after"] else "absent"
                lines.append(
                    f"- {literal(path['path'])}: {path['change']}; `{before}` → `{after}`."
                )
            lines.append("")
    trials = details["trials"]
    lines.extend(
        [
            "## Trial outcomes",
            "",
            f"Included {trials['included']} of {trials['total']} trials. "
            f"Complete: {trials['complete']}.",
            "",
            "| Trial | Candidate | Seed | Execution | Evaluation | Resources |",
            "|---|---|---:|---|---|---|",
        ]
    )
    for trial in trials["items"]:
        lines.append(
            f"| `{trial['id']}` | `{trial['candidate_id']}` | {trial['seed']} | "
            f"{trial['execution_outcome']} | {trial['evaluation_status']} | "
            f"{trial['resource_status']} |"
        )
    attempts = details["attempts"]
    lines.extend(
        [
            "",
            "## Execution evidence",
            "",
            f"Included {attempts['included']} of {attempts['total']} attempts. "
            f"Complete: {attempts['complete']}.",
            "",
            "Profiles describe historical enrollment. A full hardware descriptor may be external.",
            "",
            "| Attempt | State | Backend | Fingerprint | Exit | Elapsed ms |",
            "|---|---|---|---|---:|---:|",
        ]
    )
    for attempt in attempts["items"]:
        result = attempt["result"]
        lines.append(
            f"| `{attempt['id']}` | {attempt['state']} | "
            f"{literal(attempt['profile']['backend'])} | "
            f"`{attempt['profile']['execution_fingerprint']}` | "
            f"{result['exit_code'] if result else 'unknown'} | "
            f"{result['elapsed_ms'] if result else 'unknown'} |"
        )
    if population := details.get("population"):
        lines.extend(
            [
                "",
                "## Retained research alternatives",
                "",
                f"Captured {population['included']} of {population['total']} archive members. "
                f"Complete: {population['complete']}.",
                "Archive membership preserves research options; it does not establish a winner. "
                "Evidence must be requalified before a retained branch is selected for new work.",
            ]
        )
        for item in population["items"]:
            request = item["data"]["request"]
            lines.append(
                f"- {literal(item['id'])}, revision {item['revision']}, {literal(item['state'])}: "
                f"{literal(request['reason'])}"
            )
    if promotions := details.get("promotions"):
        lines.extend(
            [
                "",
                "## Confirmed policy selections",
                "",
                f"Captured {promotions['included']} of {promotions['total']} promotion records. "
                f"Complete: {promotions['complete']}.",
                "",
            ]
        )
        for cohort in details["confirmed_cohorts"]:
            selected = cohort["best_confirmed"]
            lines.append(
                f"Cohort `{cohort['cohort_digest']}`: "
                + (
                    f"candidate `{selected['candidate_id']}`, selected by committed promotion "
                    f"`{selected['promotion_id']}` at incumbent revision "
                    f"{selected['selected_incumbent_revision']}."
                    if selected
                    else f"{cohort['status']}; no best-confirmed claim is made."
                )
            )
            lines.append("")
        if not details["confirmed_cohorts"]:
            lines.extend(["No currently qualified committed confirmation is included.", ""])
        lines.extend(
            [
                "Each cohort keeps its comparison identity, frozen confirmation policy and "
                "baseline lineage. Selection follows committed incumbent revisions, not first-seed "
                "scores, p-values or comparator-specific effects. Raw gates and per-seed evidence "
                "remain in JSON. Historical selection does not restore an earlier incumbent.",
                "",
            ]
        )
    reservations = details["resource_disposition"]["open_reservations"]
    lines.extend(
        [
            "",
            "## Unresolved evidence and resources",
            "",
            f"Open reservations: {reservations['total']}; "
            f"{reservations['included']} owners included in JSON. "
            "Omitted rows do not establish resource closure.",
            "",
        ]
    )
    for question in details["open_questions"]["items"]:
        evidence = question["evidence"]
        lines.append(
            f"- {question['question']} Read {evidence['kind']} `{evidence['id']}` "
            f"at captured revision {evidence['revision']}."
        )
    if not details["open_questions"]["items"]:
        lines.append("No unresolved questions were derived from the captured rows.")
    lines.extend(
        [
            "",
            "These are evidence-navigation hints, not permission to mutate state. "
            "The JSON snapshot supplies capture bounds and detail queries for each section.",
            "",
        ]
    )
    return lines


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
            "Protected evaluation: unavailable. Official evaluation: unavailable. "
            "This runtime qualifies authenticated evaluators only; neither an observed result "
            "nor a confirmed selection establishes protected-panel or official evaluation.",
            "",
            "This immutable report records eligibility at capture. Later correction events can "
            "change current eligibility without rewriting this snapshot.",
            "",
        ]
    )
    lines.extend(
        [
            "| Receipt | Measurement trust | Exposure class | Eligible at capture |",
            "|---|---|---|---|",
        ]
    )
    for evaluation in snapshot["recent_evaluations"]:
        data = evaluation["data"]
        lines.append(
            f"| `{evaluation['id']}` | {data['measurement_trust']} | "
            f"{data['exposure_class']} | {'yes' if evaluation['eligible'] else 'no'} |"
        )
    lines.append("")
    if cohorts := snapshot.get("observed_cohorts"):
        lines.extend(["## Observed comparison cohorts", ""])
        for cohort in cohorts:
            lines.append(
                f"Comparison `{cohort['comparison_digest']}`; scope: {cohort['scope']}; "
                f"captured observations: {cohort['observation_count']}."
            )
            lines.append("")
            for observed in cohort["best_observed"]:
                metric = observed["metric"]
                lines.append(
                    f"- Evaluation `{observed['evaluation_id']}`: "
                    f"{literal(metric['name'])} = {metric['value']} {literal(metric['unit'])}; "
                    f"candidate `{observed['candidate_id']}`."
                )
            lines.append("")
        lines.extend(
            [
                "These are single observed extrema within comparable captured evidence. "
                "They do not establish statistical wins or a cross-hardware leaderboard.",
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
    if details := snapshot.get("research"):
        lines.extend(detail_markdown(details))
    return "\n".join(lines)


def create_report(
    transaction: Transaction,
    campaign_row: JsonObject,
    *,
    recent_limit: int = 20,
    report_id: str | None = None,
) -> JsonObject:
    campaign = CampaignData.model_validate(campaign_row["data"])
    details = research_details(transaction, campaign_row, campaign, limit=recent_limit)
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
    evaluation_ids = tuple(
        dict.fromkeys((*details["pinned_evaluation_ids"], *(item["id"] for item in recent)))
    )
    for identifier in evaluation_ids:
        row = transaction.get("evaluation_receipt", identifier)
        evidence = summarize(row)
        authorization = EvaluationAuthorization.model_validate(
            transaction.get("evaluation_request", evidence["data"]["request_id"])["data"]
        )
        request = authorization.request
        evidence["data"].update(
            candidate_id=request.candidate_id,
            trial_id=request.trial_id,
            evaluator_id=request.evaluator_id,
            evaluator_contract_digest=request.evaluator_contract_digest,
            measurement_trust=request.measurement_trust,
            exposure_class=request.exposure_class,
            exposure_authorization_id=request.exposure_authorization_id,
        )
        try:
            qualified = eligible_evaluation(transaction, row["id"], campaign_row["id"], campaign)
            evidence["eligible"] = True
            evidence["comparison_digest"] = qualified.comparison_digest
            evidence["primary_metric"] = qualified.metric.model_dump(mode="json")
        except ResearchError as error:
            evidence["eligible"] = False
            evidence["ineligibility"] = error.code.value
        evaluations.append(evidence)
    if details["baseline"] is not None:
        baseline_evaluation = next(
            item for item in evaluations if item["id"] == details["baseline"]["evaluation_id"]
        )
        details["baseline"]["eligible_at_capture"] = baseline_evaluation["eligible"]
    budget_rows = transaction.connection.execute(
        "SELECT payload FROM budgets WHERE workspace_id=? AND campaign_id=? ORDER BY dimension",
        (transaction.workspace_id, campaign_row["id"]),
    ).fetchall()
    accounts = [BudgetAccount.model_validate_json(item["payload"]) for item in budget_rows]
    watermark = transaction.connection.execute(
        "SELECT next_sequence-1 FROM workspaces WHERE id=?",
        (transaction.workspace_id,),
    ).one()[0]
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
        "evaluation_window": {
            "recent_limit": recent_limit,
            "pinned_ids": details["pinned_evaluation_ids"],
            "included": len(evaluations),
            "complete": len(evaluations)
            == sum(item["count"] for item in counts if item["kind"] == "evaluation_receipt"),
        },
        "research": details,
        "evaluation_claim_support": {
            "qualified_measurement_trust": ["authenticated"],
            "protected": False,
            "official": False,
            "reason": "Protected and project-official evaluator profiles are not qualified",
        },
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
    snapshot["observed_cohorts"] = observed_cohorts(
        evaluations, complete=snapshot["evaluation_window"]["complete"]
    )
    content: JsonObject = {"snapshot": snapshot, "markdown": markdown(snapshot)}
    content["report_digest"] = record_digest("research.report", content)
    if len(canonical_bytes(content)) > MAX_REPORT_BYTES:
        raise ResearchError(
            ErrorCode.RESOURCE_UNAVAILABLE,
            "Report exceeds its bounded capture profile; request fewer recent records",
        )
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
