"""Report measured cohorts and recorded selection without inventing a new optimizer."""

from research_swarm.control.confirmation_assessment import collect_panel, verify_planned_assessment
from research_swarm.control.confirmation_models import ConfirmationData
from research_swarm.control.models import CampaignData
from research_swarm.control.promotions import PromotionAssessment
from research_swarm.control.queries import summarize
from research_swarm.control.validity import require_current
from research_swarm.domain.canonical import record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import JsonObject, Transaction


def observed_cohorts(evaluations: list[JsonObject], *, complete: bool) -> list[JsonObject]:
    groups: dict[str, list[JsonObject]] = {}
    for evaluation in evaluations:
        if evaluation["eligible"]:
            groups.setdefault(evaluation["comparison_digest"], []).append(evaluation)
    cohorts = []
    for digest, members in sorted(groups.items()):
        direction = members[0]["primary_metric"]["direction"]
        extremum = min if direction == "minimize" else max
        best_value = extremum(item["primary_metric"]["value"] for item in members)
        cohorts.append(
            {
                "comparison_digest": digest,
                "history_complete": complete,
                "scope": "campaign_cohort" if complete else "captured_cohort",
                "observation_count": len(members),
                "best_observed": [
                    {
                        "evaluation_id": item["id"],
                        "candidate_id": item["data"]["candidate_id"],
                        "trial_id": item["data"]["trial_id"],
                        "metric": item["primary_metric"],
                    }
                    for item in members
                    if item["primary_metric"]["value"] == best_value
                ],
                "claim": "A single observed extremum is not a statistically confirmed winner",
            }
        )
    return cohorts


def confirmed_cohorts(page: JsonObject) -> list[JsonObject]:
    groups: dict[str, list[JsonObject]] = {}
    for row in page["items"]:
        if row["confirmation_status"] == "qualified":
            confirmed = row["confirmation"]
            groups.setdefault(confirmed["cohort_digest"], []).append(confirmed)
    result = []
    for digest, members in sorted(groups.items()):
        latest_revision = max(item["selected_incumbent_revision"] for item in members)
        latest = [
            item for item in members if item["selected_incumbent_revision"] == latest_revision
        ]
        selected = (
            {
                key: latest[0][key]
                for key in (
                    "candidate_id",
                    "promotion_id",
                    "confirmation_plan_id",
                    "selected_incumbent_revision",
                    "is_current_incumbent",
                )
            }
            if page["complete"] and len(latest) == 1
            else None
        )
        result.append(
            {
                "cohort_digest": digest,
                "comparison_digest": members[0]["comparison_digest"],
                "policy_digest": members[0]["policy_digest"],
                "baseline_promotion_id": members[0]["baseline_promotion_id"],
                "history_complete": page["complete"],
                "status": "incomplete"
                if not page["complete"]
                else "selected"
                if selected
                else "ambiguous",
                "basis": "latest_committed_confirmation",
                "best_confirmed": selected,
                "aggregate_metric": None,
                "claim": (
                    "Historical policy selection does not change or restore the current incumbent"
                ),
            }
        )
    return result


class SelectionProjection:
    def __init__(self, transaction: Transaction, campaign_id: str, campaign: CampaignData) -> None:
        self.transaction, self.campaign_id, self.campaign = transaction, campaign_id, campaign

    def project(self, row: JsonObject) -> JsonObject:
        assessment = PromotionAssessment.model_validate(row["data"])
        result = summarize(row)
        result.update(commit_event=None, confirmation_status="uncommitted", confirmation=None)
        event_row = self.transaction.connection.execute(
            "SELECT payload FROM events WHERE workspace_id=? "
            "AND json_extract(payload,'$.aggregate_id')=? "
            "AND json_extract(payload,'$.type')='promotion.committed' ORDER BY sequence LIMIT 1",
            (self.transaction.workspace_id, row["id"]),
        ).fetchone()
        if event_row is None:
            return result
        event = strict_json_loads(event_row["payload"])
        result["commit_event"] = {"id": event["id"], "sequence": event["sequence"]}
        result["confirmation_status"] = "not_confirmed"
        if assessment.evidence_level != "confirmed":
            return result
        try:
            result["confirmation"] = self._confirmed(row, assessment, event)
        except ResearchError as error:
            result["confirmation_status"] = "ineligible"
            result["ineligibility"] = error.code.value
        else:
            result["confirmation_status"] = "qualified"
        return result

    def _confirmed(
        self, row: JsonObject, assessment: PromotionAssessment, event: JsonObject
    ) -> JsonObject:
        decision = assessment.decision
        if (
            assessment.confirmation_plan_id is None
            or decision.outcome != "promote"
            or event["campaign_id"] != self.campaign_id
            or event["aggregate_type"] != "promotion"
            or event["payload"]["incumbent_revision"] != decision.expected_incumbent_revision + 1
        ):
            raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Committed confirmation differs")
        verify_planned_assessment(self.transaction, assessment)
        require_current(self.transaction, assessment.dependencies)
        plan_row = self.transaction.get("confirmation_plan", assessment.confirmation_plan_id)
        data = ConfirmationData.model_validate(plan_row["data"])
        plan = data.plan
        if (
            plan.campaign_id != self.campaign_id
            or plan.campaign_contract_digest != self.campaign.contract_digest
            or assessment.gate is None
            or assessment.gate.expected_pairs != len(plan.policy.seeds)
            or assessment.gate.observed_pairs != len(plan.policy.seeds)
            or assessment.gate.missing_seeds
        ):
            raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Confirmation panel differs")
        evaluations = collect_panel(self.transaction, plan, self.campaign)
        if tuple(
            item.evidence.receipt.id for item in evaluations
        ) != decision.evaluation_ids or any(
            item.comparison_digest != assessment.comparison_digest for item in evaluations
        ):
            raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Confirmation evidence differs")
        lineage = self.transaction.connection.execute(
            "SELECT r.id FROM records r JOIN events e ON e.workspace_id=r.workspace_id "
            "AND json_extract(e.payload,'$.aggregate_id')=r.id "
            "AND json_extract(e.payload,'$.type')='promotion.committed' "
            "WHERE r.workspace_id=? AND r.campaign_id=? AND r.kind='promotion' "
            "AND json_extract(r.payload,'$.decision.reason_codes[0]') "
            "IN ('baseline_initialized','baseline_reinitialized') "
            "AND e.sequence<=? ORDER BY e.sequence DESC LIMIT 1",
            (self.transaction.workspace_id, self.campaign_id, event["sequence"]),
        ).fetchone()
        if lineage is None:
            raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Confirmation lineage is missing")
        policy_digest = record_digest(
            "research.confirmation-policy", plan.policy.model_dump(mode="json")
        )
        identity = {
            "comparison_digest": assessment.comparison_digest,
            "policy_digest": policy_digest,
            "baseline_promotion_id": lineage["id"],
        }
        return {
            **identity,
            "cohort_digest": record_digest("research.report-confirmed-cohort", identity),
            "candidate_id": decision.candidate_id,
            "comparator_id": decision.comparator_id,
            "promotion_id": row["id"],
            "confirmation_plan_id": plan.id,
            "plan_digest": plan.plan_digest,
            "policy": plan.policy.model_dump(mode="json"),
            "selected_incumbent_revision": decision.expected_incumbent_revision + 1,
            "is_current_incumbent": self.campaign.incumbent_promotion_id == row["id"]
            and self.campaign.incumbent_eligibility == "eligible",
            "gate": assessment.gate.model_dump(mode="json"),
            "per_seed_evidence": [
                {
                    "seed": binding.seed,
                    "side": binding.side,
                    "trial_id": binding.trial_id,
                    "evaluation_id": evaluation.evidence.receipt.id,
                    "metric": evaluation.metric.model_dump(mode="json"),
                }
                for binding, evaluation in zip(plan.bindings, evaluations, strict=True)
            ],
            "aggregate_metric": None,
        }
