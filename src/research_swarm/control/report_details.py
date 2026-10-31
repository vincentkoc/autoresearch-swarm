"""Bounded research context captured inside the report's original transaction."""

from collections.abc import Callable

from research_swarm.control.models import (
    CampaignData,
    CandidateData,
    ProjectRevisionData,
    TrialData,
)
from research_swarm.control.queries import summarize
from research_swarm.control.report_selection import SelectionProjection, confirmed_cohorts
from research_swarm.control.worker_records import AttemptData
from research_swarm.domain.canonical import canonical_bytes, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import RunResult
from research_swarm.projects.source import SourceManifest
from research_swarm.state.database import JsonObject, Transaction

DETAIL_BYTES = 512 * 1024
DIFF_PATHS = 16
DIFF_PARENTS = 4


def reference(row: JsonObject) -> JsonObject:
    return {key: row[key] for key in ("kind", "id", "revision")}


def optional(transaction: Transaction, kind: str, identifier: str) -> JsonObject | None:
    try:
        return transaction.get(kind, identifier)
    except ResearchError as error:
        if error.code != ErrorCode.NOT_FOUND:
            raise
        return None


def source_changes(before: SourceManifest, after: SourceManifest) -> JsonObject:
    old = {entry.path: entry for entry in before.entries}
    new = {entry.path: entry for entry in after.entries}
    changed = sorted(path for path in old.keys() | new.keys() if old.get(path) != new.get(path))
    return {
        "before_tree_digest": before.tree_digest,
        "after_tree_digest": after.tree_digest,
        "total": len(changed),
        "included": min(len(changed), DIFF_PATHS),
        "complete": len(changed) <= DIFF_PATHS,
        "paths": [
            {
                "path": path,
                "change": "added"
                if path not in old
                else "removed"
                if path not in new
                else "changed",
                "before": old[path].model_dump(mode="json") if path in old else None,
                "after": new[path].model_dump(mode="json") if path in new else None,
            }
            for path in changed[:DIFF_PATHS]
        ],
    }


class DetailProjection:
    def __init__(self, transaction: Transaction, campaign_id: str, limit: int) -> None:
        self.transaction, self.campaign_id, self.limit = transaction, campaign_id, limit
        self.remaining = DETAIL_BYTES
        self.pinned: dict[tuple[str, str], JsonObject] = {}

    def pin(self, kind: str, identifier: str, project: Callable[[JsonObject], JsonObject]) -> None:
        if (kind, identifier) in self.pinned:
            return
        row = self.transaction.get(kind, identifier)
        if row["campaign_id"] != self.campaign_id:
            raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Report evidence differs")
        value = project(row)
        size = len(canonical_bytes(value))
        if size > self.remaining:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE,
                "Required report context exceeds its bounded detail profile",
            )
        self.pinned[kind, identifier] = value
        self.remaining -= size

    def page(
        self,
        kind: str,
        project: Callable[[JsonObject], JsonObject],
        *,
        pinned: tuple[str, ...] = (),
    ) -> JsonObject:
        connection = self.transaction.connection
        scope = (self.transaction.workspace_id, self.campaign_id, kind)
        total = connection.execute(
            "SELECT COUNT(*) FROM records WHERE workspace_id=? AND campaign_id=? AND kind=?",
            scope,
        ).one()[0]
        recent = connection.execute(
            "SELECT id FROM records WHERE workspace_id=? AND campaign_id=? AND kind=? "
            "ORDER BY rowid DESC LIMIT ?",
            (*scope, self.limit),
        ).fetchall()
        identifiers = tuple(dict.fromkeys((*pinned, *(row["id"] for row in recent))))
        items = []
        for identifier in identifiers:
            if cached := self.pinned.get((kind, identifier)):
                items.append(cached)
                continue
            row = self.transaction.get(kind, identifier)
            if row["campaign_id"] != self.campaign_id:
                raise ResearchError(ErrorCode.COMPARISON_INELIGIBLE, "Report evidence differs")
            value = project(row)
            size = len(canonical_bytes(value))
            if size > self.remaining:
                continue
            items.append(value)
            self.remaining -= size
        return {
            "total": total,
            "included": len(items),
            "complete": len(items) == total,
            "items": items,
            "pinned_ids": list(pinned),
            "detail_query": {"kind": kind, "campaign_id": self.campaign_id},
        }

    def reservations(self) -> JsonObject:
        scope = (self.transaction.workspace_id, self.campaign_id)
        counts = self.transaction.connection.execute(
            "SELECT state,COUNT(*) AS count FROM budget_reservations WHERE workspace_id=? "
            "AND campaign_id=? AND state!='settled' GROUP BY state ORDER BY state",
            scope,
        ).fetchall()
        rows = self.transaction.connection.execute(
            "SELECT id,owner_id,state,payload FROM budget_reservations WHERE workspace_id=? "
            "AND campaign_id=? AND state!='settled' ORDER BY id LIMIT ?",
            (*scope, self.limit),
        ).fetchall()
        items = []
        for row in rows:
            value = {
                "id": row["id"],
                "owner_id": row["owner_id"],
                "state": row["state"],
                "reservation": strict_json_loads(row["payload"]),
            }
            size = len(canonical_bytes(value))
            if size <= self.remaining:
                items.append(value)
                self.remaining -= size
        total = sum(row["count"] for row in counts)
        return {
            "total": total,
            "included": len(items),
            "complete": len(items) == total,
            "counts": [dict(row) for row in counts],
            "items": items,
            "owner_detail_queries": [
                {"kind": kind, "campaign_id": self.campaign_id}
                for kind in ("attempt", "confirmation_plan", "campaign_funding")
            ],
        }


def research_details(
    transaction: Transaction, campaign_row: JsonObject, campaign: CampaignData, *, limit: int
) -> JsonObject:
    from research_swarm.control.campaigns import target_met

    campaign_id = campaign_row["id"]
    project_row = transaction.get("project_revision", campaign.contract.project_revision_id)
    project = ProjectRevisionData.model_validate(project_row["data"])
    manifest = project.resolved.manifest
    metric = next(
        (
            item
            for item in manifest.evaluation.metrics
            if item.name == campaign.contract.primary_metric
        ),
        None,
    )
    authority = transaction.connection.execute(
        "SELECT controller_id,control_epoch FROM workspaces WHERE id=?",
        (transaction.workspace_id,),
    ).one()
    initial = transaction.connection.execute(
        "SELECT r.id,e.id AS event_id,e.sequence AS event_sequence FROM records r JOIN events e "
        "ON e.workspace_id=r.workspace_id AND json_extract(e.payload,'$.aggregate_id')=r.id "
        "AND json_extract(e.payload,'$.type')='promotion.committed' "
        "WHERE r.workspace_id=? AND r.campaign_id=? AND r.kind='promotion' "
        "AND json_extract(r.payload,'$.decision.reason_codes[0]')='baseline_initialized' "
        "ORDER BY e.sequence LIMIT 1",
        (transaction.workspace_id, campaign_id),
    ).fetchone()
    baseline = transaction.get("promotion", initial["id"]) if initial else None
    evaluation_ids = tuple(
        dict.fromkeys(
            identifier
            for identifier in (
                baseline["data"]["candidate_evaluation_id"] if baseline else None,
                campaign.incumbent_evaluation_id,
            )
            if identifier is not None
        )
    )
    pinned_trials = []
    for identifier in evaluation_ids:
        receipt = transaction.get("evaluation_receipt", identifier)
        request = transaction.get("evaluation_request", receipt["data"]["receipt"]["request_id"])
        pinned_trials.append(request["data"]["request"]["trial_id"])
    pinned = tuple(
        dict.fromkeys(
            identifier
            for identifier in (
                baseline["data"]["decision"]["candidate_id"] if baseline else None,
                campaign.incumbent_candidate_id,
            )
            if identifier is not None
        )
    )
    projection = DetailProjection(transaction, campaign_id, limit)
    selection = SelectionProjection(transaction, campaign_id, campaign)
    pinned_promotions = tuple(
        dict.fromkeys(
            identifier
            for identifier in (
                baseline["id"] if baseline else None,
                campaign.incumbent_promotion_id,
            )
            if identifier is not None
        )
    )

    def candidate(row: JsonObject) -> JsonObject:
        data = CandidateData.model_validate(row["data"])
        changes = []
        for parent_id in data.parent_ids[:DIFF_PARENTS] or (None,):
            parent_row = transaction.get("candidate", parent_id) if parent_id else project_row
            parent_source = (
                CandidateData.model_validate(parent_row["data"]).source
                if parent_id
                else project.resolved.source
            )
            changes.append(
                {"parent": reference(parent_row), **source_changes(parent_source, data.source)}
            )
        return {
            **reference(row),
            "state": row["state"],
            "parent_ids": list(data.parent_ids[:64]),
            "parent_count": len(data.parent_ids),
            "parent_ids_complete": len(data.parent_ids) <= 64,
            "lineage_depth": 1,
            "source_artifact": data.source_artifact.model_dump(mode="json"),
            "source_tree_digest": data.source.tree_digest,
            "base_revision": data.source.base_revision,
            "hypothesis": data.hypothesis[:1024],
            "hypothesis_complete": len(data.hypothesis) <= 1024,
            "proposer_id": data.proposer_id,
            "changes": changes,
            "parent_changes_complete": len(data.parent_ids) <= DIFF_PARENTS,
            "validation_query": {"kind": "candidate_validation", "parent_id": row["id"]},
        }

    def trial(row: JsonObject) -> JsonObject:
        data = TrialData.model_validate(row["data"])
        return {
            **reference(row),
            "state": row["state"],
            "candidate_id": data.candidate_id,
            "seed": data.spec.seed,
            "spec_digest": data.spec.spec_digest,
            "execution_outcome": data.execution_outcome,
            "evaluation_status": data.evaluation_status,
            "resource_status": data.resource_status,
            "confirmation_plan_id": data.confirmation_plan_id,
            "confirmation_side": data.confirmation_side,
            "jobs": [summarize(transaction.get("job", identifier)) for identifier in data.job_ids],
        }

    def attempt(row: JsonObject) -> JsonObject:
        data = AttemptData.model_validate(row["data"])
        result_row = optional(transaction, "run_result", row["id"])
        result = RunResult.model_validate(result_row["data"]) if result_row else None
        return {
            **reference(row),
            "state": row["state"],
            "job_id": data.dispatch.job_id,
            "trial_id": data.dispatch.trial_id,
            "stage_id": data.dispatch.stage_id,
            "worker_id": data.dispatch.worker_id,
            "worker_session_id": data.dispatch.worker_session_id,
            "control_epoch": data.dispatch.control_epoch,
            "fencing_generation": data.dispatch.fencing_generation,
            "publication_status": data.publication_status,
            "finding": data.finding[:1024],
            "finding_complete": len(data.finding) <= 1024,
            "profile": data.profile.model_dump(mode="json"),
            "profile_scope": (
                "Historical enrolled capabilities; full hardware descriptor may be external"
            ),
            "outcome_artifact": data.outcome_artifact.model_dump(mode="json")
            if data.outcome_artifact
            else None,
            "closure_receipt_id": data.closure_receipt_id,
            "result": {
                **reference(result_row),
                "status": result.status,
                "exit_code": result.exit_code,
                "elapsed_ms": result.elapsed_ms,
                "phase_timings_ms": result.phase_timings_ms,
                "observed_seed": result.observed_seed,
                "execution_fingerprint": result.execution_fingerprint,
                "result_digest": result.result_digest,
                "process_closure_ref": result.process_closure_ref.model_dump(mode="json"),
                "artifacts": [item.model_dump(mode="json") for item in result.artifacts],
            }
            if result is not None and result_row is not None
            else None,
            "attempt_history_query": {"kind": "attempt", "parent_id": data.dispatch.job_id},
        }

    for identifier in pinned:
        projection.pin("candidate", identifier, candidate)
    for identifier in dict.fromkeys(pinned_trials):
        projection.pin("trial", identifier, trial)
    for identifier in pinned_promotions:
        projection.pin("promotion", identifier, selection.project)
    candidates = projection.page("candidate", candidate, pinned=pinned)
    trials = projection.page("trial", trial, pinned=tuple(dict.fromkeys(pinned_trials)))
    attempts = projection.page("attempt", attempt)
    proposals = projection.page("proposal_request", summarize)
    promotions = projection.page("promotion", selection.project, pinned=pinned_promotions)
    population = projection.page("population_member", summarize)
    reservations = projection.reservations()
    resource_counts = transaction.connection.execute(
        "SELECT kind,state,COUNT(*) AS count FROM records WHERE workspace_id=? AND campaign_id=? "
        "AND kind IN ('trial','job','attempt') GROUP BY kind,state ORDER BY kind,state",
        (transaction.workspace_id, campaign_id),
    ).fetchall()
    questions = []
    for item in attempts["items"]:
        if item["state"] == "execution_unknown":
            questions.append(
                {
                    "question": "What evidence establishes closure of the original execution?",
                    "evidence": reference(item),
                }
            )
    for item in trials["items"]:
        if item["evaluation_status"] in {"invalid", "incomplete"}:
            questions.append(
                {
                    "question": "Which declared evaluation requirements remain unsatisfied?",
                    "evidence": reference(item),
                }
            )
    for item in proposals["items"]:
        if item["state"] in {"pending", "responded"}:
            questions.append(
                {
                    "question": (
                        "Does this assigned proposal require a response or admission decision?"
                    ),
                    "evidence": reference(item),
                }
            )
    return {
        "workspace_id": transaction.workspace_id,
        "authority": dict(authority),
        "objective": {
            "name": campaign.name,
            "primary_metric": metric.model_dump(mode="json")
            if metric
            else {
                "name": campaign.contract.primary_metric,
                "direction": "undeclared",
                "unit": "undeclared",
                "aggregation": "undeclared",
            },
        },
        "scope": {
            "project_revision": reference(project_row),
            "source_artifact": project.source_artifact.model_dump(mode="json"),
            "data_manifest_digest": project.resolved.data_manifest_digest,
            "environment_contract_digest": project.resolved.environment_contract_digest,
            "evaluator_contract_digest": project.resolved.evaluator_contract_digest,
            "timing": manifest.timing.model_dump(mode="json"),
            "mutation": {
                name: {
                    "patterns": list(patterns[:16]),
                    "total": len(patterns),
                    "complete": len(patterns) <= 16,
                }
                for name, patterns in (
                    ("allow", manifest.mutation.allow),
                    ("deny", manifest.mutation.deny),
                )
            },
        },
        "stopping": {
            "rule": campaign.contract.stopping.model_dump(mode="json"),
            "target_met": target_met(transaction, campaign_id, campaign),
            "recorded_reason": campaign.stopping_reason,
        },
        "baseline": {
            "promotion": reference(baseline),
            "current_promotion_state": baseline["state"],
            "commit_event": {"id": initial["event_id"], "sequence": initial["event_sequence"]},
            "candidate_id": baseline["data"]["decision"]["candidate_id"],
            "evaluation_id": baseline["data"]["candidate_evaluation_id"],
            "meaning": "Original committed baseline; later reinitializations do not rewrite it",
        }
        if baseline is not None and initial is not None
        else None,
        "pinned_evaluation_ids": list(evaluation_ids),
        "candidates": candidates,
        "trials": trials,
        "attempts": attempts,
        "proposals": proposals,
        "promotions": promotions,
        "population": population,
        "confirmed_cohorts": confirmed_cohorts(promotions),
        "resource_disposition": {
            "basis": "Controller-recorded custody; an omitted row is not proof of closure",
            "counts": [dict(row) for row in resource_counts],
            "open_reservations": reservations,
        },
        "open_questions": {
            "basis": "Recorded unresolved conditions, not scientific interpretations",
            "items": questions,
            "complete": all(page["complete"] for page in (trials, attempts, proposals)),
        },
        "next_actions": {
            "basis": (
                "Read-only evidence navigation; mutation permissions and state must be rechecked"
            ),
            "items": [{"operation": "get", "record": item["evidence"]} for item in questions],
        },
        "detail_limit": limit,
        "detail_byte_limit": DETAIL_BYTES,
    }
