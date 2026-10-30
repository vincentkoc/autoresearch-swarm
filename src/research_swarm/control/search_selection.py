"""Pin the chosen observation before any independent selection side effects."""

from research_swarm.control.models import CampaignData
from research_swarm.control.observation_models import (
    ObservationSelection,
    SelectTrialObservation,
    TrialObservation,
    observation_selection_id,
)
from research_swarm.control.proposals import search_state, write_search_state
from research_swarm.control.search_models import SearchState
from research_swarm.control.search_observations import trial_facts
from research_swarm.control.workers import authority
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.state.database import Database, JsonObject, Transaction


class SelectionControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def select(self, context: CommandContext, intent: SelectTrialObservation) -> JsonObject:
        def pin(transaction: Transaction) -> JsonObject:
            if authority(transaction) != (
                intent.expected_controller_id,
                intent.expected_control_epoch,
            ):
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Observation selection authority changed"
                )
            campaign = CampaignData.model_validate(
                transaction.get("campaign", intent.campaign_id)["data"]
            )
            previous = search_state(transaction, intent.campaign_id, campaign)
            if context.expected_revision != previous["revision"]:
                raise ResearchError(ErrorCode.REVISION_CONFLICT, "Search state revision changed")
            state = SearchState.model_validate(previous["data"])
            observed = transaction.get("search_observation", intent.observation_id)
            observation = TrialObservation.model_validate(observed["data"])
            facts = observation.facts
            if state.policy == "portfolio" and state.selection_trial_id not in {
                None,
                facts.trial_id,
            }:
                raise ResearchError(ErrorCode.RESOURCE_UNAVAILABLE, "Another trial owns selection")
            if (
                facts.campaign_id != intent.campaign_id
                or facts.trial_id not in state.active_trial_ids
            ):
                raise ResearchError(
                    ErrorCode.INVALID_TRANSITION, "Observation does not own an active policy trial"
                )
            identifier = observation_selection_id(
                transaction.workspace_id, intent.campaign_id, facts.trial_id
            )
            try:
                existing = transaction.get("policy_transition", identifier)
            except ResearchError as error:
                if error.code != ErrorCode.NOT_FOUND:
                    raise
            else:
                selected = ObservationSelection.model_validate(existing["data"])
                if selected.observation_id != intent.observation_id:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT,
                        "Trial already has a different observation pin",
                    )
                return {"transition": existing, "search": previous}
            current = trial_facts(transaction, transaction.get("trial", facts.trial_id), campaign)
            if (
                record_digest("research.trial-facts", current.model_dump(mode="json"))
                != observation.facts_digest
            ):
                raise ResearchError(
                    ErrorCode.STALE_FENCE, "Observation facts changed before selection"
                )
            if facts.outcome == "execution_unknown":
                raise ResearchError(
                    ErrorCode.EXECUTION_UNKNOWN, "Unknown execution cannot pin a selection"
                )
            if facts.outcome == "measurement" and (
                observation.incumbent_revision != campaign.incumbent_revision
                or observation.comparator_id != campaign.incumbent_candidate_id
                or observation.comparator_evaluation_id != campaign.incumbent_evaluation_id
                or campaign.incumbent_eligibility != "eligible"
            ):
                raise ResearchError(
                    ErrorCode.INCUMBENT_REVISION_CONFLICT,
                    "Observation comparator changed before selection",
                )
            selected_state = (
                state.model_copy(update={"selection_trial_id": facts.trial_id})
                if state.policy == "portfolio"
                else state
            )
            updated = write_search_state(transaction, previous, selected_state)
            selection = ObservationSelection(
                campaign_id=intent.campaign_id,
                trial_id=facts.trial_id,
                observation_id=intent.observation_id,
                observation_digest=record_digest("research.selected-observation", observed["data"]),
                facts_digest=observation.facts_digest,
                incumbent_revision=observation.incumbent_revision,
                comparator_id=observation.comparator_id,
                comparator_evaluation_id=observation.comparator_evaluation_id,
                policy_version=state.policy_version,
                before_revision=previous["revision"],
                after_revision=updated["revision"],
                committed_by=context.principal.id,
                committed_at=utc_now(),
            )
            recorded = transaction.insert(
                "policy_transition",
                identifier,
                selection.model_dump(mode="json"),
                state="immutable",
                campaign_id=intent.campaign_id,
                parent_id=facts.trial_id,
            )
            transaction.event(
                "search.observation_selected", recorded, {"observation_id": intent.observation_id}
            )
            return {"transition": recorded, "search": updated}

        return self.database.execute(
            context,
            "search.select_observation",
            intent.model_dump(mode="json"),
            "search.manage",
            pin,
        )
