"""Narrow learning bindings around the ordinary proposal response boundary."""

from research_swarm.control.learning_models import CommittedPolicyDecision, EpisodeBinding
from research_swarm.control.search_models import RespondToProposal
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import JsonObject, Transaction


def optional_record(transaction: Transaction, kind: str, identifier: str) -> JsonObject | None:
    try:
        return transaction.get(kind, identifier)
    except ResearchError as error:
        if error.code != ErrorCode.NOT_FOUND:
            raise
        return None


def require_learning_response(transaction: Transaction, response: RespondToProposal) -> None:
    binding_row = optional_record(transaction, "learning_episode", response.request_id)
    if binding_row is None or response.outcome == "failed":
        return
    binding = EpisodeBinding.model_validate(binding_row["data"])
    committed = optional_record(transaction, "learning_decision", response.request_id)
    if committed is None:
        raise ResearchError(
            ErrorCode.INVALID_TRANSITION, "Learning candidate requires a committed policy decision"
        )
    data = CommittedPolicyDecision.model_validate(committed["data"])
    if (
        data.plan_id != binding.plan_id
        or data.plan_digest != binding.plan_digest
        or data.decision.proposal_request_id != response.request_id
        or response.source_artifact is None
        or response.source_artifact.digest != data.decision.source_digest
    ):
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Candidate source differs from the committed action"
        )
