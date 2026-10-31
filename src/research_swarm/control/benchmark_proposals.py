"""Issue a reserved agent request and freeze its held-out policy in one transaction."""

from research_swarm.control.benchmark_execution import setup_evidence
from research_swarm.control.benchmark_execution_models import (
    BenchmarkArm,
    BenchmarkExecution,
    BenchmarkProposal,
    BenchmarkTrialSlot,
    RequestBenchmarkProposal,
)
from research_swarm.control.benchmark_models import PolicyBenchmark
from research_swarm.control.benchmark_trials import execution_digest
from research_swarm.control.comparisons import eligible_evaluation
from research_swarm.control.learning import LearningControl
from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_models import (
    FreezeLearningPlan,
    LearningPlan,
    PlanEpisode,
    QualifiedTrajectoryDataset,
)
from research_swarm.control.learning_training_models import PreparedPolicyInputs
from research_swarm.control.models import CampaignData
from research_swarm.control.proposals import ProposalControl, search_state
from research_swarm.control.search_models import RequestProposal
from research_swarm.control.workers import authority
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import utc_now
from research_swarm.state.database import Database, JsonObject, Transaction


def current_arm(
    transaction: Transaction, arm_id: str
) -> tuple[BenchmarkArm, BenchmarkExecution, PolicyBenchmark]:
    arm_row = transaction.get("benchmark_arm", arm_id)
    arm = BenchmarkArm.model_validate(arm_row["data"])
    benchmark, training = setup_evidence(transaction, arm.benchmark_id)
    execution_row = transaction.get("benchmark_execution", arm.benchmark_id)
    execution = BenchmarkExecution.model_validate(execution_row["data"])
    policy = benchmark.baseline_policy if arm.side == "baseline" else execution.learned_policy
    if (
        arm_row["state"] != "immutable"
        or arm.campaign_id != arm_id
        or arm_id not in execution.arm_ids
        or arm.benchmark_digest != benchmark.digest
        or arm.seed not in benchmark.request.gate.seeds
        or arm.policy_digest != policy.digest
        or arm.proposer_id != execution.request.proposer_id
        or execution_row["state"] != "immutable"
        or execution.request.benchmark_id != arm.benchmark_id
        or execution.benchmark_digest != benchmark.digest
        or execution.learned_policy != training.policy
        or execution.training_publication_digest
        != record_digest("research.training-publication.v1", training.model_dump(mode="json"))
    ):
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Benchmark execution binding differs")
    return arm, execution, benchmark


class BenchmarkProposalControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    def request(self, context: CommandContext, intent: RequestBenchmarkProposal) -> JsonObject:
        context.principal.require("search.manage")
        context.principal.require("read")

        def commit(transaction: Transaction) -> JsonObject:
            previous = optional_record(transaction, "benchmark_proposal", intent.arm_id)
            if previous is not None:
                return previous
            arm, execution, benchmark = current_arm(transaction, intent.arm_id)
            campaign_row = transaction.get("campaign", arm.campaign_id)
            campaign = CampaignData.model_validate(campaign_row["data"])
            slot_row = transaction.get("benchmark_trial_slot", arm.candidate_trial_id)
            slot = BenchmarkTrialSlot.model_validate(slot_row["data"])
            if (
                slot_row["state"] != "reserved"
                or slot.arm_id != arm.campaign_id
                or slot.benchmark_id != arm.benchmark_id
                or slot.trial_id != arm.candidate_trial_id
                or slot.role != "candidate"
                or campaign.contract_digest != arm.campaign_contract_digest
                or campaign.incumbent_candidate_id != arm.initial_candidate_id
                or campaign.incumbent_evaluation_id is None
            ):
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE,
                    "Benchmark proposal requires its reserved slot "
                    "and promoted starting comparator",
                )
            comparator = eligible_evaluation(
                transaction, campaign.incumbent_evaluation_id, arm.campaign_id, campaign
            )
            if (
                comparator.trial.spec.trial_id != arm.comparator_trial_id
                or comparator.trial.spec.seed != arm.seed
                or comparator.comparison_digest != benchmark.comparison_digest
            ):
                raise ResearchError(
                    ErrorCode.COMPARISON_INELIGIBLE, "Benchmark comparator identity differs"
                )
            inputs = PreparedPolicyInputs.model_validate(
                transaction.get("learning_inputs", benchmark.request.inputs_id)["data"]
            )
            dataset = QualifiedTrajectoryDataset.model_validate(
                transaction.get("learning_dataset", inputs.request.dataset_id)["data"]
            )
            development = LearningPlan.model_validate(
                transaction.get("learning_plan", dataset.plan_id)["data"]
            )
            if development.digest != benchmark.learning_plan_digest:
                raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Development plan changed")
            controller_id, epoch = authority(transaction)
            state = search_state(transaction, arm.campaign_id, campaign)
            ProposalControl._create(
                transaction,
                RequestProposal(
                    campaign_id=arm.campaign_id,
                    proposer_id=arm.proposer_id,
                    seed=arm.seed,
                    expected_incumbent_revision=campaign.incumbent_revision,
                    expected_controller_id=controller_id,
                    expected_control_epoch=epoch,
                    instructions=(
                        "Commit the assigned policy decision with learning.commit_decision, "
                        "then submit its exact source through search.respond."
                    ),
                ),
                identifier=arm.proposal_request_id,
                expected_revision=state["revision"],
            )
            frozen = LearningControl._freeze(
                transaction,
                FreezeLearningPlan(
                    plan_id=arm.learning_plan_id,
                    policy=benchmark.baseline_policy
                    if arm.side == "baseline"
                    else execution.learned_policy,
                    reward_spec=benchmark.reward_spec,
                    action_artifacts=development.request.action_artifacts,
                    episodes=(PlanEpisode(request_id=arm.proposal_request_id, split="held_out"),),
                ),
            )
            plan = LearningPlan.model_validate(frozen["data"])
            result = transaction.insert(
                "benchmark_proposal",
                intent.arm_id,
                BenchmarkProposal(
                    request=intent,
                    benchmark_id=arm.benchmark_id,
                    execution_digest=execution_digest(execution),
                    proposal_request_id=arm.proposal_request_id,
                    learning_plan_id=arm.learning_plan_id,
                    learning_plan_digest=plan.digest,
                    actor_id=context.principal.id,
                    recorded_at=utc_now(),
                ).model_dump(mode="json"),
                state="immutable",
                campaign_id=arm.campaign_id,
                parent_id=arm.benchmark_id,
            )
            transaction.event(
                "learning.benchmark_proposal_requested",
                result,
                {"request_id": arm.proposal_request_id, "plan_id": arm.learning_plan_id},
            )
            return result

        return self.database.execute(
            context,
            "learning.request_benchmark_proposal",
            intent.model_dump(mode="json"),
            "learning.manage",
            commit,
        )
