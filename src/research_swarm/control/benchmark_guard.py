"""Reserved learning plans and seed panels cannot be bypassed by ordinary admission."""

from uuid import NAMESPACE_URL, uuid5

from research_swarm.control.benchmark_models import BenchmarkReservation, PolicyBenchmark
from research_swarm.control.learning_guard import optional_record
from research_swarm.control.learning_training_models import (
    AdmitPolicyTraining,
    PolicyTrainingProfile,
    PreparedPolicyInputs,
)
from research_swarm.control.models import CampaignContract
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import Transaction


def benchmark_plan_key(plan_digest: str) -> str:
    return str(uuid5(NAMESPACE_URL, "research.policy-benchmark-plan.v1:" + plan_digest))


def benchmark_seed_key(protocol_digest: str, seed: int) -> str:
    return str(uuid5(NAMESPACE_URL, f"research.policy-benchmark-seed.v1:{protocol_digest}:{seed}"))


def require_benchmark_training(
    transaction: Transaction,
    intent: AdmitPolicyTraining,
    inputs: PreparedPolicyInputs,
    profile: PolicyTrainingProfile,
) -> None:
    by_plan = optional_record(transaction, "benchmark_plan", benchmark_plan_key(inputs.plan_digest))
    by_admission = optional_record(transaction, "benchmark_training", intent.admission_id)
    if by_plan is None and by_admission is None and intent.benchmark_id is None:
        return
    if by_plan is None or by_admission is None or intent.benchmark_id is None:
        raise ResearchError(
            ErrorCode.COMPARISON_INELIGIBLE, "Training belongs to a reserved benchmark"
        )
    reservation = BenchmarkReservation.model_validate(by_plan["data"])
    row = transaction.get("policy_benchmark", reservation.benchmark_id)
    benchmark = PolicyBenchmark.model_validate(row["data"])
    if (
        row["state"] != "immutable"
        or row["parent_id"] != intent.admission_id
        or by_plan["state"] != "reserved"
        or by_admission["state"] != "reserved"
        or by_plan["parent_id"] != reservation.benchmark_id
        or by_admission["parent_id"] != reservation.benchmark_id
        or benchmark.request.benchmark_id != reservation.benchmark_id
        or BenchmarkReservation.model_validate(by_admission["data"]) != reservation
        or reservation.benchmark_id != intent.benchmark_id
        or reservation.benchmark_digest != benchmark.digest
        or benchmark.request.training_admission_id != intent.admission_id
        or benchmark.request.inputs_id != intent.inputs_id
        or benchmark.request.profile_id != intent.profile_id
        or benchmark.learning_plan_digest != inputs.plan_digest
        or benchmark.dataset_digest != inputs.dataset_digest
        or benchmark.training_profile_digest != profile.digest
        or benchmark.inputs_digest
        != record_digest("research.prepared-policy-inputs.v1", inputs.model_dump(mode="json"))
    ):
        raise ResearchError(ErrorCode.EVIDENCE_INVALIDATED, "Training benchmark binding changed")


def require_prepared_training_authority(
    transaction: Transaction, contract: CampaignContract, qualified_training_id: str | None
) -> None:
    inputs = contract.input_artifacts
    if "behavior" not in inputs or "trajectories" not in inputs:
        return
    plans = transaction.connection.execute(
        "SELECT DISTINCT json_extract(payload,'$.plan_digest') AS plan_digest "
        "FROM records WHERE workspace_id=? AND kind='learning_inputs' "
        "AND json_extract(payload,'$.behavior.digest')=? "
        "AND json_extract(payload,'$.behavior.size_bytes')=? "
        "AND json_extract(payload,'$.trajectories.digest')=? "
        "AND json_extract(payload,'$.trajectories.size_bytes')=? LIMIT 2",
        (
            transaction.workspace_id,
            inputs["behavior"].digest,
            inputs["behavior"].size_bytes,
            inputs["trajectories"].digest,
            inputs["trajectories"].size_bytes,
        ),
    ).fetchall()
    if len(plans) > 1:
        raise ResearchError(
            ErrorCode.EVIDENCE_INVALIDATED, "Prepared input pair has ambiguous ownership"
        )
    if not plans:
        return
    reserved = optional_record(
        transaction, "benchmark_plan", benchmark_plan_key(plans[0]["plan_digest"])
    )
    if reserved is None:
        return
    reservation = BenchmarkReservation.model_validate(reserved["data"])
    benchmark = PolicyBenchmark.model_validate(
        transaction.get("policy_benchmark", reservation.benchmark_id)["data"]
    )
    prepared = PreparedPolicyInputs.model_validate(
        transaction.get("learning_inputs", benchmark.request.inputs_id)["data"]
    )
    if (
        qualified_training_id != benchmark.request.training_admission_id
        or reservation.benchmark_digest != benchmark.digest
        or prepared.plan_digest != plans[0]["plan_digest"]
        or any(
            (inputs[name].digest, inputs[name].size_bytes)
            != (reference.digest, reference.size_bytes)
            for name, reference in (
                ("behavior", prepared.behavior),
                ("trajectories", prepared.trajectories),
            )
        )
    ):
        raise ResearchError(
            ErrorCode.COMPARISON_INELIGIBLE,
            "Prepared benchmark inputs require qualified training admission",
        )
