"""Complete, costed held-out cases and one immutable fixed-panel assessment."""

from pydantic import Field

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.learning import Trajectory, TrajectoryEvidence
from research_swarm.domain.types import (
    ID,
    BalanceAmount,
    Digest,
    Dimension,
    Revision,
    Timestamp,
    VersionedRecord,
)
from research_swarm.evaluation.statistics import GateResult, PairedObservation


class AssessPolicyBenchmark(VersionedRecord):
    benchmark_id: ID


class BenchmarkCase(VersionedRecord):
    benchmark_id: ID
    execution_digest: Digest
    arm_id: ID
    arm_digest: Digest
    trajectory: Trajectory
    comparator_consumed: dict[Dimension, BalanceAmount] = Field(max_length=4)
    comparator_cost_evidence: tuple[TrajectoryEvidence, ...] = Field(min_length=1, max_length=128)
    total_consumed: dict[Dimension, BalanceAmount] = Field(min_length=4, max_length=4)

    @property
    def digest(self) -> str:
        return record_digest("research.benchmark-case.v1", self.model_dump(mode="json"))


class PolicyBenchmarkAssessment(VersionedRecord):
    benchmark_id: ID
    execution_digest: Digest
    baseline_policy_digest: Digest
    challenger_policy_digest: Digest
    case_digests: dict[ID, Digest] = Field(min_length=2, max_length=256)
    pairs: tuple[PairedObservation, ...] = Field(min_length=1, max_length=128)
    gate: GateResult
    total_consumed: dict[Dimension, BalanceAmount] = Field(min_length=4, max_length=4)
    evidence_through: Revision
    actor_id: ID
    recorded_at: Timestamp

    @property
    def digest(self) -> str:
        return record_digest("research.benchmark-assessment.v1", self.model_dump(mode="json"))
