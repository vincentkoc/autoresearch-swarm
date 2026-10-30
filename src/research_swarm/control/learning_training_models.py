"""Immutable input preparation for ordinary policy-training campaigns."""

from typing import Literal

from pydantic import Field

from research_swarm.control.commands import SubmitTrial
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.learning import FiniteNumber, LearningRate
from research_swarm.domain.types import ID, Digest, Name, Timestamp, VersionedRecord


class PreparePolicyInputs(VersionedRecord):
    binding_id: ID
    dataset_id: ID


class PreparedPolicyInputs(VersionedRecord):
    request: PreparePolicyInputs
    dataset_digest: Digest
    plan_digest: Digest
    behavior_policy_digest: Digest
    behavior: ArtifactRef
    trajectories: ArtifactRef
    actor_id: ID
    recorded_at: Timestamp


class PolicyTrainingConfiguration(VersionedRecord):
    algorithm: Literal["one-step-reinforce-v1"] = "one-step-reinforce-v1"
    learning_rate: LearningRate
    epochs: int = Field(strict=True, ge=1, le=1)
    panel: Name
    maximum_update_error: FiniteNumber = Field(ge=0, le=1e-12)


class RegisterPolicyTrainingProfile(VersionedRecord):
    profile_id: ID
    project_revision_id: ID
    trainer_stage_id: Name = "train"
    checker_stage_id: Name = "evaluate"


class PolicyTrainingProfile(VersionedRecord):
    request: RegisterPolicyTrainingProfile
    configuration: PolicyTrainingConfiguration
    project_id: ID
    project_revision_digest: Digest
    source_artifact: ArtifactRef
    approved_by: ID
    recorded_at: Timestamp

    @property
    def digest(self) -> str:
        return record_digest("research.policy-training-profile.v1", self.model_dump(mode="json"))


class AdmitPolicyTraining(SubmitTrial):
    admission_id: ID
    profile_id: ID
    inputs_id: ID


class PolicyTrainingAdmission(VersionedRecord):
    request: AdmitPolicyTraining
    profile_digest: Digest
    inputs_digest: Digest
    dataset_digest: Digest
    behavior_policy_digest: Digest
    trial_id: ID
    trainer_job_id: ID
    checker_job_id: ID
    actor_id: ID
    recorded_at: Timestamp
